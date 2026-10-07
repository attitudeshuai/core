"""Tests for layered recorder retention and purge progress reporting."""

from datetime import timedelta
from unittest.mock import patch

from freezegun import freeze_time
from probatio.error import MultipleInvalid
import pytest
from sqlalchemy import func

from homeassistant.components.recorder import (
    CONFIG_SCHEMA,
    DOMAIN,
    Recorder,
    purge as purge_module,
)
from homeassistant.components.recorder.db_schema import (
    Events,
    EventTypes,
    States,
    StatesMeta,
    StatisticsShortTerm,
)
from homeassistant.components.recorder.retention import (
    CONF_ENTITY_DOMAINS,
    CONF_ENTITY_ID_PREFIXES,
    CONF_ENTITY_IDS,
    CONF_INTEGRATION_DOMAINS,
    CONF_RETENTION,
    CONF_RETENTION_DEFAULT_KEEP_DAYS,
    CONF_RETENTION_ENABLED,
    CONF_RETENTION_RULES,
    MAX_RETENTION_KEEP_DAYS,
    RetentionConfig,
    RetentionRule,
    build_purge_plan,
    config_from_yaml,
)
from homeassistant.components.recorder.tasks import PurgeTask
from homeassistant.components.recorder.util import session_scope
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

from .common import async_wait_purge_done, async_wait_recording_done

from tests.typing import RecorderInstanceContextManager, WebSocketGenerator

LAYERED_TEST_EVENT = "LAYERED_TEST_EVENT"


@pytest.fixture
async def mock_recorder_before_hass(
    async_test_recorder: RecorderInstanceContextManager,
) -> None:
    """Set up recorder."""


def _rule(
    index: int,
    keep_days: int,
    *,
    domains: set[str] | None = None,
    integration_domains: set[str] | None = None,
    prefixes: tuple[str, ...] = (),
    entity_ids: set[str] | None = None,
    name: str | None = None,
) -> RetentionRule:
    return RetentionRule(
        index=index,
        keep_days=keep_days,
        entity_domains=frozenset(domains or set()),
        integration_domains=frozenset(integration_domains or set()),
        entity_id_prefixes=prefixes,
        entity_ids=frozenset(entity_ids or set()),
        name=name,
    )


def _config(default_keep_days: int = 7, rules: tuple[RetentionRule, ...] = ()):
    return RetentionConfig(default_keep_days=default_keep_days, rules=rules)


def _retention_yaml(conf: dict) -> dict:
    return CONFIG_SCHEMA({DOMAIN: {CONF_RETENTION: conf}})[DOMAIN][CONF_RETENTION]


async def _record_state(
    hass: HomeAssistant, entity_id: str, when, state: str = "on"
) -> None:
    """Record a state with a specific last_updated timestamp.

    force_update ensures every call produces a new states row even when the
    state value and attributes are unchanged.
    """
    hass.states.async_set(
        entity_id, state, timestamp=when.timestamp(), force_update=True
    )
    await hass.async_block_till_done()
    await async_wait_recording_done(hass)


async def _record_event(hass: HomeAssistant, when) -> None:
    """Fire and record a non state changed event at a specific point in time."""
    hass.bus.async_fire(
        LAYERED_TEST_EVENT, {"test_attr": 5}, time_fired=when.timestamp()
    )
    await hass.async_block_till_done()
    await async_wait_recording_done(hass)


def _make_plan(hass: HomeAssistant, config: RetentionConfig, now, keep_days: int = 10):
    global_before = now - timedelta(days=keep_days)
    return build_purge_plan(
        hass,
        config,
        purge_before=global_before,
        repack=False,
        apply_filter=False,
        now=now,
    )


async def _run_layered_purge(
    hass: HomeAssistant, instance: Recorder, plan, max_number: int = 20
) -> None:
    instance.queue_task(
        PurgeTask(plan.purge_before, repack=False, apply_filter=False, plan=plan)
    )
    await async_wait_purge_done(hass, max_number=max_number)


def _states_per_entity(hass: HomeAssistant) -> dict[str, int]:
    with session_scope(hass=hass, read_only=True) as session:
        return dict(
            session.query(StatesMeta.entity_id, func.count(States.state_id))
            .join(States, States.metadata_id == StatesMeta.metadata_id)
            .group_by(StatesMeta.entity_id)
            .all()
        )


def _layered_event_count(hass: HomeAssistant) -> int:
    with session_scope(hass=hass, read_only=True) as session:
        return (
            session.query(Events)
            .join(EventTypes, Events.event_type_id == EventTypes.event_type_id)
            .filter(EventTypes.event_type == LAYERED_TEST_EVENT)
            .count()
        )


def test_retention_config_defaults_to_disabled() -> None:
    """Without retention config, no layered retention is configured."""
    conf = CONFIG_SCHEMA({DOMAIN: {}})[DOMAIN]
    assert CONF_RETENTION not in conf
    assert config_from_yaml(conf.get(CONF_RETENTION)) is None


def test_retention_can_be_disabled_explicitly() -> None:
    """Explicitly disabled retention produces no config."""
    conf = _retention_yaml(
        {CONF_RETENTION_ENABLED: False, CONF_RETENTION_DEFAULT_KEEP_DAYS: 7}
    )
    assert config_from_yaml(conf) is None


def test_enabled_retention_requires_default_layer() -> None:
    """default_keep_days is required when retention is enabled."""
    with pytest.raises(MultipleInvalid):
        _retention_yaml(
            {
                CONF_RETENTION_ENABLED: True,
                CONF_RETENTION_RULES: [
                    {
                        "keep_days": 3,
                        CONF_ENTITY_DOMAINS: ["camera"],
                    }
                ],
            }
        )


def test_retention_rule_requires_a_condition() -> None:
    """A rule without any matching condition is rejected at startup."""
    with pytest.raises(MultipleInvalid):
        _retention_yaml(
            {
                CONF_RETENTION_ENABLED: True,
                CONF_RETENTION_DEFAULT_KEEP_DAYS: 7,
                CONF_RETENTION_RULES: [{"keep_days": 3}],
            }
        )


@pytest.mark.parametrize("keep_days", [-1, MAX_RETENTION_KEEP_DAYS + 1])
def test_retention_keep_days_out_of_range(keep_days: int) -> None:
    """Out of range keep_days is rejected at startup."""
    with pytest.raises(MultipleInvalid):
        _retention_yaml(
            {
                CONF_RETENTION_ENABLED: True,
                CONF_RETENTION_DEFAULT_KEEP_DAYS: keep_days,
            }
        )


def test_retention_invalid_entity_id_rejected() -> None:
    """Invalid entity ids in a rule are rejected at startup."""
    with pytest.raises(MultipleInvalid):
        _retention_yaml(
            {
                CONF_RETENTION_ENABLED: True,
                CONF_RETENTION_DEFAULT_KEEP_DAYS: 7,
                CONF_RETENTION_RULES: [
                    {"keep_days": 3, CONF_ENTITY_IDS: ["not-an-entity"]}
                ],
            }
        )


def test_retention_valid_config_normalized() -> None:
    """A valid config is normalized and parsed."""
    conf = _retention_yaml(
        {
            CONF_RETENTION_ENABLED: True,
            CONF_RETENTION_DEFAULT_KEEP_DAYS: 7,
            CONF_RETENTION_RULES: [
                {
                    "name": "cameras",
                    "keep_days": 0,
                    CONF_ENTITY_DOMAINS: ["Camera", "camera"],
                    CONF_INTEGRATION_DOMAINS: ["Hue"],
                    CONF_ENTITY_ID_PREFIXES: ["sensor.energy_"],
                    CONF_ENTITY_IDS: ["media_player.tv", "media_player.tv"],
                }
            ],
        }
    )
    config = config_from_yaml(conf)
    assert config is not None
    assert config.default_keep_days == 7
    rule = config.rules[0]
    assert rule.keep_days == 0
    assert rule.name == "cameras"
    assert rule.entity_domains == frozenset({"camera"})
    assert rule.integration_domains == frozenset({"hue"})
    assert rule.entity_id_prefixes == ("sensor.energy_",)
    assert rule.entity_ids == frozenset({"media_player.tv"})


def test_rule_matching_is_first_match_in_declaration_order() -> None:
    """The first rule matching an entity wins."""
    specific = _rule(0, 365, entity_ids={"camera.special"})
    camera_domain = _rule(1, 3, domains={"camera"})
    assert specific.matches("camera.special", frozenset())
    assert camera_domain.matches("camera.special", frozenset())
    assert not specific.matches("camera.other", frozenset())
    assert camera_domain.matches("camera.other", frozenset())
    assert not camera_domain.matches("sensor.other", frozenset())

    prefix_rule = _rule(0, 365, prefixes=("sensor.energy_",))
    assert prefix_rule.matches("sensor.energy_total", frozenset())
    assert not prefix_rule.matches("sensor.indoor_temp", frozenset())


async def test_build_purge_plan_resolves_integration_domains(
    hass: HomeAssistant,
    recorder_mock: Recorder,
    entity_registry: er.EntityRegistry,
) -> None:
    """Integration domain conditions resolve to entity ids at purge start."""
    entry = entity_registry.async_get_or_create("sensor", "hue", "unique-1")
    config = _config(
        rules=(_rule(0, 30, integration_domains={"hue"}),),
    )
    plan = _make_plan(hass, config, dt_util.utcnow())
    assert entry.entity_id in plan.layers[0].integration_entity_ids
    assert plan.layer_for_entity(entry.entity_id) == 0
    assert plan.layer_for_entity("sensor.other") == 1
    # The default layer uses its own cutoff
    assert plan.default_layer.purge_before == plan.started_at - timedelta(days=7)


async def test_layered_purge_uses_per_layer_cutoffs(
    hass: HomeAssistant, recorder_mock: Recorder
) -> None:
    """Each state is purged using the cutoff of its matching layer."""
    now = dt_util.utcnow()
    config = _config(
        rules=(
            _rule(0, 3, domains={"camera"}, name="cameras"),
            _rule(1, 365, domains={"sensor"}),
        ),
    )

    # camera: 5 days old state must go (3 days retention)
    await _record_state(hass, "camera.front", now - timedelta(days=5))
    await _record_state(hass, "camera.front", now)
    # sensor: 20 days old state is kept (365 days retention)
    await _record_state(hass, "sensor.energy_total", now - timedelta(days=20))
    await _record_state(hass, "sensor.energy_total", now)
    # default layer: 20 days old state must go (7 days retention)
    await _record_state(hass, "media_player.tv", now - timedelta(days=20))
    await _record_state(hass, "media_player.tv", now)

    # Events have no entity link, they use the default layer cutoff
    await _record_event(hass, now - timedelta(days=20))
    await _record_event(hass, now)

    plan = _make_plan(hass, config, now, keep_days=10)
    await _run_layered_purge(hass, recorder_mock, plan)

    states = _states_per_entity(hass)
    assert states == {"camera.front": 1, "sensor.energy_total": 2, "media_player.tv": 1}
    assert _layered_event_count(hass) == 1

    progress = recorder_mock.async_get_purge_progress()
    assert progress is not None
    assert progress["running"] is False
    assert progress["started_at"] == now.isoformat()
    assert progress["finished_at"] is not None
    assert progress["current_layer"] is None
    assert progress["processed_layers"] == 3
    layers = {layer["index"]: layer for layer in progress["layers"]}
    assert layers[0]["name"] == "cameras"
    assert layers[0]["matched_entities"] == 1
    assert layers[0]["deleted_states"] == 1
    assert layers[0]["retained_states"] == 1
    assert layers[1]["matched_entities"] == 1
    assert layers[1]["deleted_states"] == 0
    assert layers[1]["retained_states"] == 2
    assert layers[2]["name"] == "default"
    assert layers[2]["matched_entities"] == 1
    assert layers[2]["deleted_states"] == 1
    assert layers[2]["retained_states"] == 1
    assert progress["deleted_events"] == 1
    assert progress["deleted_records"] == 3
    assert progress["retained_events"] is not None


async def test_zero_keep_days_deletes_on_next_purge(
    hass: HomeAssistant, recorder_mock: Recorder
) -> None:
    """A layer with zero retention deletes states on the next purge."""
    now = dt_util.utcnow()
    config = _config(rules=(_rule(0, 0, domains={"camera"}),))
    await _record_state(hass, "camera.front", now - timedelta(hours=1))
    await _record_state(
        hass, "media_player.tv", now - timedelta(hours=1)
    )  # default 7 days

    plan = _make_plan(hass, config, now)
    await _run_layered_purge(hass, recorder_mock, plan)

    states = _states_per_entity(hass)
    assert "camera.front" not in states
    assert states["media_player.tv"] == 1


async def test_short_term_statistics_keep_global_window(
    hass: HomeAssistant, recorder_mock: Recorder
) -> None:
    """Short term statistics use purge_keep_days even with shorter layers."""
    now = dt_util.utcnow()
    config = _config(default_keep_days=7)  # states default cutoff is 7 days
    await _record_state(hass, "switch.porch", now - timedelta(days=8))
    with session_scope(hass=hass) as session:
        session.add(
            StatisticsShortTerm(
                start_ts=(now - timedelta(days=8)).timestamp(), state="1"
            )
        )

    plan = _make_plan(hass, config, now, keep_days=10)
    await _run_layered_purge(hass, recorder_mock, plan)

    # 8 day old state is beyond the 7 day default layer cutoff
    assert _states_per_entity(hass) == {}
    # but the short term statistic is within the global 10 day window
    with session_scope(hass=hass, read_only=True) as session:
        assert session.query(StatisticsShortTerm).count() == 1


async def test_layered_purge_resumes_without_double_delete(
    hass: HomeAssistant, recorder_mock: Recorder
) -> None:
    """An interrupted run resumes and ends with the exact same records."""
    now = dt_util.utcnow()
    config = _config(rules=(_rule(0, 3, domains={"camera"}),))

    for suffix in ("a", "b"):
        for _ in range(8):
            await _record_state(
                hass, f"camera.{suffix}", now - timedelta(days=5), state="old"
            )
        for _ in range(4):
            await _record_state(hass, f"camera.{suffix}", now, state="new")
    for _ in range(12):
        await _record_state(
            hass, "media_player.tv", now - timedelta(days=5), state="old"
        )

    with (
        patch.object(recorder_mock, "max_bind_vars", 10),
        patch.object(recorder_mock.database_engine, "max_bind_vars", 10),
    ):
        plan = _make_plan(hass, config, now)
        assert recorder_mock.purge_run_begin(plan)
        # Run one iteration, then simulate an interrupted restart
        finished = purge_module.purge_old_data(
            recorder_mock,
            plan.purge_before,
            repack=False,
            apply_filter=False,
            states_batch_size=1,
            events_batch_size=1,
            plan=plan,
        )
        assert finished is False
        first_run_deleted = recorder_mock.async_get_purge_progress()["deleted_states"]
        assert first_run_deleted > 0
        recorder_mock.purge_run_completed(plan)

        # Restart with the same layer configuration
        resumed_plan = _make_plan(hass, config, now)
        assert recorder_mock.purge_run_begin(resumed_plan)
        for _ in range(200):
            if purge_module.purge_old_data(
                recorder_mock,
                resumed_plan.purge_before,
                repack=False,
                apply_filter=False,
                states_batch_size=1,
                events_batch_size=1,
                plan=resumed_plan,
            ):
                break
        else:
            pytest.fail("layered purge did not finish")
        recorder_mock.purge_run_completed(resumed_plan)

    states = _states_per_entity(hass)
    assert states == {"camera.a": 4, "camera.b": 4, "media_player.tv": 12}
    progress = recorder_mock.async_get_purge_progress()
    assert progress["running"] is False
    assert progress["deleted_states"] + first_run_deleted == 16


async def test_concurrent_purge_requests_are_serialized(
    hass: HomeAssistant, recorder_mock: Recorder
) -> None:
    """A second purge waits for the active purge instead of interleaving."""
    now = dt_util.utcnow()
    plan_a = _make_plan(hass, _config(rules=(_rule(0, 3, domains={"camera"}),)), now)
    plan_b = _make_plan(
        hass,
        _config(default_keep_days=2, rules=(_rule(0, 1, domains={"sensor"}),)),
        now,
    )

    assert recorder_mock.purge_run_begin(plan_a)
    assert recorder_mock.purge_run_begin(plan_a)  # requeued chunks continue
    assert not recorder_mock.purge_run_begin(plan_b)

    task = PurgeTask(plan_b.purge_before, repack=False, apply_filter=False, plan=plan_b)
    with patch.object(recorder_mock, "queue_task") as queue_task:
        task.run(recorder_mock)
        queue_task.assert_called_once()
        requeued = queue_task.call_args.args[0]
    assert isinstance(requeued, PurgeTask)
    assert requeued.plan is plan_b

    recorder_mock.purge_run_completed(plan_a)
    assert recorder_mock.purge_run_begin(plan_b)
    recorder_mock.purge_run_completed(plan_b)


@pytest.mark.parametrize("enable_nightly_purge", [True])
async def test_nightly_purge_uses_layered_retention(
    hass: HomeAssistant, recorder_mock: Recorder
) -> None:
    """The nightly purge builds a plan from the layered retention config."""
    now = dt_util.utcnow()
    config = _config(rules=(_rule(0, 3, domains={"camera"}),))
    recorder_mock.retention_config = config

    await _record_state(hass, "camera.front", now - timedelta(days=5))
    await _record_state(hass, "camera.front", now)
    await _record_state(hass, "media_player.tv", now - timedelta(days=5))
    await _record_state(hass, "media_player.tv", now)

    with freeze_time(now):
        recorder_mock.async_nightly_tasks(now)
        await async_wait_purge_done(hass)

    states = _states_per_entity(hass)
    assert states == {"camera.front": 1, "media_player.tv": 2}
    assert recorder_mock.async_get_purge_progress()["running"] is False


async def test_retention_websocket_api_is_read_only(
    hass: HomeAssistant,
    recorder_mock: Recorder,
    hass_ws_client: WebSocketGenerator,
) -> None:
    """Config and progress are queryable and queries never trigger a purge."""
    client = await hass_ws_client(hass)

    await client.send_json({"id": 1, "type": "recorder/retention_config"})
    assert (await client.receive_json())["result"] is None

    await client.send_json({"id": 2, "type": "recorder/purge_progress"})
    assert (await client.receive_json())["result"] is None

    config = _config(rules=(_rule(0, 3, domains={"camera"}, name="cameras"),))
    recorder_mock.retention_config = config
    now = dt_util.utcnow()
    await _record_state(hass, "camera.front", now - timedelta(days=5))
    plan = _make_plan(hass, config, now)
    await _run_layered_purge(hass, recorder_mock, plan)

    await async_wait_recording_done(hass)
    queue_size = recorder_mock._queue.qsize()

    await client.send_json({"id": 3, "type": "recorder/retention_config"})
    result = (await client.receive_json())["result"]
    assert result[CONF_RETENTION_ENABLED] is True
    assert result[CONF_RETENTION_DEFAULT_KEEP_DAYS] == 7
    assert result[CONF_RETENTION_RULES][0]["name"] == "cameras"
    assert result[CONF_RETENTION_RULES][0][CONF_ENTITY_DOMAINS] == ["camera"]

    await client.send_json({"id": 4, "type": "recorder/purge_progress"})
    progress = (await client.receive_json())["result"]
    assert progress["running"] is False
    assert progress["processed_layers"] == 2
    assert progress["layers"][0]["deleted_states"] == 1

    # Read only queries must not queue anything
    assert recorder_mock._queue.qsize() == queue_size
