"""Tests for state write observation."""

from collections import deque
from datetime import datetime

import pytest

from homeassistant.core import (
    EVENT_STATE_CHANGED,
    EVENT_STATE_REPORTED,
    STATE_WRITE_CHANGED,
    STATE_WRITE_REMOVED,
    HomeAssistant,
    callback,
)
from homeassistant.helpers import entity
from homeassistant.helpers.write_observation import (
    DEFAULT_CHUNK_SIZE,
    GROUP_BY_ENTITY_DOMAIN,
    GROUP_BY_INTEGRATION,
    UNKNOWN_WRITER,
    WriteObservationConfig,
    WriteObservationError,
    WriteObservationManager,
)
from homeassistant.util import dt as dt_util

from tests.common import MockEntityPlatform


def _record(
    manager: WriteObservationManager,
    entity_id: str,
    category: int,
    timestamp: float,
    writer: str | None = "demo",
) -> None:
    """Feed one notification directly to a manager."""
    manager._async_record(entity_id, category, writer, timestamp)


async def test_disabled_by_default(hass: HomeAssistant) -> None:
    """Test there is no observation overhead without a connected manager."""
    assert hass.states._write_observer is None
    manager = WriteObservationManager(hass)
    assert not manager.connected
    assert hass.states._write_observer is None

    events: list[str] = []
    hass.bus.async_listen(
        EVENT_STATE_CHANGED,
        lambda event: events.append("changed"),
    )

    @callback
    def _report_filter(event: object) -> bool:
        return True

    hass.bus.async_listen(
        EVENT_STATE_REPORTED,
        lambda event: events.append("reported"),
        event_filter=_report_filter,
    )

    hass.states.async_set("sensor.temperature", "20")
    hass.states.async_set("sensor.temperature", "20")
    hass.states.async_set("sensor.temperature", "20", {"unit": "C"})

    assert hass.states._write_observer is None
    assert events == ["changed", "reported", "changed"]


async def test_connect_and_disconnect(hass: HomeAssistant) -> None:
    """Test connecting and disconnecting the observer."""
    manager = WriteObservationManager(hass)
    manager.async_connect()
    assert manager.connected
    assert hass.states._write_observer is not None
    manager.async_disconnect()
    assert not manager.connected
    assert hass.states._write_observer is None

    # Writes after disconnect are not counted.
    hass.states.async_set("sensor.temperature", "20", writer="demo")
    result = manager.async_get_entity_stats(entity_ids=["sensor.temperature"], limit=10)
    assert result.entries[0].writes == 0


async def test_write_classification(hass: HomeAssistant) -> None:
    """Test the three write categories through the state machine."""
    manager = WriteObservationManager(hass)
    manager.async_connect()

    hass.states.async_set("sensor.temperature", "20", writer="mqtt")
    hass.states.async_set("sensor.temperature", "20", writer="mqtt")
    hass.states.async_set("sensor.temperature", "20", {"unit": "C"}, writer="mqtt")
    hass.states.async_set("sensor.temperature", "21", writer="mqtt")

    result = manager.async_get_entity_stats(entity_ids=["sensor.temperature"], limit=10)
    entry = result.entries[0]
    assert entry.state_changes == 2
    assert entry.attribute_changes == 1
    assert entry.same_state_reports == 1
    assert entry.writes == 4
    assert entry.writers == ("mqtt",)
    assert entry.generation == 0
    assert not entry.removed
    assert result.window.sample_rate == 1.0
    assert not result.window.sampled


async def test_force_update_counts_as_change(hass: HomeAssistant) -> None:
    """Test force_update makes an identical write a state change."""
    manager = WriteObservationManager(hass)
    manager.async_connect()

    hass.states.async_set("sensor.temperature", "20")
    hass.states.async_set("sensor.temperature", "20", force_update=True)

    entry = manager.async_get_entity_stats(
        entity_ids=["sensor.temperature"], limit=10
    ).entries[0]
    assert entry.state_changes == 2
    assert entry.same_state_reports == 0


async def test_same_state_report_keeps_timestamps(hass: HomeAssistant) -> None:
    """Test duplicate reports do not touch last_changed semantics."""
    manager = WriteObservationManager(hass)
    manager.async_connect()

    hass.states.async_set("sensor.temperature", "20")
    state = hass.states.get("sensor.temperature")
    last_changed = state.last_changed
    last_updated = state.last_updated

    hass.states.async_set("sensor.temperature", "20")
    state = hass.states.get("sensor.temperature")
    assert state.last_changed == last_changed
    assert state.last_updated == last_updated
    assert state.last_reported > last_updated

    entry = manager.async_get_entity_stats(
        entity_ids=["sensor.temperature"], limit=10
    ).entries[0]
    assert entry.state_changes == 1
    assert entry.same_state_reports == 1


async def test_writer_attribution(hass: HomeAssistant) -> None:
    """Test multiple and unknown writers."""
    manager = WriteObservationManager(hass)
    manager.async_connect()

    hass.states.async_set("sensor.temperature", "1")
    hass.states.async_set("sensor.temperature", "2", writer="CustomThing")
    hass.states.async_set("sensor.temperature", "3", writer="customthing")
    hass.states.async_set("sensor.temperature", "4", writer="CustomThing")

    entry = manager.async_get_entity_stats(
        entity_ids=["sensor.temperature"], limit=10
    ).entries[0]
    # Unknown identifiers are preserved verbatim and not merged.
    assert entry.writers == ("CustomThing", "customthing", UNKNOWN_WRITER)


async def test_repeated_writes_in_one_loop_iteration(hass: HomeAssistant) -> None:
    """Test every write counts, even from the same writer in one loop pass."""
    manager = WriteObservationManager(hass)
    manager.async_connect()

    for idx in range(5):
        hass.states.async_set(
            "sensor.fast", str(idx), force_update=True, writer="automation"
        )

    entry = manager.async_get_entity_stats(
        entity_ids=["sensor.fast"], limit=10
    ).entries[0]
    assert entry.writes == 5
    assert entry.state_changes == 5
    assert entry.writers == ("automation",)


async def test_entity_platform_writer(hass: HomeAssistant) -> None:
    """Test entities attribute their writes to the platform integration."""
    manager = WriteObservationManager(hass)
    manager.async_connect()

    platform = MockEntityPlatform(
        hass, domain="sensor", platform_name="test_integration"
    )

    class TestEntity(entity.Entity):
        _attr_name = "Thing"

        @property
        def state(self) -> str:
            return "on"

    await platform.async_add_entities([TestEntity()])

    entry = manager.async_get_entity_stats(
        entity_ids=["sensor.thing"], limit=10
    ).entries[0]
    assert entry.writers == ("test_integration",)
    assert entry.state_changes == 1


async def test_remove_and_recreate_generations(hass: HomeAssistant) -> None:
    """Test removed generations are marked and recreations start at zero."""
    manager = WriteObservationManager(hass)
    manager.async_connect()

    hass.states.async_set("sensor.temp", "1", writer="demo")
    hass.states.async_set("sensor.temp", "2", writer="demo")
    assert hass.states.async_remove("sensor.temp")
    hass.states.async_set("sensor.temp", "3", writer="demo")

    result = manager.async_get_entity_stats(entity_ids=["sensor.temp"], limit=10)
    assert len(result.entries) == 2
    old, new = result.entries
    assert old.generation == 0
    assert old.state_changes == 2
    assert old.removed
    assert new.generation == 1
    assert new.state_changes == 1
    assert not new.removed


async def test_zero_entries_for_entities_without_writes(
    hass: HomeAssistant,
) -> None:
    """Test entities without writes in the window stay in the result."""
    hass.states.async_set("sensor.quiet", "1")
    manager = WriteObservationManager(hass)
    manager.async_connect()

    result = manager.async_get_entity_stats(entity_ids=["sensor.quiet"], limit=10)
    assert len(result.entries) == 1
    entry = result.entries[0]
    assert entry.writes == 0
    assert entry.state_changes == 0
    assert entry.attribute_changes == 0
    assert entry.same_state_reports == 0
    assert entry.writers == ()
    assert not entry.removed


async def test_queries_create_no_records_or_events(hass: HomeAssistant) -> None:
    """Test queries are read only."""
    clock = [1000.0]
    manager = WriteObservationManager(hass, window_seconds=10, timer=lambda: clock[0])
    manager.async_connect()

    events: list[str] = []
    hass.bus.async_listen(
        EVENT_STATE_CHANGED, lambda event: events.append(event.event_type)
    )

    @callback
    def _report_filter(event: object) -> bool:
        return True

    hass.bus.async_listen(
        EVENT_STATE_REPORTED,
        lambda event: events.append(event.event_type),
        event_filter=_report_filter,
    )

    assert manager._windows == deque()
    manager.async_get_entity_stats(limit=10)
    manager.async_get_domain_stats(limit=10)
    manager.async_get_rate_stats(rate_threshold=1, limit=10)
    assert manager._windows == deque()
    assert events == []

    # A write after the queries is counted from zero.
    hass.states.async_set("sensor.temperature", "20")
    far_future = 2_000_000_000.0
    entry = manager.async_get_entity_stats(
        entity_ids=["sensor.temperature"],
        start_time=0,
        end_time=far_future,
        limit=10,
    ).entries[0]
    assert entry.state_changes == 1


async def test_window_roll_and_retain(hass: HomeAssistant) -> None:
    """Test tumbling windows and retention."""
    clock = [0.0]
    manager = WriteObservationManager(
        hass, window_seconds=10, retain=2, timer=lambda: clock[0]
    )
    manager.async_connect()

    _record(manager, "sensor.a", STATE_WRITE_CHANGED, 1)
    clock[0] = 10
    _record(manager, "sensor.a", STATE_WRITE_CHANGED, 11)
    clock[0] = 20
    _record(manager, "sensor.a", STATE_WRITE_CHANGED, 21)

    assert [window.start for window in manager._windows] == [10.0, 20.0]

    # The query spans the two retained windows.
    result = manager.async_get_entity_stats(start_time=0, end_time=30, limit=10)
    assert result.window.start_time == dt_util.utc_from_timestamp(10)
    assert result.window.end_time == dt_util.utc_from_timestamp(30)
    assert result.entries[0].state_changes == 2

    # The evicted window returns an empty result with the requested bounds.
    empty = manager.async_get_entity_stats(start_time=0, end_time=10, limit=10)
    assert empty.window.start_time == dt_util.utc_from_timestamp(0)
    assert empty.window.end_time == dt_util.utc_from_timestamp(10)
    assert empty.entries == ()


async def test_retain_zero_keeps_active_window(hass: HomeAssistant) -> None:
    """Test retain 0 keeps only the active window."""
    clock = [0.0]
    manager = WriteObservationManager(
        hass, window_seconds=10, retain=0, timer=lambda: clock[0]
    )
    manager.async_connect()

    _record(manager, "sensor.a", STATE_WRITE_CHANGED, 1)
    clock[0] = 10
    _record(manager, "sensor.a", STATE_WRITE_CHANGED, 11)

    assert [window.start for window in manager._windows] == [10.0]
    entry = manager.async_get_entity_stats(limit=10).entries[0]
    assert entry.state_changes == 1


async def test_window_bounds_of_default_query(hass: HomeAssistant) -> None:
    """Test the default query covers the current window."""
    clock = [20.0]
    manager = WriteObservationManager(hass, window_seconds=10, timer=lambda: clock[0])
    manager.async_connect()

    _record(manager, "sensor.a", STATE_WRITE_CHANGED, 21)
    clock[0] = 25
    result = manager.async_get_entity_stats(limit=10)
    assert result.window.start_time == dt_util.utc_from_timestamp(20)
    assert result.window.end_time == dt_util.utc_from_timestamp(30)


async def test_remove_recreate_across_windows(hass: HomeAssistant) -> None:
    """Test generations are kept apart and marked across windows."""
    clock = [0.0]
    manager = WriteObservationManager(
        hass, window_seconds=10, retain=5, timer=lambda: clock[0]
    )
    manager.async_connect()

    _record(manager, "sensor.temp", STATE_WRITE_CHANGED, 1, writer="demo")
    clock[0] = 15
    manager._async_record("sensor.temp", STATE_WRITE_REMOVED, None, 15)
    clock[0] = 25
    _record(manager, "sensor.temp", STATE_WRITE_CHANGED, 25, writer="demo")

    result = manager.async_get_entity_stats(start_time=0, end_time=30, limit=10)
    assert [entry.generation for entry in result.entries] == [0, 1]
    assert result.entries[0].removed
    assert result.entries[0].state_changes == 1
    assert not result.entries[1].removed
    assert result.entries[1].state_changes == 1


async def test_exclude_list(hass: HomeAssistant) -> None:
    """Test exact and wildcard excludes are never observed."""
    manager = WriteObservationManager(
        hass, exclude=("sensor.ignored", "binary_sensor.*")
    )
    manager.async_connect()

    hass.states.async_set("sensor.ignored", "1", writer="demo")
    hass.states.async_set("binary_sensor.motion", "on", writer="demo")
    hass.states.async_set("sensor.kept", "1", writer="demo")

    assert (
        not manager.async_get_entity_stats(entity_ids=["sensor.ignored"], limit=10)
        .entries[0]
        .writes
    )
    assert (
        not manager.async_get_entity_stats(
            entity_ids=["binary_sensor.motion"], limit=10
        )
        .entries[0]
        .writes
    )
    assert (
        manager.async_get_entity_stats(entity_ids=["sensor.kept"], limit=10)
        .entries[0]
        .writes
        == 1
    )


async def test_sample_rate_zero(hass: HomeAssistant) -> None:
    """Test sample rate 0 records nothing but flags the window as sampled."""
    clock = [0.0]
    manager = WriteObservationManager(
        hass, sample_rate=0, window_seconds=10, timer=lambda: clock[0]
    )
    manager.async_connect()

    for second in range(5):
        _record(manager, "sensor.a", STATE_WRITE_CHANGED, second + 0.1, "demo")

    result = manager.async_get_entity_stats(limit=10)
    assert result.window.sampled
    assert result.window.sample_rate == 0
    assert result.entries == ()


async def test_sampling_is_stable_within_window(hass: HomeAssistant) -> None:
    """Test sampling decisions are deterministic and stable per window."""

    def build() -> int:
        clock = [0.0]
        manager = WriteObservationManager(
            hass,
            sample_rate=0.5,
            window_seconds=10,
            timer=lambda: clock[0],
        )
        manager.async_connect()
        for second in range(100):
            _record(
                manager,
                "sensor.a",
                STATE_WRITE_CHANGED,
                second / 10 + 0.01,
                "demo",
            )
        result = manager.async_get_entity_stats(limit=10)
        assert result.window.sampled
        assert result.window.sample_rate == 0.5
        writes = result.entries[0].writes
        manager.async_disconnect()
        return writes

    first = build()
    second = build()
    assert 10 < first < 90
    assert first == second


async def test_sample_rate_is_frozen_per_window(hass: HomeAssistant) -> None:
    """Test changing the rate only affects subsequent windows."""
    clock = [0.0]
    manager = WriteObservationManager(
        hass,
        sample_rate=0.5,
        window_seconds=10,
        retain=5,
        timer=lambda: clock[0],
    )
    manager.async_connect()

    _record(manager, "sensor.a", STATE_WRITE_CHANGED, 1, "demo")
    manager._sample_rate = 0.2
    clock[0] = 10
    _record(manager, "sensor.a", STATE_WRITE_CHANGED, 11, "demo")

    result = manager.async_get_entity_stats(start_time=0, end_time=20)
    assert result.window.sampled
    # Windows with different rates report None for the aggregate rate.
    assert result.window.sample_rate is None


async def test_domain_stats_totals_match_entity_stats(
    hass: HomeAssistant,
) -> None:
    """Test per domain and per entity totals reconcile."""
    manager = WriteObservationManager(hass)
    manager.async_connect()

    hass.states.async_set("sensor.a", "1", writer="mqtt")
    hass.states.async_set("sensor.a", "1", writer="mqtt")
    hass.states.async_set("sensor.a", "1", {"x": 1}, writer="mqtt")
    hass.states.async_set("sensor.b", "1", writer="modbus")
    hass.states.async_set("switch.c", "on", writer="mqtt")
    hass.states.async_set("switch.c", "on")

    entity_result = manager.async_get_entity_stats(limit=10)
    entity_total = sum(entry.writes for entry in entity_result.entries)
    assert entity_total == 6

    entity_domains = manager.async_get_domain_stats(
        group_by=GROUP_BY_ENTITY_DOMAIN, limit=10
    )
    assert {entry.domain: entry.writes for entry in entity_domains.entries} == {
        "sensor": 4,
        "switch": 2,
    }
    assert sum(entry.writes for entry in entity_domains.entries) == entity_total

    integration_domains = manager.async_get_domain_stats(
        group_by=GROUP_BY_INTEGRATION, limit=10
    )
    by_integration = {
        entry.domain: entry.writes for entry in integration_domains.entries
    }
    assert by_integration == {"mqtt": 4, "modbus": 1, UNKNOWN_WRITER: 1}
    assert sum(entry.writes for entry in integration_domains.entries) == entity_total


async def test_domain_stats_shares(hass: HomeAssistant) -> None:
    """Test category shares are returned per domain."""
    manager = WriteObservationManager(hass)
    manager.async_connect()
    hass.states.async_set("sensor.a", "1", writer="demo")
    hass.states.async_set("sensor.a", "1", writer="demo")

    domain_entry = manager.async_get_domain_stats(limit=10).entries[0]
    assert domain_entry.state_change_share == 0.5
    assert domain_entry.same_state_report_share == 0.5
    assert domain_entry.attribute_change_share == 0


async def test_domain_stats_filter_keeps_zero_domains(hass: HomeAssistant) -> None:
    """Test filtered entity domains without writes are returned with zeros."""
    hass.states.async_set("sensor.quiet", "1")
    manager = WriteObservationManager(hass)
    manager.async_connect()

    result = manager.async_get_domain_stats(entity_ids=["sensor.quiet"], limit=10)
    assert len(result.entries) == 1
    assert result.entries[0].domain == "sensor"
    assert result.entries[0].writes == 0


async def test_rate_stats(hass: HomeAssistant) -> None:
    """Test rate threshold filtering and annotations."""
    clock = [0.0]
    manager = WriteObservationManager(hass, window_seconds=10, timer=lambda: clock[0])
    manager.async_connect()

    for second in range(30):
        _record(
            manager,
            "sensor.fast",
            STATE_WRITE_CHANGED,
            second * 0.3 + 0.1,
            "demo",
        )
    for second in range(5):
        _record(
            manager,
            "sensor.slow",
            STATE_WRITE_CHANGED,
            second + 0.1,
            "demo",
        )

    result = manager.async_get_rate_stats(rate_threshold=1, limit=10)
    assert len(result.entries) == 1
    entry = result.entries[0]
    assert entry.entity_id == "sensor.fast"
    assert entry.writes == 30
    annotation = entry.rate_annotation
    assert annotation is not None
    assert annotation.window_length == 10
    assert annotation.rate == 3
    assert annotation.threshold == 1


async def test_rate_stats_recreated_entity_not_marked_removed(
    hass: HomeAssistant,
) -> None:
    """Test the removed flag follows the newest generation in rate stats."""
    clock = [0.0]
    manager = WriteObservationManager(
        hass, window_seconds=10, retain=5, timer=lambda: clock[0]
    )
    manager.async_connect()

    _record(manager, "sensor.temp", STATE_WRITE_CHANGED, 1, "demo")
    clock[0] = 15
    manager._async_record("sensor.temp", STATE_WRITE_REMOVED, None, 15)
    clock[0] = 25
    for idx in range(31):
        _record(manager, "sensor.temp", STATE_WRITE_CHANGED, 25 + idx * 0.03, "demo")

    result = manager.async_get_rate_stats(rate_threshold=1, start_time=0, end_time=30)
    assert len(result.entries) == 1
    assert result.entries[0].entity_id == "sensor.temp"
    assert result.entries[0].generation == 1
    assert not result.entries[0].removed
    assert result.entries[0].rate_annotation is not None
    assert result.entries[0].rate_annotation.window_length == 30


async def test_rate_stats_threshold_boundary(hass: HomeAssistant) -> None:
    """Test entities exactly at the threshold are not included."""
    clock = [0.0]
    manager = WriteObservationManager(hass, window_seconds=10, timer=lambda: clock[0])
    manager.async_connect()

    for second in range(10):
        _record(manager, "sensor.at_limit", STATE_WRITE_CHANGED, second + 0.1)

    assert manager.async_get_rate_stats(rate_threshold=1, limit=10).entries == ()


async def test_chunking(hass: HomeAssistant) -> None:
    """Test result chunking and continuation markers."""
    manager = WriteObservationManager(hass)
    manager.async_connect()
    for name in ("a", "b", "c"):
        hass.states.async_set(f"sensor.{name}", "1", writer="demo")

    first = manager.async_get_entity_stats(limit=2)
    assert [entry.entity_id for entry in first.entries] == [
        "sensor.a",
        "sensor.b",
    ]
    assert first.has_more
    assert first.next_offset == 2

    second = manager.async_get_entity_stats(limit=2, offset=2)
    assert [entry.entity_id for entry in second.entries] == ["sensor.c"]
    assert not second.has_more
    assert second.next_offset is None

    zero = manager.async_get_entity_stats(limit=0)
    assert zero.entries == ()
    assert zero.has_more
    assert zero.next_offset == 0

    assert DEFAULT_CHUNK_SIZE > 0


async def test_invalid_options() -> None:
    """Test invalid manager options raise."""
    with pytest.raises(WriteObservationError):
        WriteObservationConfig(sample_rate=-0.1)
    with pytest.raises(WriteObservationError):
        WriteObservationConfig(sample_rate=1.1)
    with pytest.raises(WriteObservationError):
        WriteObservationConfig(window_seconds=0)
    with pytest.raises(WriteObservationError):
        WriteObservationConfig(retain=-1)


async def test_invalid_queries(hass: HomeAssistant) -> None:
    """Test invalid query inputs raise instead of returning zeros."""
    manager = WriteObservationManager(hass)
    manager.async_connect()
    hass.states.async_set("sensor.known", "1")

    with pytest.raises(WriteObservationError):
        manager.async_get_entity_stats(
            start_time=dt_util.utc_from_timestamp(10),
            end_time=dt_util.utc_from_timestamp(5),
        )
    with pytest.raises(WriteObservationError):
        manager.async_get_entity_stats(start_time=datetime(2024, 1, 1))
    with pytest.raises(WriteObservationError):
        manager.async_get_entity_stats(entity_ids=["sensor.unknown"])
    with pytest.raises(WriteObservationError):
        manager.async_get_entity_stats(entity_ids=["not-an-entity-id"])
    with pytest.raises(WriteObservationError):
        manager.async_get_entity_stats(limit=-1)
    with pytest.raises(WriteObservationError):
        manager.async_get_entity_stats(offset=-1)
    with pytest.raises(WriteObservationError):
        manager.async_get_domain_stats(group_by="invalid")
    with pytest.raises(WriteObservationError):
        manager.async_get_rate_stats(rate_threshold=0)
    with pytest.raises(WriteObservationError):
        manager.async_get_rate_stats(rate_threshold=-1)


async def test_entity_filter_normalizes_case(hass: HomeAssistant) -> None:
    """Test entity ids in filters are case insensitive."""
    manager = WriteObservationManager(hass)
    manager.async_connect()
    hass.states.async_set("sensor.known", "1", writer="demo")

    entry = manager.async_get_entity_stats(
        entity_ids=["SENSOR.KNOWN"], limit=10
    ).entries[0]
    assert entry.entity_id == "sensor.known"
    assert entry.state_changes == 1
