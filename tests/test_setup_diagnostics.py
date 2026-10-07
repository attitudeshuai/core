"""Tests for homeassistant.setup_diagnostics."""

from collections.abc import Generator
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from freezegun.api import FrozenDateTimeFactory
import probatio
import pytest

from homeassistant import (
    bootstrap,
    config_entries,
    requirements as requirements_module,
    setup,
    setup_diagnostics,
)
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import CoreState, HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.util import dt as dt_util

from .common import (
    MockConfigEntry,
    MockModule,
    async_fire_time_changed,
    mock_integration,
    mock_platform,
)


@pytest.fixture
def flow_handlers() -> Generator[None]:
    """Register mock config flow handlers for the test domains."""

    class MockFlowHandler(config_entries.ConfigFlow):
        """Define a mock flow handler."""

        VERSION = 1
        MINOR_VERSION = 1

    with patch.dict(
        config_entries.HANDLERS,
        {
            "diag_b": MockFlowHandler,
            "reloadable": MockFlowHandler,
            "test": MockFlowHandler,
        },
        clear=False,
    ):
        yield


def _enable(
    hass: HomeAssistant, *, sample_rate: float = 1.0, max_events: int = 100
) -> setup_diagnostics.SetupDiagnostics:
    config = setup_diagnostics.SETUP_DIAGNOSTICS_CONFIG_SCHEMA(
        {
            "state": "on",
            "sample_rate": sample_rate,
            "max_events": max_events,
        }
    )
    return setup_diagnostics.async_enable(hass, config)


@pytest.mark.parametrize(
    ("config", "valid"),
    [
        ({}, True),
        ({"state": "off"}, True),
        ({"state": "on"}, True),
        ({"state": "on", "sample_rate": 0, "max_events": 0}, True),
        ({"state": "on", "sample_rate": 1, "max_events": 1}, True),
        ({"state": "on", "sample_rate": 0.5}, True),
        ({"state": "enabled"}, False),
        ({"state": 1}, False),
        ({"sample_rate": -0.1}, False),
        ({"sample_rate": 1.01}, False),
        ({"sample_rate": "x"}, False),
        ({"max_events": -1}, False),
        ({"max_events": 1.5}, False),
        ({"unknown": 1}, False),
    ],
)
async def test_config_schema(config: dict, valid: bool) -> None:
    """Test the setup diagnostics config schema."""
    if valid:
        validated = setup_diagnostics.SETUP_DIAGNOSTICS_CONFIG_SCHEMA(config)
        assert 0 <= validated["sample_rate"] <= 1
        assert validated["max_events"] >= 0
    else:
        with pytest.raises(probatio.Invalid):
            setup_diagnostics.SETUP_DIAGNOSTICS_CONFIG_SCHEMA(config)


@pytest.mark.parametrize(
    "section",
    [
        {"sample_rate": 2},
        None,
        {"state": "bogus"},
        {"max_events": -1},
    ],
)
@patch("homeassistant.bootstrap.async_enable_logging", AsyncMock())
async def test_bootstrap_invalid_diagnostics_config_aborts(
    hass: HomeAssistant, section: object
) -> None:
    """Test invalid diagnostics config (including a null section) aborts."""
    result = await bootstrap.async_from_config_dict(
        {"setup_diagnostics": section}, hass
    )
    assert result is None
    assert not setup_diagnostics.async_is_enabled(hass)


@patch("homeassistant.bootstrap.async_enable_logging", AsyncMock())
async def test_bootstrap_enables_diagnostics(hass: HomeAssistant) -> None:
    """Test a valid on config enables diagnostics before integrations load."""
    result = await bootstrap.async_from_config_dict(
        {"setup_diagnostics": {"state": "on", "sample_rate": 0.25, "max_events": 5}},
        hass,
    )
    assert result is hass
    manager = setup_diagnostics.async_get_manager(hass)
    assert manager is not None
    assert manager.sample_rate == 0.25
    assert manager.max_events == 5
    assert "setup_diagnostics" not in hass.config.components


@patch("homeassistant.bootstrap.async_enable_logging", AsyncMock())
async def test_bootstrap_config_off_keeps_diagnostics_disabled(
    hass: HomeAssistant,
) -> None:
    """Test an explicit off config does not enable diagnostics."""
    result = await bootstrap.async_from_config_dict(
        {"setup_diagnostics": {"state": "off"}}, hass
    )
    assert result is hass
    assert not setup_diagnostics.async_is_enabled(hass)
    assert "setup_diagnostics" not in hass.config.components


async def test_disabled_queries_raise(hass: HomeAssistant) -> None:
    """Test queries raise while diagnostics is disabled."""
    assert setup_diagnostics.async_get_manager(hass) is None
    with pytest.raises(setup_diagnostics.SetupDiagnosticsNotEnabled):
        setup_diagnostics.async_get_setup_health(hass)
    with pytest.raises(setup_diagnostics.SetupDiagnosticsNotEnabled):
        setup_diagnostics.async_get_diagnostic_events(hass)
    with pytest.raises(setup_diagnostics.SetupDiagnosticsNotEnabled):
        await setup_diagnostics.async_get_dependency_impact(hass, "sensor")


def _failure_events(hass: HomeAssistant) -> list:
    return [
        event
        for event in setup_diagnostics.async_get_diagnostic_events(hass)
        if event.event_type is setup_diagnostics.DiagnosticEventType.SETUP_FAILED
    ]


async def test_component_failure_integration_not_found(hass: HomeAssistant) -> None:
    """Test classification of an unknown integration."""
    _enable(hass)
    assert await setup.async_setup_component(hass, "no_such_integration", {}) is False

    reading = setup_diagnostics.async_get_setup_health(hass)["no_such_integration"]
    assert reading.failure_from_dependency is False
    assert reading.dependency_failure_kind is None
    event = _failure_events(hass)[-1]
    assert event.failure_kind is (
        setup_diagnostics.SetupFailureKind.INTEGRATION_NOT_FOUND
    )


async def test_component_failure_dependency_name_missing(hass: HomeAssistant) -> None:
    """Test classification of a dependency that does not exist by name."""
    _enable(hass)
    mock_integration(
        hass, MockModule("needs_ghost_dep", dependencies=["ghost_dep_123"])
    )
    assert await setup.async_setup_component(hass, "needs_ghost_dep", {}) is False

    reading = setup_diagnostics.async_get_setup_health(hass)["needs_ghost_dep"]
    assert reading.failure_from_dependency is True
    assert reading.dependency_failure_kind is (
        setup_diagnostics.DependencyFailureKind.DEPENDENCY_NOT_FOUND
    )
    assert reading.failed_dependencies == ("ghost_dep_123",)
    event = _failure_events(hass)[-1]
    assert event.failure_kind is (
        setup_diagnostics.SetupFailureKind.DEPENDENCY_NOT_FOUND
    )


async def test_component_failure_circular_dependency(hass: HomeAssistant) -> None:
    """Test classification of a circular dependency."""
    _enable(hass)
    mock_integration(hass, MockModule("cyc_one", dependencies=["cyc_two"]))
    mock_integration(hass, MockModule("cyc_two", dependencies=["cyc_one"]))
    assert await setup.async_setup_component(hass, "cyc_one", {}) is False

    reading = setup_diagnostics.async_get_setup_health(hass)["cyc_one"]
    assert reading.failure_from_dependency is True
    assert reading.dependency_failure_kind is (
        setup_diagnostics.DependencyFailureKind.CIRCULAR_DEPENDENCY
    )
    # The repeated starting domain is deduplicated while order is kept
    assert reading.failed_dependencies == ("cyc_one", "cyc_two")
    event = _failure_events(hass)[-1]
    assert event.failure_kind is (
        setup_diagnostics.SetupFailureKind.CIRCULAR_DEPENDENCY
    )


async def test_component_failure_dependency_setup_failed(hass: HomeAssistant) -> None:
    """Test classification of a dependency that exists but fails to load."""
    _enable(hass)
    mock_integration(
        hass, MockModule("bad_dep", async_setup=AsyncMock(return_value=False))
    )
    mock_integration(hass, MockModule("needs_bad", dependencies=["bad_dep"]))
    assert await setup.async_setup_component(hass, "needs_bad", {}) is False

    reading = setup_diagnostics.async_get_setup_health(hass)["needs_bad"]
    assert reading.failure_from_dependency is True
    assert reading.dependency_failure_kind is (
        setup_diagnostics.DependencyFailureKind.DEPENDENCY_SETUP_FAILED
    )
    assert reading.failed_dependencies == ("bad_dep",)


async def test_component_failure_requirements_not_found(hass: HomeAssistant) -> None:
    """Test classification of requirements that fail to install."""
    _enable(hass)
    hass.config.skip_pip = False
    mock_integration(hass, MockModule("needs_pkg", requirements=["package==0.0.1"]))
    with patch("homeassistant.util.package.install_package", return_value=False):
        assert await setup.async_setup_component(hass, "needs_pkg", {}) is False

    reading = setup_diagnostics.async_get_setup_health(hass)["needs_pkg"]
    assert reading.failure_from_dependency is False
    assert reading.dependency_failure_kind is (
        setup_diagnostics.DependencyFailureKind.REQUIREMENTS_NOT_FOUND
    )
    event = _failure_events(hass)[-1]
    assert event.failure_kind is (
        setup_diagnostics.SetupFailureKind.REQUIREMENTS_NOT_FOUND
    )
    assert event.requirements == ("package==0.0.1",)
    assert event.dependency_failure is False


async def test_requirements_failure_from_dependency_classification(
    hass: HomeAssistant,
) -> None:
    """Test requirements failures attributed to a dependency domain."""
    _enable(hass)
    setup._notify_deps_reqs_failure(
        hass,
        "needs_dep_pkg",
        requirements_module.RequirementsNotFound(
            "dep_with_pkg", ["dep_package==1.0.0"]
        ),
    )
    reading = setup_diagnostics.async_get_setup_health(hass)["needs_dep_pkg"]
    assert reading.failure_from_dependency is True
    assert reading.dependency_failure_kind is (
        setup_diagnostics.DependencyFailureKind.REQUIREMENTS_NOT_FOUND
    )
    assert reading.failed_dependencies == ("dep_with_pkg",)
    event = _failure_events(hass)[-1]
    assert event.requirements == ("dep_package==1.0.0",)


async def test_component_failure_disabled(hass: HomeAssistant) -> None:
    """Test classification of a disabled integration."""
    _enable(hass)
    mock_integration(
        hass,
        MockModule("disabled_dom", partial_manifest={"disabled": "manual"}),
    )
    assert await setup.async_setup_component(hass, "disabled_dom", {}) is False

    reading = setup_diagnostics.async_get_setup_health(hass)["disabled_dom"]
    assert reading.failure_from_dependency is False
    assert _failure_events(hass)[-1].failure_kind is (
        setup_diagnostics.SetupFailureKind.INTEGRATION_DISABLED
    )


async def test_component_failure_import_error(hass: HomeAssistant) -> None:
    """Test classification of an import error."""
    _enable(hass)
    integration = mock_integration(hass, MockModule("broken_import"))
    with patch.object(
        integration,
        "async_get_component",
        AsyncMock(side_effect=ImportError("nope")),
    ):
        assert await setup.async_setup_component(hass, "broken_import", {}) is False

    assert _failure_events(hass)[-1].failure_kind is (
        setup_diagnostics.SetupFailureKind.IMPORT_ERROR
    )


async def test_component_failure_invalid_config(hass: HomeAssistant) -> None:
    """Test classification of invalid YAML config."""
    _enable(hass)
    config_schema = probatio.Schema({"invalid_conf": {"hello": str}})
    mock_integration(hass, MockModule("invalid_conf", config_schema=config_schema))
    assert (
        await setup.async_setup_component(
            hass, "invalid_conf", {"invalid_conf": {"hello": 5}}
        )
        is False
    )

    assert _failure_events(hass)[-1].failure_kind is (
        setup_diagnostics.SetupFailureKind.INVALID_CONFIG
    )


async def test_component_failure_setup_error_and_recovery(hass: HomeAssistant) -> None:
    """Test setup_error classification and clearing on a later success."""
    _enable(hass)
    mock_integration(
        hass,
        MockModule("flaky_dom", async_setup=AsyncMock(side_effect=[False, True])),
    )
    assert await setup.async_setup_component(hass, "flaky_dom", {}) is False
    assert _failure_events(hass)[-1].failure_kind is (
        setup_diagnostics.SetupFailureKind.SETUP_ERROR
    )

    hass.data.pop(setup._DATA_SETUP)
    assert await setup.async_setup_component(hass, "flaky_dom", {}) is True
    reading = setup_diagnostics.async_get_setup_health(hass)["flaky_dom"]
    assert reading.failure_from_dependency is None
    assert reading.dependency_failure_kind is None


async def test_health_batch_filter_and_empty_retry_fields(
    hass: HomeAssistant,
) -> None:
    """Test health readings can be filtered and omit state/reason text."""
    _enable(hass)
    mock_integration(
        hass, MockModule("healthy_dom", async_setup=AsyncMock(return_value=True))
    )
    assert await setup.async_setup_component(hass, "healthy_dom", {}) is True

    all_health = setup_diagnostics.async_get_setup_health(hass)
    assert "healthy_dom" in all_health
    assert setup_diagnostics.async_get_setup_health(
        hass, ["healthy_dom", "never_heard_of_it"]
    ) == {"healthy_dom": all_health["healthy_dom"]}

    reading = all_health["healthy_dom"]
    assert reading.config_entry_retries == ()
    serialized = setup_diagnostics.health_to_dict(reading)
    assert "state" not in serialized
    assert "reason" not in serialized
    assert serialized["failure_from_dependency"] is None


@pytest.mark.usefixtures("flow_handlers")
@pytest.mark.freeze_time("2024-02-01 12:00:00")
async def test_retry_health_matches_backoff(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory
) -> None:
    """Test retry count and next retry time match the backoff calculation."""
    _enable(hass)
    entry = MockConfigEntry(domain="test")
    entry.add_to_hass(hass)
    mock_integration(
        hass,
        MockModule(
            "test", async_setup_entry=AsyncMock(side_effect=ConfigEntryNotReady)
        ),
    )
    mock_platform(hass, "test.config_flow", None)

    async def _retry_reading():
        return setup_diagnostics.async_get_setup_health(hass)[
            "test"
        ].config_entry_retries[0]

    with patch("homeassistant.config_entries.randint", return_value=0):
        await hass.config_entries.async_setup(entry.entry_id)
        assert entry.state is ConfigEntryState.SETUP_RETRY
        retry = await _retry_reading()
        assert retry.retry_state is setup_diagnostics.RetryState.SCHEDULED
        assert retry.retry_count == 1
        assert retry.next_retry_at == dt_util.utcnow() + timedelta(seconds=5)

        freezer.tick(5)
        async_fire_time_changed(hass)
        await hass.async_block_till_done()
        retry = await _retry_reading()
        assert retry.retry_count == 2
        assert retry.next_retry_at == dt_util.utcnow() + timedelta(seconds=10)

        freezer.tick(10)
        async_fire_time_changed(hass)
        await hass.async_block_till_done()
        retry = await _retry_reading()
        assert retry.retry_count == 3
        assert retry.next_retry_at == dt_util.utcnow() + timedelta(seconds=20)

        # Cap the wait time
        object.__setattr__(entry, "_tries", 8)
        freezer.tick(20)
        async_fire_time_changed(hass)
        await hass.async_block_till_done()
        retry = await _retry_reading()
        assert retry.retry_count == 9
        assert retry.next_retry_at == dt_util.utcnow() + timedelta(seconds=600)


@pytest.mark.usefixtures("flow_handlers")
async def test_retry_health_wait_started(hass: HomeAssistant) -> None:
    """Test retry reading while waiting for the started event during boot."""
    _enable(hass)
    hass.set_state(CoreState.starting)
    entry = MockConfigEntry(domain="test")
    entry.add_to_hass(hass)
    mock_integration(
        hass,
        MockModule(
            "test", async_setup_entry=AsyncMock(side_effect=ConfigEntryNotReady)
        ),
    )
    mock_platform(hass, "test.config_flow", None)

    with patch("homeassistant.config_entries.randint", return_value=0):
        await hass.config_entries.async_setup(entry.entry_id)
    retry = setup_diagnostics.async_get_setup_health(hass)["test"].config_entry_retries[
        0
    ]
    assert retry.retry_state is setup_diagnostics.RetryState.WAIT_STARTED
    assert retry.retry_count == 1
    assert retry.next_retry_at is None


@pytest.mark.usefixtures("flow_handlers")
async def test_retry_cancelled_on_shutdown(hass: HomeAssistant) -> None:
    """Test shutdown cancels the scheduled retry and clears its time."""
    _enable(hass)
    entry = MockConfigEntry(domain="test")
    entry.add_to_hass(hass)
    mock_integration(
        hass,
        MockModule(
            "test", async_setup_entry=AsyncMock(side_effect=ConfigEntryNotReady)
        ),
    )
    mock_platform(hass, "test.config_flow", None)
    with patch("homeassistant.config_entries.randint", return_value=0):
        await hass.config_entries.async_setup(entry.entry_id)
    assert (
        setup_diagnostics.async_get_setup_health(hass)["test"]
        .config_entry_retries[0]
        .retry_state
        is setup_diagnostics.RetryState.SCHEDULED
    )

    entry.async_shutdown(hass)
    retry = setup_diagnostics.async_get_setup_health(hass)["test"].config_entry_retries[
        0
    ]
    assert retry.retry_state is setup_diagnostics.RetryState.CANCELLED
    assert retry.next_retry_at is None


@pytest.mark.usefixtures("flow_handlers")
async def test_health_consistency_on_every_state_change(hass: HomeAssistant) -> None:
    """Test health snapshots observed on every state transition are coherent."""
    _enable(hass)
    entry = MockConfigEntry(domain="test")
    entry.add_to_hass(hass)
    mock_integration(
        hass,
        MockModule(
            "test", async_setup_entry=AsyncMock(side_effect=ConfigEntryNotReady)
        ),
    )
    mock_platform(hass, "test.config_flow", None)

    snapshots: list[setup_diagnostics.SetupHealthReading] = []

    def _capture(
        change_type: config_entries.ConfigEntryChange,
        changed_entry: config_entries.ConfigEntry,
    ) -> None:
        if changed_entry.entry_id != entry.entry_id:
            return
        health = setup_diagnostics.async_get_setup_health(hass)
        if "test" in health:
            snapshots.append(health["test"])

    unsub = async_dispatcher_connect(
        hass, config_entries.SIGNAL_CONFIG_ENTRY_CHANGED, _capture
    )
    with patch("homeassistant.config_entries.randint", return_value=0):
        await hass.config_entries.async_setup(entry.entry_id)

    # The authoritative reading right after the attempt is scheduled.
    scheduled_reading = setup_diagnostics.async_get_setup_health(hass)["test"]
    scheduled_retry = scheduled_reading.config_entry_retries[0]
    assert scheduled_retry.retry_state is setup_diagnostics.RetryState.SCHEDULED
    assert scheduled_retry.retry_count == 1
    assert scheduled_retry.next_retry_at is not None
    unsub()

    # Every snapshot taken during state transitions is internally coherent:
    # a scheduled retry always has a count and a next retry time, never a
    # half-updated mix.
    for reading in snapshots:
        assert reading.domain == "test"
        retry = reading.config_entry_retries[0]
        if retry.retry_state is setup_diagnostics.RetryState.SCHEDULED:
            assert retry.retry_count is not None
            assert retry.next_retry_at is not None
        elif retry.retry_state in (
            setup_diagnostics.RetryState.NONE,
            setup_diagnostics.RetryState.CANCELLED,
            setup_diagnostics.RetryState.WAIT_STARTED,
        ):
            assert retry.next_retry_at is None


@pytest.mark.usefixtures("flow_handlers")
async def test_retry_cancelled_has_no_future_retry(hass: HomeAssistant) -> None:
    """Test a cancelled retry reports cancelled and never fires."""
    _enable(hass)
    entry = MockConfigEntry(domain="reloadable")
    entry.add_to_hass(hass)
    setup_mock = AsyncMock(side_effect=ConfigEntryNotReady)
    mock_integration(
        hass,
        MockModule(
            "reloadable",
            async_setup_entry=setup_mock,
            async_unload_entry=AsyncMock(return_value=True),
        ),
    )
    mock_platform(hass, "reloadable.config_flow", None)

    with patch("homeassistant.config_entries.randint", return_value=0):
        await hass.config_entries.async_setup(entry.entry_id)

    reading = setup_diagnostics.async_get_setup_health(hass)["reloadable"]
    retry = reading.config_entry_retries[0]
    assert retry.retry_state is setup_diagnostics.RetryState.SCHEDULED
    cancelled_at = retry.next_retry_at
    assert cancelled_at is not None

    # Cancelling the pending retry (as reload and unload do) marks it
    # cancelled and removes the next retry time
    entry.async_cancel_retry_setup(hass)
    retry = setup_diagnostics.async_get_setup_health(hass)[
        "reloadable"
    ].config_entry_retries[0]
    assert retry.retry_state is setup_diagnostics.RetryState.CANCELLED
    assert retry.next_retry_at is None

    # Firing past the old timer must not run the cancelled retry
    setup_mock.reset_mock()
    async_fire_time_changed(hass, cancelled_at + timedelta(minutes=10), fire_all=True)
    await hass.async_block_till_done()
    assert setup_mock.call_count == 0

    # And unloading hides the integration from health while the cancel
    # event stays in the event log
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert "reloadable" not in setup_diagnostics.async_get_setup_health(hass)

    cancel_events = [
        event
        for event in setup_diagnostics.async_get_diagnostic_events(hass)
        if event.event_type is setup_diagnostics.DiagnosticEventType.RETRY_CANCELLED
    ]
    assert cancel_events


@pytest.mark.usefixtures("flow_handlers")
async def test_unloaded_entry_omitted_from_health_but_events_kept(
    hass: HomeAssistant,
) -> None:
    """Test an unloaded integration disappears from health, events remain."""
    _enable(hass)
    entry = MockConfigEntry(domain="reloadable")
    entry.add_to_hass(hass)
    mock_integration(
        hass,
        MockModule(
            "reloadable",
            async_setup_entry=AsyncMock(side_effect=ConfigEntryNotReady),
            async_unload_entry=AsyncMock(return_value=True),
        ),
    )
    mock_platform(hass, "reloadable.config_flow", None)
    with patch("homeassistant.config_entries.randint", return_value=0):
        await hass.config_entries.async_setup(entry.entry_id)
    assert "reloadable" in setup_diagnostics.async_get_setup_health(hass)
    assert setup_diagnostics.async_get_diagnostic_events(hass)

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.NOT_LOADED
    assert "reloadable" not in setup_diagnostics.async_get_setup_health(hass)
    # Historical events are retained
    assert setup_diagnostics.async_get_diagnostic_events(hass, ["reloadable"])


@pytest.mark.usefixtures("flow_handlers")
async def test_removed_entry_omitted_and_events_kept(hass: HomeAssistant) -> None:
    """Test a removed entry disappears while its events stay."""
    _enable(hass)
    entry = MockConfigEntry(domain="reloadable")
    entry.add_to_hass(hass)
    mock_integration(
        hass,
        MockModule(
            "reloadable",
            async_setup_entry=AsyncMock(side_effect=ConfigEntryNotReady),
            async_unload_entry=AsyncMock(return_value=True),
        ),
    )
    mock_platform(hass, "reloadable.config_flow", None)
    with patch("homeassistant.config_entries.randint", return_value=0):
        await hass.config_entries.async_setup(entry.entry_id)

    await hass.config_entries.async_remove(entry.entry_id)
    await hass.async_block_till_done()
    assert "reloadable" not in setup_diagnostics.async_get_setup_health(hass)
    assert setup_diagnostics.async_get_diagnostic_events(hass, ["reloadable"])


async def test_sampling_zero_one_and_rolling(hass: HomeAssistant) -> None:
    """Test sample rate 0/1 and the rolling event limit."""
    manager = _enable(hass, sample_rate=0, max_events=100)
    await setup.async_setup_component(hass, "ghost_a", {})
    assert manager.get_events(None) == ()

    manager.sample_rate = 1
    await setup.async_setup_component(hass, "ghost_b", {})
    assert any(event.domain == "ghost_b" for event in manager.get_events(None))

    manager.max_events = 3
    manager._events = type(manager._events)(maxlen=3)
    for idx in range(5):
        await setup.async_setup_component(hass, f"ghost_seq_{idx}", {})
    events = manager.get_events(None)
    assert len(events) == 3
    assert [event.domain for event in events] == [
        "ghost_seq_2",
        "ghost_seq_3",
        "ghost_seq_4",
    ]


@pytest.mark.usefixtures("flow_handlers")
async def test_failure_and_retry_events_share_flow_decision(
    hass: HomeAssistant,
) -> None:
    """Test not ready failure and its retry are recorded together or not at all."""
    manager = _enable(hass, sample_rate=1)
    entry = MockConfigEntry(domain="test")
    entry.add_to_hass(hass)
    mock_integration(
        hass,
        MockModule(
            "test", async_setup_entry=AsyncMock(side_effect=ConfigEntryNotReady)
        ),
    )
    mock_platform(hass, "test.config_flow", None)
    with patch("homeassistant.config_entries.randint", return_value=0):
        await hass.config_entries.async_setup(entry.entry_id)

    events = manager.get_events(("test",))
    assert [event.event_type for event in events] == [
        setup_diagnostics.DiagnosticEventType.SETUP_FAILED,
        setup_diagnostics.DiagnosticEventType.RETRY_SCHEDULED,
    ]
    assert events[0].failure_kind is (
        setup_diagnostics.SetupFailureKind.CONFIG_ENTRY_NOT_READY
    )
    assert events[1].retry_count == 1


async def test_impact_target_not_found(hass: HomeAssistant) -> None:
    """Test impact query for a domain that does not exist."""
    _enable(hass)
    impact = await setup_diagnostics.async_get_dependency_impact(
        hass, "no_such_impact_target"
    )
    assert impact.target_found is False
    assert impact.dependencies == ()
    assert impact.dependent_integrations == ()


async def test_impact_forward_statuses(hass: HomeAssistant) -> None:
    """Test forward dependencies distinguish loaded, not loaded and not found."""
    _enable(hass)
    mock_integration(hass, MockModule("loaded_dep"))
    mock_integration(hass, MockModule("after_dep"))
    mock_integration(
        hass,
        MockModule(
            "impact_target",
            dependencies=["loaded_dep", "missing_dep_xyz"],
            partial_manifest={"after_dependencies": ["after_dep"]},
        ),
    )
    hass.config.components.add("loaded_dep")

    impact = await setup_diagnostics.async_get_dependency_impact(hass, "impact_target")
    assert impact.target_found is True
    statuses = {ref.domain: ref.status for ref in impact.dependencies}
    assert statuses["loaded_dep"] is setup_diagnostics.DependencyStatus.LOADED
    assert statuses["missing_dep_xyz"] is setup_diagnostics.DependencyStatus.NOT_FOUND
    after = {ref.domain: ref.status for ref in impact.after_dependencies}
    assert after["after_dep"] is setup_diagnostics.DependencyStatus.NOT_LOADED
    assert "missing_dep_xyz" in impact.missing_dependencies


async def test_impact_dependency_cycle_marker(hass: HomeAssistant) -> None:
    """Test a dependency cycle is reported as a cycle marker."""
    _enable(hass)
    mock_integration(hass, MockModule("ring_a", dependencies=["ring_b"]))
    mock_integration(hass, MockModule("ring_b", dependencies=["ring_a"]))

    impact = await setup_diagnostics.async_get_dependency_impact(hass, "ring_a")
    assert impact.target_found is True
    assert ("ring_a", "ring_b") in impact.dependency_cycles


@pytest.mark.usefixtures("flow_handlers")
async def test_impact_reverse_dependencies_with_entries(hass: HomeAssistant) -> None:
    """Test reverse impact separates strong and after dependencies."""
    _enable(hass)
    mock_integration(hass, MockModule("impact_x"))
    mock_integration(hass, MockModule("strong_a", dependencies=["impact_x"]))
    mock_integration(
        hass,
        MockModule(
            "diag_b",
            async_setup_entry=AsyncMock(return_value=True),
            partial_manifest={"after_dependencies": ["impact_x"]},
        ),
    )
    mock_integration(hass, MockModule("unrelated_c"))

    loaded_entry = MockConfigEntry(domain="diag_b", state=ConfigEntryState.LOADED)
    unloaded_entry = MockConfigEntry(domain="diag_b", state=ConfigEntryState.NOT_LOADED)
    loaded_entry.add_to_hass(hass)
    unloaded_entry.add_to_hass(hass)

    impact = await setup_diagnostics.async_get_dependency_impact(hass, "impact_x")
    strong = {dep.domain: dep for dep in impact.dependent_integrations}
    after = {dep.domain: dep for dep in impact.after_dependent_integrations}
    assert set(strong) == {"strong_a"}
    assert set(after) == {"diag_b"}
    assert "unrelated_c" not in strong
    assert "unrelated_c" not in after
    assert strong["strong_a"].config_entries == ()
    entry_refs = {ref.entry_id: ref.state for ref in after["diag_b"].config_entries}
    assert entry_refs == {
        loaded_entry.entry_id: ConfigEntryState.LOADED.value,
        unloaded_entry.entry_id: ConfigEntryState.NOT_LOADED.value,
    }


async def test_health_snapshot_is_point_in_time(hass: HomeAssistant) -> None:
    """Test retry fields in a reading are always internally consistent."""
    _enable(hass)
    mock_integration(
        hass, MockModule("healthy_dom2", async_setup=AsyncMock(return_value=True))
    )
    assert await setup.async_setup_component(hass, "healthy_dom2", {}) is True

    # Plain loaded domain: snapshot assembled in one synchronous callback
    reading = setup_diagnostics.async_get_setup_health(hass)["healthy_dom2"]
    assert reading.failure_from_dependency is None
    assert reading.dependency_failure_kind is None
    assert reading.failed_dependencies == ()

    # Snapshot for an unknown tracked failure must keep its marker consistent
    assert await setup.async_setup_component(hass, "snap_ghost", {}) is False
    failure_reading = setup_diagnostics.async_get_setup_health(hass)["snap_ghost"]
    assert failure_reading.failure_from_dependency is False
    assert failure_reading.dependency_failure_kind is None


async def test_impact_query_is_read_only(hass: HomeAssistant) -> None:
    """Test queries do not load/unload anything or create records."""
    _enable(hass)
    mock_integration(hass, MockModule("ro_target"))
    mock_integration(hass, MockModule("ro_dep", dependencies=["ro_target"]))

    components_before = set(hass.config.components)
    setup_futures_before = set(hass.data.get(setup._DATA_SETUP, {}))
    events_before = setup_diagnostics.async_get_diagnostic_events(hass)

    await setup_diagnostics.async_get_dependency_impact(hass, "ro_target")
    setup_diagnostics.async_get_setup_health(hass)
    setup_diagnostics.async_get_diagnostic_events(hass)

    assert set(hass.config.components) == components_before
    assert set(hass.data.get(setup._DATA_SETUP, {})) == setup_futures_before
    assert setup_diagnostics.async_get_diagnostic_events(hass) == events_before
