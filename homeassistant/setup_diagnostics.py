"""Read-only diagnostics for integration loading.

Provides loading health readings (retry count, next retry time, whether a
failure was caused by a dependency), dependency impact analysis and a sampled,
rolling event log of setup failures and retries.

The diagnostics is disabled by default. When disabled, the hook functions
called from the setup path only perform a single ``hass.data`` lookup and
return immediately: no awaits, timers or I/O are added to the loading path.
"""

from collections import deque
from collections.abc import Iterable, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
import pathlib
import random
from typing import TYPE_CHECKING, Any, Final

import probatio

from .core import CALLBACK_TYPE, HomeAssistant, callback
from .exceptions import HomeAssistantError
from .helpers.dispatcher import async_dispatcher_connect
from .util.dt import utcnow
from .util.hass_dict import HassKey

if TYPE_CHECKING:
    from .config_entries import ConfigEntry
    from .loader import Integration

DOMAIN: Final = "setup_diagnostics"

CONF_STATE: Final = "state"
CONF_SAMPLE_RATE: Final = "sample_rate"
CONF_MAX_EVENTS: Final = "max_events"

DEFAULT_SAMPLE_RATE: Final = 1.0
DEFAULT_MAX_EVENTS: Final = 100

DATA_SETUP_DIAGNOSTICS: HassKey[SetupDiagnostics] = HassKey(DOMAIN)
DATA_BUILTIN_INTEGRATION_DOMAINS: HassKey[frozenset[str]] = HassKey(
    "builtin_integration_domains"
)


class SetupDiagnosticsNotEnabled(HomeAssistantError):
    """Raised when setup diagnostics is queried but not enabled."""


class SetupDiagnosticsState(StrEnum):
    """Configured state of the setup diagnostics."""

    ON = "on"
    OFF = "off"


class DiagnosticEventType(StrEnum):
    """The type of a diagnostic event."""

    SETUP_FAILED = "setup_failed"
    RETRY_SCHEDULED = "retry_scheduled"
    RETRY_CANCELLED = "retry_cancelled"


class SetupFailureKind(StrEnum):
    """Classification of a setup failure.

    Component level failures describe why an integration failed to load.
    Config entry level failures describe why a config entry setup failed.
    """

    # Component level
    INTEGRATION_NOT_FOUND = "integration_not_found"
    INTEGRATION_DISABLED = "integration_disabled"
    CIRCULAR_DEPENDENCY = "circular_dependency"
    DEPENDENCY_NOT_FOUND = "dependency_not_found"
    DEPENDENCY_SETUP_FAILED = "dependency_setup_failed"
    REQUIREMENTS_NOT_FOUND = "requirements_not_found"
    IMPORT_ERROR = "import_error"
    INVALID_CONFIG = "invalid_config"
    SETUP_ERROR = "setup_error"
    # Config entry level
    CONFIG_ENTRY_NOT_READY = "config_entry_not_ready"
    CONFIG_ENTRY_AUTH_FAILED = "config_entry_auth_failed"
    CONFIG_ENTRY_SETUP_ERROR = "config_entry_setup_error"
    CONFIG_ENTRY_MIGRATION_ERROR = "config_entry_migration_error"
    CONFIG_ENTRY_IMPORT_ERROR = "config_entry_import_error"


class DependencyFailureKind(StrEnum):
    """Component setup failures that are caused by a dependency."""

    CIRCULAR_DEPENDENCY = SetupFailureKind.CIRCULAR_DEPENDENCY
    DEPENDENCY_NOT_FOUND = SetupFailureKind.DEPENDENCY_NOT_FOUND
    DEPENDENCY_SETUP_FAILED = SetupFailureKind.DEPENDENCY_SETUP_FAILED
    REQUIREMENTS_NOT_FOUND = SetupFailureKind.REQUIREMENTS_NOT_FOUND


_DEPENDENCY_FAILURE_KIND_VALUES: Final = frozenset(
    kind.value for kind in DependencyFailureKind
)


class RetryState(StrEnum):
    """The state of a config entry retry."""

    NONE = "none"
    SCHEDULED = "scheduled"
    WAIT_STARTED = "wait_started"
    CANCELLED = "cancelled"


class DependencyStatus(StrEnum):
    """Status of a declared dependency."""

    LOADED = "loaded"
    NOT_LOADED = "not_loaded"
    NOT_FOUND = "not_found"


def _validate_state(value: object) -> SetupDiagnosticsState:
    """Validate and normalize the state option.

    YAML parses the bare words on/off as booleans, so both forms are
    accepted.
    """
    if isinstance(value, bool):
        return SetupDiagnosticsState.ON if value else SetupDiagnosticsState.OFF
    if isinstance(value, SetupDiagnosticsState):
        return value
    if value in ("on", "off"):
        return SetupDiagnosticsState(value)
    raise probatio.Invalid(
        f"expected one of {[state.value for state in SetupDiagnosticsState]}, "
        f"got {value!r}"
    )


SETUP_DIAGNOSTICS_CONFIG_SCHEMA: Final = probatio.Schema(
    {
        probatio.Optional(
            CONF_STATE, default=SetupDiagnosticsState.OFF
        ): _validate_state,
        probatio.Optional(CONF_SAMPLE_RATE, default=DEFAULT_SAMPLE_RATE): probatio.All(
            float, probatio.Range(min=0, max=1)
        ),
        probatio.Optional(CONF_MAX_EVENTS, default=DEFAULT_MAX_EVENTS): probatio.All(
            int, probatio.Range(min=0)
        ),
    },
    extra=probatio.PREVENT_EXTRA,
)


@dataclass(frozen=True, slots=True)
class SetupDiagnosticEvent:
    """A recorded setup failure or retry event."""

    timestamp: datetime
    event_type: DiagnosticEventType
    domain: str
    failure_kind: SetupFailureKind | None = None
    entry_id: str | None = None
    dependency_failure: bool | None = None
    related_domains: tuple[str, ...] | None = None
    requirements: tuple[str, ...] | None = None
    retry_count: int | None = None
    wait_seconds: float | None = None
    next_retry_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class EntryRetryReading:
    """Retry health reading for one config entry."""

    entry_id: str
    retry_state: RetryState
    retry_count: int | None
    next_retry_at: datetime | None


@dataclass(frozen=True, slots=True)
class SetupHealthReading:
    """Loading health reading for one integration."""

    domain: str
    failure_from_dependency: bool | None
    dependency_failure_kind: DependencyFailureKind | None
    failed_dependencies: tuple[str, ...]
    config_entry_retries: tuple[EntryRetryReading, ...]


@dataclass(frozen=True, slots=True)
class DependencyRef:
    """A dependency declared by an integration."""

    domain: str
    status: DependencyStatus


@dataclass(frozen=True, slots=True)
class ConfigEntryRef:
    """A reference to a config entry of a dependent integration."""

    entry_id: str
    state: str


@dataclass(frozen=True, slots=True)
class DependentIntegration:
    """An integration that depends on the queried integration."""

    domain: str
    loaded: bool
    config_entries: tuple[ConfigEntryRef, ...]


@dataclass(frozen=True, slots=True)
class DependencyImpact:
    """The dependency impact of an integration."""

    domain: str
    target_found: bool
    dependencies: tuple[DependencyRef, ...]
    after_dependencies: tuple[DependencyRef, ...]
    missing_dependencies: tuple[str, ...]
    dependency_cycles: tuple[tuple[str, ...], ...]
    dependent_integrations: tuple[DependentIntegration, ...]
    after_dependent_integrations: tuple[DependentIntegration, ...]


@dataclass(slots=True)
class _DomainFailure:
    """The latest component setup failure recorded for a domain."""

    kind: SetupFailureKind
    dependency_failure: bool
    related_domains: tuple[str, ...]
    requirements: tuple[str, ...]


@dataclass(slots=True)
class _RetryTracking:
    """Live retry tracking for a config entry."""

    domain: str
    retry_state: RetryState
    retry_count: int
    next_retry_at: datetime | None


# The sampling decision of the current load flow. None means no flow is active
# and an event hook gets to draw an ad-hoc decision for its own event.
_current_flow_sampled: ContextVar[bool | None] = ContextVar(
    "setup_diagnostics_flow_sampled", default=None
)


@callback
def async_get_manager(hass: HomeAssistant) -> SetupDiagnostics | None:
    """Return the setup diagnostics manager, or None when disabled."""
    return hass.data.get(DATA_SETUP_DIAGNOSTICS)


@callback
def async_is_enabled(hass: HomeAssistant) -> bool:
    """Return if setup diagnostics is enabled."""
    return DATA_SETUP_DIAGNOSTICS in hass.data


@callback
def async_enable(hass: HomeAssistant, config: dict[str, Any]) -> SetupDiagnostics:
    """Enable setup diagnostics with the already validated config."""
    if (manager := hass.data.get(DATA_SETUP_DIAGNOSTICS)) is not None:
        return manager
    manager = SetupDiagnostics(
        hass,
        sample_rate=config[CONF_SAMPLE_RATE],
        max_events=config[CONF_MAX_EVENTS],
    )
    hass.data[DATA_SETUP_DIAGNOSTICS] = manager
    manager.async_start()
    return manager


@callback
def async_begin_load_flow(hass: HomeAssistant) -> Token[bool | None] | None:
    """Draw the sampling decision for a load flow.

    Returns a token to reset the decision, or None when diagnostics is
    disabled or the current context already belongs to a flow (for example a
    dependency setup spawned by the parent load flow).
    """
    manager = hass.data.get(DATA_SETUP_DIAGNOSTICS)
    if manager is None or _current_flow_sampled.get() is not None:
        return None
    return _current_flow_sampled.set(manager._draw_sample())  # noqa: SLF001


@callback
def async_end_load_flow(token: Token[bool | None] | None) -> None:
    """Reset the sampling decision of a load flow."""
    if token is not None:
        _current_flow_sampled.reset(token)


def _manager_required(hass: HomeAssistant) -> SetupDiagnostics:
    manager = hass.data.get(DATA_SETUP_DIAGNOSTICS)
    if manager is None:
        raise SetupDiagnosticsNotEnabled("Setup diagnostics is not enabled")
    return manager


@callback
def async_notify_setup_failure(
    hass: HomeAssistant,
    domain: str,
    kind: SetupFailureKind,
    *,
    dependency_failure: bool,
    related_domains: tuple[str, ...] = (),
    requirements: tuple[str, ...] = (),
) -> None:
    """Record a component setup failure."""
    manager = hass.data.get(DATA_SETUP_DIAGNOSTICS)
    if manager is None:
        return
    manager.handle_setup_failure(
        domain,
        kind,
        dependency_failure=dependency_failure,
        related_domains=related_domains,
        requirements=requirements,
    )


@callback
def async_notify_unresolved_dependencies(
    hass: HomeAssistant, integration: Integration
) -> None:
    """Record a dependency resolution failure for an integration.

    The stored exception is either a circular dependency or a missing
    dependency declared by name.
    """
    from .loader import CircularDependency, IntegrationNotFound  # noqa: PLC0415

    manager = hass.data.get(DATA_SETUP_DIAGNOSTICS)
    if manager is None:
        return
    kind = SetupFailureKind.SETUP_ERROR
    dependency_failure = False
    related_domains: tuple[str, ...] = ()
    # resolve_dependencies() returned None, so the stored result is the
    # exception raised while resolving the declared dependencies.
    resolution_error = integration._all_dependencies  # noqa: SLF001
    if isinstance(resolution_error, CircularDependency):
        kind = SetupFailureKind.CIRCULAR_DEPENDENCY
        dependency_failure = True
        cycle = resolution_error.args[1]
        if isinstance(cycle, list):
            # The stored cycle repeats the starting domain, deduplicate it.
            related_domains = tuple(dict.fromkeys(str(domain) for domain in cycle))
    elif isinstance(resolution_error, IntegrationNotFound):
        kind = SetupFailureKind.DEPENDENCY_NOT_FOUND
        dependency_failure = True
        related_domains = (resolution_error.domain,)
    manager.handle_setup_failure(
        integration.domain,
        kind,
        dependency_failure=dependency_failure,
        related_domains=related_domains,
    )


@callback
def async_notify_setup_success(hass: HomeAssistant, domain: str) -> None:
    """Record that a component was set up successfully."""
    manager = hass.data.get(DATA_SETUP_DIAGNOSTICS)
    if manager is None:
        return
    manager.handle_setup_success(domain)


@callback
def async_notify_entry_setup_failure(
    hass: HomeAssistant, entry: ConfigEntry, kind: SetupFailureKind
) -> None:
    """Record a config entry setup failure (other than not ready)."""
    manager = hass.data.get(DATA_SETUP_DIAGNOSTICS)
    if manager is None:
        return
    manager.handle_entry_setup_failure(entry, kind)


@callback
def async_notify_retry_scheduled(
    hass: HomeAssistant,
    entry: ConfigEntry,
    wait_seconds: float,
    scheduled: bool,
) -> None:
    """Record that a config entry retry was scheduled.

    ``scheduled`` is True when a timer was scheduled while Home Assistant is
    running, False when the retry waits for the started event during boot.
    """
    manager = hass.data.get(DATA_SETUP_DIAGNOSTICS)
    if manager is None:
        return
    manager.handle_retry_scheduled(entry, wait_seconds, scheduled)


@callback
def async_notify_retry_fired(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Record that a scheduled config entry retry has fired."""
    manager = hass.data.get(DATA_SETUP_DIAGNOSTICS)
    if manager is None:
        return
    manager.handle_retry_fired(entry)


@callback
def async_notify_retry_cancelled(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Record that a pending config entry retry was cancelled."""
    manager = hass.data.get(DATA_SETUP_DIAGNOSTICS)
    if manager is None:
        return
    manager.handle_retry_cancelled(entry)


@callback
def async_get_setup_health(
    hass: HomeAssistant, domains: Iterable[str] | None = None
) -> dict[str, SetupHealthReading]:
    """Return current loading health readings.

    The snapshot is assembled synchronously and reflects a single point in
    time. Domains that are not currently loaded and have no active config
    entries are omitted.
    """
    manager = _manager_required(hass)
    return manager.get_health(domains)


async def async_get_dependency_impact(
    hass: HomeAssistant, domain: str
) -> DependencyImpact:
    """Return the dependency impact of an integration.

    Only reads manifests and registries; never sets up or unloads an
    integration and produces no diagnostic events.
    """
    manager = _manager_required(hass)
    return await manager.get_dependency_impact(domain)


@callback
def async_get_diagnostic_events(
    hass: HomeAssistant, domains: Iterable[str] | None = None
) -> tuple[SetupDiagnosticEvent, ...]:
    """Return recorded diagnostic events, oldest first."""
    manager = _manager_required(hass)
    return manager.get_events(domains)


def event_to_dict(event: SetupDiagnosticEvent) -> dict[str, Any]:
    """Serialize a diagnostic event for JSON."""
    return {
        "timestamp": event.timestamp.isoformat(),
        "event_type": event.event_type.value,
        "domain": event.domain,
        "failure_kind": event.failure_kind.value if event.failure_kind else None,
        "entry_id": event.entry_id,
        "dependency_failure": event.dependency_failure,
        "related_domains": (
            list(event.related_domains) if event.related_domains is not None else None
        ),
        "requirements": (
            list(event.requirements) if event.requirements is not None else None
        ),
        "retry_count": event.retry_count,
        "wait_seconds": event.wait_seconds,
        "next_retry_at": (
            event.next_retry_at.isoformat() if event.next_retry_at else None
        ),
    }


def health_to_dict(reading: SetupHealthReading) -> dict[str, Any]:
    """Serialize a health reading for JSON."""
    return {
        "domain": reading.domain,
        "failure_from_dependency": reading.failure_from_dependency,
        "dependency_failure_kind": (
            reading.dependency_failure_kind.value
            if reading.dependency_failure_kind
            else None
        ),
        "failed_dependencies": list(reading.failed_dependencies),
        "config_entry_retries": [
            {
                "entry_id": retry.entry_id,
                "retry_state": retry.retry_state.value,
                "retry_count": retry.retry_count,
                "next_retry_at": (
                    retry.next_retry_at.isoformat() if retry.next_retry_at else None
                ),
            }
            for retry in reading.config_entry_retries
        ],
    }


def impact_to_dict(impact: DependencyImpact) -> dict[str, Any]:
    """Serialize a dependency impact for JSON."""
    return {
        "domain": impact.domain,
        "target_found": impact.target_found,
        "dependencies": [
            {"domain": ref.domain, "status": ref.status.value}
            for ref in impact.dependencies
        ],
        "after_dependencies": [
            {"domain": ref.domain, "status": ref.status.value}
            for ref in impact.after_dependencies
        ],
        "missing_dependencies": list(impact.missing_dependencies),
        "dependency_cycles": [list(cycle) for cycle in impact.dependency_cycles],
        "dependent_integrations": [
            _dependent_to_dict(dependent) for dependent in impact.dependent_integrations
        ],
        "after_dependent_integrations": [
            _dependent_to_dict(dependent)
            for dependent in impact.after_dependent_integrations
        ],
    }


def _dependent_to_dict(dependent: DependentIntegration) -> dict[str, Any]:
    return {
        "domain": dependent.domain,
        "loaded": dependent.loaded,
        "config_entries": [
            {"entry_id": ref.entry_id, "state": ref.state}
            for ref in dependent.config_entries
        ],
    }


class SetupDiagnostics:
    """Hold loading diagnostics state for a Home Assistant instance."""

    __slots__ = (
        "_domain_failures",
        "_entry_domains",
        "_events",
        "_remove_dispatch",
        "_retries",
        "hass",
        "max_events",
        "sample_rate",
    )

    def __init__(
        self, hass: HomeAssistant, *, sample_rate: float, max_events: int
    ) -> None:
        """Initialize the diagnostics manager."""
        self.hass = hass
        self.sample_rate = sample_rate
        self.max_events = max_events
        self._events: deque[SetupDiagnosticEvent] = deque(maxlen=max_events or None)
        self._domain_failures: dict[str, _DomainFailure] = {}
        self._retries: dict[str, _RetryTracking] = {}
        # Domains that have owned a config entry while diagnostics is on.
        # Their visibility follows active entries instead of the component
        # module staying in hass.config.components after unload/remove.
        self._entry_domains: set[str] = set()
        self._remove_dispatch: CALLBACK_TYPE | None = None

    @callback
    def async_start(self) -> None:
        """Subscribe to config entry changes."""
        from .config_entries import SIGNAL_CONFIG_ENTRY_CHANGED  # noqa: PLC0415

        self._remove_dispatch = async_dispatcher_connect(
            self.hass,
            SIGNAL_CONFIG_ENTRY_CHANGED,
            self._async_config_entry_changed,
        )

    def _draw_sample(self) -> bool:
        if self.sample_rate <= 0:
            return False
        if self.sample_rate >= 1:
            return True
        return random.random() < self.sample_rate

    def _should_record(self) -> bool:
        decision = _current_flow_sampled.get()
        if decision is not None:
            return decision
        return self._draw_sample()

    def _record(self, event: SetupDiagnosticEvent) -> None:
        if self.max_events and self._should_record():
            self._events.append(event)

    @callback
    def handle_setup_failure(
        self,
        domain: str,
        kind: SetupFailureKind,
        *,
        dependency_failure: bool,
        related_domains: tuple[str, ...],
        requirements: tuple[str, ...] = (),
    ) -> None:
        """Track and (possibly) record a component setup failure."""
        self._domain_failures[domain] = _DomainFailure(
            kind=kind,
            dependency_failure=dependency_failure,
            related_domains=related_domains,
            requirements=requirements,
        )
        self._record(
            SetupDiagnosticEvent(
                timestamp=utcnow(),
                event_type=DiagnosticEventType.SETUP_FAILED,
                domain=domain,
                failure_kind=kind,
                dependency_failure=dependency_failure,
                related_domains=related_domains or None,
                requirements=requirements or None,
            )
        )

    @callback
    def handle_setup_success(self, domain: str) -> None:
        """Clear the failure tracking of a successfully set up domain."""
        self._domain_failures.pop(domain, None)

    @callback
    def handle_entry_setup_failure(
        self, entry: ConfigEntry, kind: SetupFailureKind
    ) -> None:
        """Record a config entry setup failure without domain tracking."""
        self._record(
            SetupDiagnosticEvent(
                timestamp=utcnow(),
                event_type=DiagnosticEventType.SETUP_FAILED,
                domain=entry.domain,
                failure_kind=kind,
                entry_id=entry.entry_id,
                dependency_failure=False,
            )
        )

    @callback
    def handle_retry_scheduled(
        self, entry: ConfigEntry, wait_seconds: float, scheduled: bool
    ) -> None:
        """Track and record a scheduled config entry retry."""
        retry_state = RetryState.SCHEDULED if scheduled else RetryState.WAIT_STARTED
        retry_count = entry._tries  # noqa: SLF001
        next_retry_at = (
            utcnow() + timedelta(seconds=wait_seconds) if scheduled else None
        )
        self._retries[entry.entry_id] = _RetryTracking(
            domain=entry.domain,
            retry_state=retry_state,
            retry_count=retry_count,
            next_retry_at=next_retry_at,
        )
        self._entry_domains.add(entry.domain)
        # The not ready failure and the retry it schedules are part of the
        # same load flow and share the sampling decision.
        if self.max_events and self._should_record():
            timestamp = utcnow()
            self._events.append(
                SetupDiagnosticEvent(
                    timestamp=timestamp,
                    event_type=DiagnosticEventType.SETUP_FAILED,
                    domain=entry.domain,
                    failure_kind=SetupFailureKind.CONFIG_ENTRY_NOT_READY,
                    entry_id=entry.entry_id,
                    dependency_failure=False,
                )
            )
            self._events.append(
                SetupDiagnosticEvent(
                    timestamp=timestamp,
                    event_type=DiagnosticEventType.RETRY_SCHEDULED,
                    domain=entry.domain,
                    entry_id=entry.entry_id,
                    retry_count=retry_count,
                    wait_seconds=wait_seconds,
                    next_retry_at=next_retry_at,
                )
            )

    @callback
    def handle_retry_fired(self, entry: ConfigEntry) -> None:
        """Clear the scheduled retry when it fires."""
        tracking = self._retries.get(entry.entry_id)
        if tracking is not None:
            tracking.retry_state = RetryState.NONE
            tracking.next_retry_at = None

    @callback
    def handle_retry_cancelled(self, entry: ConfigEntry) -> None:
        """Mark a pending retry cancelled and record the cancellation."""
        tracking = self._retries.get(entry.entry_id)
        if tracking is None or tracking.retry_state not in (
            RetryState.SCHEDULED,
            RetryState.WAIT_STARTED,
        ):
            return
        tracking.retry_state = RetryState.CANCELLED
        tracking.next_retry_at = None
        self._record(
            SetupDiagnosticEvent(
                timestamp=utcnow(),
                event_type=DiagnosticEventType.RETRY_CANCELLED,
                domain=entry.domain,
                entry_id=entry.entry_id,
                retry_count=tracking.retry_count,
            )
        )

    @callback
    def _async_config_entry_changed(self, change_type: Any, entry: ConfigEntry) -> None:
        """Keep tracking in sync with config entry state changes."""
        from .config_entries import ConfigEntryChange, ConfigEntryState  # noqa: PLC0415

        self._entry_domains.add(entry.domain)

        if change_type is ConfigEntryChange.REMOVED:
            self._retries.pop(entry.entry_id, None)
            self._async_clear_domain_if_inactive(entry.domain)
            return
        if change_type is not ConfigEntryChange.UPDATED:
            return
        if entry.state is ConfigEntryState.SETUP_IN_PROGRESS:
            # A new attempt is starting: any pending retry has fired or been
            # cancelled, also covering callers that cancelled without passing
            # the diagnostics hook a HomeAssistant instance.
            tracking = self._retries.get(entry.entry_id)
            if tracking is not None and tracking.retry_state is not RetryState.NONE:
                tracking.retry_state = RetryState.NONE
                tracking.next_retry_at = None
            return
        if entry.state is ConfigEntryState.NOT_LOADED:
            self._retries.pop(entry.entry_id, None)
            self._async_clear_domain_if_inactive(entry.domain)
            return
        if entry.state in (
            ConfigEntryState.LOADED,
            ConfigEntryState.SETUP_ERROR,
            ConfigEntryState.MIGRATION_ERROR,
            ConfigEntryState.FAILED_UNLOAD,
        ):
            self._retries.pop(entry.entry_id, None)

    @callback
    def _async_clear_domain_if_inactive(self, domain: str) -> None:
        """Drop the health state of an entry backed domain without active entry."""
        from .config_entries import ConfigEntryState  # noqa: PLC0415

        if any(
            entry.state is not ConfigEntryState.NOT_LOADED
            for entry in self.hass.config_entries.async_entries(
                domain, include_ignore=False
            )
        ):
            return
        self._domain_failures.pop(domain, None)

    @callback
    def _domain_is_visible(
        self,
        domain: str,
        tracked_domains: set[str],
        active_entry_domains: set[str],
        entry_domains: set[str],
    ) -> bool:
        """Return if a domain has a current load generation to report on."""
        if domain in tracked_domains:
            return True
        if domain in entry_domains:
            # A config entry backed integration is only visible while at
            # least one of its entries is active; unloaded ones are omitted.
            return domain in active_entry_domains
        # A domain whose entries have all been removed stays hidden, even if
        # its component module is still part of hass.config.components.
        if domain in self._entry_domains:
            return False
        return domain in self.hass.config.components

    @callback
    def get_health(
        self, requested_domains: Iterable[str] | None
    ) -> dict[str, SetupHealthReading]:
        """Assemble the health snapshot without any await."""
        from .config_entries import ConfigEntryState  # noqa: PLC0415

        all_entries = self.hass.config_entries.async_entries(include_ignore=False)
        entry_domains = {entry.domain for entry in all_entries}
        active_entry_domains = {
            entry.domain
            for entry in all_entries
            if entry.state is not ConfigEntryState.NOT_LOADED
        }
        tracked_domains = set(self._domain_failures)
        tracked_domains.update(tracking.domain for tracking in self._retries.values())
        candidate_domains = (
            set(self.hass.config.components) | entry_domains | tracked_domains
        )
        if requested_domains is not None:
            candidate_domains &= set(requested_domains)
        visible_domains = {
            domain
            for domain in candidate_domains
            if self._domain_is_visible(
                domain, tracked_domains, active_entry_domains, entry_domains
            )
        }

        readings: dict[str, SetupHealthReading] = {}
        for domain in visible_domains:
            failure = self._domain_failures.get(domain)
            entries = self.hass.config_entries.async_entries(
                domain, include_ignore=False
            )
            readings[domain] = SetupHealthReading(
                domain=domain,
                failure_from_dependency=(
                    None if failure is None else failure.dependency_failure
                ),
                dependency_failure_kind=(
                    DependencyFailureKind(failure.kind)
                    if failure is not None
                    and failure.kind.value in _DEPENDENCY_FAILURE_KIND_VALUES
                    else None
                ),
                failed_dependencies=(
                    failure.related_domains if failure is not None else ()
                ),
                config_entry_retries=tuple(
                    self._entry_retry_reading(entry) for entry in entries
                ),
            )
        return readings

    @callback
    def _entry_retry_reading(self, entry: ConfigEntry) -> EntryRetryReading:
        from .config_entries import ConfigEntryState  # noqa: PLC0415

        tracking = self._retries.get(entry.entry_id)
        if (
            tracking is not None
            and tracking.retry_state is not RetryState.NONE
            and entry.state
            in (ConfigEntryState.SETUP_RETRY, ConfigEntryState.SETUP_IN_PROGRESS)
        ):
            return EntryRetryReading(
                entry_id=entry.entry_id,
                retry_state=tracking.retry_state,
                retry_count=tracking.retry_count,
                next_retry_at=tracking.next_retry_at,
            )
        return EntryRetryReading(
            entry_id=entry.entry_id,
            retry_state=RetryState.NONE,
            retry_count=None,
            next_retry_at=None,
        )

    @callback
    def get_events(
        self, requested_domains: Iterable[str] | None
    ) -> tuple[SetupDiagnosticEvent, ...]:
        """Return a snapshot of recorded events, oldest first."""
        if requested_domains is None:
            return tuple(self._events)
        domain_filter = set(requested_domains)
        return tuple(event for event in self._events if event.domain in domain_filter)

    async def get_dependency_impact(self, domain: str) -> DependencyImpact:
        """Build the dependency impact report from manifests and registries."""
        from . import loader  # noqa: PLC0415

        target_result = await loader.async_get_integrations(self.hass, [domain])
        target = target_result.get(domain)
        if not isinstance(target, loader.Integration):
            return DependencyImpact(
                domain=domain,
                target_found=False,
                dependencies=(),
                after_dependencies=(),
                missing_dependencies=(),
                dependency_cycles=(),
                dependent_integrations=(),
                after_dependent_integrations=(),
            )

        declared_dependencies = target.dependencies
        declared_after_dependencies = target.after_dependencies
        declared = {*declared_dependencies, *declared_after_dependencies}
        resolved = await loader.async_get_integrations(self.hass, declared)
        loaded_components = self.hass.config.components

        def _ref_status(dep_domain: str) -> DependencyStatus:
            result = resolved.get(dep_domain)
            if not isinstance(result, loader.Integration):
                return DependencyStatus.NOT_FOUND
            if dep_domain in loaded_components:
                return DependencyStatus.LOADED
            return DependencyStatus.NOT_LOADED

        dependency_refs = tuple(
            DependencyRef(dep_domain, _ref_status(dep_domain))
            for dep_domain in declared_dependencies
        )
        after_dependency_refs = tuple(
            DependencyRef(dep_domain, _ref_status(dep_domain))
            for dep_domain in declared_after_dependencies
        )

        # Expand the transitive dependency graph without touching the
        # integration's own dependency resolution cache.
        graph, missing = await self._async_resolve_dependency_graph(
            {domain}, {domain: set(declared)}
        )
        cycles = _find_dependency_cycles(graph)

        reverse_domains = await self._async_get_reverse_scan_domains()
        reverse_resolved = await loader.async_get_integrations(
            self.hass, reverse_domains
        )
        strong_dependents: list[str] = []
        after_dependents: list[str] = []
        for dependent_domain in sorted(reverse_resolved):
            integration = reverse_resolved[dependent_domain]
            if not isinstance(integration, loader.Integration):
                continue
            if dependent_domain == domain:
                continue
            if domain in integration.dependencies:
                strong_dependents.append(dependent_domain)
            if domain in integration.after_dependencies:
                after_dependents.append(dependent_domain)

        return DependencyImpact(
            domain=domain,
            target_found=True,
            dependencies=dependency_refs,
            after_dependencies=after_dependency_refs,
            missing_dependencies=tuple(sorted(missing)),
            dependency_cycles=cycles,
            dependent_integrations=tuple(
                self._snapshot_dependent_integration(dependent_domain)
                for dependent_domain in strong_dependents
            ),
            after_dependent_integrations=tuple(
                self._snapshot_dependent_integration(dependent_domain)
                for dependent_domain in after_dependents
            ),
        )

    async def _async_resolve_dependency_graph(
        self,
        frontier: set[str],
        graph: dict[str, set[str]],
    ) -> tuple[dict[str, set[str]], set[str]]:
        """Resolve the transitive strong plus after dependency graph."""
        from . import loader  # noqa: PLC0415

        missing: set[str] = set()
        while frontier:
            results = await loader.async_get_integrations(self.hass, frontier)
            next_frontier: set[str] = set()
            for dep_domain in frontier:
                result = results.get(dep_domain)
                if not isinstance(result, loader.Integration):
                    missing.add(dep_domain)
                    continue
                edges = {*result.dependencies, *result.after_dependencies}
                graph[dep_domain] = edges
                next_frontier.update(
                    edge for edge in edges if edge not in graph and edge not in missing
                )
            frontier = next_frontier
        return graph, missing

    async def _async_get_reverse_scan_domains(self) -> set[str]:
        """Return all domains to scan for reverse dependencies."""
        from . import loader  # noqa: PLC0415

        if (
            builtin_domains := self.hass.data.get(DATA_BUILTIN_INTEGRATION_DOMAINS)
        ) is None:
            builtin_domains = await self.hass.async_add_executor_job(
                _scan_builtin_integration_domains
            )
            self.hass.data[DATA_BUILTIN_INTEGRATION_DOMAINS] = builtin_domains
        custom = await loader.async_get_custom_components(self.hass)
        return (
            set(builtin_domains)
            | set(custom)
            | set(self.hass.config.components)
            | {
                domain
                for domain, integration in self.hass.data[
                    loader.DATA_INTEGRATIONS
                ].items()
                if type(integration) is loader.Integration
            }
            | {
                entry.domain
                for entry in self.hass.config_entries.async_entries(
                    include_ignore=False
                )
            }
        )

    @callback
    def _snapshot_dependent_integration(
        self, dependent_domain: str
    ) -> DependentIntegration:
        """Collect the reverse result for one integration without awaits."""
        entries = tuple(
            ConfigEntryRef(entry_id=entry.entry_id, state=entry.state.value)
            for entry in self.hass.config_entries.async_entries(
                dependent_domain, include_ignore=False
            )
        )
        return DependentIntegration(
            domain=dependent_domain,
            loaded=dependent_domain in self.hass.config.components,
            config_entries=entries,
        )


def _find_dependency_cycles(
    graph: Mapping[str, set[str]],
) -> tuple[tuple[str, ...], ...]:
    """Find cycles in a dependency graph using depth first search."""
    resolved: set[str] = set()
    on_stack: set[str] = set()
    cycles: set[tuple[str, ...]] = set()

    def normalize_cycle(cycle: list[str]) -> tuple[str, ...]:
        # Rotate the cycle so it has a canonical representation regardless of
        # where the traversal entered it.
        start_index = min(range(len(cycle)), key=cycle.__getitem__)
        return tuple(cycle[start_index:] + cycle[:start_index])

    def visit(node: str, path: list[str]) -> None:
        if node in resolved or node not in graph:
            return
        if node in on_stack:
            cycles.add(normalize_cycle(path[path.index(node) :]))
            return
        on_stack.add(node)
        path.append(node)
        for edge in sorted(graph[node]):
            if edge in graph:
                visit(edge, path)
        path.pop()
        on_stack.remove(node)
        resolved.add(node)

    for root in sorted(graph):
        visit(root, [])
    return tuple(sorted(cycles))


def _scan_builtin_integration_domains() -> frozenset[str]:
    """List built-in integration domains from the components package."""
    from . import components  # noqa: PLC0415

    domains: set[str] = set()
    for base in components.__path__:
        base_path = pathlib.Path(base)
        try:
            children = list(base_path.iterdir())
        except OSError:
            continue
        for child in children:
            if (child / "manifest.json").is_file():
                domains.add(child.name)
    return frozenset(domains)
