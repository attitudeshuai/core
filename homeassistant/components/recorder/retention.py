"""Layered retention configuration, purge planning and progress reporting.

When layered retention is disabled (the default), purging keeps its historical
single-cutoff behavior. When enabled, the recorder state is partitioned into
layers at purge start: each state row is purged using the cutoff of the first
rule whose conditions match its entity, with a default layer catching
everything else.

Events are not linked to an entity in the current schema, so they are always
purged using the cutoff of the default layer. Short term statistics,
statistics runs and recorder runs keep using the global ``purge_keep_days``
cutoff regardless of the layered configuration.
"""

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import threading
from uuid import uuid4

from homeassistant.core import HomeAssistant, callback, split_entity_id
from homeassistant.helpers import entity_registry as er

# Keep durations are expressed in whole days, zero means do not keep.
MAX_RETENTION_KEEP_DAYS = 3650

CONF_RETENTION = "retention"
CONF_RETENTION_ENABLED = "enabled"
CONF_RETENTION_DEFAULT_KEEP_DAYS = "default_keep_days"
CONF_RETENTION_RULES = "rules"

CONF_KEEP_DAYS = "keep_days"
CONF_RULE_NAME = "name"
CONF_ENTITY_DOMAINS = "domains"
CONF_INTEGRATION_DOMAINS = "integration_domains"
CONF_ENTITY_ID_PREFIXES = "entity_id_prefixes"
CONF_ENTITY_IDS = "entity_ids"

DEFAULT_LAYER_NAME = "default"


@dataclass(frozen=True, slots=True)
class RetentionRule:
    """A single layered retention rule.

    Conditions are matched in declaration order together with the other
    rules; the first matching rule wins.
    """

    index: int
    keep_days: int
    entity_domains: frozenset[str]
    integration_domains: frozenset[str]
    entity_id_prefixes: tuple[str, ...]
    entity_ids: frozenset[str]
    name: str | None = None

    def matches(self, entity_id: str, integration_entity_ids: frozenset[str]) -> bool:
        """Return if an entity matches this rule."""
        return (
            split_entity_id(entity_id)[0] in self.entity_domains
            or entity_id in self.entity_ids
            or entity_id in integration_entity_ids
            or any(entity_id.startswith(prefix) for prefix in self.entity_id_prefixes)
        )

    def as_dict(self) -> dict:
        """Return a JSON serializable representation of the rule."""
        return {
            "index": self.index,
            "name": self.name,
            CONF_KEEP_DAYS: self.keep_days,
            CONF_ENTITY_DOMAINS: sorted(self.entity_domains),
            CONF_INTEGRATION_DOMAINS: sorted(self.integration_domains),
            CONF_ENTITY_ID_PREFIXES: list(self.entity_id_prefixes),
            CONF_ENTITY_IDS: sorted(self.entity_ids),
        }


@dataclass(frozen=True, slots=True)
class RetentionConfig:
    """Validated layered retention configuration."""

    default_keep_days: int
    rules: tuple[RetentionRule, ...]

    def as_dict(self) -> dict:
        """Return a JSON serializable representation of the configuration."""
        return {
            CONF_RETENTION_ENABLED: True,
            CONF_RETENTION_DEFAULT_KEEP_DAYS: self.default_keep_days,
            CONF_RETENTION_RULES: [rule.as_dict() for rule in self.rules],
        }


def config_from_yaml(conf: dict | None) -> RetentionConfig | None:
    """Build a RetentionConfig from validated yaml configuration."""
    if not conf or not conf[CONF_RETENTION_ENABLED]:
        return None
    rules = tuple(
        RetentionRule(
            index=index,
            name=rule.get(CONF_RULE_NAME),
            keep_days=rule[CONF_KEEP_DAYS],
            entity_domains=frozenset(rule[CONF_ENTITY_DOMAINS]),
            integration_domains=frozenset(rule[CONF_INTEGRATION_DOMAINS]),
            entity_id_prefixes=tuple(rule[CONF_ENTITY_ID_PREFIXES]),
            entity_ids=frozenset(rule[CONF_ENTITY_IDS]),
        )
        for index, rule in enumerate(conf[CONF_RETENTION_RULES])
    )
    return RetentionConfig(
        default_keep_days=conf[CONF_RETENTION_DEFAULT_KEEP_DAYS], rules=rules
    )


@dataclass(slots=True)
class PurgeLayer:
    """A layer with its resolved cutoff for a specific purge run."""

    rule: RetentionRule | None
    name: str
    keep_days: int
    purge_before: datetime
    # Entity ids provided by the matching integrations, resolved from the
    # entity registry when the purge is started.
    integration_entity_ids: frozenset[str] = frozenset()

    def matches(self, entity_id: str) -> bool:
        """Return if an entity matches this layer."""
        return self.rule is not None and self.rule.matches(
            entity_id, self.integration_entity_ids
        )


@dataclass(slots=True)
class PurgePlan:
    """An immutable snapshot of a layered purge run.

    The same plan is carried by every PurgeTask re-queued for the run, so
    interruptions continue with the same cutoffs and the same progress.
    """

    run_id: str
    config: RetentionConfig
    # Cutoff used for short term statistics, statistics runs and recorder
    # runs, derived from the global purge_keep_days setting.
    purge_before: datetime
    layers: tuple[PurgeLayer, ...]
    default_layer: PurgeLayer
    repack: bool
    apply_filter: bool
    started_at: datetime
    # Cache of the metadata ids matched by each layer, the default layer is
    # the last entry. Built on the recorder thread during the first purge
    # iteration from the states_meta snapshot.
    metadata_groups: list[set[int]] | None = field(default=None)

    def layer_for_entity(self, entity_id: str) -> int:
        """Return the index of the first layer matching an entity."""
        for layer in self.layers:
            rule = layer.rule
            if rule is not None and layer.matches(entity_id):
                return rule.index
        return len(self.layers)


@callback
def build_purge_plan(
    hass: HomeAssistant,
    config: RetentionConfig,
    purge_before: datetime,
    repack: bool,
    apply_filter: bool,
    now: datetime,
) -> PurgePlan:
    """Build a purge plan from a configuration snapshot.

    Must be called from the event loop thread as it reads the entity
    registry. Matching against recorded entities happens later on the
    recorder thread using the resolved entity id sets.
    """
    registry = er.async_get(hass)
    wanted_integration_domains = set().union(
        *(rule.integration_domains for rule in config.rules)
    )
    integration_entities: dict[str, set[str]] = {
        domain: set() for domain in wanted_integration_domains
    }
    if wanted_integration_domains:
        for entity_id, entry in registry.entities.items():
            if entry.platform in integration_entities:
                integration_entities[entry.platform].add(entity_id)

    layers: list[PurgeLayer] = []
    for rule in config.rules:
        integration_entity_ids = frozenset(
            entity_id
            for domain in rule.integration_domains
            for entity_id in integration_entities.get(domain, ())
        )
        layers.append(
            PurgeLayer(
                rule=rule,
                name=rule.name or f"rule_{rule.index}",
                keep_days=rule.keep_days,
                purge_before=now - timedelta(days=rule.keep_days),
                integration_entity_ids=integration_entity_ids,
            )
        )
    default_layer = PurgeLayer(
        rule=None,
        name=DEFAULT_LAYER_NAME,
        keep_days=config.default_keep_days,
        purge_before=now - timedelta(days=config.default_keep_days),
    )
    return PurgePlan(
        run_id=uuid4().hex,
        config=config,
        purge_before=purge_before,
        layers=tuple(layers),
        default_layer=default_layer,
        repack=repack,
        apply_filter=apply_filter,
        started_at=now,
    )


@dataclass(slots=True)
class LayerProgress:
    """Progress of a single layer within a purge run."""

    index: int
    name: str
    keep_days: int
    purge_before: datetime
    matched_entities: int = 0
    deleted_states: int = 0
    retained_states: int | None = None
    processed: bool = False

    def as_dict(self) -> dict:
        """Return a JSON serializable representation."""
        return {
            "index": self.index,
            "name": self.name,
            CONF_KEEP_DAYS: self.keep_days,
            "purge_before": self.purge_before.isoformat(),
            "matched_entities": self.matched_entities,
            "deleted_states": self.deleted_states,
            "retained_states": self.retained_states,
            "processed": self.processed,
        }


@dataclass(slots=True)
class PurgeProgress:
    """Progress of a layered purge run.

    Updated on the recorder thread, read from the event loop thread.
    Counters are only updated after a purge iteration commits, so a failed
    iteration never inflates the counts.
    """

    run_id: str
    started_at: datetime
    layers: list[LayerProgress]
    default_layer: LayerProgress
    deleted_events: int = 0
    retained_events: int | None = None
    finished_at: datetime | None = None
    running: bool = True
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def all_layers(self) -> list[LayerProgress]:
        """Return rule layers followed by the default layer."""
        return [*self.layers, self.default_layer]

    def commit_iteration(
        self,
        deleted_states_by_layer: dict[int, int],
        deleted_events: int,
        completed_layers: Iterable[int],
    ) -> None:
        """Merge the counters of a successfully committed iteration."""
        with self._lock:
            for index, deleted in deleted_states_by_layer.items():
                self._layer(index).deleted_states += deleted
            for index in completed_layers:
                self._layer(index).processed = True
            self.deleted_events += deleted_events

    def _layer(self, index: int) -> LayerProgress:
        if index < len(self.layers):
            return self.layers[index]
        return self.default_layer

    def set_matched_entities(self, matched: dict[int, int]) -> None:
        """Set the number of entities matched by each layer."""
        with self._lock:
            for index, count in matched.items():
                self._layer(index).matched_entities = count

    def set_retained(
        self, retained_states_by_layer: dict[int, int], retained_events: int
    ) -> None:
        """Set the final retained counts of the run."""
        with self._lock:
            for index, retained in retained_states_by_layer.items():
                self._layer(index).retained_states = retained
            self.retained_events = retained_events

    def finish(self, finished_at: datetime) -> None:
        """Mark the run as finished."""
        with self._lock:
            self.running = False
            self.finished_at = finished_at
            for layer in self.all_layers:
                layer.processed = True

    def as_dict(self) -> dict:
        """Return a JSON serializable snapshot of the progress."""
        with self._lock:
            layers = [layer.as_dict() for layer in self.all_layers]
            deleted_states = sum(layer.deleted_states for layer in self.all_layers)
            processed_layers = sum(1 for layer in self.all_layers if layer.processed)
            first_unprocessed = next(
                (layer.index for layer in self.all_layers if not layer.processed),
                None,
            )
            return {
                "run_id": self.run_id,
                "running": self.running,
                "started_at": self.started_at.isoformat(),
                "finished_at": (
                    self.finished_at.isoformat() if self.finished_at else None
                ),
                "current_layer": first_unprocessed,
                "processed_layers": processed_layers,
                "deleted_states": deleted_states,
                "deleted_events": self.deleted_events,
                "deleted_records": deleted_states + self.deleted_events,
                "retained_events": self.retained_events,
                "layers": layers,
            }


def progress_for_plan(plan: PurgePlan) -> PurgeProgress:
    """Create the progress tracker for a purge plan."""
    layers = []
    for layer in plan.layers:
        assert layer.rule is not None
        layers.append(
            LayerProgress(
                index=layer.rule.index,
                name=layer.name,
                keep_days=layer.keep_days,
                purge_before=layer.purge_before,
            )
        )
    default_layer = LayerProgress(
        index=len(layers),
        name=DEFAULT_LAYER_NAME,
        keep_days=plan.default_layer.keep_days,
        purge_before=plan.default_layer.purge_before,
    )
    return PurgeProgress(
        run_id=plan.run_id,
        started_at=plan.started_at,
        layers=layers,
        default_layer=default_layer,
    )
