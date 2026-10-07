"""Observation of state machine writes.

Provides per-entity write attribution for the state machine: every write is
classified as a state change, an attribute-only change or an identical repeat
report, and the writer identifier (integration domain) of every write is
recorded. Statistics are accumulated in fixed length tumbling windows.

The observation is disabled by default and is only active while a
``WriteObservationManager`` is connected to the state machine with
``async_connect``. When disconnected, the state machine performs no extra
work on the write path.
"""

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
import datetime
import fnmatch
import math
import random
import time
from typing import Final

from homeassistant.core import (
    STATE_WRITE_ATTRIBUTES,
    STATE_WRITE_CHANGED,
    STATE_WRITE_REMOVED,
    STATE_WRITE_REPORTED,
    HomeAssistant,
    callback,
    split_entity_id,
    valid_entity_id,
)
from homeassistant.util import dt as dt_util

DATA_WRITE_OBSERVATION: Final = "write_observation_manager"

UNKNOWN_WRITER: Final = "unknown"
"""Writer bucket used for writes without a writer identifier."""

GROUP_BY_ENTITY_DOMAIN: Final = "entity_domain"
GROUP_BY_INTEGRATION: Final = "integration"

DEFAULT_SAMPLE_RATE: Final = 1.0
DEFAULT_WINDOW_SECONDS: Final = 30.0
DEFAULT_RETAIN: Final = 100
DEFAULT_CHUNK_SIZE: Final = 100

_CATEGORY_INDEX: Final = {
    STATE_WRITE_CHANGED: 0,
    STATE_WRITE_ATTRIBUTES: 1,
    STATE_WRITE_REPORTED: 2,
}


class WriteObservationError(ValueError):
    """Raised when write observation input is invalid."""


@dataclass(frozen=True)
class WriteObservationConfig:
    """Configuration for state write observation."""

    sample_rate: float = DEFAULT_SAMPLE_RATE
    """Fraction of writes to sample, between 0 and 1 inclusive."""

    window_seconds: float = DEFAULT_WINDOW_SECONDS
    """Length of each statistics window in seconds."""

    retain: int = DEFAULT_RETAIN
    """Number of windows to keep around, including the active one."""

    exclude: tuple[str, ...] = ()
    """Entity ids or fnmatch patterns that are never observed."""

    def __post_init__(self) -> None:
        """Validate the configuration."""
        validate_options(
            sample_rate=self.sample_rate,
            window_seconds=self.window_seconds,
            retain=self.retain,
        )


def validate_options(*, sample_rate: float, window_seconds: float, retain: int) -> None:
    """Validate write observation options."""
    if not 0 <= sample_rate <= 1:
        raise WriteObservationError(
            f"sample_rate must be between 0 and 1, got {sample_rate}"
        )
    if window_seconds <= 0:
        raise WriteObservationError(
            f"window_seconds must be greater than 0, got {window_seconds}"
        )
    if retain < 0:
        raise WriteObservationError(f"retain must not be negative, got {retain}")


@dataclass(frozen=True)
class RateAnnotation:
    """Annotation marking an entity as a fast state writer."""

    window_length: float
    rate: float
    threshold: float

    def as_dict(self) -> dict[str, float]:
        """Return a dict representation."""
        return {
            "window_length": self.window_length,
            "rate": self.rate,
            "threshold": self.threshold,
        }


@dataclass(frozen=True)
class ObservationWindow:
    """Time bounds and sampling metadata of a statistics response."""

    start_time: datetime.datetime
    end_time: datetime.datetime
    sampled: bool
    sample_rate: float | None

    def as_dict(self) -> dict[str, object]:
        """Return a dict representation."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "sampled": self.sampled,
            "sample_rate": self.sample_rate,
        }


@dataclass(frozen=True)
class EntityWriteStat:
    """Attributed write counters for one entity generation."""

    entity_id: str
    generation: int
    state_changes: int
    attribute_changes: int
    same_state_reports: int
    writes: int
    writers: tuple[str, ...]
    removed: bool
    rate_annotation: RateAnnotation | None = None

    def as_dict(self) -> dict[str, object]:
        """Return a dict representation."""
        return {
            "entity_id": self.entity_id,
            "generation": self.generation,
            "state_changes": self.state_changes,
            "attribute_changes": self.attribute_changes,
            "same_state_reports": self.same_state_reports,
            "writes": self.writes,
            "writers": list(self.writers),
            "removed": self.removed,
            "rate_annotation": (
                self.rate_annotation.as_dict()
                if self.rate_annotation is not None
                else None
            ),
        }


@dataclass(frozen=True)
class DomainWriteStat:
    """Aggregated write counters for one domain."""

    domain: str
    state_changes: int
    attribute_changes: int
    same_state_reports: int
    writes: int

    @property
    def state_change_share(self) -> float:
        """Fraction of writes that changed the state."""
        return self.state_changes / self.writes if self.writes else 0.0

    @property
    def attribute_change_share(self) -> float:
        """Fraction of writes that only changed attributes."""
        return self.attribute_changes / self.writes if self.writes else 0.0

    @property
    def same_state_report_share(self) -> float:
        """Fraction of writes that were identical repeat reports."""
        return self.same_state_reports / self.writes if self.writes else 0.0

    def as_dict(self) -> dict[str, object]:
        """Return a dict representation."""
        return {
            "domain": self.domain,
            "state_changes": self.state_changes,
            "attribute_changes": self.attribute_changes,
            "same_state_reports": self.same_state_reports,
            "writes": self.writes,
            "state_change_share": self.state_change_share,
            "attribute_change_share": self.attribute_change_share,
            "same_state_report_share": self.same_state_report_share,
        }


@dataclass(frozen=True)
class WriteObservationResult:
    """Result of a write observation query."""

    window: ObservationWindow
    entries: tuple[EntityWriteStat, ...] | tuple[DomainWriteStat, ...]
    has_more: bool
    next_offset: int | None

    def as_dict(self) -> dict[str, object]:
        """Return a dict representation."""
        return {
            "window": self.window.as_dict(),
            "entries": [entry.as_dict() for entry in self.entries],
            "has_more": self.has_more,
            "next_offset": self.next_offset,
        }


@dataclass
class _Row:
    """Per entity generation counters within one or more windows."""

    entity_id: str
    generation: int
    first_write: float
    last_write: float
    state_changes: int = 0
    attribute_changes: int = 0
    same_state_reports: int = 0
    removed: bool = False
    writer_counts: dict[str, list[int]] = field(default_factory=dict)


@dataclass
class _Window:
    """A fixed length tumbling window of per entity counters."""

    start: float
    sample_rate: float
    rng: random.Random
    rows: dict[tuple[str, int], _Row] = field(default_factory=dict)


def _merge_row(target: _Row, row: _Row) -> None:
    """Merge one row into another."""
    target.state_changes += row.state_changes
    target.attribute_changes += row.attribute_changes
    target.same_state_reports += row.same_state_reports
    target.first_write = min(target.first_write, row.first_write)
    target.last_write = max(target.last_write, row.last_write)
    target.removed |= row.removed
    for writer, counts in row.writer_counts.items():
        target_counts = target.writer_counts.setdefault(writer, [0, 0, 0])
        target_counts[0] += counts[0]
        target_counts[1] += counts[1]
        target_counts[2] += counts[2]


class WriteObservationManager:
    """Accumulate state write statistics in tumbling windows.

    All methods are callback safe and must be run in the event loop.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        sample_rate: float = DEFAULT_SAMPLE_RATE,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        retain: int = DEFAULT_RETAIN,
        exclude: tuple[str, ...] | list[str] = (),
        timer: Callable[[], float] = time.time,
    ) -> None:
        """Initialize the manager."""
        validate_options(
            sample_rate=sample_rate,
            window_seconds=window_seconds,
            retain=retain,
        )
        self._hass = hass
        self._sample_rate = float(sample_rate)
        self._window_seconds = float(window_seconds)
        self._retain = int(retain)
        self._exclude = tuple(pattern.lower() for pattern in exclude)
        self._timer = timer
        self._epoch: float | None = None
        self._windows: deque[_Window] = deque()
        self._generations: dict[str, int] = {}
        self._live: dict[str, int] = {}
        self._connected = False

    @property
    def config(self) -> WriteObservationConfig:
        """Return the current configuration."""
        return WriteObservationConfig(
            sample_rate=self._sample_rate,
            window_seconds=self._window_seconds,
            retain=self._retain,
            exclude=self._exclude,
        )

    @property
    def connected(self) -> bool:
        """Return if the manager is observing state writes."""
        return self._connected

    @callback
    def async_connect(self) -> None:
        """Start observing state machine writes."""
        if self._connected:
            return
        self._epoch = self._timer()
        self._hass.states.async_set_write_observer(self._async_record)
        self._connected = True

    @callback
    def async_disconnect(self) -> None:
        """Stop observing state machine writes."""
        if not self._connected:
            return
        self._hass.states.async_set_write_observer(None)
        self._connected = False

    @callback
    def _async_record(
        self,
        entity_id: str,
        category: int,
        writer: str | None,
        timestamp: float,
    ) -> None:
        """Record one state machine notification."""
        if self._is_excluded(entity_id):
            return
        if category == STATE_WRITE_REMOVED:
            self._async_record_removal(entity_id)
            return
        window = self._async_window_for(timestamp)
        # Generation bookkeeping happens before sampling so the remove/recreate
        # lineage stays correct even when writes are sampled out.
        generation = self._live.get(entity_id)
        if generation is None:
            generation = self._generations.get(entity_id, 0)
            self._live[entity_id] = generation
        self._generations[entity_id] = generation
        # The sampling decision is made per write with a deterministic RNG
        # seeded per window, so the rule is stable for the whole window.
        if window.sample_rate < 1.0 and window.rng.random() >= window.sample_rate:
            return
        key = (entity_id, generation)
        row = window.rows.get(key)
        if row is None:
            row = _Row(entity_id, generation, timestamp, timestamp)
            window.rows[key] = row
        else:
            row.first_write = min(row.first_write, timestamp)
            row.last_write = timestamp
        counts_index = _CATEGORY_INDEX[category]
        if counts_index == 0:
            row.state_changes += 1
        elif counts_index == 1:
            row.attribute_changes += 1
        else:
            row.same_state_reports += 1
        writer_key = writer if writer is not None else UNKNOWN_WRITER
        writer_counts = row.writer_counts.setdefault(writer_key, [0, 0, 0])
        writer_counts[counts_index] += 1

    @callback
    def _async_record_removal(self, entity_id: str) -> None:
        """Mark the live generation of an entity as removed."""
        generation = self._live.pop(entity_id, None)
        if generation is None:
            generation = self._generations.get(entity_id, 0)
        self._generations[entity_id] = generation + 1
        # Mark the most recent window that recorded this generation.
        for window in reversed(self._windows):
            row = window.rows.get((entity_id, generation))
            if row is not None:
                row.removed = True
                break

    @callback
    def _async_window_for(self, timestamp: float) -> _Window:
        """Get the window a timestamp belongs to, rolling over if needed."""
        windows = self._windows
        length = self._window_seconds
        epoch = self._epoch
        assert epoch is not None
        if windows:
            current = windows[-1]
            if timestamp < current.start + length:
                return current
            new_start = current.start + length
            if timestamp >= new_start + length:
                new_start = epoch + math.floor((timestamp - epoch) / length) * length
        else:
            new_start = epoch + math.floor((timestamp - epoch) / length) * length
        while windows and len(windows) >= self._retain:
            windows.popleft()
        window = _Window(
            start=new_start,
            sample_rate=self._sample_rate,
            rng=random.Random(f"write_observation:{new_start}"),
        )
        windows.append(window)
        return window

    def _is_excluded(self, entity_id: str) -> bool:
        """Return if an entity id is excluded from observation."""
        return any(fnmatch.fnmatchcase(entity_id, pattern) for pattern in self._exclude)

    def _normalize_entity_ids(
        self, entity_ids: list[str] | tuple[str, ...] | None
    ) -> list[str] | None:
        """Validate and normalize an entity id filter."""
        if entity_ids is None:
            return None
        normalized = [entity_id.lower() for entity_id in entity_ids]
        unknown = [
            entity_id
            for entity_id in normalized
            if not valid_entity_id(entity_id)
            or (
                self._hass.states.get(entity_id) is None
                and entity_id not in self._generations
            )
        ]
        if unknown:
            raise WriteObservationError(
                "Unknown entity ids: " + ", ".join(sorted(set(unknown)))
            )
        return normalized

    @staticmethod
    def _normalize_time(
        value: float | datetime.datetime | None, name: str
    ) -> float | None:
        """Normalize a query time bound to a timestamp."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, datetime.datetime):
            if value.tzinfo is None:
                raise WriteObservationError(f"{name} must be timezone aware")
            return value.timestamp()
        raise WriteObservationError(f"{name} must be a datetime")

    def _validate_paging(self, limit: int | None, offset: int) -> tuple[int, int]:
        """Validate and normalize chunk paging parameters."""
        if limit is None:
            limit = DEFAULT_CHUNK_SIZE
        if type(limit) is not int or limit < 0:
            raise WriteObservationError(f"limit must not be negative, got {limit}")
        if type(offset) is not int or offset < 0:
            raise WriteObservationError(f"offset must not be negative, got {offset}")
        return limit, offset

    def _select_windows(
        self, start: float | None, end: float | None
    ) -> tuple[list[_Window], float, float]:
        """Select windows intersecting the query range.

        Queries do not create windows; an empty query returns the computed
        bounds with zero entries.
        """
        now = self._timer()
        if start is not None and end is not None and start > end:
            raise WriteObservationError("start_time must not be after end_time")
        epoch = self._epoch if self._epoch is not None else now
        length = self._window_seconds
        current_index = max(0, math.floor((now - epoch) / length))
        current_start = epoch + current_index * length
        low = start if start is not None else current_start
        high = end if end is not None else current_start + length
        selected = [
            window
            for window in self._windows
            if window.start < high and window.start + length > low
        ]
        if selected:
            window_start = min(window.start for window in selected)
            window_end = max(window.start + length for window in selected)
        else:
            window_start, window_end = low, high
        return selected, window_start, window_end

    def _merge_windows(self, windows: list[_Window]) -> dict[tuple[str, int], _Row]:
        """Merge the rows of multiple windows."""
        merged: dict[tuple[str, int], _Row] = {}
        latest_start: dict[tuple[str, int], float] = {}
        for window in windows:
            for key, row in window.rows.items():
                existing = merged.get(key)
                if existing is None:
                    merged[key] = _Row(
                        row.entity_id,
                        row.generation,
                        row.first_write,
                        row.last_write,
                        row.state_changes,
                        row.attribute_changes,
                        row.same_state_reports,
                        removed=row.removed,
                        writer_counts={
                            writer: counts.copy()
                            for writer, counts in row.writer_counts.items()
                        },
                    )
                    latest_start[key] = window.start
                else:
                    _merge_row(existing, row)
                    if window.start > latest_start[key]:
                        existing.removed = row.removed
                        latest_start[key] = window.start
        return merged

    def _result_window(
        self, windows: list[_Window], start: float, end: float
    ) -> ObservationWindow:
        """Build the response window metadata."""
        if windows:
            sampled = any(window.sample_rate < 1.0 for window in windows)
            rates = {window.sample_rate for window in windows}
            sample_rate = next(iter(rates)) if len(rates) == 1 else None
        else:
            # No recorded windows yet; report the configured sampling rule.
            sampled = self._sample_rate < 1.0
            sample_rate = self._sample_rate
        return ObservationWindow(
            start_time=dt_util.utc_from_timestamp(start),
            end_time=dt_util.utc_from_timestamp(end),
            sampled=sampled,
            sample_rate=sample_rate,
        )

    def _current_generation(self, entity_id: str) -> tuple[int, bool]:
        """Return the current generation and removed state of an entity."""
        live_generation = self._live.get(entity_id)
        if live_generation is not None:
            return live_generation, False
        next_generation = self._generations.get(entity_id, 0)
        if self._hass.states.get(entity_id) is not None:
            # Existing entity without observed live writes in the window.
            return next_generation, False
        # The entity is gone; next_generation is the generation after the
        # removed one, so the last actual generation is one less.
        return max(0, next_generation - 1), True

    def _synthetic_row(self, entity_id: str) -> _Row:
        """Create a zeroed row for an entity without writes in the window."""
        generation, removed = self._current_generation(entity_id)
        return _Row(entity_id, generation, 0.0, 0.0, removed=removed)

    @callback
    def async_get_entity_stats(
        self,
        *,
        entity_ids: list[str] | tuple[str, ...] | None = None,
        start_time: float | datetime.datetime | None = None,
        end_time: float | datetime.datetime | None = None,
        limit: int | None = DEFAULT_CHUNK_SIZE,
        offset: int = 0,
    ) -> WriteObservationResult:
        """Return per entity generation write attribution."""
        limit, offset = self._validate_paging(limit, offset)
        start = self._normalize_time(start_time, "start_time")
        end = self._normalize_time(end_time, "end_time")
        windows, window_start, window_end = self._select_windows(start, end)
        merged = self._merge_windows(windows)
        filter_ids = self._normalize_entity_ids(entity_ids)
        if filter_ids is not None:
            rows: list[_Row] = []
            for entity_id in filter_ids:
                entity_rows = [
                    row for key, row in merged.items() if key[0] == entity_id
                ]
                rows.extend(entity_rows)
                generation, removed = self._current_generation(entity_id)
                if not any(row.generation == generation for row in entity_rows):
                    # Current generation has no writes in the window; keep a
                    # zeroed entry so the entity stays in the result.
                    rows.append(_Row(entity_id, generation, 0.0, 0.0, removed=removed))
        else:
            rows = list(merged.values())
        rows.sort(key=lambda row: (row.entity_id, row.generation))
        total = len(rows)
        chunk = rows[offset : offset + limit]
        has_more = offset + limit < total
        next_offset = offset + limit if has_more else None
        return WriteObservationResult(
            window=self._result_window(windows, window_start, window_end),
            entries=tuple(_entity_stat(row) for row in chunk),
            has_more=has_more,
            next_offset=next_offset,
        )

    @callback
    def async_get_domain_stats(
        self,
        *,
        group_by: str = GROUP_BY_ENTITY_DOMAIN,
        entity_ids: list[str] | tuple[str, ...] | None = None,
        start_time: float | datetime.datetime | None = None,
        end_time: float | datetime.datetime | None = None,
        limit: int | None = DEFAULT_CHUNK_SIZE,
        offset: int = 0,
    ) -> WriteObservationResult:
        """Return write counters aggregated by integration or entity domain."""
        if group_by not in (GROUP_BY_ENTITY_DOMAIN, GROUP_BY_INTEGRATION):
            raise WriteObservationError(
                f"group_by must be 'entity_domain' or 'integration', got {group_by!r}"
            )
        limit, offset = self._validate_paging(limit, offset)
        start = self._normalize_time(start_time, "start_time")
        end = self._normalize_time(end_time, "end_time")
        windows, window_start, window_end = self._select_windows(start, end)
        merged = self._merge_windows(windows)
        filter_ids = self._normalize_entity_ids(entity_ids)
        if filter_ids is not None:
            filter_set = set(filter_ids)
            merged = {key: row for key, row in merged.items() if key[0] in filter_set}
        counters: dict[str, list[int]] = {}
        for row in merged.values():
            if group_by == GROUP_BY_ENTITY_DOMAIN:
                target = counters.setdefault(
                    split_entity_id(row.entity_id)[0], [0, 0, 0]
                )
                target[0] += row.state_changes
                target[1] += row.attribute_changes
                target[2] += row.same_state_reports
            else:
                for writer, writer_counts in row.writer_counts.items():
                    target = counters.setdefault(writer, [0, 0, 0])
                    target[0] += writer_counts[0]
                    target[1] += writer_counts[1]
                    target[2] += writer_counts[2]
        # Entity domains of filtered entities stay present even with zero writes.
        if filter_ids is not None and group_by == GROUP_BY_ENTITY_DOMAIN:
            for entity_id in filter_ids:
                counters.setdefault(split_entity_id(entity_id)[0], [0, 0, 0])
        domains = sorted(counters)
        total = len(domains)
        chunk = domains[offset : offset + limit]
        has_more = offset + limit < total
        next_offset = offset + limit if has_more else None
        entries = tuple(
            DomainWriteStat(
                domain=domain,
                state_changes=counters[domain][0],
                attribute_changes=counters[domain][1],
                same_state_reports=counters[domain][2],
                writes=sum(counters[domain]),
            )
            for domain in chunk
        )
        return WriteObservationResult(
            window=self._result_window(windows, window_start, window_end),
            entries=entries,
            has_more=has_more,
            next_offset=next_offset,
        )

    @callback
    def async_get_rate_stats(
        self,
        *,
        rate_threshold: float,
        entity_ids: list[str] | tuple[str, ...] | None = None,
        start_time: float | datetime.datetime | None = None,
        end_time: float | datetime.datetime | None = None,
        limit: int | None = DEFAULT_CHUNK_SIZE,
        offset: int = 0,
    ) -> WriteObservationResult:
        """Return entities whose write rate exceeds a threshold."""
        if type(rate_threshold) not in (int, float) or rate_threshold <= 0:
            raise WriteObservationError(
                f"rate_threshold must be greater than 0, got {rate_threshold}"
            )
        limit, offset = self._validate_paging(limit, offset)
        start = self._normalize_time(start_time, "start_time")
        end = self._normalize_time(end_time, "end_time")
        windows, window_start, window_end = self._select_windows(start, end)
        window_length = window_end - window_start
        merged = self._merge_windows(windows)
        filter_ids = self._normalize_entity_ids(entity_ids)
        if filter_ids is not None:
            filter_set = set(filter_ids)
            merged = {key: row for key, row in merged.items() if key[0] in filter_set}
        per_entity: dict[str, _Row] = {}
        for row in merged.values():
            existing = per_entity.get(row.entity_id)
            if existing is None:
                per_entity[row.entity_id] = _Row(
                    row.entity_id,
                    row.generation,
                    row.first_write,
                    row.last_write,
                    row.state_changes,
                    row.attribute_changes,
                    row.same_state_reports,
                    removed=row.removed,
                    writer_counts={
                        writer: counts.copy()
                        for writer, counts in row.writer_counts.items()
                    },
                )
            else:
                _merge_row(existing, row)
                # Rows are merged chronologically; the latest generation
                # decides the entity level removed flag.
                existing.generation = row.generation
                existing.removed = row.removed
        annotated: list[tuple[float, str, _Row]] = []
        for entity_id, row in per_entity.items():
            writes = row.state_changes + row.attribute_changes + row.same_state_reports
            rate = writes / window_length if window_length > 0 else 0.0
            if rate > rate_threshold:
                annotated.append((rate, entity_id, row))
        annotated.sort(key=lambda item: (-item[0], item[1]))
        total = len(annotated)
        chunk = annotated[offset : offset + limit]
        has_more = offset + limit < total
        next_offset = offset + limit if has_more else None
        entries = tuple(
            _entity_stat(
                row,
                RateAnnotation(
                    window_length=window_length,
                    rate=rate,
                    threshold=float(rate_threshold),
                ),
            )
            for rate, _entity_id, row in chunk
        )
        return WriteObservationResult(
            window=self._result_window(windows, window_start, window_end),
            entries=entries,
            has_more=has_more,
            next_offset=next_offset,
        )


def _entity_stat(
    row: _Row, rate_annotation: RateAnnotation | None = None
) -> EntityWriteStat:
    """Build an EntityWriteStat from an internal row."""
    return EntityWriteStat(
        entity_id=row.entity_id,
        generation=row.generation,
        state_changes=row.state_changes,
        attribute_changes=row.attribute_changes,
        same_state_reports=row.same_state_reports,
        writes=row.state_changes + row.attribute_changes + row.same_state_reports,
        writers=tuple(sorted(row.writer_counts)),
        removed=row.removed,
        rate_annotation=rate_annotation,
    )
