"""Purge old data helper."""

from collections.abc import Callable
from datetime import datetime
import logging
import time
from typing import TYPE_CHECKING

from sqlalchemy import func, select
from sqlalchemy.orm.session import Session

from homeassistant.util import dt as dt_util
from homeassistant.util.collection import chunked_or_all

from .db_schema import Events, States, StatesMeta
from .models import DatabaseEngine
from .queries import (
    attributes_ids_exist_in_states,
    attributes_ids_exist_in_states_with_fast_in_distinct,
    count_states_by_metadata_id,
    data_ids_exist_in_events,
    data_ids_exist_in_events_with_fast_in_distinct,
    delete_event_data_rows,
    delete_event_rows,
    delete_event_types_rows,
    delete_recorder_runs_rows,
    delete_states_attributes_rows,
    delete_states_meta_rows,
    delete_states_rows,
    delete_statistics_runs_rows,
    delete_statistics_short_term_rows,
    disconnect_states_rows,
    find_all_states_metadata_ids,
    find_entity_ids_to_purge,
    find_event_types_to_purge,
    find_events_to_purge,
    find_latest_statistics_runs_run_id,
    find_legacy_detached_states_and_attributes_to_purge,
    find_legacy_event_state_and_attributes_and_data_ids_to_purge,
    find_legacy_row,
    find_short_term_statistics_to_purge,
    find_states_to_purge,
    find_states_to_purge_for_metadata,
    find_statistics_runs_to_purge,
)
from .repack import repack_database
from .retention import PurgePlan, PurgeProgress
from .util import retryable_database_job, session_scope

if TYPE_CHECKING:
    from . import Recorder

_LOGGER = logging.getLogger(__name__)


DEFAULT_STATES_BATCHES_PER_PURGE = 20  # We expect ~95% de-dupe rate
DEFAULT_EVENTS_BATCHES_PER_PURGE = 15  # We expect ~92% de-dupe rate


@retryable_database_job("purge")
def purge_old_data(
    instance: Recorder,
    purge_before: datetime,
    repack: bool,
    apply_filter: bool = False,
    events_batch_size: int = DEFAULT_EVENTS_BATCHES_PER_PURGE,
    states_batch_size: int = DEFAULT_STATES_BATCHES_PER_PURGE,
    plan: PurgePlan | None = None,
) -> bool:
    """Purge events and states.

    With a purge plan, states are purged per layer using the cutoff of the
    first matching retention rule while events use the default layer
    cutoff. Without a plan, every state and event older than purge_before is
    purged, which is the historical single-cutoff behavior.

    Cleans up a timeframe of an hour, based on the oldest record.
    """
    if plan is None:
        return _purge_old_data_single_cutoff(
            instance,
            purge_before,
            repack,
            apply_filter,
            events_batch_size,
            states_batch_size,
        )
    return _purge_old_data_layered(
        instance,
        plan,
        apply_filter,
        events_batch_size,
        states_batch_size,
    )


def _purge_old_data_single_cutoff(
    instance: Recorder,
    purge_before: datetime,
    repack: bool,
    apply_filter: bool,
    events_batch_size: int,
    states_batch_size: int,
) -> bool:
    """Purge events and states older than purge_before."""
    _LOGGER.debug(
        "Purging states and events before target %s",
        purge_before.isoformat(sep=" ", timespec="seconds"),
    )
    with session_scope(session=instance.get_session()) as session:
        # Purge a max of max_bind_vars, based on the oldest states or events record
        has_more_to_purge = False
        if instance.use_legacy_events_index and _purging_legacy_format(session):
            _LOGGER.debug(
                "Purge running in legacy format as there are states with event_id"
                " remaining"
            )
            has_more_to_purge |= _purge_legacy_format(instance, session, purge_before)
        else:
            _LOGGER.debug(
                "Purge running in new format as there are NO states with event_id"
                " remaining"
            )
            # Once we are done purging legacy rows, we use the new method
            has_more_to_purge |= _purge_states_and_attributes_ids(
                instance, session, states_batch_size, purge_before
            )
            has_more_to_purge |= _purge_events_and_data_ids(
                instance, session, events_batch_size, purge_before
            )

        statistics_runs, statistics_runs_has_more = _select_statistics_runs_to_purge(
            session, purge_before, instance.max_bind_vars
        )
        short_term_statistics, short_term_statistics_has_more = (
            _select_short_term_statistics_to_purge(
                session, purge_before, instance.max_bind_vars
            )
        )
        if statistics_runs:
            _purge_statistics_runs(session, statistics_runs)

        if short_term_statistics:
            _purge_short_term_statistics(session, short_term_statistics)

        if (
            has_more_to_purge
            or statistics_runs_has_more
            or short_term_statistics_has_more
        ):
            # Return false, as we might not be done yet.
            _LOGGER.debug("Purging hasn't fully completed yet")
            return False

        if apply_filter and not _purge_filtered_data(instance, session):
            _LOGGER.debug("Cleanup filtered data hasn't fully completed yet")
            return False

        # This purge cycle is finished, clean up old event types and
        # recorder runs
        _purge_old_event_types(instance, session)
        _purge_old_entity_ids(instance, session)

        _purge_old_recorder_runs(instance, session, purge_before)
    with session_scope(session=instance.get_session(), read_only=True) as session:
        instance.recorder_runs_manager.load_from_db(session)
        instance.states_manager.load_from_db(session)
    if repack:
        repack_database(instance)
    return True


def _purge_old_data_layered(
    instance: Recorder,
    plan: PurgePlan,
    apply_filter: bool,
    events_batch_size: int,
    states_batch_size: int,
) -> bool:
    """Purge states per retention layer and events using the default cutoff.

    Short term statistics, statistics runs and recorder runs keep using the
    global purge_keep_days cutoff carried by the plan.
    """
    progress = instance.purge_progress
    if progress is None or progress.run_id != plan.run_id:
        _LOGGER.error(
            "Layered purge %s started without matching progress tracking",
            plan.run_id,
        )
        return True
    _LOGGER.debug("Running layered purge %s", plan.run_id)
    if instance.use_legacy_events_index:
        # States still linked by event ids are migration leftovers, drain
        # them with the global cutoff before layered purging takes over.
        with session_scope(
            session=instance.get_session(), read_only=True
        ) as legacy_session:
            legacy_rows_remaining = _purging_legacy_format(legacy_session)
        if legacy_rows_remaining:
            return _purge_old_data_single_cutoff(
                instance,
                plan.purge_before,
                plan.repack,
                apply_filter,
                events_batch_size,
                states_batch_size,
            )

    deleted_states_by_layer: dict[int, int] = {}
    completed_layers: list[int] = []
    deleted_events = 0
    finished = True
    with session_scope(session=instance.get_session()) as session:
        groups = _resolve_layer_groups(instance, session, plan, progress)
        default_layer_index = len(groups) - 1
        states_batch_budget = states_batch_size
        for index, metadata_ids in enumerate(groups):
            layer_progress = progress.all_layers[index]
            if layer_progress.processed:
                continue
            purge_before_ts = _layer_purge_before(plan, index).timestamp()
            attributes_ids_batch: set[int] = set()
            layer_states_done = True
            while states_batch_budget and metadata_ids:
                state_ids, attributes_ids = _select_layer_state_attributes_ids(
                    session,
                    metadata_ids,
                    purge_before_ts,
                    instance.max_bind_vars,
                )
                if not state_ids:
                    break
                deleted_states_by_layer[index] = deleted_states_by_layer.get(
                    index, 0
                ) + _purge_state_ids(instance, session, state_ids)
                attributes_ids_batch |= attributes_ids
                states_batch_budget -= 1
                layer_states_done = False
            _purge_unused_attributes_ids(instance, session, attributes_ids_batch)
            if layer_states_done:
                # The default layer also covers events and is only marked
                # processed once the entire run finishes.
                if index != default_layer_index:
                    completed_layers.append(index)
            else:
                finished = False
                break

        events_has_more, deleted_events = _purge_layered_events_and_data_ids(
            instance, session, events_batch_size, plan.default_layer.purge_before
        )

        statistics_runs, statistics_runs_has_more = _select_statistics_runs_to_purge(
            session, plan.purge_before, instance.max_bind_vars
        )
        short_term_statistics, short_term_statistics_has_more = (
            _select_short_term_statistics_to_purge(
                session, plan.purge_before, instance.max_bind_vars
            )
        )
        if statistics_runs:
            _purge_statistics_runs(session, statistics_runs)

        if short_term_statistics:
            _purge_short_term_statistics(session, short_term_statistics)

        if (
            not finished
            or events_has_more
            or statistics_runs_has_more
            or short_term_statistics_has_more
        ):
            _LOGGER.debug("Layered purge hasn't fully completed yet")
            finished = False
        elif apply_filter and not _purge_filtered_data(instance, session):
            _LOGGER.debug("Cleanup filtered data hasn't fully completed yet")
            finished = False
        else:
            # This purge cycle is finished, clean up old event types and
            # recorder runs
            _purge_old_event_types(instance, session)
            _purge_old_entity_ids(instance, session)
            _purge_old_recorder_runs(instance, session, plan.purge_before)

    # The iteration has committed at this point, so the progress counters
    # can safely be published.
    progress.commit_iteration(deleted_states_by_layer, deleted_events, completed_layers)
    if not finished:
        return False

    with session_scope(session=instance.get_session(), read_only=True) as read_session:
        instance.recorder_runs_manager.load_from_db(read_session)
        instance.states_manager.load_from_db(read_session)
        retained_states, retained_events = _count_retained_records(read_session, plan)
    progress.set_retained(retained_states, retained_events)
    progress.finish(dt_util.utcnow())
    if plan.repack:
        repack_database(instance)
    return True


def _layer_purge_before(plan: PurgePlan, index: int) -> datetime:
    """Return the cutoff of a layer, the default layer is last."""
    if index < len(plan.layers):
        return plan.layers[index].purge_before
    return plan.default_layer.purge_before


def _resolve_layer_groups(
    instance: Recorder,
    session: Session,
    plan: PurgePlan,
    progress: PurgeProgress,
) -> list[set[int]]:
    """Partition states_meta metadata ids into layer groups.

    The partition is cached on the plan, so an interrupted run continues
    using the same entities even if new entities appear while it is running.
    """
    if plan.metadata_groups is not None:
        return plan.metadata_groups
    groups: list[set[int]] = [set() for _ in range(len(plan.layers) + 1)]
    matched = dict.fromkeys(range(len(groups)), 0)
    for metadata_id, entity_id in session.execute(find_all_states_metadata_ids()).all():
        index = plan.layer_for_entity(entity_id)
        groups[index].add(metadata_id)
        matched[index] += 1
    plan.metadata_groups = groups
    progress.set_matched_entities(matched)
    return groups


def _select_layer_state_attributes_ids(
    session: Session,
    metadata_ids: set[int],
    purge_before_timestamp: float,
    max_bind_vars: int,
) -> tuple[set[int], set[int]]:
    """Return sets of state and attribute ids to purge for a layer."""
    state_ids: set[int] = set()
    attributes_ids: set[int] = set()
    for metadata_ids_chunk in chunked_or_all(metadata_ids, max_bind_vars):
        for state_id, attributes_id in session.execute(
            find_states_to_purge_for_metadata(
                metadata_ids_chunk, purge_before_timestamp, max_bind_vars
            )
        ).all():
            state_ids.add(state_id)
            if attributes_id:
                attributes_ids.add(attributes_id)
    _LOGGER.debug(
        "Selected %s layer state ids and %s attributes_ids to remove",
        len(state_ids),
        len(attributes_ids),
    )
    return state_ids, attributes_ids


def _purge_layered_events_and_data_ids(
    instance: Recorder,
    session: Session,
    events_batch_size: int,
    purge_before: datetime,
) -> tuple[bool, int]:
    """Purge events older than the default layer cutoff in a batch.

    Returns true if there are more events to purge and the number of
    deleted event rows.
    """
    has_remaining_event_ids_to_purge = True
    data_ids_batch: set[int] = set()
    max_bind_vars = instance.max_bind_vars
    deleted_events = 0
    for _ in range(events_batch_size):
        event_ids, data_ids = _select_event_data_ids_to_purge(
            session, purge_before, max_bind_vars
        )
        if not event_ids:
            has_remaining_event_ids_to_purge = False
            break
        deleted_events += _purge_event_ids(session, event_ids)
        data_ids_batch = data_ids_batch | data_ids

    _purge_unused_data_ids(instance, session, data_ids_batch)
    _LOGGER.debug(
        "After layered event purge remaining=%s", has_remaining_event_ids_to_purge
    )
    return has_remaining_event_ids_to_purge, deleted_events


def _count_retained_records(
    session: Session, plan: PurgePlan
) -> tuple[dict[int, int], int]:
    """Count the states retained per layer and the remaining events."""
    groups = plan.metadata_groups
    assert groups is not None
    metadata_layer = {
        metadata_id: index
        for index, metadata_ids in enumerate(groups)
        for metadata_id in metadata_ids
    }
    retained = dict.fromkeys(range(len(groups)), 0)
    for metadata_id, count in session.execute(count_states_by_metadata_id()).all():
        if (index := metadata_layer.get(metadata_id)) is not None:
            retained[index] += count
    retained_events = (
        session.execute(select(func.count()).select_from(Events)).scalar() or 0
    )
    return retained, retained_events


def _purging_legacy_format(session: Session) -> bool:
    """Check if there are any legacy event_id linked states rows remaining."""
    return bool(session.execute(find_legacy_row()).scalar())


def _purge_legacy_format(
    instance: Recorder, session: Session, purge_before: datetime
) -> bool:
    """Purge rows that are still linked by the event_ids."""
    (
        event_ids,
        state_ids,
        attributes_ids,
        data_ids,
    ) = _select_legacy_event_state_and_attributes_and_data_ids_to_purge(
        session, purge_before, instance.max_bind_vars
    )
    _purge_state_ids(instance, session, state_ids)
    _purge_unused_attributes_ids(instance, session, attributes_ids)
    _purge_event_ids(session, event_ids)
    _purge_unused_data_ids(instance, session, data_ids)

    # The database may still have some rows that have an event_id but are not
    # linked to any event. These rows are not linked to any event because the
    # event was deleted. We need to purge these rows as well or we will never
    # switch to the new format which will prevent us from purging any events
    # that happened after the detached states.
    (
        detached_state_ids,
        detached_attributes_ids,
    ) = _select_legacy_detached_state_and_attributes_and_data_ids_to_purge(
        session, purge_before, instance.max_bind_vars
    )
    _purge_state_ids(instance, session, detached_state_ids)
    _purge_unused_attributes_ids(instance, session, detached_attributes_ids)
    return bool(
        event_ids
        or state_ids
        or attributes_ids
        or data_ids
        or detached_state_ids
        or detached_attributes_ids
    )


def _purge_states_and_attributes_ids(
    instance: Recorder,
    session: Session,
    states_batch_size: int,
    purge_before: datetime,
) -> bool:
    """Purge states and linked attributes id in a batch.

    Returns true if there are more states to purge.
    """
    database_engine = instance.database_engine
    assert database_engine is not None
    has_remaining_state_ids_to_purge = True
    # There are more states relative to attributes_ids so
    # we purge enough state_ids to try to generate a full
    # size batch of attributes_ids that will be around the size
    # max_bind_vars
    attributes_ids_batch: set[int] = set()
    max_bind_vars = instance.max_bind_vars
    for _ in range(states_batch_size):
        state_ids, attributes_ids = _select_state_attributes_ids_to_purge(
            session, purge_before, max_bind_vars
        )
        if not state_ids:
            has_remaining_state_ids_to_purge = False
            break
        _purge_state_ids(instance, session, state_ids)
        attributes_ids_batch = attributes_ids_batch | attributes_ids

    _purge_unused_attributes_ids(instance, session, attributes_ids_batch)
    _LOGGER.debug(
        "After purging states and attributes_ids remaining=%s",
        has_remaining_state_ids_to_purge,
    )
    return has_remaining_state_ids_to_purge


def _purge_events_and_data_ids(
    instance: Recorder,
    session: Session,
    events_batch_size: int,
    purge_before: datetime,
) -> bool:
    """Purge states and linked attributes id in a batch.

    Returns true if there are more states to purge.
    """
    has_remaining_event_ids_to_purge = True
    # There are more events relative to data_ids so
    # we purge enough event_ids to try to generate a full
    # size batch of data_ids that will be around the size
    # max_bind_vars
    data_ids_batch: set[int] = set()
    max_bind_vars = instance.max_bind_vars
    for _ in range(events_batch_size):
        event_ids, data_ids = _select_event_data_ids_to_purge(
            session, purge_before, max_bind_vars
        )
        if not event_ids:
            has_remaining_event_ids_to_purge = False
            break
        _purge_event_ids(session, event_ids)
        data_ids_batch = data_ids_batch | data_ids

    _purge_unused_data_ids(instance, session, data_ids_batch)
    _LOGGER.debug(
        "After purging event and data_ids remaining=%s",
        has_remaining_event_ids_to_purge,
    )
    return has_remaining_event_ids_to_purge


def _select_state_attributes_ids_to_purge(
    session: Session, purge_before: datetime, max_bind_vars: int
) -> tuple[set[int], set[int]]:
    """Return sets of state and attribute ids to purge."""
    state_ids = set()
    attributes_ids = set()
    for state_id, attributes_id in session.execute(
        find_states_to_purge(purge_before.timestamp(), max_bind_vars)
    ).all():
        state_ids.add(state_id)
        if attributes_id:
            attributes_ids.add(attributes_id)
    _LOGGER.debug(
        "Selected %s state ids and %s attributes_ids to remove",
        len(state_ids),
        len(attributes_ids),
    )
    return state_ids, attributes_ids


def _select_event_data_ids_to_purge(
    session: Session, purge_before: datetime, max_bind_vars: int
) -> tuple[set[int], set[int]]:
    """Return sets of event and data ids to purge."""
    event_ids = set()
    data_ids = set()
    for event_id, data_id in session.execute(
        find_events_to_purge(purge_before.timestamp(), max_bind_vars)
    ).all():
        event_ids.add(event_id)
        if data_id:
            data_ids.add(data_id)
    _LOGGER.debug(
        "Selected %s event ids and %s data_ids to remove", len(event_ids), len(data_ids)
    )
    return event_ids, data_ids


def _select_unused_attributes_ids(
    instance: Recorder,
    session: Session,
    attributes_ids: set[int],
    database_engine: DatabaseEngine,
) -> set[int]:
    """Return a set of attributes ids that are not used by any states in the db."""
    if not attributes_ids:
        return set()

    seen_ids: set[int] = set()
    if not database_engine.optimizer.slow_range_in_select:
        query = attributes_ids_exist_in_states_with_fast_in_distinct
        # SQLite has a superior query optimizer for the distinct query below as it uses
        # the covering index without having to examine the rows directly for both of the
        # queries below.
    else:
        query = attributes_ids_exist_in_states
        # This branch is for DBMS that cannot optimize the distinct query well and has
        # to examine all the rows that match.
    for attributes_ids_chunk in chunked_or_all(attributes_ids, instance.max_bind_vars):
        seen_ids.update(
            state[0] for state in session.execute(query(attributes_ids_chunk)).all()
        )
    to_remove = attributes_ids - seen_ids
    _LOGGER.debug(
        "Selected %s shared attributes to remove",
        len(to_remove),
    )
    return to_remove


def _purge_unused_attributes_ids(
    instance: Recorder,
    session: Session,
    attributes_ids_batch: set[int],
) -> None:
    """Purge unused attributes ids."""
    database_engine = instance.database_engine
    assert database_engine is not None
    if unused_attribute_ids_set := _select_unused_attributes_ids(
        instance, session, attributes_ids_batch, database_engine
    ):
        _purge_batch_attributes_ids(instance, session, unused_attribute_ids_set)


def _select_unused_event_data_ids(
    instance: Recorder,
    session: Session,
    data_ids: set[int],
    database_engine: DatabaseEngine,
) -> set[int]:
    """Return a set of event data ids that are not used by any events in the db."""
    if not data_ids:
        return set()

    seen_ids: set[int] = set()
    # See _select_unused_attributes_ids for why this function
    # branches for non-sqlite databases.
    if not database_engine.optimizer.slow_range_in_select:
        query = data_ids_exist_in_events_with_fast_in_distinct
    else:
        query = data_ids_exist_in_events
    for data_ids_chunk in chunked_or_all(data_ids, instance.max_bind_vars):
        seen_ids.update(
            state[0] for state in session.execute(query(data_ids_chunk)).all()
        )
    to_remove = data_ids - seen_ids
    _LOGGER.debug("Selected %s shared event data to remove", len(to_remove))
    return to_remove


def _purge_unused_data_ids(
    instance: Recorder, session: Session, data_ids_batch: set[int]
) -> None:
    database_engine = instance.database_engine
    assert database_engine is not None
    if unused_data_ids_set := _select_unused_event_data_ids(
        instance, session, data_ids_batch, database_engine
    ):
        _purge_batch_data_ids(instance, session, unused_data_ids_set)


def _select_statistics_runs_to_purge(
    session: Session, purge_before: datetime, max_bind_vars: int
) -> tuple[list[int], bool]:
    """Return a list of statistic runs to purge and if there may be more.

    Takes care to keep the newest run.
    """
    statistic_runs = session.execute(
        find_statistics_runs_to_purge(purge_before, max_bind_vars)
    ).all()
    # Only a full batch can have more rows to purge
    has_more = len(statistic_runs) == max_bind_vars
    statistic_runs_list = [run_id for (run_id,) in statistic_runs]
    # Exclude the newest statistics run
    if (
        last_run := session.execute(find_latest_statistics_runs_run_id()).scalar()
    ) and last_run in statistic_runs_list:
        statistic_runs_list.remove(last_run)

    _LOGGER.debug("Selected %s statistic runs to remove", len(statistic_runs))
    return statistic_runs_list, has_more


def _select_short_term_statistics_to_purge(
    session: Session, purge_before: datetime, max_bind_vars: int
) -> tuple[list[int], bool]:
    """Return a list of short term statistics to purge and if there may be more."""
    statistics = session.execute(
        find_short_term_statistics_to_purge(purge_before, max_bind_vars)
    ).all()
    _LOGGER.debug("Selected %s short term statistics to remove", len(statistics))
    # Only a full batch can have more rows to purge
    has_more = len(statistics) == max_bind_vars
    return [statistic_id for (statistic_id,) in statistics], has_more


def _select_legacy_detached_state_and_attributes_and_data_ids_to_purge(
    session: Session, purge_before: datetime, max_bind_vars: int
) -> tuple[set[int], set[int]]:
    """Return a list of state, and attribute ids to purge.

    We do not link these anymore since state_change events
    do not exist in the events table anymore, however we
    still need to be able to purge them.
    """
    states = session.execute(
        find_legacy_detached_states_and_attributes_to_purge(
            purge_before.timestamp(), max_bind_vars
        )
    ).all()
    _LOGGER.debug("Selected %s state ids to remove", len(states))
    state_ids = set()
    attributes_ids = set()
    for state_id, attributes_id in states:
        if state_id:
            state_ids.add(state_id)
        if attributes_id:
            attributes_ids.add(attributes_id)
    return state_ids, attributes_ids


def _select_legacy_event_state_and_attributes_and_data_ids_to_purge(
    session: Session, purge_before: datetime, max_bind_vars: int
) -> tuple[set[int], set[int], set[int], set[int]]:
    """Return a list of event, state, and attribute ids to purge linked by the event_id.

    We do not link these anymore since state_change events
    do not exist in the events table anymore, however we
    still need to be able to purge them.
    """
    events = session.execute(
        find_legacy_event_state_and_attributes_and_data_ids_to_purge(
            purge_before.timestamp(), max_bind_vars
        )
    ).all()
    _LOGGER.debug("Selected %s event ids to remove", len(events))
    event_ids = set()
    state_ids = set()
    attributes_ids = set()
    data_ids = set()
    for event_id, data_id, state_id, attributes_id in events:
        event_ids.add(event_id)
        if state_id:
            state_ids.add(state_id)
        if attributes_id:
            attributes_ids.add(attributes_id)
        if data_id:
            data_ids.add(data_id)
    return event_ids, state_ids, attributes_ids, data_ids


def _purge_state_ids(instance: Recorder, session: Session, state_ids: set[int]) -> int:
    """Disconnect states and delete by state id."""
    if not state_ids:
        return 0

    # Update old_state_id to NULL before deleting to ensure
    # the delete does not fail due to a foreign key constraint
    # since some databases (MSSQL) cannot do the ON DELETE SET NULL
    # for us.
    disconnected_rows = session.execute(disconnect_states_rows(state_ids))
    _LOGGER.debug("Updated %s states to remove old_state_id", disconnected_rows)

    result = session.connection().execute(delete_states_rows(state_ids))
    _LOGGER.debug("Deleted %s states", result.rowcount)

    # Evict eny entries in the old_states cache referring to a purged state
    instance.states_manager.evict_purged_state_ids(state_ids)
    return result.rowcount


def _purge_batch_attributes_ids(
    instance: Recorder, session: Session, attributes_ids: set[int]
) -> None:
    """Delete old attributes ids in batches of max_bind_vars."""
    for attributes_ids_chunk in chunked_or_all(attributes_ids, instance.max_bind_vars):
        deleted_rows = session.execute(
            delete_states_attributes_rows(attributes_ids_chunk)
        )
        _LOGGER.debug("Deleted %s attribute states", deleted_rows)

    # Evict any entries in the state_attributes_ids cache referring to a purged state
    instance.state_attributes_manager.evict_purged(attributes_ids)


def _purge_batch_data_ids(
    instance: Recorder, session: Session, data_ids: set[int]
) -> None:
    """Delete old event data ids in batches of max_bind_vars."""
    for data_ids_chunk in chunked_or_all(data_ids, instance.max_bind_vars):
        deleted_rows = session.execute(delete_event_data_rows(data_ids_chunk))
        _LOGGER.debug("Deleted %s data events", deleted_rows)

    # Evict any entries in the event_data_ids cache referring to a purged state
    instance.event_data_manager.evict_purged(data_ids)


def _purge_statistics_runs(session: Session, statistics_runs: list[int]) -> None:
    """Delete by run_id."""
    deleted_rows = session.execute(delete_statistics_runs_rows(statistics_runs))
    _LOGGER.debug("Deleted %s statistic runs", deleted_rows)


def _purge_short_term_statistics(
    session: Session, short_term_statistics: list[int]
) -> None:
    """Delete by id."""
    deleted_rows = session.execute(
        delete_statistics_short_term_rows(short_term_statistics)
    )
    _LOGGER.debug("Deleted %s short term statistics", deleted_rows)


def _purge_event_ids(session: Session, event_ids: set[int]) -> int:
    """Delete by event id."""
    if not event_ids:
        return 0
    result = session.connection().execute(delete_event_rows(event_ids))
    _LOGGER.debug("Deleted %s events", result.rowcount)
    return result.rowcount


def _purge_old_recorder_runs(
    instance: Recorder, session: Session, purge_before: datetime
) -> None:
    """Purge all old recorder runs."""
    # Recorder runs is small, no need to batch run it
    deleted_rows = session.execute(
        delete_recorder_runs_rows(
            purge_before, instance.recorder_runs_manager.current.run_id
        )
    )
    _LOGGER.debug("Deleted %s recorder_runs", deleted_rows)


def _purge_old_event_types(instance: Recorder, session: Session) -> None:
    """Purge all old event types."""
    # Event types is small, no need to batch run it
    purge_event_types = set()
    event_type_ids = set()
    for event_type_id, event_type in session.execute(find_event_types_to_purge()):
        purge_event_types.add(event_type)
        event_type_ids.add(event_type_id)

    if not event_type_ids:
        return

    deleted_rows = session.execute(delete_event_types_rows(event_type_ids))
    _LOGGER.debug("Deleted %s event types", deleted_rows)

    # Evict any entries in the event_type cache referring to a purged state
    instance.event_type_manager.evict_purged(purge_event_types)


def _purge_old_entity_ids(instance: Recorder, session: Session) -> None:
    """Purge all old entity_ids."""
    # entity_ids are small, no need to batch run it
    purge_entity_ids = set()
    states_metadata_ids = set()
    for metadata_id, entity_id in session.execute(find_entity_ids_to_purge()):
        purge_entity_ids.add(entity_id)
        states_metadata_ids.add(metadata_id)

    if not states_metadata_ids:
        return

    deleted_rows = session.execute(delete_states_meta_rows(states_metadata_ids))
    _LOGGER.debug("Deleted %s states meta", deleted_rows)

    # Evict any entries in the event_type cache referring to a purged state
    instance.states_meta_manager.evict_purged(purge_entity_ids)
    instance.states_manager.evict_purged_entity_ids(purge_entity_ids)


def _purge_filtered_data(instance: Recorder, session: Session) -> bool:
    """Remove filtered states and events that shouldn't be in the database.

    Returns true if all states and events are purged.
    """
    _LOGGER.debug("Cleanup filtered data")
    database_engine = instance.database_engine
    assert database_engine is not None
    now_timestamp = time.time()

    # Check if excluded entity_ids are in database
    entity_filter = instance.entity_filter
    has_more_to_purge = False
    excluded_metadata_ids: list[str] = [
        metadata_id
        for (metadata_id, entity_id) in session.query(
            StatesMeta.metadata_id, StatesMeta.entity_id
        ).all()
        if entity_filter and not entity_filter(entity_id)
    ]
    if excluded_metadata_ids:
        has_more_to_purge |= not _purge_filtered_states(
            instance, session, excluded_metadata_ids, database_engine, now_timestamp
        )

    # Check if excluded event_types are in database
    if (
        event_type_to_event_type_ids := instance.event_type_manager.get_many(
            instance.exclude_event_types, session
        )
    ) and (
        excluded_event_type_ids := [
            event_type_id
            for event_type_id in event_type_to_event_type_ids.values()
            if event_type_id is not None
        ]
    ):
        has_more_to_purge |= not _purge_filtered_events(
            instance, session, excluded_event_type_ids, now_timestamp
        )

    # Purge has completed if there are not more state or events to purge
    return not has_more_to_purge


def _purge_filtered_states(
    instance: Recorder,
    session: Session,
    metadata_ids_to_purge: list[str],
    database_engine: DatabaseEngine,
    purge_before_timestamp: float,
) -> bool:
    """Remove filtered states and linked events.

    Return true if all states are purged
    """
    state_ids: tuple[int, ...]
    attributes_ids: tuple[int, ...]
    event_ids: tuple[int, ...]
    to_purge = list(
        session.query(States.state_id, States.attributes_id, States.event_id)
        .filter(States.metadata_id.in_(metadata_ids_to_purge))
        .filter(States.last_updated_ts < purge_before_timestamp)
        .limit(instance.max_bind_vars)
        .all()
    )
    if not to_purge:
        return True
    state_ids, attributes_ids, event_ids = zip(*to_purge, strict=False)
    filtered_event_ids = {id_ for id_ in event_ids if id_ is not None}
    _LOGGER.debug(
        "Selected %s state_ids to remove that should be filtered", len(state_ids)
    )
    _purge_state_ids(instance, session, set(state_ids))
    # These are legacy events that are linked to a state that are no longer
    # created but since we did not remove them when we stopped adding new ones
    # we will need to purge them here.
    _purge_event_ids(session, filtered_event_ids)
    unused_attribute_ids_set = _select_unused_attributes_ids(
        instance,
        session,
        {id_ for id_ in attributes_ids if id_ is not None},
        database_engine,
    )
    _purge_batch_attributes_ids(instance, session, unused_attribute_ids_set)
    return False


def _purge_filtered_events(
    instance: Recorder,
    session: Session,
    excluded_event_type_ids: list[int],
    purge_before_timestamp: float,
) -> bool:
    """Remove filtered events and linked states.

    Return true if all events are purged.
    """
    database_engine = instance.database_engine
    assert database_engine is not None
    to_purge = list(
        session.query(Events.event_id, Events.data_id)
        .filter(Events.event_type_id.in_(excluded_event_type_ids))
        .filter(Events.time_fired_ts < purge_before_timestamp)
        .limit(instance.max_bind_vars)
        .all()
    )
    if not to_purge:
        return True
    event_ids, data_ids = zip(*to_purge, strict=False)
    event_ids_set = set(event_ids)
    _LOGGER.debug(
        "Selected %s event_ids to remove that should be filtered", len(event_ids_set)
    )
    if (
        instance.use_legacy_events_index
        and (
            states := session.query(States.state_id)
            .filter(States.event_id.in_(event_ids_set))
            .all()
        )
        and (state_ids := {state_id for (state_id,) in states})
    ):
        # These are legacy states that are linked to an event that are no longer
        # created but since we did not remove them when we stopped adding new ones
        # we will need to purge them here.
        _purge_state_ids(instance, session, state_ids)
    _purge_event_ids(session, event_ids_set)
    if unused_data_ids_set := _select_unused_event_data_ids(
        instance, session, set(data_ids), database_engine
    ):
        _purge_batch_data_ids(instance, session, unused_data_ids_set)
    return False


@retryable_database_job("purge_entity_data")
def purge_entity_data(
    instance: Recorder,
    entity_filter: Callable[[str], bool] | None,
    purge_before: datetime,
) -> bool:
    """Purge states and events of specified entities."""
    database_engine = instance.database_engine
    assert database_engine is not None
    purge_before_timestamp = purge_before.timestamp()
    with session_scope(session=instance.get_session()) as session:
        selected_metadata_ids: list[str] = [
            metadata_id
            for (metadata_id, entity_id) in session.query(
                StatesMeta.metadata_id, StatesMeta.entity_id
            ).all()
            if entity_filter and entity_filter(entity_id)
        ]
        _LOGGER.debug("Purging entity data for %s", selected_metadata_ids)
        if not selected_metadata_ids:
            return True

        # Purge a max of max_bind_vars, based on the oldest states
        # or events record.
        if not _purge_filtered_states(
            instance,
            session,
            selected_metadata_ids,
            database_engine,
            purge_before_timestamp,
        ):
            _LOGGER.debug("Purging entity data hasn't fully completed yet")
            return False

        _purge_old_entity_ids(instance, session)

    return True
