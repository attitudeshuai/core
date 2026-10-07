"""Find and clean up broken references between the registries.

The entity, device, area and floor registries and the config entries reference
each other:

- an area points at its temperature and humidity entities and at a floor,
- an entity points at a device and carries labels,
- a device (main device or child device) points at its config entry and area,
  and carries labels.

The deletion paths clear these references in a targeted way, for example removing
a config entry removes its devices and entities, and removing an area clears the
area id on devices and entities. A reference can nevertheless be left behind if
a cleanup is interrupted or by older data. The temperature and humidity entity
references of an area are only validated when set or updated, so deleting the
referenced entity leaves an otherwise invisible dangling reference.

This module adds a read-only view of those references and an opt-in cleanup:

- :func:`async_check_broken_references` scans all the registries and the config
  entries and reports every reference that points at a deleted or missing
  object. The scan never writes to a registry or storage.
- :func:`async_get_object_references` reports the impact of deleting one entity,
  device, area or config entry: every reference that would be cleared.
- :func:`async_cleanup_broken_references` previews (by default) or executes the
  cleanup, processing one source object at a time in a configurable hierarchy
  order and within a per-run item limit.

A referenced object which is in a registry's deleted collection (a deleted
device or deleted entity kept for possible restoration) is reported separately
from one which no longer exists at all. Such references are not cleaned up, as
the referenced object may be restored with the same id.
"""

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal

import attr

from homeassistant.core import HomeAssistant, callback

from . import (
    area_registry as ar,
    device_registry as dr,
    entity_registry as er,
    floor_registry as fr,
    label_registry as lr,
)
from .registry import BaseRegistry

# Default maximum number of broken references cleaned up in one run. Reaching
# the limit ends the run with an explicit result; another run continues with
# the remaining references.
DEFAULT_CLEANUP_LIMIT = 1000


class ReferenceObjectType(StrEnum):
    """Object types involved in registry references."""

    AREA = "area"
    CONFIG_ENTRY = "config_entry"
    DEVICE = "device"
    ENTITY = "entity"
    FLOOR = "floor"
    LABEL = "label"


class ReferenceField(StrEnum):
    """Fields which can hold a reference to another registry object."""

    AREA_ID = "area_id"
    CONFIG_ENTRY_ID = "config_entry_id"
    DEVICE_ID = "device_id"
    FLOOR_ID = "floor_id"
    HUMIDITY_ENTITY_ID = "humidity_entity_id"
    LABELS = "labels"
    TEMPERATURE_ENTITY_ID = "temperature_entity_id"


class ReferenceTargetStatus(StrEnum):
    """Status of a referenced object.

    A broken reference points either at an object which is in a registry's
    deleted collection (kept for possible restoration) or at one which does not
    exist at all.
    """

    DELETED = "deleted"
    MISSING = "missing"


# Object types which hold outgoing references covered by the check, and the
# default order in which they are cleaned up, leaf objects first.
SOURCE_OBJECT_TYPES: tuple[ReferenceObjectType, ...] = (
    ReferenceObjectType.ENTITY,
    ReferenceObjectType.DEVICE,
    ReferenceObjectType.AREA,
)
DEFAULT_SOURCE_TYPE_ORDER = SOURCE_OBJECT_TYPES


@dataclass(frozen=True, kw_only=True, slots=True)
class BrokenReference:
    """A reference from a registry entry to a deleted or missing object."""

    source_type: ReferenceObjectType
    source_id: str
    field: ReferenceField
    target_type: ReferenceObjectType
    target_id: str
    target_status: ReferenceTargetStatus
    # True when the source is a child device. Only meaningful for device sources.
    is_child_device: bool = False


@dataclass(frozen=True, kw_only=True, slots=True)
class ObjectReference:
    """A reference from another registry entry to a given object.

    Returned by the impact view, grouped by the type of the source object.
    """

    source_id: str
    fields: tuple[ReferenceField, ...]
    # True when the source is a child device. Only meaningful for device sources.
    is_child_device: bool = False


@dataclass(frozen=True, kw_only=True, slots=True)
class ReferenceCleanupRecord:
    """One broken reference handled by a cleanup run.

    A record is either processed (``action`` describes what was done, or would
    be done in a preview) or skipped with the reason why it was left untouched.
    """

    reference: BrokenReference
    action: Literal["cleared_field", "removed_device"] | None
    reason: str | None


@dataclass(frozen=True, kw_only=True, slots=True)
class ReferenceCleanupResult:
    """Result of a preview or an executed cleanup run."""

    dry_run: bool
    processed: tuple[ReferenceCleanupRecord, ...]
    skipped: tuple[ReferenceCleanupRecord, ...]
    # Broken references still present after the run.
    remaining: int
    # True when the run ended because the per-run limit was reached.
    limit_reached: bool


@dataclass(slots=True)
class _Registries:
    """Loaded registries used by a scan or cleanup."""

    hass: HomeAssistant
    area: ar.AreaRegistry
    device: dr.DeviceRegistry
    entity: er.EntityRegistry
    floor: fr.FloorRegistry
    label: lr.LabelRegistry
    # Entity ids present in the entity registry deleted collection.
    deleted_entity_ids: set[str]


@callback
def _get_registries(hass: HomeAssistant) -> _Registries:
    """Get the registries covered by the check."""
    entity_registry = er.async_get(hass)
    return _Registries(
        hass=hass,
        area=ar.async_get(hass),
        device=dr.async_get(hass),
        entity=entity_registry,
        floor=fr.async_get(hass),
        label=lr.async_get(hass),
        deleted_entity_ids={
            deleted_entity.entity_id
            for deleted_entity in entity_registry.deleted_entities.values()
        },
    )


@callback
def _entity_target_status(
    registries: _Registries, entity_id: str
) -> ReferenceTargetStatus | None:
    """Return the status of a referenced entity, or None when it exists."""
    if registries.entity.async_get(entity_id) is not None:
        return None
    if entity_id in registries.deleted_entity_ids:
        return ReferenceTargetStatus.DELETED
    return ReferenceTargetStatus.MISSING


@callback
def _device_target_status(
    registries: _Registries, device_id: str
) -> ReferenceTargetStatus | None:
    """Return the status of a referenced device, or None when it exists."""
    if registries.device.async_get(device_id) is not None:
        return None
    if device_id in registries.device._deleted_devices.data:  # noqa: SLF001
        return ReferenceTargetStatus.DELETED
    return ReferenceTargetStatus.MISSING


@callback
def _broken_reference_key(reference: BrokenReference) -> tuple[str, str, str, str]:
    """Sort key: source object type and id, then field and target id."""
    return (
        reference.source_type.value,
        reference.source_id,
        reference.field.value,
        reference.target_id,
    )


@callback
def async_check_broken_references(hass: HomeAssistant) -> list[BrokenReference]:
    """Scan the registries and config entries for broken references.

    Reports every reference pointing at a deleted or missing object. This is a
    read-only operation: no registry or storage is written, no state is changed
    and no registry events are fired. The result is sorted deterministically.
    """
    registries = _get_registries(hass)
    references: list[BrokenReference] = []

    for area in registries.area.async_list_areas():
        if area.temperature_entity_id is not None:
            status = _entity_target_status(registries, area.temperature_entity_id)
            if status is not None:
                references.append(
                    BrokenReference(
                        source_type=ReferenceObjectType.AREA,
                        source_id=area.id,
                        field=ReferenceField.TEMPERATURE_ENTITY_ID,
                        target_type=ReferenceObjectType.ENTITY,
                        target_id=area.temperature_entity_id,
                        target_status=status,
                    )
                )
        if area.humidity_entity_id is not None:
            status = _entity_target_status(registries, area.humidity_entity_id)
            if status is not None:
                references.append(
                    BrokenReference(
                        source_type=ReferenceObjectType.AREA,
                        source_id=area.id,
                        field=ReferenceField.HUMIDITY_ENTITY_ID,
                        target_type=ReferenceObjectType.ENTITY,
                        target_id=area.humidity_entity_id,
                        target_status=status,
                    )
                )
        if area.floor_id is not None and (
            registries.floor.async_get_floor(area.floor_id) is None
        ):
            # Floors have no deleted collection, a missing floor is gone.
            references.append(
                BrokenReference(
                    source_type=ReferenceObjectType.AREA,
                    source_id=area.id,
                    field=ReferenceField.FLOOR_ID,
                    target_type=ReferenceObjectType.FLOOR,
                    target_id=area.floor_id,
                    target_status=ReferenceTargetStatus.MISSING,
                )
            )

    for entity in registries.entity.entities.values():
        if entity.device_id is not None:
            status = _device_target_status(registries, entity.device_id)
            if status is not None:
                references.append(
                    BrokenReference(
                        source_type=ReferenceObjectType.ENTITY,
                        source_id=entity.entity_id,
                        field=ReferenceField.DEVICE_ID,
                        target_type=ReferenceObjectType.DEVICE,
                        target_id=entity.device_id,
                        target_status=status,
                    )
                )
        references.extend(
            BrokenReference(
                source_type=ReferenceObjectType.ENTITY,
                source_id=entity.entity_id,
                field=ReferenceField.LABELS,
                target_type=ReferenceObjectType.LABEL,
                target_id=label_id,
                target_status=ReferenceTargetStatus.MISSING,
            )
            for label_id in entity.labels
            if registries.label.async_get_label(label_id) is None
        )

    for device in registries.device._devices.values():  # noqa: SLF001
        _scan_device_references(registries, references, device, False)
    for child_device in registries.device._child_devices.values():  # noqa: SLF001
        _scan_device_references(registries, references, child_device, True)

    references.sort(key=_broken_reference_key)
    return references


@callback
def _scan_device_references(
    registries: _Registries,
    references: list[BrokenReference],
    device: dr.AnyDeviceEntry,
    is_child_device: bool,
) -> None:
    """Add broken references held by a main or child device."""
    if device.config_entry_id is not None and (
        registries.hass.config_entries.async_get_entry(device.config_entry_id) is None
    ):
        # Config entries have no deleted collection; only live devices are
        # scanned as sources, so this cannot match a deleted-device tombstone.
        references.append(
            BrokenReference(
                source_type=ReferenceObjectType.DEVICE,
                source_id=device.id,
                field=ReferenceField.CONFIG_ENTRY_ID,
                target_type=ReferenceObjectType.CONFIG_ENTRY,
                target_id=device.config_entry_id,
                target_status=ReferenceTargetStatus.MISSING,
                is_child_device=is_child_device,
            )
        )
    if device.area_id is not None and (
        registries.area.async_get_area(device.area_id) is None
    ):
        references.append(
            BrokenReference(
                source_type=ReferenceObjectType.DEVICE,
                source_id=device.id,
                field=ReferenceField.AREA_ID,
                target_type=ReferenceObjectType.AREA,
                target_id=device.area_id,
                target_status=ReferenceTargetStatus.MISSING,
                is_child_device=is_child_device,
            )
        )
    references.extend(
        BrokenReference(
            source_type=ReferenceObjectType.DEVICE,
            source_id=device.id,
            field=ReferenceField.LABELS,
            target_type=ReferenceObjectType.LABEL,
            target_id=label_id,
            target_status=ReferenceTargetStatus.MISSING,
            is_child_device=is_child_device,
        )
        for label_id in device.labels
        if registries.label.async_get_label(label_id) is None
    )


@callback
def async_get_object_references(
    hass: HomeAssistant,
    object_type: ReferenceObjectType | str,
    object_id: str,
) -> dict[ReferenceObjectType, list[ObjectReference]]:
    """Report the references cleared when an object is deleted.

    Takes an entity, device, area or config entry id and returns all current
    references pointing at it, grouped by the type of the referencing object.
    Raises ValueError when the object type is not supported or the object does
    not exist.
    """
    try:
        target_type = ReferenceObjectType(object_type)
    except ValueError as error:
        raise ValueError(f"Unknown object type {object_type!r}") from error

    registries = _get_registries(hass)
    grouped: dict[ReferenceObjectType, list[ObjectReference]] = defaultdict(list)

    if target_type is ReferenceObjectType.ENTITY:
        if registries.entity.async_get(object_id) is None:
            raise ValueError(f"Unknown entity {object_id!r}")
        for area in registries.area.async_list_areas():
            fields: list[ReferenceField] = []
            if area.temperature_entity_id == object_id:
                fields.append(ReferenceField.TEMPERATURE_ENTITY_ID)
            if area.humidity_entity_id == object_id:
                fields.append(ReferenceField.HUMIDITY_ENTITY_ID)
            if fields:
                grouped[ReferenceObjectType.AREA].append(
                    ObjectReference(source_id=area.id, fields=tuple(fields))
                )

    elif target_type is ReferenceObjectType.DEVICE:
        if registries.device.async_get(object_id) is None:
            raise ValueError(f"Unknown device {object_id!r}")
        for entity in registries.entity.entities.get_entries_for_device_id(
            object_id, include_disabled_entities=True
        ):
            grouped[ReferenceObjectType.ENTITY].append(
                ObjectReference(
                    source_id=entity.entity_id,
                    fields=(ReferenceField.DEVICE_ID,),
                )
            )

    elif target_type is ReferenceObjectType.AREA:
        if registries.area.async_get_area(object_id) is None:
            raise ValueError(f"Unknown area {object_id!r}")
        for device in registries.device._devices.get_devices_for_area_id(  # noqa: SLF001
            object_id
        ):
            grouped[ReferenceObjectType.DEVICE].append(
                ObjectReference(
                    source_id=device.id,
                    fields=(ReferenceField.AREA_ID,),
                )
            )
        for child_device in registries.device._child_devices.get_devices_for_area_id(  # noqa: SLF001
            object_id
        ):
            grouped[ReferenceObjectType.DEVICE].append(
                ObjectReference(
                    source_id=child_device.id,
                    fields=(ReferenceField.AREA_ID,),
                    is_child_device=True,
                )
            )

    elif target_type is ReferenceObjectType.CONFIG_ENTRY:
        if registries.hass.config_entries.async_get_entry(object_id) is None:
            raise ValueError(f"Unknown config entry {object_id!r}")
        for device in registries.device._devices.get_devices_for_config_entry_id(  # noqa: SLF001
            object_id
        ):
            grouped[ReferenceObjectType.DEVICE].append(
                ObjectReference(
                    source_id=device.id,
                    fields=(ReferenceField.CONFIG_ENTRY_ID,),
                )
            )
        for (
            child_device
        ) in registries.device._child_devices.get_devices_for_config_entry_id(  # noqa: SLF001
            object_id
        ):
            grouped[ReferenceObjectType.DEVICE].append(
                ObjectReference(
                    source_id=child_device.id,
                    fields=(ReferenceField.CONFIG_ENTRY_ID,),
                    is_child_device=True,
                )
            )

    else:
        raise ValueError(
            f"Impact view is not supported for object type {target_type.value!r}"
        )

    for references in grouped.values():
        references.sort(key=lambda reference: reference.source_id)
    return {
        object_type_: grouped[object_type_]
        for object_type_ in sorted(grouped, key=lambda entry: entry.value)
    }


@dataclass(slots=True)
class _Plan:
    """Decisions for the broken references of one source object."""

    processed: list[ReferenceCleanupRecord] = field(default_factory=list)
    skipped: list[ReferenceCleanupRecord] = field(default_factory=list)


def _processed(
    reference: BrokenReference,
    action: Literal["cleared_field", "removed_device"] = "cleared_field",
) -> ReferenceCleanupRecord:
    """Build a processed cleanup record."""
    return ReferenceCleanupRecord(reference=reference, action=action, reason=None)


def _skipped(reference: BrokenReference, reason: str) -> ReferenceCleanupRecord:
    """Build a skipped cleanup record."""
    return ReferenceCleanupRecord(reference=reference, action=None, reason=reason)


def _deleted_target_reason(reference: BrokenReference) -> str:
    """Reason for not cleaning a reference to an object in a deleted collection."""
    return (
        f"the referenced {reference.target_type.value} {reference.target_id!r} is "
        "kept in the deleted registry collection for possible restoration"
    )


def _recreated_target_reason(reference: BrokenReference) -> str:
    """Reason for skipping a reference whose target was re-created."""
    return (
        f"the referenced {reference.target_type.value} {reference.target_id!r} "
        "has been re-created"
    )


def _changed_reference_reason(reference: BrokenReference) -> str:
    """Reason for skipping a reference no longer present in its field."""
    return (
        f"the {reference.field.value} of the "
        f"{reference.source_type.value} {reference.source_id!r} no longer "
        f"references {reference.target_id!r}"
    )


@callback
def _plan_entity_group(
    registries: _Registries,
    references_: list[BrokenReference],
    *,
    apply: bool,
) -> _Plan:
    """Plan or apply the cleanup of one entity's broken references."""
    plan = _Plan()
    source_id = references_[0].source_id
    entity = registries.entity.async_get(source_id)
    if entity is None:
        reason = f"the source entity {source_id!r} no longer exists"
        plan.skipped = [_skipped(reference, reason) for reference in references_]
        return plan

    clear_device_id = False
    drop_labels: set[str] = set()
    for reference in references_:
        if reference.field is ReferenceField.DEVICE_ID:
            if entity.device_id != reference.target_id:
                plan.skipped.append(
                    _skipped(reference, _changed_reference_reason(reference))
                )
                continue
            status = _device_target_status(registries, reference.target_id)
            if status is None:
                plan.skipped.append(
                    _skipped(reference, _recreated_target_reason(reference))
                )
                continue
            if status is ReferenceTargetStatus.DELETED:
                plan.skipped.append(
                    _skipped(reference, _deleted_target_reason(reference))
                )
                continue
            clear_device_id = True
            plan.processed.append(_processed(reference))
        else:  # ReferenceField.LABELS
            if reference.target_id not in entity.labels:
                plan.skipped.append(
                    _skipped(reference, _changed_reference_reason(reference))
                )
                continue
            if registries.label.async_get_label(reference.target_id) is not None:
                plan.skipped.append(
                    _skipped(reference, _recreated_target_reason(reference))
                )
                continue
            drop_labels.add(reference.target_id)
            plan.processed.append(_processed(reference))

    if apply and (clear_device_id or drop_labels):
        updates: dict[str, Any] = {}
        if clear_device_id:
            updates["device_id"] = None
        if drop_labels:
            updates["labels"] = entity.labels - drop_labels
        registries.entity.async_update_entity(source_id, **updates)
    return plan


@callback
def _plan_device_group(
    registries: _Registries,
    references_: list[BrokenReference],
    *,
    apply: bool,
) -> _Plan:
    """Plan or apply the cleanup of one device's broken references."""
    plan = _Plan()
    source_id = references_[0].source_id
    device = registries.device.async_get(source_id)
    if device is None:
        reason = f"the source device {source_id!r} no longer exists"
        plan.skipped = [_skipped(reference, reason) for reference in references_]
        return plan

    is_child_device = isinstance(device, dr.ChildDeviceEntry)
    remove_config_entry = False
    clear_area_id = False
    drop_labels: set[str] = set()
    for reference in references_:
        if reference.field is ReferenceField.CONFIG_ENTRY_ID:
            if device.config_entry_id != reference.target_id:
                plan.skipped.append(
                    _skipped(reference, _changed_reference_reason(reference))
                )
            elif (
                registries.hass.config_entries.async_get_entry(reference.target_id)
                is not None
            ):
                plan.skipped.append(
                    _skipped(reference, _recreated_target_reason(reference))
                )
            else:
                remove_config_entry = True
                plan.processed.append(_processed(reference, "removed_device"))
        elif reference.field is ReferenceField.AREA_ID:
            if device.area_id != reference.target_id:
                plan.skipped.append(
                    _skipped(reference, _changed_reference_reason(reference))
                )
            elif registries.area.async_get_area(reference.target_id) is not None:
                plan.skipped.append(
                    _skipped(reference, _recreated_target_reason(reference))
                )
            else:
                clear_area_id = True
                plan.processed.append(_processed(reference))
        elif reference.target_id not in device.labels:
            plan.skipped.append(
                _skipped(reference, _changed_reference_reason(reference))
            )
        elif registries.label.async_get_label(reference.target_id) is not None:
            plan.skipped.append(
                _skipped(reference, _recreated_target_reason(reference))
            )
        else:  # ReferenceField.LABELS
            drop_labels.add(reference.target_id)
            plan.processed.append(_processed(reference))

    if not apply or not plan.processed:
        return plan

    if remove_config_entry:
        _remove_device_and_clear_other_references(
            registries,
            source_id,
            device,
            clear_area_id=clear_area_id,
            drop_labels=drop_labels,
        )
        return plan

    updates: dict[str, Any] = {}
    if clear_area_id:
        updates["area_id"] = None
    if drop_labels:
        updates["labels"] = device.labels - drop_labels
    if updates:
        if is_child_device:
            registries.device._async_update_child_device(  # noqa: SLF001
                source_id, **updates
            )
        else:
            registries.device._async_update_device(  # noqa: SLF001
                source_id, **updates
            )
    return plan


@callback
def _remove_device_and_clear_other_references(
    registries: _Registries,
    source_id: str,
    device: dr.AnyDeviceEntry,
    *,
    clear_area_id: bool,
    drop_labels: set[str],
) -> None:
    """Remove a device whose config entry is gone and clear its other refs.

    This mirrors device_registry.async_cleanup: the device is removed and lands
    in the deleted devices collection. The other broken references are then
    cleared either on the surviving device (if a pending move kept it alive) or
    on the deleted entry, the same way area and label removal already do.
    """
    if isinstance(device, dr.ChildDeviceEntry):
        registries.device.async_remove_device(source_id)
    else:
        registries.device._async_update_device(  # noqa: SLF001
            source_id, remove_config_entry_id=device.config_entry_id
        )
    deleted_device = registries.device._deleted_devices.data.get(  # noqa: SLF001
        source_id
    )
    if deleted_device is not None:
        deleted_updates: dict[str, Any] = {}
        if clear_area_id:
            deleted_updates["area_id"] = None
        if drop_labels:
            deleted_updates["labels"] = deleted_device.labels - drop_labels
        if deleted_updates:
            registries.device._deleted_devices[source_id] = attr.evolve(  # noqa: SLF001
                deleted_device, **deleted_updates
            )
            registries.device.async_schedule_save()
        return

    # The device survived (e.g. a pending move took it to another config entry);
    # clear its other broken references on the live device.
    survivor = registries.device.async_get(source_id)
    if survivor is None:
        return
    survivor_updates: dict[str, Any] = {}
    if clear_area_id:
        survivor_updates["area_id"] = None
    if drop_labels:
        survivor_updates["labels"] = survivor.labels - drop_labels
    if not survivor_updates:
        return
    if isinstance(survivor, dr.ChildDeviceEntry):
        registries.device._async_update_child_device(  # noqa: SLF001
            source_id, **survivor_updates
        )
    else:
        registries.device._async_update_device(  # noqa: SLF001
            source_id, **survivor_updates
        )


@callback
def _plan_area_group(
    registries: _Registries,
    references_: list[BrokenReference],
    *,
    apply: bool,
) -> _Plan:
    """Plan or apply the cleanup of one area's broken references."""
    plan = _Plan()
    source_id = references_[0].source_id
    area = registries.area.async_get_area(source_id)
    if area is None:
        reason = f"the source area {source_id!r} no longer exists"
        plan.skipped = [_skipped(reference, reason) for reference in references_]
        return plan

    updates: dict[str, Any] = {}
    for reference in references_:
        current_value = getattr(area, reference.field.value)
        if current_value != reference.target_id:
            plan.skipped.append(
                _skipped(reference, _changed_reference_reason(reference))
            )
            continue
        if reference.target_type is ReferenceObjectType.ENTITY:
            status = _entity_target_status(registries, reference.target_id)
            if status is None:
                plan.skipped.append(
                    _skipped(reference, _recreated_target_reason(reference))
                )
                continue
            if status is ReferenceTargetStatus.DELETED:
                plan.skipped.append(
                    _skipped(reference, _deleted_target_reason(reference))
                )
                continue
        elif registries.floor.async_get_floor(reference.target_id) is not None:
            plan.skipped.append(
                _skipped(reference, _recreated_target_reason(reference))
            )
            continue
        updates[reference.field.value] = None
        plan.processed.append(_processed(reference))

    if apply and updates:
        registries.area.async_update(source_id, **updates)
    return plan


_PLANNERS = {
    ReferenceObjectType.AREA: _plan_area_group,
    ReferenceObjectType.DEVICE: _plan_device_group,
    ReferenceObjectType.ENTITY: _plan_entity_group,
}


async def async_cleanup_broken_references(
    hass: HomeAssistant,
    *,
    confirmed: bool = False,
    limit: int = DEFAULT_CLEANUP_LIMIT,
    references: Sequence[BrokenReference] | None = None,
    source_type_order: Sequence[ReferenceObjectType] = DEFAULT_SOURCE_TYPE_ORDER,
) -> ReferenceCleanupResult:
    """Preview or execute the cleanup of broken registry references.

    Without ``confirmed`` the cleanup is a preview: it reports what would be
    processed and skipped without writing anything. With ``confirmed`` the
    references are actually cleared, one source object at a time, in the given
    hierarchy order.

    ``references`` is the set of broken references the caller has reviewed and
    confirmed (for example the result of an earlier check or preview). It is
    revalidated against the live registries before each write: a target which
    has been re-created, or a field which no longer holds the stale value, is a
    conflict and is reported as skipped instead of overwriting the new value.
    When ``references`` is not given, a fresh check is used.

    References to objects in a deleted registry collection are also skipped
    with a reason, as the object may be restored with the same id. At most
    ``limit`` references are processed; when the limit is reached the run ends
    with ``limit_reached`` set and another run continues with the remaining
    references. Processed changes are written to storage before the run
    returns, so an interrupted run is simply resumed: already cleared
    references no longer show up in the check.
    """
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
        raise ValueError("limit must be a non-negative integer")

    order = tuple(source_type_order)
    for source_type in order:
        if source_type not in SOURCE_OBJECT_TYPES:
            raise ValueError(f"Unsupported source object type {source_type!r}")
    if len(order) != len(set(order)):
        raise ValueError("source_type_order must not contain duplicates")

    registries = _get_registries(hass)
    broken = (
        list(references)
        if references is not None
        else async_check_broken_references(hass)
    )
    order_index = {source_type: index for index, source_type in enumerate(order)}

    # Group the references per source object, in the configured hierarchy
    # order and with source ids and fields sorted deterministically.
    grouped: dict[tuple[ReferenceObjectType, str], list[BrokenReference]] = {}
    group_order: list[tuple[ReferenceObjectType, str]] = []
    for reference in broken:
        if reference.source_type not in order_index:
            # Source type not part of this run; leave its references untouched.
            continue
        key = (reference.source_type, reference.source_id)
        if key not in grouped:
            grouped[key] = []
            group_order.append(key)
        grouped[key].append(reference)
    group_order.sort(key=lambda group_key: (order_index[group_key[0]], group_key[1]))
    for references_ in grouped.values():
        references_.sort(
            key=lambda reference: (reference.field.value, reference.target_id)
        )

    processed_records: list[ReferenceCleanupRecord] = []
    skipped_records: list[ReferenceCleanupRecord] = []
    processed_count = 0
    limit_reached = False
    modified_registries: set[BaseRegistry] = set()
    registries_by_type: dict[ReferenceObjectType, BaseRegistry] = {
        ReferenceObjectType.AREA: registries.area,
        ReferenceObjectType.DEVICE: registries.device,
        ReferenceObjectType.ENTITY: registries.entity,
    }

    for group_key in group_order:
        references_ = grouped[group_key]
        planner = _PLANNERS[group_key[0]]
        preview = planner(registries, references_, apply=False)
        processable = len(preview.processed)
        if processable > 0 and processed_count + processable > limit:
            if processed_count > 0 or limit == 0:
                # The whole source object would exceed the remaining limit;
                # end the run on this boundary instead of partially updating it.
                limit_reached = True
                break
            # The first object of the run alone exceeds the limit; process it
            # as a whole so repeated runs always make progress.
            limit_reached = True
        if not preview.processed:
            skipped_records.extend(preview.skipped)
            continue
        if confirmed:
            plan = planner(registries, references_, apply=True)
            modified_registries.add(registries_by_type[group_key[0]])
            if group_key[0] is ReferenceObjectType.DEVICE:
                # Removing a device updates entities through the normal event
                # listeners (they are removed or detached), so persist that
                # registry too.
                modified_registries.add(registries.entity)
        else:
            plan = preview
        processed_records.extend(plan.processed)
        skipped_records.extend(plan.skipped)
        processed_count += len(plan.processed)

    if confirmed:
        # Let registry events from the applied changes settle (e.g. removing a
        # device detaches its entities through the normal listeners) before the
        # final check, and persist the run so resuming after an interruption
        # does not redo already cleared references.
        if modified_registries:
            await registries.hass.async_block_till_done()
        for registry in modified_registries:
            await registry._store.async_save(  # noqa: SLF001
                registry._data_to_save()  # noqa: SLF001
            )
        remaining = len(async_check_broken_references(hass))
    else:
        remaining = len(broken) - processed_count

    return ReferenceCleanupResult(
        dry_run=not confirmed,
        processed=tuple(processed_records),
        skipped=tuple(skipped_records),
        remaining=remaining,
        limit_reached=limit_reached,
    )
