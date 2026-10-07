"""Tests for the registry reference check and cleanup."""

from collections.abc import Callable
import dataclasses
from typing import Any

import attr
import pytest

from homeassistant.core import HomeAssistant
from homeassistant.helpers import (
    area_registry as ar,
    device_registry as dr,
    entity_registry as er,
    floor_registry as fr,
    label_registry as lr,
    registry_cleanup as rc,
)
from homeassistant.helpers.registry import BaseRegistry

from tests.common import MockConfigEntry, async_capture_events

MISSING_AREA = "nonexistent-area"
MISSING_CONFIG_ENTRY = "nonexistent-config-entry"
MISSING_DEVICE = "nonexistent-device"
MISSING_FLOOR = "nonexistent-floor"
MISSING_LABEL = "nonexistent-label"
MISSING_TEMPERATURE_ENTITY = "sensor.nonexistent_temperature"
MISSING_HUMIDITY_ENTITY = "sensor.nonexistent_humidity"


def _inject_area_references(
    area_registry: ar.AreaRegistry, area: ar.AreaEntry, **changes: Any
) -> None:
    """Inject broken references on an area, bypassing the set-time validation."""
    area_registry.areas[area.id] = dataclasses.replace(area, **changes)


def _inject_entity_references(
    entity_registry: er.EntityRegistry, entity: er.RegistryEntry, **changes: Any
) -> None:
    """Inject broken references on an entity."""
    entity_registry.entities[entity.entity_id] = attr.evolve(entity, **changes)


def _inject_device_references(
    device_registry: dr.DeviceRegistry, device: dr.DeviceEntry, **changes: Any
) -> None:
    """Inject broken references on a main device."""
    device_registry._devices[device.id] = attr.evolve(device, **changes)


def _inject_child_device_references(
    device_registry: dr.DeviceRegistry,
    child_device: dr.ChildDeviceEntry,
    **changes: Any,
) -> None:
    """Inject broken references on a child device."""
    device_registry._child_devices[child_device.id] = attr.evolve(
        child_device, **changes
    )


def _reference_tuple(reference: rc.BrokenReference) -> tuple[Any, ...]:
    """Flatten a broken reference for easy assertions."""
    return (
        reference.source_type,
        reference.source_id,
        reference.field,
        reference.target_type,
        reference.target_id,
        reference.target_status,
        reference.is_child_device,
    )


async def test_check_no_broken_references(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
    floor_registry: fr.FloorRegistry,
    label_registry: lr.LabelRegistry,
) -> None:
    """A consistent set of registries has no broken references."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    floor = floor_registry.async_create("downstairs")
    label = label_registry.async_create("label")
    area = area_registry.async_create("living room", floor_id=floor.floor_id)
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", "device")},
    )
    device_registry._async_update_device(
        device.id, area_id=area.id, labels={label.label_id}
    )
    entity = entity_registry.async_get_or_create(
        "light",
        "test",
        "1",
        config_entry=config_entry,
        device_id=device.id,
    )
    entity_registry.async_update_entity(entity.entity_id, labels={label.label_id})

    assert rc.async_check_broken_references(hass) == []


async def test_check_finds_all_reference_categories(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """All covered reference categories are reported with their details."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    area = area_registry.async_create("area")
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", "parent")},
    )
    child_device = device_registry.async_get_or_create_child(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", "child")},
        parent_device_id=device.id,
    )
    entity = entity_registry.async_get_or_create("light", "test", "1")

    _inject_area_references(
        area_registry,
        area,
        floor_id=MISSING_FLOOR,
        temperature_entity_id=MISSING_TEMPERATURE_ENTITY,
        humidity_entity_id=MISSING_HUMIDITY_ENTITY,
    )
    _inject_entity_references(
        entity_registry,
        entity,
        device_id=MISSING_DEVICE,
        labels={MISSING_LABEL},
    )
    _inject_device_references(
        device_registry,
        device,
        config_entry_id=MISSING_CONFIG_ENTRY,
        area_id=MISSING_AREA,
        labels={MISSING_LABEL},
    )
    _inject_child_device_references(
        device_registry,
        child_device,
        config_entry_id=MISSING_CONFIG_ENTRY,
        area_id=MISSING_AREA,
        labels={MISSING_LABEL},
    )

    references = rc.async_check_broken_references(hass)

    assert {_reference_tuple(reference) for reference in references} == {
        (
            rc.ReferenceObjectType.AREA,
            area.id,
            rc.ReferenceField.HUMIDITY_ENTITY_ID,
            rc.ReferenceObjectType.ENTITY,
            MISSING_HUMIDITY_ENTITY,
            rc.ReferenceTargetStatus.MISSING,
            False,
        ),
        (
            rc.ReferenceObjectType.AREA,
            area.id,
            rc.ReferenceField.TEMPERATURE_ENTITY_ID,
            rc.ReferenceObjectType.ENTITY,
            MISSING_TEMPERATURE_ENTITY,
            rc.ReferenceTargetStatus.MISSING,
            False,
        ),
        (
            rc.ReferenceObjectType.AREA,
            area.id,
            rc.ReferenceField.FLOOR_ID,
            rc.ReferenceObjectType.FLOOR,
            MISSING_FLOOR,
            rc.ReferenceTargetStatus.MISSING,
            False,
        ),
        (
            rc.ReferenceObjectType.DEVICE,
            child_device.id,
            rc.ReferenceField.AREA_ID,
            rc.ReferenceObjectType.AREA,
            MISSING_AREA,
            rc.ReferenceTargetStatus.MISSING,
            True,
        ),
        (
            rc.ReferenceObjectType.DEVICE,
            child_device.id,
            rc.ReferenceField.CONFIG_ENTRY_ID,
            rc.ReferenceObjectType.CONFIG_ENTRY,
            MISSING_CONFIG_ENTRY,
            rc.ReferenceTargetStatus.MISSING,
            True,
        ),
        (
            rc.ReferenceObjectType.DEVICE,
            child_device.id,
            rc.ReferenceField.LABELS,
            rc.ReferenceObjectType.LABEL,
            MISSING_LABEL,
            rc.ReferenceTargetStatus.MISSING,
            True,
        ),
        (
            rc.ReferenceObjectType.DEVICE,
            device.id,
            rc.ReferenceField.AREA_ID,
            rc.ReferenceObjectType.AREA,
            MISSING_AREA,
            rc.ReferenceTargetStatus.MISSING,
            False,
        ),
        (
            rc.ReferenceObjectType.DEVICE,
            device.id,
            rc.ReferenceField.CONFIG_ENTRY_ID,
            rc.ReferenceObjectType.CONFIG_ENTRY,
            MISSING_CONFIG_ENTRY,
            rc.ReferenceTargetStatus.MISSING,
            False,
        ),
        (
            rc.ReferenceObjectType.DEVICE,
            device.id,
            rc.ReferenceField.LABELS,
            rc.ReferenceObjectType.LABEL,
            MISSING_LABEL,
            rc.ReferenceTargetStatus.MISSING,
            False,
        ),
        (
            rc.ReferenceObjectType.ENTITY,
            entity.entity_id,
            rc.ReferenceField.DEVICE_ID,
            rc.ReferenceObjectType.DEVICE,
            MISSING_DEVICE,
            rc.ReferenceTargetStatus.MISSING,
            False,
        ),
        (
            rc.ReferenceObjectType.ENTITY,
            entity.entity_id,
            rc.ReferenceField.LABELS,
            rc.ReferenceObjectType.LABEL,
            MISSING_LABEL,
            rc.ReferenceTargetStatus.MISSING,
            False,
        ),
    }


async def test_check_marks_targets_in_deleted_collections(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """References into the deleted collections are marked deleted, not missing."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", "device")},
    )
    device_id = device.id
    temperature_entity = entity_registry.async_get_or_create("sensor", "test", "temp")

    area = area_registry.async_create("area")
    entity = entity_registry.async_get_or_create("light", "test", "1")

    device_registry.async_remove_device(device_id)
    entity_registry.async_remove(temperature_entity.entity_id)

    _inject_area_references(
        area_registry, area, temperature_entity_id=temperature_entity.entity_id
    )
    _inject_entity_references(entity_registry, entity, device_id=device_id)

    references = rc.async_check_broken_references(hass)

    assert {(reference.field, reference.target_status) for reference in references} == {
        (
            rc.ReferenceField.TEMPERATURE_ENTITY_ID,
            rc.ReferenceTargetStatus.DELETED,
        ),
        (rc.ReferenceField.DEVICE_ID, rc.ReferenceTargetStatus.DELETED),
    }


async def test_check_is_sorted_and_read_only(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """The check result is stably sorted and triggers no registry changes."""
    area_1 = area_registry.async_create("area 1")
    area_2 = area_registry.async_create("area 2")
    entity_1 = entity_registry.async_get_or_create("light", "test", "1")
    entity_2 = entity_registry.async_get_or_create("light", "test", "2")

    _inject_area_references(area_registry, area_2, floor_id="ghost-floor-2")
    _inject_area_references(area_registry, area_1, floor_id="ghost-floor-1")
    _inject_entity_references(entity_registry, entity_2, device_id="ghost-device-2")
    _inject_entity_references(entity_registry, entity_1, device_id="ghost-device-1")

    area_events = async_capture_events(hass, ar.EVENT_AREA_REGISTRY_UPDATED)
    device_events = async_capture_events(hass, dr.EVENT_DEVICE_REGISTRY_UPDATED)
    entity_events = async_capture_events(hass, er.EVENT_ENTITY_REGISTRY_UPDATED)

    first = rc.async_check_broken_references(hass)
    second = rc.async_check_broken_references(hass)

    assert first == second
    assert [
        (reference.source_type, reference.source_id, reference.target_id)
        for reference in first
    ] == [
        (rc.ReferenceObjectType.AREA, area_1.id, "ghost-floor-1"),
        (rc.ReferenceObjectType.AREA, area_2.id, "ghost-floor-2"),
        (rc.ReferenceObjectType.ENTITY, entity_1.entity_id, "ghost-device-1"),
        (rc.ReferenceObjectType.ENTITY, entity_2.entity_id, "ghost-device-2"),
    ]
    assert not area_events
    assert not device_events
    assert not entity_events
    # The dangling values are still in place.
    unchanged_area = area_registry.async_get_area(area_1.id)
    assert unchanged_area is not None
    assert unchanged_area.floor_id == "ghost-floor-1"
    unchanged_entity = entity_registry.async_get(entity_1.entity_id)
    assert unchanged_entity is not None
    assert unchanged_entity.device_id == "ghost-device-1"


async def test_check_ignores_references_outside_the_covered_categories(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """References which are not part of the feature are not reported."""
    area = area_registry.async_create("area")
    entity = entity_registry.async_get_or_create("light", "test", "1")
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "device")}
    )

    # Area labels, entity areas and the device via-device link are not covered.
    _inject_area_references(area_registry, area, labels={"ghost-area-label"})
    _inject_entity_references(entity_registry, entity, area_id="ghost-entity-area")
    _inject_device_references(device_registry, device, via_device_id="ghost-via")

    assert rc.async_check_broken_references(hass) == []


async def test_impact_view_for_entity(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """The impact view reports areas referencing an entity."""
    entity = entity_registry.async_get_or_create("sensor", "test", "temperature")
    area_1 = area_registry.async_create("area 1")
    area_2 = area_registry.async_create("area 2")
    area_3 = area_registry.async_create("area 3")
    _inject_area_references(
        area_registry,
        area_1,
        temperature_entity_id=entity.entity_id,
        humidity_entity_id=entity.entity_id,
    )
    _inject_area_references(area_registry, area_2, humidity_entity_id=entity.entity_id)

    impact = rc.async_get_object_references(
        hass, rc.ReferenceObjectType.ENTITY, entity.entity_id
    )

    assert list(impact) == [rc.ReferenceObjectType.AREA]
    impact_areas = impact[rc.ReferenceObjectType.AREA]
    assert [reference.source_id for reference in impact_areas] == [
        area_1.id,
        area_2.id,
    ]
    assert impact_areas[0].fields == (
        rc.ReferenceField.TEMPERATURE_ENTITY_ID,
        rc.ReferenceField.HUMIDITY_ENTITY_ID,
    )
    assert impact_areas[1].fields == (rc.ReferenceField.HUMIDITY_ENTITY_ID,)
    # The area without a reference is not reported.
    assert area_3.id not in {reference.source_id for reference in impact_areas}


async def test_impact_view_for_device(
    hass: HomeAssistant,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """The impact view reports entities referencing a device."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "device")}
    )
    entity_1 = entity_registry.async_get_or_create("light", "test", "1")
    entity_2 = entity_registry.async_get_or_create("light", "test", "2")
    _inject_entity_references(entity_registry, entity_1, device_id=device.id)
    _inject_entity_references(entity_registry, entity_2, device_id=device.id)

    impact = rc.async_get_object_references(hass, "device", device.id)

    assert list(impact) == [rc.ReferenceObjectType.ENTITY]
    assert [
        reference.source_id for reference in impact[rc.ReferenceObjectType.ENTITY]
    ] == [entity_1.entity_id, entity_2.entity_id]


async def test_impact_view_for_area(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    device_registry: dr.DeviceRegistry,
) -> None:
    """The impact view reports main and child devices referencing an area."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    area = area_registry.async_create("area")
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "parent")}
    )
    child_device = device_registry.async_get_or_create_child(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", "child")},
        parent_device_id=device.id,
    )
    _inject_device_references(device_registry, device, area_id=area.id)
    _inject_child_device_references(device_registry, child_device, area_id=area.id)

    impact = rc.async_get_object_references(hass, "area", area.id)

    devices = impact[rc.ReferenceObjectType.DEVICE]
    assert {
        (reference.source_id, reference.is_child_device) for reference in devices
    } == {(child_device.id, True), (device.id, False)}


async def test_impact_view_for_config_entry(
    hass: HomeAssistant,
    device_registry: dr.DeviceRegistry,
) -> None:
    """The impact view reports main and child devices of a config entry."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "parent")}
    )
    child_device = device_registry.async_get_or_create_child(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", "child")},
        parent_device_id=device.id,
    )

    impact = rc.async_get_object_references(hass, "config_entry", config_entry.entry_id)

    devices = impact[rc.ReferenceObjectType.DEVICE]
    assert {
        (reference.source_id, reference.is_child_device) for reference in devices
    } == {(child_device.id, True), (device.id, False)}


async def test_impact_view_invalid_input(
    hass: HomeAssistant,
    entity_registry: er.EntityRegistry,
) -> None:
    """Unknown object types and ids raise instead of returning empty results."""
    with pytest.raises(ValueError, match="Unknown object type"):
        rc.async_get_object_references(hass, "automobile", "whatever")
    with pytest.raises(ValueError, match="not supported"):
        rc.async_get_object_references(hass, "floor", "whatever")
    with pytest.raises(ValueError, match="Unknown entity"):
        rc.async_get_object_references(hass, "entity", "sensor.unknown")
    with pytest.raises(ValueError, match="Unknown device"):
        rc.async_get_object_references(hass, "device", "unknown-device")
    with pytest.raises(ValueError, match="Unknown area"):
        rc.async_get_object_references(hass, "area", "unknown-area")
    with pytest.raises(ValueError, match="Unknown config entry"):
        rc.async_get_object_references(hass, "config_entry", "unknown-config-entry")


async def test_cleanup_is_preview_by_default(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """Without confirmation the cleanup reports the plan but writes nothing."""
    area = area_registry.async_create("area")
    deleted_entity = entity_registry.async_get_or_create("sensor", "sensor", "gone")
    entity = entity_registry.async_get_or_create("light", "test", "1")

    entity_registry.async_remove(deleted_entity.entity_id)
    _inject_area_references(
        area_registry,
        area,
        floor_id=MISSING_FLOOR,
        temperature_entity_id=MISSING_TEMPERATURE_ENTITY,
        humidity_entity_id=deleted_entity.entity_id,
    )
    _inject_entity_references(entity_registry, entity, device_id=MISSING_DEVICE)

    area_events = async_capture_events(hass, ar.EVENT_AREA_REGISTRY_UPDATED)
    entity_events = async_capture_events(hass, er.EVENT_ENTITY_REGISTRY_UPDATED)

    result = await rc.async_cleanup_broken_references(hass)

    assert result.dry_run is True
    processed = {
        (record.reference.source_type, record.reference.field, record.action)
        for record in result.processed
    }
    assert processed == {
        (
            rc.ReferenceObjectType.AREA,
            rc.ReferenceField.FLOOR_ID,
            "cleared_field",
        ),
        (
            rc.ReferenceObjectType.AREA,
            rc.ReferenceField.TEMPERATURE_ENTITY_ID,
            "cleared_field",
        ),
        (
            rc.ReferenceObjectType.ENTITY,
            rc.ReferenceField.DEVICE_ID,
            "cleared_field",
        ),
    }
    # The reference to the deleted entity is skipped, even in the preview.
    assert len(result.skipped) == 1
    skipped = result.skipped[0]
    assert skipped.reference.field is rc.ReferenceField.HUMIDITY_ENTITY_ID
    assert (
        skipped.reason is not None and "deleted registry collection" in skipped.reason
    )
    assert result.remaining == 1
    assert result.limit_reached is False

    # Nothing was changed.
    assert not area_events
    assert not entity_events
    unchanged_area = area_registry.async_get_area(area.id)
    assert unchanged_area is not None
    assert unchanged_area.floor_id == MISSING_FLOOR
    assert unchanged_area.temperature_entity_id == MISSING_TEMPERATURE_ENTITY
    assert unchanged_area.humidity_entity_id == deleted_entity.entity_id
    unchanged_entity = entity_registry.async_get(entity.entity_id)
    assert unchanged_entity is not None
    assert unchanged_entity.device_id == MISSING_DEVICE


async def test_cleanup_clears_area_references_in_one_update(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
) -> None:
    """All broken references of one area are cleared with a single update."""
    area = area_registry.async_create("area")
    area_events = async_capture_events(hass, ar.EVENT_AREA_REGISTRY_UPDATED)
    _inject_area_references(
        area_registry,
        area,
        floor_id=MISSING_FLOOR,
        temperature_entity_id=MISSING_TEMPERATURE_ENTITY,
        humidity_entity_id=MISSING_HUMIDITY_ENTITY,
    )

    result = await rc.async_cleanup_broken_references(hass, confirmed=True)

    assert result.dry_run is False
    assert len(result.processed) == 3
    assert not result.skipped
    assert result.remaining == 0
    updated_area = area_registry.async_get_area(area.id)
    assert updated_area is not None
    assert updated_area.floor_id is None
    assert updated_area.temperature_entity_id is None
    assert updated_area.humidity_entity_id is None
    # One object, one update event even with three broken references.
    assert len(area_events) == 1
    assert area_events[0].data == {"action": "update", "area_id": area.id}


async def test_cleanup_clears_entity_references(
    hass: HomeAssistant,
    entity_registry: er.EntityRegistry,
    label_registry: lr.LabelRegistry,
) -> None:
    """A dangling device id and stale labels are cleared on an entity."""
    kept_label = label_registry.async_create("kept label")
    entity = entity_registry.async_get_or_create("light", "test", "1")
    _inject_entity_references(
        entity_registry,
        entity,
        device_id=MISSING_DEVICE,
        labels={kept_label.label_id, MISSING_LABEL},
    )

    result = await rc.async_cleanup_broken_references(hass, confirmed=True)

    assert len(result.processed) == 2
    updated_entity = entity_registry.async_get(entity.entity_id)
    assert updated_entity is not None
    assert updated_entity.device_id is None
    assert updated_entity.labels == {kept_label.label_id}
    assert result.remaining == 0


async def test_cleanup_clears_device_references(
    hass: HomeAssistant,
    device_registry: dr.DeviceRegistry,
    label_registry: lr.LabelRegistry,
) -> None:
    """A dangling area id and stale labels are cleared on a main device."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    kept_label = label_registry.async_create("kept label")
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "device")}
    )
    _inject_device_references(
        device_registry,
        device,
        area_id=MISSING_AREA,
        labels={kept_label.label_id, MISSING_LABEL},
    )

    result = await rc.async_cleanup_broken_references(hass, confirmed=True)

    assert len(result.processed) == 2
    updated_device = device_registry.async_get(device.id)
    assert updated_device is not None
    assert updated_device.area_id is None
    assert updated_device.labels == {kept_label.label_id}
    assert result.remaining == 0


async def test_cleanup_clears_child_device_references(
    hass: HomeAssistant,
    device_registry: dr.DeviceRegistry,
) -> None:
    """A dangling area id and stale labels are cleared on a child device."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    parent = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "parent")}
    )
    child = device_registry.async_get_or_create_child(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", "child")},
        parent_device_id=parent.id,
    )
    _inject_child_device_references(
        device_registry, child, area_id=MISSING_AREA, labels={MISSING_LABEL}
    )

    result = await rc.async_cleanup_broken_references(hass, confirmed=True)

    assert len(result.processed) == 2
    updated_child = device_registry.async_get(child.id)
    assert updated_child is not None
    assert updated_child.area_id is None
    assert updated_child.labels == set()
    assert result.remaining == 0


async def test_cleanup_removes_device_with_missing_config_entry(
    hass: HomeAssistant,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A device whose config entry is gone is removed and its other refs cleared."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", "device")},
    )
    entity = entity_registry.async_get_or_create("light", "other", "1")
    _inject_entity_references(entity_registry, entity, device_id=device.id)
    _inject_device_references(
        device_registry,
        device,
        area_id=MISSING_AREA,
        labels={MISSING_LABEL},
        config_entry_id=MISSING_CONFIG_ENTRY,
    )

    saved: list[object] = []
    for registry in (device_registry, entity_registry):
        original = registry._store.async_save

        async def async_save(data: Any, _original=original, _registry=registry) -> None:
            saved.append(_registry)
            await _original(data)

        monkeypatch.setattr(registry._store, "async_save", async_save)

    result = await rc.async_cleanup_broken_references(hass, confirmed=True)
    await hass.async_block_till_done()

    actions = {record.action for record in result.processed}
    assert actions == {"cleared_field", "removed_device"}
    assert device_registry.async_get(device.id) is None
    deleted_device = device_registry._deleted_devices.data[device.id]
    assert deleted_device.area_id is None
    assert deleted_device.labels == set()
    # Entities not owned by the removed entry are detached through the normal
    # device removal cascade.
    detached_entity = entity_registry.async_get(entity.entity_id)
    assert detached_entity is not None
    assert detached_entity.device_id is None
    # The device and entity registries are persisted, including the cascade.
    assert set(saved) == {device_registry, entity_registry}
    # The removed device reference is no longer reported.
    assert result.remaining == 0


async def test_cleanup_skips_references_to_deleted_objects(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """References to deleted but retained objects are skipped with a reason."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "device")}
    )
    deleted_entity = entity_registry.async_get_or_create("sensor", "test", "temp")
    area = area_registry.async_create("area")
    entity = entity_registry.async_get_or_create("light", "test", "1")

    device_registry.async_remove_device(device.id)
    entity_registry.async_remove(deleted_entity.entity_id)
    _inject_area_references(
        area_registry, area, temperature_entity_id=deleted_entity.entity_id
    )
    _inject_entity_references(entity_registry, entity, device_id=device.id)

    result = await rc.async_cleanup_broken_references(hass, confirmed=True)

    assert not result.processed
    assert {record.reference.field for record in result.skipped} == {
        rc.ReferenceField.TEMPERATURE_ENTITY_ID,
        rc.ReferenceField.DEVICE_ID,
    }
    assert all(
        "deleted registry collection" in (record.reason or "")
        for record in result.skipped
    )
    unchanged_area = area_registry.async_get_area(area.id)
    assert unchanged_area is not None
    assert unchanged_area.temperature_entity_id == deleted_entity.entity_id
    unchanged_entity = entity_registry.async_get(entity.entity_id)
    assert unchanged_entity is not None
    assert unchanged_entity.device_id == device.id


async def test_cleanup_skips_recreated_and_changed_references(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
    floor_registry: fr.FloorRegistry,
    label_registry: lr.LabelRegistry,
) -> None:
    """Confirmed references whose target returned or field changed are skipped."""
    # Snapshot the broken references first; this is the set a caller confirms.
    area_temp = area_registry.async_create("area temp")
    _inject_area_references(
        area_registry, area_temp, temperature_entity_id="sensor.back_from_the_dead"
    )
    area_floor = area_registry.async_create("area floor")
    _inject_area_references(area_registry, area_floor, floor_id="back_floor")
    entity = entity_registry.async_get_or_create("light", "test", "1")
    _inject_entity_references(entity_registry, entity, labels={"back_label"})
    entity_changed = entity_registry.async_get_or_create("light", "test", "2")
    _inject_entity_references(
        entity_registry, entity_changed, device_id="changed_away_device"
    )
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    restored_device = device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "restored")}
    )
    restored_id = restored_device.id
    entity_restored = entity_registry.async_get_or_create("light", "test", "3")
    device_registry.async_remove_device(restored_id)
    _inject_entity_references(entity_registry, entity_restored, device_id=restored_id)
    confirmed_references = rc.async_check_broken_references(hass)

    # The referenced objects come back (or the field is cleared) before confirm.
    entity_registry.async_get_or_create(
        "sensor",
        "test",
        "back_from_the_dead",
        suggested_object_id="back_from_the_dead",
    )
    floor_registry.async_create("back floor")  # id is derived from the name
    label_registry.async_create("back label")
    entity_registry.async_update_entity(entity_changed.entity_id, device_id=None)
    device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", "restored")},
    )

    result = await rc.async_cleanup_broken_references(
        hass, confirmed=True, references=confirmed_references
    )

    assert not result.processed
    assert len(result.skipped) == 5
    reasons = " ".join(record.reason or "" for record in result.skipped)
    assert "re-created" in reasons
    assert "no longer references" in reasons
    unchanged_temp_area = area_registry.async_get_area(area_temp.id)
    assert unchanged_temp_area is not None
    assert unchanged_temp_area.temperature_entity_id == "sensor.back_from_the_dead"
    unchanged_floor_area = area_registry.async_get_area(area_floor.id)
    assert unchanged_floor_area is not None
    assert unchanged_floor_area.floor_id == "back_floor"
    unchanged_entity = entity_registry.async_get(entity.entity_id)
    assert unchanged_entity is not None
    assert unchanged_entity.labels == {"back_label"}
    changed_entity = entity_registry.async_get(entity_changed.entity_id)
    assert changed_entity is not None
    assert changed_entity.device_id is None
    assert device_registry.async_get(restored_id) is not None
    restored_entity = entity_registry.async_get(entity_restored.entity_id)
    assert restored_entity is not None
    assert restored_entity.device_id == restored_id
    # None of the re-created references are broken anymore.
    assert result.remaining == 0


async def test_cleanup_limit_is_respected_and_runs_are_resumable(
    hass: HomeAssistant,
    entity_registry: er.EntityRegistry,
) -> None:
    """The per-run limit bounds processing and another run continues."""
    entities = [
        entity_registry.async_get_or_create("light", "test", str(index))
        for index in range(3)
    ]
    for entity in entities:
        _inject_entity_references(
            entity_registry, entity, labels={f"ghost-label-{entity.entity_id}"}
        )

    first = await rc.async_cleanup_broken_references(hass, confirmed=True, limit=2)
    assert len(first.processed) == 2
    assert first.limit_reached is True
    assert first.remaining == 1

    second = await rc.async_cleanup_broken_references(hass, confirmed=True)
    assert len(second.processed) == 1
    assert second.limit_reached is False
    assert second.remaining == 0

    third = await rc.async_cleanup_broken_references(hass, confirmed=True)
    assert not third.processed
    assert third.remaining == 0


async def test_cleanup_limit_zero_processes_nothing(
    hass: HomeAssistant,
    entity_registry: er.EntityRegistry,
) -> None:
    """A limit of zero ends the run without processing any reference."""
    entity = entity_registry.async_get_or_create("light", "test", "1")
    _inject_entity_references(entity_registry, entity, labels={MISSING_LABEL})

    result = await rc.async_cleanup_broken_references(hass, confirmed=True, limit=0)

    assert not result.processed
    assert result.remaining == 1
    assert result.limit_reached is True
    unchanged_entity = entity_registry.async_get(entity.entity_id)
    assert unchanged_entity is not None
    assert unchanged_entity.labels == {MISSING_LABEL}


async def test_cleanup_oversized_first_object_is_processed_as_a_whole(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
) -> None:
    """One object is never split across runs even if it exceeds the limit."""
    area = area_registry.async_create("area")
    _inject_area_references(
        area_registry,
        area,
        floor_id=MISSING_FLOOR,
        temperature_entity_id=MISSING_TEMPERATURE_ENTITY,
        humidity_entity_id=MISSING_HUMIDITY_ENTITY,
    )

    result = await rc.async_cleanup_broken_references(hass, confirmed=True, limit=1)

    assert len(result.processed) == 3
    assert result.limit_reached is True
    assert result.remaining == 0


@pytest.mark.parametrize("invalid_limit", [-1, True, "10"])
async def test_cleanup_rejects_invalid_limit(
    hass: HomeAssistant, invalid_limit: Any
) -> None:
    """Negative or non-integer limits raise."""
    with pytest.raises(ValueError, match="limit"):
        await rc.async_cleanup_broken_references(hass, limit=invalid_limit)


async def test_cleanup_rejects_invalid_source_type_order(hass: HomeAssistant) -> None:
    """Unknown or duplicated source types in the order raise."""
    with pytest.raises(ValueError, match="source object type"):
        await rc.async_cleanup_broken_references(
            hass,
            source_type_order=("floor",),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="duplicates"):
        await rc.async_cleanup_broken_references(
            hass,
            source_type_order=(
                rc.ReferenceObjectType.AREA,
                rc.ReferenceObjectType.AREA,
            ),
        )


async def test_cleanup_processes_in_configured_hierarchy_order(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """The source type order configures which hierarchy level is handled first."""
    area = area_registry.async_create("area")
    _inject_area_references(area_registry, area, floor_id=MISSING_FLOOR)
    entity = entity_registry.async_get_or_create("light", "test", "1")
    _inject_entity_references(entity_registry, entity, device_id=MISSING_DEVICE)

    default_order = await rc.async_cleanup_broken_references(hass)
    assert [record.reference.source_type for record in default_order.processed] == [
        rc.ReferenceObjectType.ENTITY,
        rc.ReferenceObjectType.AREA,
    ]

    # Reset the dangling references for a second run with a custom order.
    _inject_area_references(area_registry, area, floor_id=MISSING_FLOOR)
    _inject_entity_references(entity_registry, entity, device_id=MISSING_DEVICE)
    reversed_order = await rc.async_cleanup_broken_references(
        hass,
        source_type_order=(
            rc.ReferenceObjectType.AREA,
            rc.ReferenceObjectType.DEVICE,
            rc.ReferenceObjectType.ENTITY,
        ),
    )
    assert [record.reference.source_type for record in reversed_order.processed] == [
        rc.ReferenceObjectType.AREA,
        rc.ReferenceObjectType.ENTITY,
    ]


async def test_cleanup_source_type_order_can_exclude_categories(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    entity_registry: er.EntityRegistry,
) -> None:
    """Source types missing from the order are left untouched."""
    area = area_registry.async_create("area")
    _inject_area_references(area_registry, area, floor_id=MISSING_FLOOR)
    entity = entity_registry.async_get_or_create("light", "test", "1")
    _inject_entity_references(entity_registry, entity, device_id=MISSING_DEVICE)

    result = await rc.async_cleanup_broken_references(
        hass, confirmed=True, source_type_order=(rc.ReferenceObjectType.ENTITY,)
    )

    assert len(result.processed) == 1
    assert result.processed[0].reference.source_type is rc.ReferenceObjectType.ENTITY
    assert result.remaining == 1
    unchanged_area = area_registry.async_get_area(area.id)
    assert unchanged_area is not None
    assert unchanged_area.floor_id == MISSING_FLOOR


async def test_cleanup_flushes_modified_registries(
    hass: HomeAssistant,
    area_registry: ar.AreaRegistry,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirmed runs wait for the modified registry stores to be written."""
    saved: list[str] = []
    registries_by_name: dict[str, BaseRegistry] = {
        "area": area_registry,
        "device": device_registry,
        "entity": entity_registry,
    }

    def _spy_on_save(registry_name: str) -> Callable[[Any], Any]:
        original = registries_by_name[registry_name]._store.async_save

        async def async_save(data: Any) -> None:
            saved.append(registry_name)
            await original(data)

        return async_save

    area = area_registry.async_create("area")
    entity = entity_registry.async_get_or_create("light", "test", "1")
    _inject_area_references(area_registry, area, floor_id=MISSING_FLOOR)
    _inject_entity_references(entity_registry, entity, device_id=MISSING_DEVICE)

    monkeypatch.setattr(
        area_registry._store,
        "async_save",
        _spy_on_save("area"),
    )
    monkeypatch.setattr(
        entity_registry._store,
        "async_save",
        _spy_on_save("entity"),
    )

    result = await rc.async_cleanup_broken_references(hass, confirmed=True)

    assert result.remaining == 0
    assert set(saved) == {"area", "entity"}
