"""Tests for the state write observability websocket API."""

from datetime import datetime
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.write_observation import (
    DATA_WRITE_OBSERVATION,
    GROUP_BY_INTEGRATION,
)
from homeassistant.setup import async_setup_component
from homeassistant.util import dt as dt_util

from tests.typing import WebSocketGenerator

DOMAIN = "write_observability"


async def _call(websocket_client: Any, msg_id: int, **payload: Any) -> dict[str, Any]:
    await websocket_client.send_json({"id": msg_id, **payload})
    return await websocket_client.receive_json()


async def test_setup_enables_observation(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test setting up the integration connects the manager."""
    assert await async_setup_component(
        hass,
        DOMAIN,
        {DOMAIN: {"sample_rate": 0.5, "window_seconds": 10, "retain": 5}},
    )
    manager = hass.data[DATA_WRITE_OBSERVATION]
    assert manager.connected
    assert manager.config.sample_rate == 0.5
    assert manager.config.window_seconds == 10
    assert manager.config.retain == 5

    client = await hass_ws_client()
    msg = await _call(client, 1, type="write_observability/entity_stats")
    assert msg["success"]
    result = msg["result"]
    assert result["entries"] == []
    assert result["has_more"] is False
    assert result["next_offset"] is None
    assert result["window"]["sampled"] is True
    assert "start_time" in result["window"]
    assert "end_time" in result["window"]


async def test_entity_stats(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test the entity stats command end to end."""
    assert await async_setup_component(hass, DOMAIN, {DOMAIN: {}})

    hass.states.async_set("sensor.temperature", "20", writer="mqtt")
    hass.states.async_set("sensor.temperature", "20", writer="mqtt")
    hass.states.async_set("sensor.temperature", "20", {"unit": "C"}, writer="mqtt")
    hass.states.async_set("sensor.temperature", "21", writer="mqtt")

    client = await hass_ws_client()
    msg = await _call(
        client,
        1,
        type="write_observability/entity_stats",
        entity_ids=["sensor.temperature"],
    )
    assert msg["success"]
    entry = msg["result"]["entries"][0]
    assert entry["entity_id"] == "sensor.temperature"
    assert entry["state_changes"] == 2
    assert entry["attribute_changes"] == 1
    assert entry["same_state_reports"] == 1
    assert entry["writes"] == 4
    assert entry["writers"] == ["mqtt"]
    assert entry["removed"] is False
    assert entry["rate_annotation"] is None


async def test_domain_stats(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test the domain stats command for both groupings."""
    assert await async_setup_component(hass, DOMAIN, {DOMAIN: {}})

    hass.states.async_set("sensor.a", "1", writer="mqtt")
    hass.states.async_set("sensor.a", "1", writer="mqtt")
    hass.states.async_set("switch.b", "on", writer="mqtt")
    hass.states.async_set("switch.b", "on")

    client = await hass_ws_client()
    msg = await _call(client, 1, type="write_observability/domain_stats")
    assert msg["success"]
    by_entity_domain = {
        entry["domain"]: entry["writes"] for entry in msg["result"]["entries"]
    }
    assert by_entity_domain == {"sensor": 2, "switch": 2}

    msg = await _call(
        client,
        2,
        type="write_observability/domain_stats",
        group_by=GROUP_BY_INTEGRATION,
    )
    assert msg["success"]
    by_integration = {
        entry["domain"]: entry["writes"] for entry in msg["result"]["entries"]
    }
    assert by_integration == {"mqtt": 3, "unknown": 1}


async def test_rate_stats(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test the rate stats command and its annotation."""
    assert await async_setup_component(hass, DOMAIN, {DOMAIN: {"window_seconds": 10}})
    manager = hass.data[DATA_WRITE_OBSERVATION]

    now = dt_util.utcnow().timestamp()
    for idx in range(30):
        manager._async_record("sensor.fast", 0, "mqtt", now + idx * 0.1)

    client = await hass_ws_client()
    msg = await _call(
        client, 1, type="write_observability/rate_stats", rate_threshold=1
    )
    assert msg["success"]
    entries = msg["result"]["entries"]
    assert len(entries) == 1
    annotation = entries[0]["rate_annotation"]
    assert annotation["threshold"] == 1
    assert annotation["window_length"] == 10
    assert annotation["rate"] == 3
    assert entries[0]["writes"] == 30


async def test_chunking(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test chunked responses with continuation markers."""
    assert await async_setup_component(hass, DOMAIN, {DOMAIN: {}})
    for name in ("a", "b", "c"):
        hass.states.async_set(f"sensor.{name}", "1", writer="demo")

    client = await hass_ws_client()
    msg = await _call(client, 1, type="write_observability/entity_stats", limit=2)
    assert msg["success"]
    assert [entry["entity_id"] for entry in msg["result"]["entries"]] == [
        "sensor.a",
        "sensor.b",
    ]
    assert msg["result"]["has_more"] is True
    assert msg["result"]["next_offset"] == 2

    msg = await _call(
        client,
        2,
        type="write_observability/entity_stats",
        limit=2,
        offset=2,
    )
    assert [entry["entity_id"] for entry in msg["result"]["entries"]] == ["sensor.c"]
    assert msg["result"]["has_more"] is False


async def test_unknown_entity_id(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test unknown entity ids return an error."""
    assert await async_setup_component(hass, DOMAIN, {DOMAIN: {}})

    client = await hass_ws_client()
    msg = await _call(
        client,
        1,
        type="write_observability/entity_stats",
        entity_ids=["sensor.does_not_exist"],
    )
    assert not msg["success"]
    assert msg["error"]["code"] == "invalid_format"


async def test_start_after_end(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test a reversed window returns an error."""
    assert await async_setup_component(hass, DOMAIN, {DOMAIN: {}})

    client = await hass_ws_client()
    msg = await _call(
        client,
        1,
        type="write_observability/entity_stats",
        start_time=dt_util.utcnow().isoformat(),
        end_time=dt_util.utc_from_timestamp(0).isoformat(),
    )
    assert not msg["success"]
    assert msg["error"]["code"] == "invalid_format"


async def test_naive_timestamp(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test naive timestamps are rejected."""
    assert await async_setup_component(hass, DOMAIN, {DOMAIN: {}})

    client = await hass_ws_client()
    msg = await _call(
        client,
        1,
        type="write_observability/entity_stats",
        start_time=datetime(2024, 1, 1).isoformat(),
    )
    assert not msg["success"]
    assert msg["error"]["code"] == "invalid_format"


async def test_non_positive_threshold(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test rate threshold must be positive."""
    assert await async_setup_component(hass, DOMAIN, {DOMAIN: {}})

    client = await hass_ws_client()
    msg = await _call(
        client, 1, type="write_observability/rate_stats", rate_threshold=0
    )
    assert not msg["success"]
    assert msg["error"]["code"] == "invalid_format"


async def test_negative_limit(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test a negative limit is rejected by the schema."""
    assert await async_setup_component(hass, DOMAIN, {DOMAIN: {}})

    client = await hass_ws_client()
    msg = await _call(client, 1, type="write_observability/entity_stats", limit=-1)
    assert not msg["success"]
    assert msg["error"]["code"] == "invalid_format"


async def test_command_unknown_when_disabled(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test commands are not registered without the integration."""
    client = await hass_ws_client()
    msg = await _call(client, 1, type="write_observability/entity_stats")
    assert not msg["success"]
    assert msg["error"]["code"] == "unknown_command"


async def test_invalid_config(hass: HomeAssistant) -> None:
    """Test invalid configuration is rejected."""
    assert not await async_setup_component(hass, DOMAIN, {DOMAIN: {"sample_rate": 2}})
    assert not await async_setup_component(
        hass, DOMAIN, {DOMAIN: {"window_seconds": -1}}
    )
    assert not await async_setup_component(hass, DOMAIN, {DOMAIN: {"retain": -1}})
    assert DATA_WRITE_OBSERVATION not in hass.data
