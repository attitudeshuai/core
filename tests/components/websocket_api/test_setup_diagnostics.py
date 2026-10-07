"""Tests for the setup diagnostics websocket commands."""

from unittest.mock import AsyncMock

import pytest

from homeassistant import setup, setup_diagnostics
from homeassistant.components.websocket_api import const
from homeassistant.core import HomeAssistant

from tests.common import MockModule, MockUser, mock_integration
from tests.typing import MockHAClientWebSocket, WebSocketGenerator


def _enable(hass: HomeAssistant) -> None:
    config = setup_diagnostics.SETUP_DIAGNOSTICS_CONFIG_SCHEMA(
        {"state": "on", "sample_rate": 1, "max_events": 10}
    )
    setup_diagnostics.async_enable(hass, config)


async def test_health_command(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test the health command."""
    _enable(hass)
    mock_integration(
        hass, MockModule("ws_dom", async_setup=AsyncMock(return_value=True))
    )
    assert await setup.async_setup_component(hass, "ws_dom", {}) is True

    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {"type": "setup_diagnostics/health", "domains": ["ws_dom"]}
    )
    msg = await client.receive_json()
    assert msg["success"]
    assert msg["result"] == [
        {
            "domain": "ws_dom",
            "failure_from_dependency": None,
            "dependency_failure_kind": None,
            "failed_dependencies": [],
            "config_entry_retries": [],
        }
    ]


async def test_impact_command(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test the impact command."""
    _enable(hass)
    mock_integration(hass, MockModule("ws_target"))
    mock_integration(hass, MockModule("ws_dep", dependencies=["ws_target"]))
    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {"type": "setup_diagnostics/impact", "domain": "ws_target"}
    )
    msg = await client.receive_json()
    assert msg["success"]
    result = msg["result"]
    assert result["domain"] == "ws_target"
    assert result["target_found"] is True
    assert {"domain": "ws_dep", "loaded": False, "config_entries": []} in result[
        "dependent_integrations"
    ]


async def test_events_command(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test the events command serializes events."""
    _enable(hass)
    assert await setup.async_setup_component(hass, "ws_ghost", {}) is False
    client = await hass_ws_client(hass)
    await client.send_json_auto_id({"type": "setup_diagnostics/events"})
    msg = await client.receive_json()
    assert msg["success"]
    assert msg["result"]
    event = next(event for event in msg["result"] if event["domain"] == "ws_ghost")
    assert event["event_type"] == "setup_failed"
    assert event["failure_kind"] == "integration_not_found"
    assert isinstance(event["timestamp"], str)


async def test_commands_disabled(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Test the commands return not supported while diagnostics is disabled."""
    client = await hass_ws_client(hass)
    for command in ("health", "events"):
        await client.send_json_auto_id({"type": f"setup_diagnostics/{command}"})
        msg = await client.receive_json()
        assert not msg["success"]
        assert msg["error"]["code"] == const.ERR_NOT_SUPPORTED
    await client.send_json_auto_id(
        {"type": "setup_diagnostics/impact", "domain": "sensor"}
    )
    msg = await client.receive_json()
    assert not msg["success"]
    assert msg["error"]["code"] == const.ERR_NOT_SUPPORTED


async def test_commands_require_admin(
    hass: HomeAssistant,
    websocket_client: MockHAClientWebSocket,
    hass_admin_user: MockUser,
) -> None:
    """Test non admin users cannot use the commands."""
    _enable(hass)
    hass_admin_user.groups = []
    for payload in (
        {"type": "setup_diagnostics/health"},
        {"type": "setup_diagnostics/events"},
        {"type": "setup_diagnostics/impact", "domain": "sensor"},
    ):
        await websocket_client.send_json_auto_id(payload)
        msg = await websocket_client.receive_json()
        assert not msg["success"]
        assert msg["error"]["code"] == const.ERR_UNAUTHORIZED


@pytest.mark.parametrize(
    "payload",
    [
        {"type": "setup_diagnostics/health", "domains": "not-a-list"},
        {"type": "setup_diagnostics/impact"},
        {"type": "setup_diagnostics/impact", "domain": 5},
        {"type": "setup_diagnostics/events", "domains": [1, 2]},
    ],
)
async def test_commands_schema_validation(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator, payload: dict
) -> None:
    """Test malformed commands are rejected."""
    _enable(hass)
    client = await hass_ws_client(hass)
    await client.send_json_auto_id(payload)
    msg = await client.receive_json()
    assert not msg["success"]
