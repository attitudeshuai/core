"""Tests for websocket subscription governance."""

from typing import Any

import probatio
import pytest

from homeassistant.components.websocket_api import (
    CONFIG_SCHEMA,
    DOMAIN,
    async_register_command,
    const,
    messages,
    websocket_command,
)
from homeassistant.components.websocket_api.connection import ActiveConnection
from homeassistant.components.websocket_api.governance import QueuedSubscriptionMessage
from homeassistant.components.websocket_api.http import WebSocketHandler
from homeassistant.const import EVENT_STATE_CHANGED
from homeassistant.core import HomeAssistant, callback
from homeassistant.setup import async_setup_component

from tests.typing import WebSocketGenerator

GOVERNANCE = const.CONF_SUBSCRIPTION_GOVERNANCE


def governance_config(
    *,
    max_per_connection: int = 0,
    max_system: int = 0,
    slow_client: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a websocket_api component config with governance enabled."""
    governance: dict[str, Any] = {
        const.CONF_MAX_PER_CONNECTION: max_per_connection,
        const.CONF_MAX_SYSTEM: max_system,
    }
    if slow_client is not None:
        governance[const.CONF_SLOW_CLIENT] = slow_client
    return {DOMAIN: {GOVERNANCE: governance}}


@pytest.fixture
def handler_instance(monkeypatch: pytest.MonkeyPatch):
    """Capture the WebSocketHandler instance of the next websocket client."""
    instances: list[WebSocketHandler] = []
    original_init = WebSocketHandler.__init__

    def capturing_init(self: WebSocketHandler, *args: Any) -> None:
        original_init(self, *args)
        instances.append(self)

    monkeypatch.setattr(WebSocketHandler, "__init__", capturing_init)
    return instances


@pytest.mark.parametrize(
    "governance",
    [
        {const.CONF_MAX_PER_CONNECTION: -1},
        {const.CONF_MAX_SYSTEM: -2},
        {
            const.CONF_SLOW_CLIENT: {
                const.CONF_THRESHOLD: 0,
                const.CONF_ACTION: const.SLOW_CLIENT_ACTION_DROP,
            }
        },
        {
            const.CONF_SLOW_CLIENT: {
                const.CONF_THRESHOLD: 3,
                const.CONF_ACTION: "unknown",
            }
        },
        {"unexpected_key": 1},
    ],
)
def test_invalid_config_rejected_before_startup(governance: dict[str, Any]) -> None:
    """Invalid governance configuration must fail config validation."""
    with pytest.raises(probatio.Invalid):
        CONFIG_SCHEMA({DOMAIN: {GOVERNANCE: governance}})


def test_valid_config() -> None:
    """Zero limits are valid and default to unlimited."""
    validated = CONFIG_SCHEMA(
        {
            DOMAIN: {
                GOVERNANCE: {
                    const.CONF_SLOW_CLIENT: {
                        const.CONF_THRESHOLD: 100,
                        const.CONF_ACTION: const.SLOW_CLIENT_ACTION_MERGE,
                    }
                }
            }
        }
    )
    assert validated[DOMAIN][GOVERNANCE][const.CONF_MAX_PER_CONNECTION] == 0
    assert validated[DOMAIN][GOVERNANCE][const.CONF_MAX_SYSTEM] == 0


async def test_disabled_by_default(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """Without configuration the governance layer is off."""
    assert await async_setup_component(hass, DOMAIN, {})
    client = await hass_ws_client(hass)

    await client.send_json({"id": 1, "type": "subscription_governance/info"})
    msg = await client.receive_json()
    assert msg["success"]
    assert msg["result"] == {"enabled": False, "active_connections": 1}
    assert const.DATA_SUBSCRIPTION_GOVERNANCE not in hass.data


async def test_per_connection_quota(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    handler_instance: list[WebSocketHandler],
) -> None:
    """The per-connection quota rejects new subscriptions, not the client."""
    assert await async_setup_component(
        hass,
        DOMAIN,
        governance_config(max_per_connection=2),
    )
    client = await hass_ws_client(hass)

    rolled_back: list[int] = []

    @callback
    @websocket_command({"type": "fake_subscription"})
    def fake_subscription(
        hass: HomeAssistant, connection: ActiveConnection, msg: dict[str, Any]
    ) -> None:
        """Register a fake subscription."""
        msg_id = msg["id"]
        connection.subscriptions[msg_id] = lambda: rolled_back.append(msg_id)
        connection.send_result(msg_id)

    async_register_command(hass, fake_subscription)

    await client.send_json(
        {"id": 2, "type": "subscribe_events", "event_type": "governance_test"}
    )
    assert (await client.receive_json())["success"]
    await client.send_json({"id": 3, "type": "fake_subscription"})
    assert (await client.receive_json())["success"]

    # Quota full: a third subscription is rejected with a clear error.
    await client.send_json({"id": 4, "type": "fake_subscription"})
    msg = await client.receive_json()
    assert not msg["success"]
    assert msg["error"]["code"] == const.ERR_SUBSCRIPTION_QUOTA_EXCEEDED
    assert "per-connection" in msg["error"]["message"]
    # The subscription registration done by the handler is rolled back.
    assert rolled_back == [4]

    # Existing subscriptions keep working and the connection stays open.
    hass.bus.async_fire("governance_test")
    event = await client.receive_json()
    assert event["id"] == 2 and event["type"] == "event"
    await client.send_json({"id": 5, "type": "ping"})
    pong = await client.receive_json()
    assert pong["id"] == 5 and pong["type"] == "pong"

    # Cancelling a subscription immediately frees the quota slot.
    await client.send_json({"id": 6, "type": "unsubscribe_events", "subscription": 3})
    assert (await client.receive_json())["success"]
    await client.send_json({"id": 7, "type": "fake_subscription"})
    assert (await client.receive_json())["success"]

    governor = handler_instance[0]._governor
    assert governor is not None
    assert governor.active_subscriptions == 2
    assert governor.rejected == 1


async def test_system_quota(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    handler_instance: list[WebSocketHandler],
) -> None:
    """The system-wide quota applies across connections and never evicts."""
    assert await async_setup_component(hass, DOMAIN, governance_config(max_system=1))
    client1 = await hass_ws_client(hass)
    client2 = await hass_ws_client(hass)

    await client1.send_json({"id": 1, "type": "subscribe_events"})
    assert (await client1.receive_json())["success"]

    await client2.send_json({"id": 1, "type": "subscribe_events"})
    msg = await client2.receive_json()
    assert not msg["success"]
    assert msg["error"]["code"] == const.ERR_SUBSCRIPTION_QUOTA_EXCEEDED
    assert "system-wide" in msg["error"]["message"]

    # The rejected client is not disconnected and can run other commands.
    await client2.send_json({"id": 2, "type": "ping"})
    pong = await client2.receive_json()
    assert pong["type"] == "pong"

    # The first client's subscription is untouched and still delivers.
    hass.bus.async_fire("some_event")
    event = await client1.receive_json()
    assert event["id"] == 1 and event["type"] == "event"
    await client1.send_json({"id": 2, "type": "ping"})
    assert (await client1.receive_json())["type"] == "pong"

    # Releasing the slot on one connection allows a new one elsewhere.
    await client1.send_json({"id": 3, "type": "unsubscribe_events", "subscription": 1})
    assert (await client1.receive_json())["success"]
    await client2.send_json({"id": 4, "type": "subscribe_events"})
    assert (await client2.receive_json())["success"]


async def test_zero_quota_means_unlimited(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator
) -> None:
    """A quota of zero disables the quota dimension."""
    assert await async_setup_component(hass, DOMAIN, governance_config())
    client = await hass_ws_client(hass)

    for subscription_id in range(1, 6):
        await client.send_json({"id": subscription_id, "type": "subscribe_events"})
        assert (await client.receive_json())["success"]


async def test_drop_action(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    handler_instance: list[WebSocketHandler],
) -> None:
    """Excess messages are dropped per subscription and delivery resumes."""
    assert await async_setup_component(
        hass,
        DOMAIN,
        governance_config(
            slow_client={
                const.CONF_THRESHOLD: 3,
                const.CONF_ACTION: const.SLOW_CLIENT_ACTION_DROP,
            }
        ),
    )
    client = await hass_ws_client(hass)
    instance = handler_instance[0]

    @callback
    @websocket_command({"type": "fake_subscription"})
    def fake_subscription(
        hass: HomeAssistant, connection: ActiveConnection, msg: dict[str, Any]
    ) -> None:
        """Register a fake subscription."""
        connection.subscriptions[msg["id"]] = callback(lambda: None)
        connection.send_result(msg["id"])

    async_register_command(hass, fake_subscription)

    await client.send_json({"id": 2, "type": "fake_subscription"})
    assert (await client.receive_json())["success"]

    # Burst without awaiting: the writer cannot drain between calls.
    for value in range(5):
        instance._async_governed_send_message(
            messages.event_message(2, {"value": value})
        )

    governor = instance._governor
    assert governor is not None
    attributed = [
        item
        for item in instance._message_queue
        if isinstance(item, QueuedSubscriptionMessage)
    ]
    # Only threshold many attributed messages are queued.
    assert len(instance._message_queue) == 3
    assert len(attributed) == 3
    stats = governor.snapshot()["subscriptions"]["2"]
    assert stats["produced"] == 5
    assert stats["pending"] == 3
    assert stats["dropped"] == 2
    assert stats["merged"] == 0

    # Drain the queued messages.
    for _ in range(3):
        await client.receive_json()

    stats = governor.snapshot()["subscriptions"]["2"]
    assert stats["pending"] == 0
    assert stats["delivered"] == 3

    # Once the backlog is drained, delivery resumes message by message.
    for value in range(5, 7):
        instance._async_governed_send_message(
            messages.event_message(2, {"value": value})
        )
    stats = governor.snapshot()["subscriptions"]["2"]
    assert stats["pending"] == 2
    assert stats["dropped"] == 2
    for _ in range(2):
        await client.receive_json()


async def test_merge_action(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    handler_instance: list[WebSocketHandler],
) -> None:
    """State change messages are merged towards the latest state."""
    assert await async_setup_component(
        hass,
        DOMAIN,
        governance_config(
            slow_client={
                const.CONF_THRESHOLD: 2,
                const.CONF_ACTION: const.SLOW_CLIENT_ACTION_MERGE,
            }
        ),
    )
    client = await hass_ws_client(hass)
    instance = handler_instance[0]

    @callback
    @websocket_command({"type": "fake_subscription"})
    def fake_subscription(
        hass: HomeAssistant, connection: ActiveConnection, msg: dict[str, Any]
    ) -> None:
        """Register a fake subscription."""
        connection.subscriptions[msg["id"]] = callback(lambda: None)
        connection.send_result(msg["id"])

    async_register_command(hass, fake_subscription)

    await client.send_json({"id": 2, "type": "fake_subscription"})
    assert (await client.receive_json())["success"]

    def state_diff(value: int) -> dict[str, Any]:
        return {"c": {"light.test": {"s": value}}}

    for value in range(4):
        instance._async_governed_send_message(
            messages.event_message(2, state_diff(value))
        )
    # A non-state event is not mergeable and is queued even over threshold.
    instance._async_governed_send_message(messages.event_message(2, {"ping": 1}))

    governor = instance._governor
    assert governor is not None
    # m0, m1, the merge holder and the non-state event are queued.
    assert len(instance._message_queue) == 4
    assert all(
        isinstance(item, QueuedSubscriptionMessage) for item in instance._message_queue
    )
    # The coalescing holder carries the latest state of the merged messages.
    assert governor.coalescing == {2: instance._message_queue[2]}
    assert governor.coalescing[2].payload == messages.message_to_json_bytes(
        messages.event_message(2, state_diff(3))
    )
    stats = governor.snapshot()["subscriptions"]["2"]
    assert stats["produced"] == 5
    assert stats["merged"] == 1
    assert stats["dropped"] == 0
    assert stats["pending"] == 4

    received = [await client.receive_json() for _ in range(4)]
    assert received[0]["event"] == state_diff(0)
    assert received[1]["event"] == state_diff(1)
    # The third state message was merged away; the holder reflects last state.
    assert received[2]["event"] == state_diff(3)
    assert received[3]["event"] == {"ping": 1}

    # After draining, new state messages are delivered individually again.
    instance._async_governed_send_message(messages.event_message(2, state_diff(4)))
    assert (await client.receive_json())["event"] == state_diff(4)


async def test_merge_real_state_changed_events(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    handler_instance: list[WebSocketHandler],
) -> None:
    """Merging cached state_changed events delivers the latest state only."""
    assert await async_setup_component(
        hass,
        DOMAIN,
        governance_config(
            slow_client={
                const.CONF_THRESHOLD: 2,
                const.CONF_ACTION: const.SLOW_CLIENT_ACTION_MERGE,
            }
        ),
    )
    client = await hass_ws_client(hass)

    await client.send_json(
        {
            "id": 1,
            "type": "subscribe_events",
            "event_type": EVENT_STATE_CHANGED,
        }
    )
    assert (await client.receive_json())["success"]

    instance = handler_instance[0]
    # Pause the writer side by filling the queue in a single synchronous burst.
    for value in range(4):
        hass.states.async_set("light.test", str(value))

    governor = instance._governor
    assert governor is not None
    stats = governor.snapshot()["subscriptions"]["1"]
    assert stats["produced"] == 4
    assert stats["merged"] == 1
    assert stats["dropped"] == 0

    received = [await client.receive_json() for _ in range(3)]
    delivered_states = [msg["event"]["data"]["new_state"]["state"] for msg in received]
    # The merged message carries the final state, which is also the stored state.
    assert delivered_states == ["0", "1", "3"]
    assert hass.states.get("light.test").state == "3"


async def test_drop_counters_per_subscription(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    handler_instance: list[WebSocketHandler],
) -> None:
    """Drop counters are attributed to the producing subscription accurately."""
    assert await async_setup_component(
        hass,
        DOMAIN,
        governance_config(
            slow_client={
                const.CONF_THRESHOLD: 1,
                const.CONF_ACTION: const.SLOW_CLIENT_ACTION_DROP,
            }
        ),
    )
    client = await hass_ws_client(hass)
    instance = handler_instance[0]

    # Subscription ids 2 and 3, one queued message each, second one dropped.
    instance._async_governed_send_message(messages.event_message(2, {"v": 0}))
    instance._async_governed_send_message(messages.event_message(3, {"v": 0}))
    instance._async_governed_send_message(messages.event_message(2, {"v": 1}))
    instance._async_governed_send_message(messages.event_message(3, {"v": 1}))

    governor = instance._governor
    assert governor is not None
    assert governor.snapshot()["subscriptions"]["2"]["dropped"] == 1
    assert governor.snapshot()["subscriptions"]["3"]["dropped"] == 1
    assert governor.snapshot()["subscriptions"]["2"]["pending"] == 1
    assert governor.snapshot()["subscriptions"]["3"]["pending"] == 1

    await client.close()


async def test_info_command_sorted_and_admin_only(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    hass_read_only_access_token: str,
) -> None:
    """The info command is admin only and sorts connections by id."""
    assert await async_setup_component(
        hass,
        DOMAIN,
        governance_config(max_per_connection=10, max_system=100),
    )
    admin_client = await hass_ws_client(hass)
    read_only_client = await hass_ws_client(hass, hass_read_only_access_token)

    await read_only_client.send_json({"id": 1, "type": "subscription_governance/info"})
    msg = await read_only_client.receive_json()
    assert not msg["success"]
    assert msg["error"]["code"] == const.ERR_UNAUTHORIZED

    await admin_client.send_json({"id": 1, "type": "subscribe_events"})
    assert (await admin_client.receive_json())["success"]

    await admin_client.send_json({"id": 2, "type": "subscription_governance/info"})
    msg = await admin_client.receive_json()
    assert msg["success"]
    result = msg["result"]
    assert result["enabled"] is True
    assert result["limits"] == {
        const.CONF_MAX_PER_CONNECTION: 10,
        const.CONF_MAX_SYSTEM: 100,
    }
    assert result["active_connections"] == 2
    assert result["active_subscriptions"] == 1
    connection_ids = [conn["connection_id"] for conn in result["connections"]]
    assert connection_ids == sorted(connection_ids)
    admin_conn = next(
        conn for conn in result["connections"] if conn["subscription_count"]
    )
    assert admin_conn["subscription_count"] == 1
    assert "1" in admin_conn["subscriptions"]


async def test_disconnect_releases_system_quota(
    hass: HomeAssistant,
    hass_ws_client: WebSocketGenerator,
    handler_instance: list[WebSocketHandler],
) -> None:
    """Closing a connection frees its subscription slots and registry entry."""
    assert await async_setup_component(hass, DOMAIN, governance_config(max_system=1))
    client1 = await hass_ws_client(hass)

    await client1.send_json({"id": 1, "type": "subscribe_events"})
    assert (await client1.receive_json())["success"]

    await client1.close()
    await hass.async_block_till_done()

    client2 = await hass_ws_client(hass)
    await client2.send_json({"id": 1, "type": "subscribe_events"})
    assert (await client2.receive_json())["success"]

    governor = handler_instance[-1]._governor
    assert governor is not None
    assert governor.governance.total_subscriptions == 1
    assert governor.governance.async_snapshot()["active_connections"] == 1
