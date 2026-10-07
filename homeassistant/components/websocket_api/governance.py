"""Subscription governance: quotas and slow-client backpressure.

The governance layer is opt-in and only exists when the user configures
``subscription_governance`` for the websocket_api integration. When it is not
configured, none of the objects in this module are instantiated and message
delivery follows the original code path.

Two mechanisms are implemented:

- Subscription quotas on two levels, per connection and system wide. The
  connection's subscription dictionary acquires a slot on insertion and
  releases it when the subscription is removed.
- Slow-client handling per subscription. Messages attributed to a
  subscription are counted as produced, delivered, dropped or merged. When a
  subscription has more messages pending than the configured threshold, state
  change messages are either merged towards the latest state or dropped.

Dropping and merging only affect messages queued for delivery over the
long-lived websocket connection. Entity state, event bus dispatch and recorder
writes are never touched.
"""

import re
from typing import TYPE_CHECKING, Any, Final

from homeassistant.const import EVENT_STATE_CHANGED
from homeassistant.exceptions import HomeAssistantError

from . import const

if TYPE_CHECKING:
    from .connection import ActiveConnection


QUOTA_SCOPE_CONNECTION: Final = "connection"
QUOTA_SCOPE_SYSTEM: Final = "system"

# Event messages produced by the websocket API use one of two compact layouts:
# cached_event_message/cached_state_diff_message put the subscription id last:
#   b'{"type":"event","event":...,"id":12}'
# construct_event_message and the initial subscribe_entities payload put it
# first:
#   b'{"id":12,"type":"event","event":...}'
_TRAILING_SUBSCRIPTION_ID: Final = re.compile(rb',"id":(\d+)}$')
_LEADING_EVENT_SUBSCRIPTION_ID: Final = re.compile(rb'^\{"id":(\d+),"type":"event"')

_STATE_CHANGED_EVENT_MARKER: Final = b'"event_type":"state_changed"'
_STATE_DIFF_EVENT_MARKERS: Final = (
    b'"event":{"a"',
    b'"event":{"c"',
    b'"event":{"r"',
)
_STATE_DIFF_EVENT_KEYS: Final = frozenset(("a", "c", "r"))


class SubscriptionQuotaExceeded(HomeAssistantError):
    """Raised when a new subscription is rejected because a quota is full."""

    def __init__(self, scope: str, limit: int) -> None:
        """Initialize the error."""
        self.scope = scope
        self.limit = limit
        if scope == QUOTA_SCOPE_CONNECTION:
            message = (
                f"Subscription quota exceeded: the per-connection limit of "
                f"{limit} active subscriptions has been reached."
            )
        else:
            message = (
                f"Subscription quota exceeded: the system-wide limit of "
                f"{limit} active subscriptions has been reached."
            )
        super().__init__(message)


class QueuedSubscriptionMessage:
    """A queued message attributed to a subscription.

    The payload is mutable so that, in merge mode, several state change
    messages can share one queued slot, always carrying the latest state.
    """

    __slots__ = ("payload", "subscription_id")

    def __init__(self, payload: bytes, subscription_id: int) -> None:
        """Initialize the queued message."""
        self.payload = payload
        self.subscription_id = subscription_id


def async_classify_event_message(message: Any, raw: bytes) -> tuple[int | None, bool]:
    """Classify an outgoing message.

    Returns a tuple of the subscription id the event message belongs to (or
    None when the message is not an event message) and whether the event is a
    state change that is eligible for merging.
    """
    if isinstance(message, dict):
        if message.get("type") != "event":
            return None, False
        subscription_id = message.get("id")
        event = message.get("event")
        is_state_change = isinstance(event, dict) and (
            event.get("event_type") == EVENT_STATE_CHANGED
            or (bool(event) and all(key in _STATE_DIFF_EVENT_KEYS for key in event))
        )
        if type(subscription_id) is not int:
            return None, False
        return subscription_id, is_state_change

    if (match := _TRAILING_SUBSCRIPTION_ID.search(raw)) is None:
        match = _LEADING_EVENT_SUBSCRIPTION_ID.match(raw)
        if match is None:
            return None, False
    is_state_change = _STATE_CHANGED_EVENT_MARKER in raw or any(
        marker in raw for marker in _STATE_DIFF_EVENT_MARKERS
    )
    return int(match.group(1)), is_state_change


class ConnectionGovernor:
    """Subscription governance state for a single connection."""

    __slots__ = (
        "active_subscriptions",
        "coalescing",
        "connection",
        "connection_id",
        "delivered",
        "dropped",
        "governance",
        "merged",
        "pending",
        "produced",
        "rejected",
    )

    def __init__(
        self, governance: SubscriptionGovernance, connection: ActiveConnection
    ) -> None:
        """Initialize the connection governor."""
        self.governance = governance
        self.connection = connection
        self.connection_id = governance.next_connection_id()
        self.active_subscriptions = 0
        self.rejected = 0
        self.pending: dict[int, int] = {}
        self.produced: dict[int, int] = {}
        self.delivered: dict[int, int] = {}
        self.dropped: dict[int, int] = {}
        self.merged: dict[int, int] = {}
        self.coalescing: dict[int, QueuedSubscriptionMessage] = {}

    @staticmethod
    def _increment(counters: dict[int, int], subscription_id: int) -> None:
        """Increment a counter for one subscription."""
        counters[subscription_id] = counters.get(subscription_id, 0) + 1

    def async_message_produced(self, subscription_id: int) -> None:
        """Record one message produced by a subscription."""
        self._increment(self.produced, subscription_id)

    def async_message_enqueued(self, subscription_id: int) -> None:
        """Record one attributed message added to the send queue."""
        self.pending[subscription_id] = self.pending.get(subscription_id, 0) + 1

    def async_message_popped(self, item: bytes | QueuedSubscriptionMessage) -> bytes:
        """Settle stats when the writer pops a queued item and return bytes."""
        if isinstance(item, QueuedSubscriptionMessage):
            subscription_id = item.subscription_id
            self.coalescing.pop(subscription_id, None)
            self.pending[subscription_id] -= 1
            self._increment(self.delivered, subscription_id)
            return item.payload
        return item

    def async_message_dropped(self, subscription_id: int) -> None:
        """Record one dropped message for a subscription."""
        self._increment(self.dropped, subscription_id)

    def async_message_merged(self, subscription_id: int) -> None:
        """Record one message merged into a previously queued message."""
        self._increment(self.merged, subscription_id)

    def async_rejected(self) -> None:
        """Record one subscription request rejected by the quota."""
        self.rejected += 1

    def snapshot(self) -> dict[str, Any]:
        """Build a JSON-serializable snapshot of the connection state."""
        subscription_ids = (
            self.produced.keys()
            | self.delivered.keys()
            | self.dropped.keys()
            | self.merged.keys()
            | self.pending.keys()
            | {key for key in self.connection.subscriptions if type(key) is int}
        )
        connection = self.connection
        return {
            "connection_id": self.connection_id,
            "user_id": connection.user.id,
            "user": connection.user.name,
            "remote": connection.remote,
            "subscription_count": self.active_subscriptions,
            "rejected": self.rejected,
            "subscriptions": {
                str(subscription_id): {
                    "produced": self.produced.get(subscription_id, 0),
                    "delivered": self.delivered.get(subscription_id, 0),
                    "pending": self.pending.get(subscription_id, 0),
                    "dropped": self.dropped.get(subscription_id, 0),
                    "merged": self.merged.get(subscription_id, 0),
                }
                for subscription_id in sorted(subscription_ids)
            },
        }


class SubscriptionGovernance:
    """System-wide subscription governance state."""

    __slots__ = (
        "_connection_seq",
        "_connections",
        "max_per_connection",
        "max_system",
        "slow_client_action",
        "slow_client_threshold",
        "total_rejected",
        "total_subscriptions",
    )

    def __init__(
        self,
        max_per_connection: int,
        max_system: int,
        slow_client_threshold: int,
        slow_client_action: str | None,
    ) -> None:
        """Initialize the governance manager."""
        self.max_per_connection = max_per_connection
        self.max_system = max_system
        self.slow_client_threshold = slow_client_threshold
        self.slow_client_action = slow_client_action
        self.total_subscriptions = 0
        self.total_rejected = 0
        self._connection_seq = 0
        self._connections: dict[int, ConnectionGovernor] = {}

    def next_connection_id(self) -> int:
        """Return the next stable connection identifier."""
        self._connection_seq += 1
        return self._connection_seq

    def async_register_connection(
        self, connection: ActiveConnection
    ) -> ConnectionGovernor:
        """Register a newly authenticated connection."""
        governor = ConnectionGovernor(self, connection)
        self._connections[governor.connection_id] = governor
        return governor

    def async_unregister_connection(self, governor: ConnectionGovernor) -> None:
        """Unregister a closed connection."""
        self._connections.pop(governor.connection_id, None)

    def async_acquire_subscription(self, governor: ConnectionGovernor) -> None:
        """Acquire an active subscription slot.

        Raises SubscriptionQuotaExceeded when the per-connection or the
        system-wide quota is reached.
        """
        if (
            self.max_per_connection
            and governor.active_subscriptions >= self.max_per_connection
        ):
            self.total_rejected += 1
            governor.async_rejected()
            raise SubscriptionQuotaExceeded(
                QUOTA_SCOPE_CONNECTION, self.max_per_connection
            )
        if self.max_system and self.total_subscriptions >= self.max_system:
            self.total_rejected += 1
            governor.async_rejected()
            raise SubscriptionQuotaExceeded(QUOTA_SCOPE_SYSTEM, self.max_system)
        governor.active_subscriptions += 1
        self.total_subscriptions += 1

    def async_release_subscription(self, governor: ConnectionGovernor) -> None:
        """Release one active subscription slot."""
        governor.active_subscriptions -= 1
        self.total_subscriptions -= 1

    def async_release_all(self, governor: ConnectionGovernor, count: int) -> None:
        """Release all active subscription slots of a connection."""
        governor.active_subscriptions -= count
        self.total_subscriptions -= count

    def async_snapshot(self) -> dict[str, Any]:
        """Build a JSON-serializable snapshot for the read-only info command."""
        slow_client: dict[str, Any] | None
        if self.slow_client_action is None:
            slow_client = None
        else:
            slow_client = {
                const.CONF_THRESHOLD: self.slow_client_threshold,
                const.CONF_ACTION: self.slow_client_action,
            }
        return {
            "enabled": True,
            "limits": {
                const.CONF_MAX_PER_CONNECTION: self.max_per_connection,
                const.CONF_MAX_SYSTEM: self.max_system,
            },
            "slow_client": slow_client,
            "active_connections": len(self._connections),
            "active_subscriptions": self.total_subscriptions,
            "rejected": self.total_rejected,
            "connections": [
                self._connections[connection_id].snapshot()
                for connection_id in sorted(self._connections)
            ],
        }
