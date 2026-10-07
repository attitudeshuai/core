"""WebSocket based API for Home Assistant."""

from typing import Final, cast

import probatio

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.typing import ConfigType, VolSchemaType

from . import (  # noqa: F401
    commands,
    connection,
    const,
    decorators,
    governance,
    http,
    messages,
)
from .connection import ActiveConnection, current_connection  # noqa: F401
from .const import (  # noqa: F401
    ERR_HOME_ASSISTANT_ERROR,
    ERR_INVALID_FORMAT,
    ERR_NOT_ALLOWED,
    ERR_NOT_FOUND,
    ERR_NOT_SUPPORTED,
    ERR_SERVICE_VALIDATION_ERROR,
    ERR_TEMPLATE_ERROR,
    ERR_TIMEOUT,
    ERR_UNAUTHORIZED,
    ERR_UNKNOWN_COMMAND,
    ERR_UNKNOWN_ERROR,
    TYPE_RESULT,
    AsyncWebSocketCommandHandler,
    WebSocketCommandHandler,
)
from .decorators import (  # noqa: F401
    async_response,
    require_admin,
    websocket_command,
    ws_require_user,
)
from .messages import (  # noqa: F401
    BASE_COMMAND_MESSAGE_SCHEMA,
    error_message,
    event_message,
    result_message,
)

DOMAIN: Final = const.DOMAIN

DEPENDENCIES: Final[tuple[str]] = ("http",)

# 0 means the quota dimension is disabled. Negative limits are rejected.
_SUBSCRIPTION_QUOTA = probatio.All(probatio.Coerce(int), probatio.Range(min=0))

_SLOW_CLIENT_SCHEMA: Final = probatio.Schema(
    {
        probatio.Required(const.CONF_THRESHOLD): probatio.All(
            probatio.Coerce(int), probatio.Range(min=1)
        ),
        probatio.Required(const.CONF_ACTION): probatio.In(
            [
                const.SLOW_CLIENT_ACTION_MERGE,
                const.SLOW_CLIENT_ACTION_DROP,
            ]
        ),
    }
)

_SUBSCRIPTION_GOVERNANCE_SCHEMA: Final = probatio.Schema(
    {
        probatio.Optional(
            const.CONF_MAX_PER_CONNECTION, default=0
        ): _SUBSCRIPTION_QUOTA,
        probatio.Optional(const.CONF_MAX_SYSTEM, default=0): _SUBSCRIPTION_QUOTA,
        probatio.Optional(const.CONF_SLOW_CLIENT): _SLOW_CLIENT_SCHEMA,
    }
)

CONFIG_SCHEMA: Final = probatio.Schema(
    {
        probatio.Optional(DOMAIN, default={}): probatio.Schema(
            {
                probatio.Optional(const.CONF_SUBSCRIPTION_GOVERNANCE): (
                    _SUBSCRIPTION_GOVERNANCE_SCHEMA
                )
            }
        )
    },
    extra=probatio.ALLOW_EXTRA,
)


@callback
def async_register_command(
    hass: HomeAssistant,
    command_or_handler: str | const.WebSocketCommandHandler,
    handler: const.WebSocketCommandHandler | None = None,
    schema: VolSchemaType | None = None,
) -> None:
    """Register a websocket command."""
    if handler is None:
        handler = cast(const.WebSocketCommandHandler, command_or_handler)
        command = handler._ws_command  # type: ignore[attr-defined]  # noqa: SLF001
        schema = handler._ws_schema  # type: ignore[attr-defined]  # noqa: SLF001
    else:
        command = command_or_handler
    if (handlers := hass.data.get(DOMAIN)) is None:
        handlers = hass.data[DOMAIN] = {}
    handlers[command] = (handler, schema)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Initialize the websocket API."""
    hass.http.register_view(http.WebsocketAPIView())
    commands.async_register_commands(hass, async_register_command)

    if governance_config := (config.get(DOMAIN) or {}).get(
        const.CONF_SUBSCRIPTION_GOVERNANCE
    ):
        slow_client_config = governance_config.get(const.CONF_SLOW_CLIENT)
        hass.data[const.DATA_SUBSCRIPTION_GOVERNANCE] = (
            governance.SubscriptionGovernance(
                max_per_connection=governance_config[const.CONF_MAX_PER_CONNECTION],
                max_system=governance_config[const.CONF_MAX_SYSTEM],
                slow_client_threshold=(
                    slow_client_config[const.CONF_THRESHOLD]
                    if slow_client_config
                    else 0
                ),
                slow_client_action=(
                    slow_client_config[const.CONF_ACTION]
                    if slow_client_config
                    else None
                ),
            )
        )

    return True
