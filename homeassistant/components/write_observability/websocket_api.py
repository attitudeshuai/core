"""Websocket API for the state write observability integration."""

import datetime
from typing import Any

import probatio

from homeassistant.components import websocket_api
from homeassistant.components.websocket_api import ActiveConnection
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.write_observation import (
    DATA_WRITE_OBSERVATION,
    DEFAULT_CHUNK_SIZE,
    GROUP_BY_ENTITY_DOMAIN,
    GROUP_BY_INTEGRATION,
    WriteObservationError,
    WriteObservationManager,
)
from homeassistant.util import dt as dt_util

_STATS_SCHEMA: dict[str | probatio.Marker, Any] = {
    probatio.Optional("entity_ids"): probatio.All(
        probatio.EnsureList(), [cv.entity_id]
    ),
    probatio.Optional("start_time"): str,
    probatio.Optional("end_time"): str,
    probatio.Optional("limit", default=DEFAULT_CHUNK_SIZE): probatio.All(
        probatio.Coerce(int), probatio.Range(min=0)
    ),
    probatio.Optional("offset", default=0): probatio.All(
        probatio.Coerce(int), probatio.Range(min=0)
    ),
}


@callback
def async_load_websocket_api(hass: HomeAssistant) -> None:
    """Register the websocket commands."""
    websocket_api.async_register_command(hass, handle_entity_stats)
    websocket_api.async_register_command(hass, handle_domain_stats)
    websocket_api.async_register_command(hass, handle_rate_stats)


def _get_manager(
    hass: HomeAssistant, connection: ActiveConnection, msg_id: int
) -> WriteObservationManager | None:
    """Get the manager or send an error if observation is not enabled."""
    manager: WriteObservationManager | None = hass.data.get(DATA_WRITE_OBSERVATION)
    if manager is None:
        connection.send_error(
            msg_id,
            websocket_api.ERR_NOT_FOUND,
            "State write observation is not enabled",
        )
    return manager


def _parse_time(value: str | None, name: str) -> datetime.datetime | None:
    """Parse an ISO 8601 timestamp with time zone."""
    if value is None:
        return None
    parsed = dt_util.parse_datetime(value)
    if parsed is None or parsed.tzinfo is None:
        raise WriteObservationError(
            f"{name} must be an ISO 8601 timestamp with time zone"
        )
    return parsed


def _parse_time_range(
    msg: dict[str, Any],
) -> tuple[datetime.datetime | None, datetime.datetime | None]:
    """Parse and validate the requested window bounds."""
    start = _parse_time(msg.get("start_time"), "start_time")
    end = _parse_time(msg.get("end_time"), "end_time")
    if start is not None and end is not None and start > end:
        raise WriteObservationError("start_time must not be after end_time")
    return start, end


@websocket_api.websocket_command(
    {
        probatio.Required("type"): "write_observability/entity_stats",
        **_STATS_SCHEMA,
    }
)
@callback
def handle_entity_stats(
    hass: HomeAssistant, connection: ActiveConnection, msg: dict[str, Any]
) -> None:
    """Handle per entity write attribution queries."""
    manager = _get_manager(hass, connection, msg["id"])
    if manager is None:
        return
    try:
        start, end = _parse_time_range(msg)
        result = manager.async_get_entity_stats(
            entity_ids=msg.get("entity_ids"),
            start_time=start,
            end_time=end,
            limit=msg["limit"],
            offset=msg["offset"],
        )
    except WriteObservationError as err:
        connection.send_error(msg["id"], websocket_api.ERR_INVALID_FORMAT, str(err))
        return
    connection.send_result(msg["id"], result.as_dict())


@websocket_api.websocket_command(
    {
        probatio.Required("type"): "write_observability/domain_stats",
        probatio.Optional("group_by", default=GROUP_BY_ENTITY_DOMAIN): probatio.In(
            [GROUP_BY_ENTITY_DOMAIN, GROUP_BY_INTEGRATION]
        ),
        **_STATS_SCHEMA,
    }
)
@callback
def handle_domain_stats(
    hass: HomeAssistant, connection: ActiveConnection, msg: dict[str, Any]
) -> None:
    """Handle per domain write aggregation queries."""
    manager = _get_manager(hass, connection, msg["id"])
    if manager is None:
        return
    try:
        start, end = _parse_time_range(msg)
        result = manager.async_get_domain_stats(
            group_by=msg["group_by"],
            entity_ids=msg.get("entity_ids"),
            start_time=start,
            end_time=end,
            limit=msg["limit"],
            offset=msg["offset"],
        )
    except WriteObservationError as err:
        connection.send_error(msg["id"], websocket_api.ERR_INVALID_FORMAT, str(err))
        return
    connection.send_result(msg["id"], result.as_dict())


@websocket_api.websocket_command(
    {
        probatio.Required("type"): "write_observability/rate_stats",
        probatio.Required("rate_threshold"): probatio.All(
            probatio.Coerce(float), probatio.Range(min=0, min_included=False)
        ),
        **_STATS_SCHEMA,
    }
)
@callback
def handle_rate_stats(
    hass: HomeAssistant, connection: ActiveConnection, msg: dict[str, Any]
) -> None:
    """Handle write rate threshold queries."""
    manager = _get_manager(hass, connection, msg["id"])
    if manager is None:
        return
    try:
        start, end = _parse_time_range(msg)
        result = manager.async_get_rate_stats(
            rate_threshold=msg["rate_threshold"],
            entity_ids=msg.get("entity_ids"),
            start_time=start,
            end_time=end,
            limit=msg["limit"],
            offset=msg["offset"],
        )
    except WriteObservationError as err:
        connection.send_error(msg["id"], websocket_api.ERR_INVALID_FORMAT, str(err))
        return
    connection.send_result(msg["id"], result.as_dict())
