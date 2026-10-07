"""State write observability integration.

Observes state machine writes and attributes them per entity and per domain.
Disabled unless configured.
"""

import probatio

from homeassistant.const import CONF_EXCLUDE, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.typing import ConfigType
from homeassistant.helpers.write_observation import (
    DATA_WRITE_OBSERVATION,
    DEFAULT_RETAIN,
    DEFAULT_SAMPLE_RATE,
    DEFAULT_WINDOW_SECONDS,
    WriteObservationManager,
)

from . import websocket_api
from .const import CONF_RETAIN, CONF_SAMPLE_RATE, CONF_WINDOW_SECONDS, DOMAIN

CONFIG_SCHEMA = probatio.Schema(
    {
        DOMAIN: probatio.Schema(
            {
                probatio.Optional(
                    CONF_SAMPLE_RATE, default=DEFAULT_SAMPLE_RATE
                ): probatio.All(probatio.Coerce(float), probatio.Range(min=0, max=1)),
                probatio.Optional(
                    CONF_WINDOW_SECONDS, default=DEFAULT_WINDOW_SECONDS
                ): probatio.All(
                    probatio.Coerce(float),
                    probatio.Range(min=0, min_included=False),
                ),
                probatio.Optional(CONF_RETAIN, default=DEFAULT_RETAIN): probatio.All(
                    probatio.Coerce(int), probatio.Range(min=0)
                ),
                probatio.Optional(CONF_EXCLUDE, default=list): probatio.All(
                    probatio.EnsureList(), [cv.string]
                ),
            }
        )
    },
    extra=probatio.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the state write observability integration."""
    conf = config.get(DOMAIN, {})
    manager = WriteObservationManager(
        hass,
        sample_rate=conf[CONF_SAMPLE_RATE],
        window_seconds=conf[CONF_WINDOW_SECONDS],
        retain=conf[CONF_RETAIN],
        exclude=tuple(conf[CONF_EXCLUDE]),
    )
    manager.async_connect()
    hass.data[DATA_WRITE_OBSERVATION] = manager
    websocket_api.async_load_websocket_api(hass)

    @callback
    def _disconnect(_event: object) -> None:
        manager.async_disconnect()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _disconnect)
    return True
