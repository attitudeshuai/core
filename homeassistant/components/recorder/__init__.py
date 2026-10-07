"""Support for recording details."""

import logging
from typing import Any

import probatio

from homeassistant.const import (
    CONF_EXCLUDE,
    EVENT_RECORDER_5MIN_STATISTICS_GENERATED,  # noqa: F401
    EVENT_RECORDER_HOURLY_STATISTICS_GENERATED,  # noqa: F401
    EVENT_STATE_CHANGED,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.entityfilter import (
    INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA,
    INCLUDE_EXCLUDE_FILTER_SCHEMA_INNER,
    convert_include_exclude_filter,
)
from homeassistant.helpers.integration_platform import (
    async_process_integration_platforms,
)
from homeassistant.helpers.recorder import DATA_INSTANCE
from homeassistant.helpers.typing import ConfigType
from homeassistant.util.event_type import EventType

# Pre-import backup to avoid it being imported
# later when the import executor is busy and delaying
# startup
from . import (
    backup,  # noqa: F401
    entity_options,
    entity_registry,
    websocket_api,
)
from .const import (  # noqa: F401
    CONF_DB_INTEGRITY_CHECK,
    DOMAIN,
    INTEGRATION_PLATFORM_COMPILE_STATISTICS,
    INTEGRATION_PLATFORM_METHODS,
    SQLITE_URL_PREFIX,
    SupportedDialect,
)
from .core import Recorder
from .entity_options import is_entity_recorded  # noqa: F401
from .retention import (
    CONF_ENTITY_DOMAINS,
    CONF_ENTITY_ID_PREFIXES,
    CONF_ENTITY_IDS,
    CONF_INTEGRATION_DOMAINS,
    CONF_KEEP_DAYS as CONF_RETENTION_KEEP_DAYS,
    CONF_RETENTION,
    CONF_RETENTION_DEFAULT_KEEP_DAYS,
    CONF_RETENTION_ENABLED,
    CONF_RETENTION_RULES,
    CONF_RULE_NAME,
    MAX_RETENTION_KEEP_DAYS,
    config_from_yaml,
)
from .services import async_setup_services
from .tasks import AddRecorderPlatformTask
from .util import get_instance

_LOGGER = logging.getLogger(__name__)


DEFAULT_URL = "sqlite:///{hass_config_path}"
DEFAULT_DB_FILE = "home-assistant_v2.db"
DEFAULT_DB_INTEGRITY_CHECK = True
DEFAULT_DB_MAX_RETRIES = 10
DEFAULT_DB_RETRY_WAIT = 3
DEFAULT_COMMIT_INTERVAL = 5

CONF_AUTO_PURGE = "auto_purge"
CONF_AUTO_REPACK = "auto_repack"
CONF_DB_URL = "db_url"
CONF_DB_MAX_RETRIES = "db_max_retries"
CONF_DB_RETRY_WAIT = "db_retry_wait"
CONF_PURGE_KEEP_DAYS = "purge_keep_days"
CONF_PURGE_INTERVAL = "purge_interval"
CONF_EVENT_TYPES = "event_types"
CONF_COMMIT_INTERVAL = "commit_interval"


EXCLUDE_SCHEMA = INCLUDE_EXCLUDE_FILTER_SCHEMA_INNER.extend(
    {
        probatio.Optional(CONF_EVENT_TYPES): probatio.All(
            probatio.EnsureList(), [cv.string]
        )
    }
)

FILTER_SCHEMA = INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA.extend(
    {probatio.Optional(CONF_EXCLUDE, default=EXCLUDE_SCHEMA({})): EXCLUDE_SCHEMA}
)


_RETENTION_KEEP_DAYS_SCHEMA = probatio.All(
    probatio.Coerce(int), probatio.Range(min=0, max=MAX_RETENTION_KEEP_DAYS)
)

_RETENTION_RULE_CONDITION_KEYS = (
    CONF_ENTITY_DOMAINS,
    CONF_INTEGRATION_DOMAINS,
    CONF_ENTITY_ID_PREFIXES,
    CONF_ENTITY_IDS,
)


def _normalize_retention_rule(rule: dict) -> dict:
    """Normalize a retention rule, lowercasing domain conditions."""
    rule[CONF_ENTITY_DOMAINS] = sorted(
        {domain.lower() for domain in rule[CONF_ENTITY_DOMAINS]}
    )
    rule[CONF_INTEGRATION_DOMAINS] = sorted(
        {domain.lower() for domain in rule[CONF_INTEGRATION_DOMAINS]}
    )
    rule[CONF_ENTITY_ID_PREFIXES] = list(rule[CONF_ENTITY_ID_PREFIXES])
    rule[CONF_ENTITY_IDS] = sorted(set(rule[CONF_ENTITY_IDS]))
    return rule


_RETENTION_RULE_SCHEMA = probatio.All(
    probatio.Schema(
        {
            probatio.Optional(CONF_RULE_NAME): cv.string,
            probatio.Required(CONF_RETENTION_KEEP_DAYS): _RETENTION_KEEP_DAYS_SCHEMA,
            probatio.Optional(CONF_ENTITY_DOMAINS, default=list): probatio.All(
                probatio.EnsureList(), [cv.string]
            ),
            probatio.Optional(CONF_INTEGRATION_DOMAINS, default=list): probatio.All(
                probatio.EnsureList(), [cv.string]
            ),
            probatio.Optional(CONF_ENTITY_ID_PREFIXES, default=list): probatio.All(
                probatio.EnsureList(), [cv.string]
            ),
            probatio.Optional(CONF_ENTITY_IDS, default=list): probatio.All(
                probatio.EnsureList(), [cv.entity_id]
            ),
        }
    ),
    probatio.Any(
        *(
            probatio.Schema(
                {probatio.Required(key): probatio.IsTrue()},
                extra=probatio.ALLOW_EXTRA,
            )
            for key in _RETENTION_RULE_CONDITION_KEYS
        ),
        msg=(
            "A retention rule must match entities using at least one of "
            "'domains', 'integration_domains', 'entity_id_prefixes' or "
            "'entity_ids'"
        ),
    ),
    _normalize_retention_rule,
)


def _validate_retention_schema(conf: dict) -> dict:
    """Validate the retention section once the whole section is parsed."""
    if conf[CONF_RETENTION_ENABLED] and CONF_RETENTION_DEFAULT_KEEP_DAYS not in conf:
        raise probatio.Invalid(
            f"'{CONF_RETENTION_DEFAULT_KEEP_DAYS}' is required when "
            f"'{CONF_RETENTION_ENABLED}' is true"
        )
    return conf


RETENTION_SCHEMA = probatio.All(
    probatio.Schema(
        {
            probatio.Optional(CONF_RETENTION_ENABLED, default=False): cv.boolean,
            probatio.Optional(
                CONF_RETENTION_DEFAULT_KEEP_DAYS
            ): _RETENTION_KEEP_DAYS_SCHEMA,
            probatio.Optional(CONF_RETENTION_RULES, default=list): [
                _RETENTION_RULE_SCHEMA
            ],
        }
    ),
    _validate_retention_schema,
)


ALLOW_IN_MEMORY_DB = False


def validate_db_url(db_url: str) -> Any:
    """Validate database URL."""
    # Don't allow on-memory sqlite databases
    if (
        db_url == SQLITE_URL_PREFIX
        or (db_url.startswith(SQLITE_URL_PREFIX) and ":memory:" in db_url)
    ) and not ALLOW_IN_MEMORY_DB:
        raise probatio.Invalid("In-memory SQLite database is not supported")

    return db_url


CONFIG_SCHEMA = probatio.Schema(
    {
        probatio.Optional(DOMAIN, default=dict): probatio.All(
            cv.deprecated(CONF_PURGE_INTERVAL),
            cv.deprecated(CONF_DB_INTEGRITY_CHECK),
            FILTER_SCHEMA.extend(
                {
                    probatio.Optional(CONF_AUTO_PURGE, default=True): cv.boolean,
                    probatio.Optional(CONF_AUTO_REPACK, default=True): cv.boolean,
                    probatio.Optional(CONF_PURGE_KEEP_DAYS, default=10): probatio.All(
                        probatio.Coerce(int), probatio.Range(min=1)
                    ),
                    probatio.Optional(CONF_PURGE_INTERVAL, default=1): cv.positive_int,
                    probatio.Optional(CONF_DB_URL): probatio.All(
                        cv.string, validate_db_url
                    ),
                    probatio.Optional(
                        CONF_COMMIT_INTERVAL, default=DEFAULT_COMMIT_INTERVAL
                    ): cv.positive_int,
                    probatio.Optional(
                        CONF_DB_MAX_RETRIES, default=DEFAULT_DB_MAX_RETRIES
                    ): cv.positive_int,
                    probatio.Optional(
                        CONF_DB_RETRY_WAIT, default=DEFAULT_DB_RETRY_WAIT
                    ): cv.positive_int,
                    probatio.Optional(
                        CONF_DB_INTEGRITY_CHECK, default=DEFAULT_DB_INTEGRITY_CHECK
                    ): cv.boolean,
                    probatio.Optional(CONF_RETENTION): RETENTION_SCHEMA,
                }
            ),
        )
    },
    extra=probatio.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the recorder."""
    conf = config[DOMAIN]
    _filter = convert_include_exclude_filter(conf)
    entity_filter = None if _filter.empty_filter else _filter.get_filter()
    auto_purge = conf[CONF_AUTO_PURGE]
    auto_repack = conf[CONF_AUTO_REPACK]
    keep_days = conf[CONF_PURGE_KEEP_DAYS]
    commit_interval = conf[CONF_COMMIT_INTERVAL]
    db_max_retries = conf[CONF_DB_MAX_RETRIES]
    db_retry_wait = conf[CONF_DB_RETRY_WAIT]
    db_url = conf.get(CONF_DB_URL) or get_default_url(hass)
    exclude = conf[CONF_EXCLUDE]
    exclude_event_types: set[EventType[Any] | str] = set(
        exclude.get(CONF_EVENT_TYPES, [])
    )
    if EVENT_STATE_CHANGED in exclude_event_types:
        _LOGGER.error("State change events cannot be excluded, use a filter instead")
        exclude_event_types.remove(EVENT_STATE_CHANGED)
    retention_config = config_from_yaml(conf.get(CONF_RETENTION))
    instance = hass.data[DATA_INSTANCE] = Recorder(
        hass=hass,
        auto_purge=auto_purge,
        auto_repack=auto_repack,
        keep_days=keep_days,
        commit_interval=commit_interval,
        uri=db_url,
        db_max_retries=db_max_retries,
        db_retry_wait=db_retry_wait,
        entity_filter=entity_filter,
        exclude_event_types=exclude_event_types,
        retention_config=retention_config,
    )
    get_instance.cache_clear()
    entity_registry.async_setup(hass)
    entity_options.async_setup(hass)
    instance.async_initialize()
    instance.async_register()
    instance.start()
    async_setup_services(hass)
    websocket_api.async_setup(hass)

    await _async_setup_integration_platform(hass, instance)

    return await instance.async_db_ready


async def _async_setup_integration_platform(
    hass: HomeAssistant, instance: Recorder
) -> None:
    """Set up a recorder integration platform."""

    @callback
    def _process_recorder_platform(
        hass: HomeAssistant, domain: str, platform: Any
    ) -> None:
        """Process a recorder platform."""
        # If the platform has a compile_statistics method, we need to
        # add it to the recorder queue to be processed.
        if any(hasattr(platform, _attr) for _attr in INTEGRATION_PLATFORM_METHODS):
            instance.queue_task(AddRecorderPlatformTask(domain, platform))

    await async_process_integration_platforms(hass, DOMAIN, _process_recorder_platform)


def get_default_url(hass: HomeAssistant) -> str:
    """Return the default URL."""
    return DEFAULT_URL.format(hass_config_path=hass.config.path(DEFAULT_DB_FILE))
