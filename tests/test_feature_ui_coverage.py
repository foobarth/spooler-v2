"""Every feature and config field that exists server-side must be reachable
in the UI somewhere.

The Features page renders groups from a hardcoded key list in
public/app-integrations.js (_FEATURE_GROUPS), the Integrations page from
_INTEGRATION_GROUPS, and notification channels from _NOTIF_CHANNELS in
app-settings.js. A feature or config field that no list claims simply never
shows up — which bit us twice in practice (the bridge config, then the
bridge's own feature flag). These tests pin the coupling.
"""

import re
from pathlib import Path

import config
import features

_FRONTEND = Path(__file__).parent.parent / "public"

# Features managed elsewhere in the UI, deliberately not on the Features page:
# external_api lives on the API-keys page; file_upload and report_problem are
# preconditions consumed by other features.
_NOT_ON_FEATURES_PAGE = {"external_api", "file_upload", "report_problem"}

# Config-field prefixes managed on their own pages, not the Integrations page:
# the notification channels on Notifications, the backup interval on Backup,
# the advanced-control log on the printer controls.
_NOT_INTEGRATIONS_PREFIXES = {"ntfy.", "telegram.", "discord.", "webhook.",
                              "backup.", "control."}


def _block(const_name: str) -> str:
    source = (_FRONTEND / "app-integrations.js").read_text()
    start = source.index(f"const {const_name}")
    return source[start:source.index("];", start)]


def test_every_feature_is_listed_on_the_features_page():
    listed = set(re.findall(r'"([a-z_0-9]+)"', _block("_FEATURE_GROUPS")))
    missing = [k for k in features.FEATURES
               if k not in listed and k not in _NOT_ON_FEATURES_PAGE
               and not k.startswith("notify_")]   # channels: pinned separately
    assert not missing, (
        f"features with no row on the Features page: {missing} — add them to "
        f"_FEATURE_GROUPS in public/app-integrations.js or to the allowlist here"
    )


def test_every_notification_feature_is_a_channel_on_the_notifications_page():
    source = (_FRONTEND / "app-settings.js").read_text()
    start = source.index("const _NOTIF_CHANNELS")
    block = source[start:source.index("];", start)]
    channels = set(re.findall(r'feature: "([a-z_0-9]+)"', block))
    missing = [k for k in features.FEATURES
               if k.startswith("notify_") and k not in channels]
    assert not missing, (
        f"notification features with no channel row on the Notifications page: "
        f"{missing} — add them to _NOTIF_CHANNELS in public/app-settings.js"
    )


def test_every_config_field_has_an_integrations_group():
    prefixes = set(re.findall(r'prefix: "([a-z_0-9]+\.)"', _block("_INTEGRATION_GROUPS")))
    unclaimed = sorted({k.split(".", 1)[0] + "." for k in config.FIELDS}
                       - prefixes - _NOT_INTEGRATIONS_PREFIXES)
    assert not unclaimed, (
        f"config field prefixes belonging to no Integrations group: {unclaimed} — "
        f"add a prefix group in _INTEGRATION_GROUPS or to the allowlist here"
    )
