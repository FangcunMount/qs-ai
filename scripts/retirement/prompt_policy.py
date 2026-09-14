"""Conservative reference audit for the five explicitly retired Prompt identities."""

import json

TEMPLATE_ID = "cross-dimension-participant-scale"
VERSIONS = ("v1", "v2", "v3", "v4", "v5")


def candidate(row):
    return row.get("template_id") == TEMPLATE_ID and row.get("version") in VERSIONS


def references(value, asset):
    """Unknown/unversioned uses retain the asset; no timestamp or status inference."""
    if isinstance(value, dict):
        identity_keys = {"id", "identity", "template_id", "TemplateID"}
        version_keys = {"version", "Version"}
        identities = [value[key] for key in identity_keys & value.keys()]
        versions = [value[key] for key in version_keys & value.keys()]
        if TEMPLATE_ID in identities:
            if not versions or asset["version"] in versions:
                return True
            # A fully qualified different version does not refer to this asset.
            value = {k: v for k, v in value.items() if k not in identity_keys | version_keys}
        return any(references(item, asset) for item in value.values())
    if isinstance(value, (tuple, list)):
        return any(references(item, asset) for item in value)
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if not isinstance(value, str):
        return False
    for key in ("fingerprint", "package_sha256"):
        token = asset.get(key)
        if token and token in value:
            return True
    try:
        decoded = json.loads(value)
    except (ValueError, TypeError):
        return TEMPLATE_ID in value
    if isinstance(decoded, (dict, list)):
        return references(decoded, asset)
    return TEMPLATE_ID in value


def reference_tables(asset, tables):
    key = (asset["template_id"], asset["version"])
    return sorted(
        name
        for name, rows in tables.items()
        if any(
            references(row, asset)
            for row in rows
            if not (name == "prompt_assets" and (row.get("template_id"), row.get("version")) == key)
        )
    )


def unreferenced(assets, tables):
    return {
        (asset["template_id"], asset["version"])
        for asset in assets
        if candidate(asset)
        and asset.get("fingerprint")
        and asset.get("package_sha256")
        and not reference_tables(asset, tables)
    }
