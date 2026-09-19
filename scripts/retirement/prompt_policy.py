"""Conservative reference audit for the five explicitly retired Prompt identities."""

import json

TEMPLATE_ID = "cross-dimension-participant-scale"
VERSIONS = ("v1", "v2", "v3", "v4", "v5")


def candidate(row):
    return row.get("template_id") == TEMPLATE_ID and row.get("version") in VERSIONS


def references(value, asset):
    """Unknown/unversioned uses retain the asset; no timestamp or status inference."""
    if isinstance(value, dict):
        # Pair fields by their actual schema. A Profile's own `version` must
        # never qualify an otherwise unversioned prompt_template_id.
        field_groups = (
            ({"prompt_template_id"}, {"prompt_version"}),
            ({"id", "identity", "template_id", "TemplateID"}, {"version", "Version"}),
        )
        value = dict(value)
        for identity_keys, version_keys in field_groups:
            matching = {key for key in identity_keys & value.keys() if value[key] == TEMPLATE_ID}
            if not matching:
                continue
            versions = [value[key] for key in version_keys & value.keys()]
            if (
                not versions
                or any(not isinstance(version, str) or not version for version in versions)
                or len(set(versions)) != 1
                or asset["version"] in versions
            ):
                return True
            # Remove only this qualified identity. Other fields, nested proof,
            # raw digests and unknown mentions still retain their own references.
            value = {k: v for k, v in value.items() if k not in matching | version_keys}
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
