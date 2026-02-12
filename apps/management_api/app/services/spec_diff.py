from __future__ import annotations

from typing import Any

from app.schemas.pipeline_spec import StructuredDiff


def _flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            nested_prefix = f"{prefix}.{key}" if prefix else key
            out.update(_flatten(value, nested_prefix))
        return out
    if isinstance(obj, list):
        for idx, value in enumerate(obj):
            nested_prefix = f"{prefix}[{idx}]"
            out.update(_flatten(value, nested_prefix))
        if not obj:
            out[prefix] = []
        return out
    out[prefix] = obj
    return out


def build_structured_diff(old_spec: dict[str, Any], new_spec: dict[str, Any]) -> StructuredDiff:
    old_flat = _flatten(old_spec)
    new_flat = _flatten(new_spec)

    changed_fields: list[str] = []
    for key in sorted(set(old_flat.keys()) | set(new_flat.keys())):
        if old_flat.get(key) != new_flat.get(key):
            changed_fields.append(key)

    old_stages = {stage["stage_id"]: stage for stage in old_spec.get("stages", [])}
    new_stages = {stage["stage_id"]: stage for stage in new_spec.get("stages", [])}

    stage_changes: list[dict[str, Any]] = []
    for stage_id in sorted(set(old_stages.keys()) | set(new_stages.keys())):
        if stage_id not in old_stages:
            stage_changes.append({"stage_id": stage_id, "change": "ADDED", "after": new_stages[stage_id]})
            continue
        if stage_id not in new_stages:
            stage_changes.append({"stage_id": stage_id, "change": "REMOVED", "before": old_stages[stage_id]})
            continue
        if old_stages[stage_id] != new_stages[stage_id]:
            stage_changes.append(
                {
                    "stage_id": stage_id,
                    "change": "MODIFIED",
                    "before": old_stages[stage_id],
                    "after": new_stages[stage_id],
                }
            )

    return StructuredDiff(changed_fields=changed_fields, stage_changes=stage_changes)
