# Pipeline Spec Schema

Versioned pipeline metadata is stored as JSON in `pipeline_versions.spec_json`.

## Top-level fields
- `pipeline_id`, `name`, `description`, `tags`
- `owners`, `team_ids`
- `execution_mode`: `streaming | batch | serving`
- `stages`: ordered list
- `edges`: DAG-like edges
- `io`, `runtime`, `observability`, `metadata_links`

## Stage
Each stage includes:
- `stage_id`, `name`
- one of:
  - `python_import_path` (`module:ClassName`), or
  - `stage_template` (registered builtin template)
- `resources`: `cpus`, `gpus`, optional `memory_mb`
- `batch_size`, `concurrency_hint`, `retries`, `params`

## Linear Validation Rules (v1)
- At least one stage.
- Unique stage IDs.
- Exactly one root and one leaf.
- No fan-in or fan-out (`in_degree <= 1`, `out_degree <= 1`).
- Topological order must match the `stages` array order.
