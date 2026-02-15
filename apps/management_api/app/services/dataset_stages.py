from __future__ import annotations

import hashlib
import json
import random
import re
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from app.services.dataset_types import DatasetRef, DatasetRuntimeContext, DatasetStage


def _require_daft() -> Any:
    try:
        import daft  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError("Daft is required for dataset stages. Install daft with Lance support.") from exc
    return daft


def _stable_signature(stage_name: str, params: dict[str, Any], inputs: dict[str, DatasetRef]) -> str:
    payload = {
        "stage": stage_name,
        "params": params,
        "inputs": [(stage_id, ref.uri, ref.format) for stage_id, ref in sorted(inputs.items())],
    }
    blob = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()[:12]


def _default_output_path(
    ctx: DatasetRuntimeContext,
    stage_name: str,
    params: dict[str, Any],
    inputs: dict[str, DatasetRef],
) -> str:
    signature = _stable_signature(stage_name, params, inputs)
    output_path = Path(ctx.work_dir) / f"{stage_name}-{signature}.lance"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return str(output_path)


def _resolve_write_target(raw_output_uri: str) -> str:
    if raw_output_uri.startswith("file://"):
        return unquote(urlparse(raw_output_uri).path)

    if "://" not in raw_output_uri:
        local_path = Path(raw_output_uri).expanduser().resolve()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        return str(local_path)

    return raw_output_uri


def _is_external_uri(uri: str) -> bool:
    parsed = urlparse(uri)
    if parsed.scheme and parsed.scheme != "file":
        return True
    return "://" in uri and not uri.startswith("file://")


def _local_uri_candidates(ctx: DatasetRuntimeContext, raw_uri: str) -> list[Path]:
    raw_path = Path(raw_uri)
    if raw_path.is_absolute():
        return [raw_path]

    normalized = raw_uri.strip().lstrip("./")
    variants = [normalized]
    if normalized.startswith("app/"):
        variants.append(normalized[len("app/") :])
    if normalized.startswith("apps/management_api/"):
        variants.append(normalized[len("apps/management_api/") :])

    roots = [
        Path.cwd(),
        Path("/"),
        Path("/app"),
        Path("/app/apps/management_api"),
        Path(__file__).resolve().parents[2],
        Path(ctx.work_dir),
    ]

    candidates: list[Path] = []
    seen = set()
    for rel in variants:
        rel_path = Path(rel)
        for root in roots:
            candidate = (root / rel_path).resolve()
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
    return candidates


def _resolve_read_source(ctx: DatasetRuntimeContext, uri: str) -> str:
    if not uri:
        return uri
    if _is_external_uri(uri):
        return uri
    if uri.startswith("file://"):
        return unquote(urlparse(uri).path)

    for candidate in _local_uri_candidates(ctx, uri):
        if candidate.exists():
            return str(candidate)
    return uri


def _read_lance(ctx: DatasetRuntimeContext, uri: str) -> Any:
    daft = _require_daft()
    resolved_uri = _resolve_read_source(ctx, uri)
    if ctx.io_config is None:
        return daft.read_lance(resolved_uri)
    return daft.read_lance(resolved_uri, io_config=ctx.io_config)


def _write_lance(ctx: DatasetRuntimeContext, df: Any, uri: str, mode: str) -> None:
    if ctx.io_config is None:
        df.write_lance(uri, mode=mode)
        return
    df.write_lance(uri, mode=mode, io_config=ctx.io_config)


def _single_input(inputs: dict[str, DatasetRef], stage_name: str) -> DatasetRef:
    if len(inputs) != 1:
        raise ValueError(f"{stage_name} expects exactly one input")
    return next(iter(inputs.values()))


def _multi_input(
    inputs: dict[str, DatasetRef],
    stage_name: str,
) -> list[tuple[str, DatasetRef]]:
    if len(inputs) < 2:
        raise ValueError(f"{stage_name} expects at least two inputs")
    return sorted(inputs.items(), key=lambda item: item[0])


def _normalize_join_key(raw_key: Any) -> Any:
    if raw_key is None:
        return None
    if isinstance(raw_key, tuple):
        return list(raw_key)
    if isinstance(raw_key, str):
        if "," in raw_key:
            parts = [part.strip() for part in raw_key.split(",") if part.strip()]
            return parts
        return raw_key.strip()
    return raw_key


def _filter_predicate(params: dict[str, Any]) -> str:
    predicate = params.get("predicate") or params.get("where") or params.get("filter")
    if isinstance(predicate, str) and predicate.strip():
        return predicate.strip()

    column = params.get("column")
    value = params.get("value")
    op = str(params.get("op", "=="))
    if isinstance(column, str) and column.strip() and "value" in params:
        return f"{column.strip()} {op} {json.dumps(value)}"

    raise ValueError("FilterStage requires either `predicate` or (`column`, `op`, `value`) parameters")


def _materialize(
    ctx: DatasetRuntimeContext,
    stage_name: str,
    params: dict[str, Any],
    inputs: dict[str, DatasetRef],
    df: Any,
    output_uri: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> DatasetRef:
    raw_output_uri = output_uri or _default_output_path(ctx, stage_name, params, inputs)
    write_target = _resolve_write_target(raw_output_uri)
    write_mode = str(params.get("write_mode") or params.get("mode") or "overwrite")
    _write_lance(ctx, df, write_target, write_mode)
    return DatasetRef(uri=write_target, format="lance", metadata=metadata or {})


def _source_uri_from_context(ctx: DatasetRuntimeContext) -> str | None:
    source = getattr(ctx.pipeline_io, "source", None)
    if source is None:
        return None
    uri = getattr(source, "uri", None)
    if uri is None:
        return None
    if not isinstance(uri, str):
        return None
    uri = uri.strip()
    return uri or None


def _sink_uri_from_context(ctx: DatasetRuntimeContext) -> str | None:
    sink = getattr(ctx.pipeline_io, "sink", None)
    if sink is None:
        return None
    uri = getattr(sink, "uri", None)
    if uri is None:
        return None
    if not isinstance(uri, str):
        return None
    uri = uri.strip()
    return uri or None


class LanceReaderStage(DatasetStage):
    def run(
        self,
        ctx: DatasetRuntimeContext,
        inputs: dict[str, DatasetRef],
    ) -> DatasetRef:
        del inputs
        source_uri = str(
            self.params.get("uri") or self.params.get("source_uri") or _source_uri_from_context(ctx) or ""
        ).strip()
        if not source_uri:
            raise ValueError("LanceReaderStage requires a source uri via params or pipeline io.source.uri")

        df = _read_lance(ctx, source_uri)
        return _materialize(
            ctx,
            stage_name="lance_reader",
            params=self.params,
            inputs={},
            df=df,
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": source_uri},
        )


class LanceWriterStage(DatasetStage):
    def run(
        self,
        ctx: DatasetRuntimeContext,
        inputs: dict[str, DatasetRef],
    ) -> DatasetRef:
        upstream = _single_input(inputs, "LanceWriterStage")
        output_uri = str(self.params.get("output_uri") or _sink_uri_from_context(ctx) or "").strip()
        if not output_uri:
            raise ValueError("LanceWriterStage requires output_uri param or pipeline io.sink.uri")

        df = _read_lance(ctx, upstream.uri)
        return _materialize(
            ctx,
            stage_name="lance_writer",
            params=self.params,
            inputs=inputs,
            df=df,
            output_uri=output_uri,
            metadata={"writer_stage": True, "source_uri": upstream.uri},
        )


class FilterStage(DatasetStage):
    def run(
        self,
        ctx: DatasetRuntimeContext,
        inputs: dict[str, DatasetRef],
    ) -> DatasetRef:
        upstream = _single_input(inputs, "FilterStage")
        df = _read_lance(ctx, upstream.uri)
        filtered = df.filter(_filter_predicate(self.params))
        return _materialize(
            ctx,
            stage_name="filter",
            params=self.params,
            inputs=inputs,
            df=filtered,
            output_uri=self.params.get("output_uri"),
            metadata={
                "source_uri": upstream.uri,
                "predicate": self.params.get("predicate") or self.params.get("where"),
            },
        )


class ColumnSelectStage(DatasetStage):
    def run(
        self,
        ctx: DatasetRuntimeContext,
        inputs: dict[str, DatasetRef],
    ) -> DatasetRef:
        upstream = _single_input(inputs, "ColumnSelectStage")
        columns = self.params.get("columns") or self.params.get("select")
        if isinstance(columns, str):
            columns = [item.strip() for item in columns.split(",") if item.strip()]
        if not isinstance(columns, list) or not columns:
            raise ValueError("ColumnSelectStage requires non-empty columns list")

        df = _read_lance(ctx, upstream.uri)
        try:
            selected = df.select(*columns)
        except TypeError:
            selected = df.select(columns)
        return _materialize(
            ctx,
            stage_name="column_select",
            params=self.params,
            inputs=inputs,
            df=selected,
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "columns": columns},
        )


class ShufflerStage(DatasetStage):
    def run(
        self,
        ctx: DatasetRuntimeContext,
        inputs: dict[str, DatasetRef],
    ) -> DatasetRef:
        upstream = _single_input(inputs, "ShufflerStage")
        df = _read_lance(ctx, upstream.uri)

        seed = self.params.get("seed")
        sample_frac = float(self.params.get("sample_frac", 1.0))
        sample_fn = getattr(df, "sample", None)
        if callable(sample_fn):
            try:
                shuffled = sample_fn(fraction=sample_frac, with_replacement=False, seed=seed)
            except TypeError:
                shuffled = sample_fn(frac=sample_frac, with_replacement=False, seed=seed)
        else:  # pragma: no cover - method availability depends on Daft version
            shuffled = df
        return _materialize(
            ctx,
            stage_name="shuffle",
            params=self.params,
            inputs=inputs,
            df=shuffled,
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "sample_frac": sample_frac, "seed": seed},
        )


class UnionByNameStage(DatasetStage):
    def run(
        self,
        ctx: DatasetRuntimeContext,
        inputs: dict[str, DatasetRef],
    ) -> DatasetRef:
        ordered_inputs = _multi_input(inputs, "UnionByNameStage")
        frames = [_read_lance(ctx, ref.uri) for _, ref in ordered_inputs]

        distinct = bool(self.params.get("distinct", True))
        union_df = frames[0]
        for frame in frames[1:]:
            if distinct:
                union_df = union_df.union_by_name(frame)
            else:
                union_all_by_name = getattr(union_df, "union_all_by_name", None)
                if callable(union_all_by_name):
                    union_df = union_all_by_name(frame)
                else:
                    union_df = union_df.union_by_name(frame)

        return _materialize(
            ctx,
            stage_name="union_by_name",
            params=self.params,
            inputs=inputs,
            df=union_df,
            output_uri=self.params.get("output_uri"),
            metadata={"union_inputs": [stage_id for stage_id, _ in ordered_inputs], "distinct": distinct},
        )


class JoinStage(DatasetStage):
    def run(
        self,
        ctx: DatasetRuntimeContext,
        inputs: dict[str, DatasetRef],
    ) -> DatasetRef:
        ordered_inputs = _multi_input(inputs, "JoinStage")
        frames = [_read_lance(ctx, ref.uri) for _, ref in ordered_inputs]

        on = _normalize_join_key(self.params.get("on"))
        left_on = _normalize_join_key(self.params.get("left_on"))
        right_on = _normalize_join_key(self.params.get("right_on"))
        how = str(self.params.get("how", "inner"))

        join_kwargs: dict[str, Any] = {"how": how}
        if on is not None:
            join_kwargs["on"] = on
        elif left_on is not None and right_on is not None:
            join_kwargs["left_on"] = left_on
            join_kwargs["right_on"] = right_on
        elif how != "cross":
            raise ValueError("JoinStage requires `on` or (`left_on`, `right_on`) unless how='cross'")

        joined_df = frames[0]
        for frame in frames[1:]:
            joined_df = joined_df.join(frame, **join_kwargs)

        return _materialize(
            ctx,
            stage_name="join",
            params=self.params,
            inputs=inputs,
            df=joined_df,
            output_uri=self.params.get("output_uri"),
            metadata={
                "join_inputs": [stage_id for stage_id, _ in ordered_inputs],
                "join_params": {k: v for k, v in join_kwargs.items()},
            },
        )


def _rows_from_df(df: Any) -> list[dict[str, Any]]:
    table = df.to_arrow()
    return [dict(row) for row in table.to_pylist()]


def _rows_to_df(rows: list[dict[str, Any]], *, fallback_df: Any | None = None) -> Any:
    daft = _require_daft()
    if rows:
        return daft.from_pylist(rows)
    if fallback_df is not None:
        return fallback_df.limit(0)
    return daft.from_pylist([])


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _first_dataset_df(
    ctx: DatasetRuntimeContext,
    inputs: dict[str, DatasetRef],
    stage_name: str,
) -> tuple[Any, DatasetRef]:
    upstream = _single_input(inputs, stage_name)
    return _read_lance(ctx, upstream.uri), upstream


class SplitterStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "SplitterStage")
        rows = _rows_from_df(df)

        mode = str(self.params.get("mode", "mod")).lower()
        partitions = max(1, int(self.params.get("partitions", 2)))
        partition_id = int(self.params.get("partition_id", 0)) % partitions
        fraction = min(1.0, max(0.0, float(self.params.get("fraction", 0.5))))

        if mode == "ratio":
            cutoff = int(len(rows) * fraction)
            selected = rows[:cutoff] if partition_id == 0 else rows[cutoff:]
        else:
            selected = [row for idx, row in enumerate(rows) if idx % partitions == partition_id]

        return _materialize(
            ctx,
            stage_name="splitter",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(selected, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "mode": mode, "partition_id": partition_id, "partitions": partitions},
        )


class VisualizerStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "VisualizerStage")
        rows = _rows_from_df(df)
        limit = max(1, int(self.params.get("limit", 20)))
        preview = rows[:limit]

        output_path = Path(str(self.params.get("preview_path") or f"{ctx.work_dir}/visualizer_preview.json"))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(preview, ensure_ascii=False, indent=2), encoding="utf-8")

        return _materialize(
            ctx,
            stage_name="visualizer",
            params=self.params,
            inputs=inputs,
            df=df,
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "preview_path": str(output_path), "preview_count": len(preview)},
        )


class SchemaStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "SchemaStage")
        schema = {field.name: str(field.type) for field in df.to_arrow().schema}
        return _materialize(
            ctx,
            stage_name="schema",
            params=self.params,
            inputs=inputs,
            df=df,
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "schema": schema},
        )


class RowNumberStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "RowNumberStage")
        rows = _rows_from_df(df)
        column = str(self.params.get("column", "row_number"))
        start = int(self.params.get("start", 1))
        for idx, row in enumerate(rows, start=start):
            row[column] = idx
        return _materialize(
            ctx,
            stage_name="row_number",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "row_number_column": column},
        )


class StatStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "StatStage")
        rows = _rows_from_df(df)

        numeric_columns = self.params.get("columns")
        if isinstance(numeric_columns, str):
            numeric_columns = [item.strip() for item in numeric_columns.split(",") if item.strip()]
        if not isinstance(numeric_columns, list):
            numeric_columns = sorted(
                {
                    key
                    for row in rows
                    for key, value in row.items()
                    if isinstance(value, (int, float)) and not isinstance(value, bool)
                }
            )

        stats: dict[str, dict[str, float]] = {}
        for column in numeric_columns:
            values = [
                float(row[column])
                for row in rows
                if isinstance(row.get(column), (int, float)) and not isinstance(row.get(column), bool)
            ]
            if not values:
                continue
            stats[str(column)] = {
                "count": float(len(values)),
                "min": min(values),
                "max": max(values),
                "mean": sum(values) / len(values),
            }

        stats_path_raw = self.params.get("stats_path")
        if stats_path_raw:
            stats_path = Path(str(stats_path_raw))
            stats_path.parent.mkdir(parents=True, exist_ok=True)
            stats_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

        return _materialize(
            ctx,
            stage_name="stat",
            params=self.params,
            inputs=inputs,
            df=df,
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "stats": stats},
        )


class ColumnDropStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "ColumnDropStage")
        rows = _rows_from_df(df)
        columns = self.params.get("columns") or self.params.get("drop")
        if isinstance(columns, str):
            columns = [item.strip() for item in columns.split(",") if item.strip()]
        if not isinstance(columns, list) or not columns:
            raise ValueError("ColumnDropStage requires columns/drop list")
        to_drop = {str(column) for column in columns}

        for row in rows:
            for column in to_drop:
                row.pop(column, None)

        return _materialize(
            ctx,
            stage_name="column_drop",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "dropped_columns": sorted(to_drop)},
        )


class ColumnAliasStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "ColumnAliasStage")
        rows = _rows_from_df(df)
        aliases = self.params.get("aliases")
        if not isinstance(aliases, dict):
            source = self.params.get("source")
            target = self.params.get("target")
            if source and target:
                aliases = {str(source): str(target)}
            else:
                raise ValueError("ColumnAliasStage requires aliases map or source/target")

        normalized_aliases = {str(old): str(new) for old, new in aliases.items()}
        for row in rows:
            for old, new in normalized_aliases.items():
                if old in row:
                    row[new] = row.pop(old)

        return _materialize(
            ctx,
            stage_name="column_alias",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "aliases": normalized_aliases},
        )


class FilterByRatioStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "FilterByRatioStage")
        rows = _rows_from_df(df)
        keep_ratio = min(1.0, max(0.0, float(self.params.get("keep_ratio", 0.5))))
        seed = self.params.get("seed")
        rng = random.Random(seed)
        kept_rows = [row for row in rows if rng.random() <= keep_ratio]

        return _materialize(
            ctx,
            stage_name="filter_by_ratio",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(kept_rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "keep_ratio": keep_ratio},
        )


class SelectorStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "SelectorStage")
        rows = _rows_from_df(df)

        indices = self.params.get("indices")
        if isinstance(indices, list):
            chosen = [rows[int(idx)] for idx in indices if isinstance(idx, int) and 0 <= idx < len(rows)]
        else:
            offset = max(0, int(self.params.get("offset", 0)))
            limit = max(0, int(self.params.get("limit", len(rows))))
            chosen = rows[offset : offset + limit]

        return _materialize(
            ctx,
            stage_name="selector",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(chosen, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "selected_count": len(chosen)},
        )


class ReorderStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "ReorderStage")
        rows = _rows_from_df(df)
        by = self.params.get("by") or self.params.get("columns")
        if isinstance(by, str):
            columns = [item.strip() for item in by.split(",") if item.strip()]
        elif isinstance(by, list):
            columns = [str(item) for item in by]
        else:
            raise ValueError("ReorderStage requires by/columns parameter")

        desc = self.params.get("desc", False)
        if isinstance(desc, list):
            desc_flags = [bool(item) for item in desc]
        else:
            desc_flags = [bool(desc)] * len(columns)
        if len(desc_flags) < len(columns):
            desc_flags.extend([False] * (len(columns) - len(desc_flags)))

        ordered = list(rows)
        for column, is_desc in reversed(list(zip(columns, desc_flags, strict=False))):
            ordered.sort(
                key=lambda row: (_stringify(row.get(column)) == "", _stringify(row.get(column))), reverse=is_desc
            )

        return _materialize(
            ctx,
            stage_name="reorder",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(ordered, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "order_by": columns, "desc": desc_flags},
        )


class InterleavedReorderStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "InterleavedReorderStage")
        rows = _rows_from_df(df)

        group_by = self.params.get("group_by") or self.params.get("column")
        if isinstance(group_by, str):
            group_columns = [item.strip() for item in group_by.split(",") if item.strip()]
        elif isinstance(group_by, list):
            group_columns = [str(item) for item in group_by]
        else:
            raise ValueError("InterleavedReorderStage requires group_by/column")

        buckets: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in rows:
            key = tuple(row.get(col) for col in group_columns)
            buckets.setdefault(key, []).append(row)

        interleaved: list[dict[str, Any]] = []
        keys = sorted(buckets.keys(), key=lambda item: tuple(_stringify(value) for value in item))
        while True:
            added = 0
            for key in keys:
                bucket = buckets[key]
                if bucket:
                    interleaved.append(bucket.pop(0))
                    added += 1
            if added == 0:
                break

        return _materialize(
            ctx,
            stage_name="interleaved_reorder",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(interleaved, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "group_by": group_columns},
        )


class UnionByPositionStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        ordered_inputs = _multi_input(inputs, "UnionByPositionStage")
        frames = [_read_lance(ctx, ref.uri) for _, ref in ordered_inputs]
        if not frames:
            raise ValueError("UnionByPositionStage requires at least one input")

        base_columns = list(frames[0].column_names)
        merged_rows: list[dict[str, Any]] = []
        for frame in frames:
            column_names = list(frame.column_names)
            for row in _rows_from_df(frame):
                projected: dict[str, Any] = {}
                for idx, base_col in enumerate(base_columns):
                    source_col = column_names[idx] if idx < len(column_names) else base_col
                    projected[base_col] = row.get(source_col)
                merged_rows.append(projected)

        return _materialize(
            ctx,
            stage_name="union_by_position",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(merged_rows, fallback_df=frames[0]),
            output_uri=self.params.get("output_uri"),
            metadata={"union_inputs": [stage_id for stage_id, _ in ordered_inputs], "columns": base_columns},
        )


class JoinerStage(JoinStage):
    pass


class AddConstantsStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "AddConstantsStage")
        rows = _rows_from_df(df)
        constants = self.params.get("constants")
        if not isinstance(constants, dict):
            raise ValueError("AddConstantsStage requires constants map")
        for row in rows:
            row.update(constants)
        return _materialize(
            ctx,
            stage_name="add_constants",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "constants": constants},
        )


class ConversationToParagraphStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "ConversationToParagraphStage")
        rows = _rows_from_df(df)
        source_col = str(self.params.get("source_column", "conversation"))
        output_col = str(self.params.get("output_column", "paragraph"))
        separator = str(self.params.get("separator", " "))

        for row in rows:
            value = row.get(source_col)
            if isinstance(value, list):
                fragments: list[str] = []
                for item in value:
                    if isinstance(item, dict):
                        fragments.append(_stringify(item.get("content") or item.get("text")))
                    else:
                        fragments.append(_stringify(item))
                row[output_col] = separator.join(fragment for fragment in fragments if fragment)
            else:
                row[output_col] = _stringify(value)

        return _materialize(
            ctx,
            stage_name="conversation_to_paragraph",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "source_column": source_col, "output_column": output_col},
        )


class ConcatenateColumnsStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "ConcatenateColumnsStage")
        rows = _rows_from_df(df)
        columns = self.params.get("columns")
        if isinstance(columns, str):
            columns = [item.strip() for item in columns.split(",") if item.strip()]
        if not isinstance(columns, list) or not columns:
            raise ValueError("ConcatenateColumnsStage requires columns list")

        separator = str(self.params.get("separator", " "))
        output_column = str(self.params.get("output_column", "concatenated"))

        for row in rows:
            row[output_column] = separator.join(
                _stringify(row.get(column)) for column in columns if _stringify(row.get(column))
            )

        return _materialize(
            ctx,
            stage_name="concatenate_columns",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "columns": columns, "output_column": output_column},
        )


class ConcatStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        if len(inputs) >= 2:
            ordered_inputs = _multi_input(inputs, "ConcatStage")
            frames = [_read_lance(ctx, ref.uri) for _, ref in ordered_inputs]
            merged = frames[0]
            for frame in frames[1:]:
                merged = merged.union_by_name(frame)
            return _materialize(
                ctx,
                stage_name="concat",
                params=self.params,
                inputs=inputs,
                df=merged,
                output_uri=self.params.get("output_uri"),
                metadata={"concat_inputs": [stage_id for stage_id, _ in ordered_inputs]},
            )

        df, upstream = _first_dataset_df(ctx, inputs, "ConcatStage")
        return _materialize(
            ctx,
            stage_name="concat",
            params=self.params,
            inputs=inputs,
            df=df,
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri},
        )


class DuplicateSampleRatioStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "DuplicateSampleRatioStage")
        rows = _rows_from_df(df)
        ratio = max(0.0, float(self.params.get("ratio", 2.0)))
        integer_part = int(ratio)
        remainder = ratio - integer_part
        rng = random.Random(self.params.get("seed"))

        duplicated: list[dict[str, Any]] = []
        for row in rows:
            for _ in range(integer_part):
                duplicated.append(dict(row))
            if remainder > 0 and rng.random() < remainder:
                duplicated.append(dict(row))

        return _materialize(
            ctx,
            stage_name="duplicate_sample_ratio",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(duplicated, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "ratio": ratio},
        )


class SamplerStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "SamplerStage")
        rows = _rows_from_df(df)
        rng = random.Random(self.params.get("seed"))
        with_replacement = bool(self.params.get("with_replacement", False))

        sample_size = self.params.get("sample_size")
        if isinstance(sample_size, int):
            if with_replacement:
                sampled = [dict(rng.choice(rows)) for _ in range(max(0, sample_size))] if rows else []
            else:
                sampled = rng.sample(rows, min(max(0, sample_size), len(rows)))
        else:
            fraction = min(1.0, max(0.0, float(self.params.get("fraction", 0.5))))
            sampled = [row for row in rows if rng.random() <= fraction]

        return _materialize(
            ctx,
            stage_name="sampler",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(sampled, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "sampled_count": len(sampled)},
        )


class FlattenStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "FlattenStage")
        rows = _rows_from_df(df)
        column = str(self.params.get("column", "items"))
        output_column = str(self.params.get("output_column", column))
        keep_empty = bool(self.params.get("keep_empty", False))

        flattened: list[dict[str, Any]] = []
        for row in rows:
            value = row.get(column)
            if isinstance(value, list):
                if not value and keep_empty:
                    copied = dict(row)
                    copied[output_column] = None
                    flattened.append(copied)
                for element in value:
                    copied = dict(row)
                    copied[output_column] = element
                    flattened.append(copied)
            else:
                copied = dict(row)
                copied[output_column] = value
                flattened.append(copied)

        return _materialize(
            ctx,
            stage_name="flatten",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(flattened, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "column": column, "output_column": output_column},
        )


class GroupFlattenStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "GroupFlattenStage")
        rows = _rows_from_df(df)
        group_by = self.params.get("group_by")
        if isinstance(group_by, str):
            group_columns = [item.strip() for item in group_by.split(",") if item.strip()]
        elif isinstance(group_by, list):
            group_columns = [str(item) for item in group_by]
        else:
            raise ValueError("GroupFlattenStage requires group_by columns")

        flatten_column = str(self.params.get("flatten_column", "items"))
        output_column = str(self.params.get("output_column", flatten_column))

        grouped: dict[tuple[Any, ...], list[Any]] = {}
        for row in rows:
            key = tuple(row.get(column) for column in group_columns)
            grouped.setdefault(key, [])
            value = row.get(flatten_column)
            if isinstance(value, list):
                grouped[key].extend(value)
            elif value is not None:
                grouped[key].append(value)

        flattened_rows: list[dict[str, Any]] = []
        for key, values in grouped.items():
            item = {column: value for column, value in zip(group_columns, key, strict=False)}
            item[output_column] = values
            flattened_rows.append(item)

        return _materialize(
            ctx,
            stage_name="group_flatten",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(flattened_rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "group_by": group_columns, "output_column": output_column},
        )


class FastTextScorerStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "FastTextScorerStage")
        rows = _rows_from_df(df)
        text_column = str(self.params.get("text_column", "text"))
        score_column = str(self.params.get("score_column", "fasttext_score"))
        label_column = str(self.params.get("label_column", "fasttext_label"))
        labels = self.params.get("labels")
        if not isinstance(labels, list) or not labels:
            labels = ["negative", "positive"]

        for row in rows:
            text = _stringify(row.get(text_column))
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
            score = int(digest[:8], 16) / 0xFFFFFFFF
            row[score_column] = round(score, 6)
            row[label_column] = str(labels[int(score * len(labels)) % len(labels)])

        return _materialize(
            ctx,
            stage_name="fasttext_scorer",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "text_column": text_column, "labels": labels},
        )


class FastTextFilterStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "FastTextFilterStage")
        rows = _rows_from_df(df)
        score_column = str(self.params.get("score_column", "fasttext_score"))
        min_score = float(self.params.get("min_score", 0.5))
        max_score = float(self.params.get("max_score", 1.0))
        filtered = [
            row
            for row in rows
            if isinstance(row.get(score_column), (int, float))
            and min_score <= float(row.get(score_column)) <= max_score
        ]
        return _materialize(
            ctx,
            stage_name="fasttext_filter",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(filtered, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={
                "source_uri": upstream.uri,
                "score_column": score_column,
                "min_score": min_score,
                "max_score": max_score,
            },
        )


class SeqClassifierScorerStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "SeqClassifierScorerStage")
        rows = _rows_from_df(df)
        text_column = str(self.params.get("text_column", "text"))
        output_prefix = str(self.params.get("output_prefix", "seq_classifier"))
        labels = self.params.get("labels")
        if not isinstance(labels, list) or not labels:
            labels = ["label_a", "label_b", "label_c"]

        for row in rows:
            text = _stringify(row.get(text_column))
            digest = hashlib.md5(text.encode("utf-8")).hexdigest()  # noqa: S324
            score = int(digest[:8], 16) / 0xFFFFFFFF
            label = str(labels[int(score * len(labels)) % len(labels)])
            row[f"{output_prefix}_label"] = label
            row[f"{output_prefix}_score"] = round(score, 6)

        return _materialize(
            ctx,
            stage_name="seq_classifier_scorer",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "labels": labels, "text_column": text_column},
        )


class MinHashStage(DatasetStage):
    _TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")

    def _signature(self, text: str, num_hashes: int, shingle_size: int) -> str:
        tokens = self._TOKEN_RE.findall(text.lower())
        if not tokens:
            return ",".join(["0"] * num_hashes)

        shingles: set[str] = set()
        if len(tokens) < shingle_size:
            shingles.add(" ".join(tokens))
        else:
            for idx in range(len(tokens) - shingle_size + 1):
                shingles.add(" ".join(tokens[idx : idx + shingle_size]))

        mins: list[int] = []
        for seed in range(num_hashes):
            values = [
                int(hashlib.sha1(f"{seed}:{shingle}".encode("utf-8")).hexdigest()[:12], 16) for shingle in shingles
            ]
            mins.append(min(values))
        return ",".join(str(item) for item in mins)

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "MinHashStage")
        rows = _rows_from_df(df)
        text_column = str(self.params.get("text_column", "text"))
        output_column = str(self.params.get("output_column", "minhash"))
        num_hashes = max(1, int(self.params.get("num_hashes", 16)))
        shingle_size = max(1, int(self.params.get("shingle_size", 3)))
        deduplicate = bool(self.params.get("deduplicate", False))

        seen: set[str] = set()
        transformed: list[dict[str, Any]] = []
        for row in rows:
            text = _stringify(row.get(text_column))
            signature = self._signature(text, num_hashes=num_hashes, shingle_size=shingle_size)
            row[output_column] = signature
            if deduplicate and signature in seen:
                continue
            seen.add(signature)
            transformed.append(row)

        return _materialize(
            ctx,
            stage_name="minhash",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(transformed, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "deduplicate": deduplicate, "output_column": output_column},
        )


class AddRankQuantileStage(DatasetStage):
    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "AddRankQuantileStage")
        rows = _rows_from_df(df)
        score_column = str(self.params.get("score_column", "fasttext_score"))
        rank_column = str(self.params.get("rank_column", "rank"))
        quantile_column = str(self.params.get("quantile_column", "quantile"))
        quantiles = max(1, int(self.params.get("quantiles", 4)))

        ordered = sorted(rows, key=lambda row: float(row.get(score_column) or 0.0), reverse=True)
        total = max(1, len(ordered))
        for idx, row in enumerate(ordered, start=1):
            row[rank_column] = idx
            row[quantile_column] = min(quantiles, int(((idx - 1) / total) * quantiles) + 1)

        return _materialize(
            ctx,
            stage_name="add_rank_quantile",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(ordered, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "score_column": score_column, "quantiles": quantiles},
        )


class TokenCounterV2Stage(DatasetStage):
    _TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")

    def run(self, ctx: DatasetRuntimeContext, inputs: dict[str, DatasetRef]) -> DatasetRef:
        df, upstream = _first_dataset_df(ctx, inputs, "TokenCounterV2Stage")
        rows = _rows_from_df(df)
        text_column = str(self.params.get("text_column", "text"))
        output_column = str(self.params.get("output_column", "token_count_v2"))

        for row in rows:
            tokens = self._TOKEN_RE.findall(_stringify(row.get(text_column)))
            row[output_column] = len(tokens)

        return _materialize(
            ctx,
            stage_name="token_counter_v2",
            params=self.params,
            inputs=inputs,
            df=_rows_to_df(rows, fallback_df=df),
            output_uri=self.params.get("output_uri"),
            metadata={"source_uri": upstream.uri, "text_column": text_column, "output_column": output_column},
        )
