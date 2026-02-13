from __future__ import annotations

import hashlib
import json
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
