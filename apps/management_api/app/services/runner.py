from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.models import PipelineRun, PipelineRunStatus, PipelineVersion, RunEvent, RunLogsIndex
from app.schemas.pipeline_spec import PipelineSpecDocument
from app.services.log_store import run_log_store
from app.services.stage_registry import build_stage_executor, instantiate_stage
from app.services.xenna_adapter import try_run_with_xenna

logger = logging.getLogger(__name__)


class PipelineRunnerService:
    def __init__(self) -> None:
        settings = get_settings()
        self._executor = ThreadPoolExecutor(max_workers=settings.runner_max_workers)
        self._cancel_flags: dict[str, threading.Event] = {}
        self._futures: dict[str, Future[None]] = {}
        self._lock = threading.Lock()

    def submit_run(self, run_id: str) -> None:
        with self._lock:
            if run_id in self._futures:
                return
            cancel_event = threading.Event()
            self._cancel_flags[run_id] = cancel_event
            self._futures[run_id] = self._executor.submit(self._execute_run, run_id, cancel_event)

    def request_stop(self, run_id: str) -> bool:
        with self._lock:
            if run_id not in self._cancel_flags:
                return False
            self._cancel_flags[run_id].set()
            return True

    def _append_log(self, run_id: str, message: str) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        line = f"[{ts}] {message}"
        run_log_store.append(run_id, line)
        logger.info("run=%s %s", run_id, message)

    def _create_event(
        self,
        db: Session,
        run_id: str,
        event_type: str,
        message: str,
        stage_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event = RunEvent(
            run_id=run_id,
            event_type=event_type,
            stage_id=stage_id,
            message=message,
            payload=payload or {},
        )
        db.add(event)

    def _execute_run(self, run_id: str, cancel_event: threading.Event) -> None:
        db = SessionLocal()
        start_monotonic = time.perf_counter()
        try:
            run = db.get(PipelineRun, run_id)
            if run is None:
                return

            version = db.get(PipelineVersion, run.pipeline_version_id)
            if version is None:
                run.status = PipelineRunStatus.FAILED
                run.error_message = "Pipeline version not found"
                run.end_time = datetime.now(timezone.utc)
                db.commit()
                return

            run.status = PipelineRunStatus.RUNNING
            run.start_time = datetime.now(timezone.utc)
            db.add(run)
            self._create_event(db, run_id, "run_started", "Run started")
            db.commit()
            self._append_log(run_id, "Run entered RUNNING state")

            spec = PipelineSpecDocument.model_validate(version.spec_json)
            input_data = spec.io.source.static_data or []

            if cancel_event.is_set():
                raise InterruptedError("Run stop requested before execution")

            stage_instances: list[Any] = []
            for stage in spec.stages:
                stage_instances.append(instantiate_stage(stage))

            xenna_ok, xenna_result = try_run_with_xenna(
                spec,
                stage_instances,
                input_data,
                lambda msg: self._append_log(run_id, msg),
            )

            stage_metrics: list[dict[str, Any]] = []
            output_data = input_data

            if xenna_ok:
                output_data = xenna_result
                self._create_event(db, run_id, "execution_mode", "Executed with native Xenna adapter")
                self._append_log(run_id, "Execution completed using Xenna adapter")
            else:
                self._create_event(db, run_id, "execution_mode", "Executed with local simulated linear adapter")
                for stage in spec.stages:
                    if cancel_event.is_set():
                        raise InterruptedError("Run stop requested")

                    self._create_event(db, run_id, "stage_started", f"Stage {stage.name} started", stage.stage_id)
                    db.commit()
                    self._append_log(run_id, f"Starting stage {stage.stage_id} ({stage.name})")

                    stage_start = time.perf_counter()
                    executor = build_stage_executor(stage)
                    output_data = executor.run(output_data)
                    stage_duration = time.perf_counter() - stage_start

                    metric = {
                        "stage_id": stage.stage_id,
                        "duration_seconds": round(stage_duration, 4),
                        "output_count": len(output_data),
                    }
                    stage_metrics.append(metric)

                    self._create_event(
                        db,
                        run_id,
                        "stage_completed",
                        f"Stage {stage.name} completed",
                        stage.stage_id,
                        payload=metric,
                    )
                    db.commit()
                    self._append_log(run_id, f"Completed stage {stage.stage_id} in {stage_duration:.3f}s")

            end_ts = datetime.now(timezone.utc)
            duration_seconds = time.perf_counter() - start_monotonic

            run.status = PipelineRunStatus.SUCCEEDED
            run.end_time = end_ts
            run.duration_seconds = duration_seconds
            run.error_message = None
            run.metrics_summary = {
                "input_count": len(input_data),
                "output_count": len(output_data),
                "stages": stage_metrics,
                "execution_mode": spec.execution_mode,
                "duration_seconds": round(duration_seconds, 4),
            }
            run.artifact_pointers = {
                "sink": spec.io.sink.uri,
                "kind": spec.io.sink.kind,
            }
            db.add(run)

            logs_index = RunLogsIndex(
                run_id=run_id,
                storage_type="in_memory",
                pointer=run_id,
                metadata_json={"line_count": len(run_log_store.get_since(run_id, 0)[0])},
            )
            db.add(logs_index)
            self._create_event(db, run_id, "run_completed", "Run completed successfully")
            db.commit()
            self._append_log(run_id, "Run completed successfully")

        except InterruptedError as exc:
            run = db.get(PipelineRun, run_id)
            if run is not None:
                run.status = PipelineRunStatus.STOPPED
                run.end_time = datetime.now(timezone.utc)
                run.duration_seconds = time.perf_counter() - start_monotonic
                run.error_message = str(exc)
                db.add(run)
                self._create_event(db, run_id, "run_stopped", str(exc))
                db.commit()
            self._append_log(run_id, f"Run stopped: {exc}")
        except Exception as exc:  # noqa: BLE001
            run = db.get(PipelineRun, run_id)
            if run is not None:
                run.status = PipelineRunStatus.FAILED
                run.end_time = datetime.now(timezone.utc)
                run.duration_seconds = time.perf_counter() - start_monotonic
                run.error_message = str(exc)
                db.add(run)
                self._create_event(db, run_id, "run_failed", str(exc))
                db.commit()
            self._append_log(run_id, f"Run failed: {exc}")
            logger.exception("Run execution failed")
        finally:
            db.close()
            with self._lock:
                self._futures.pop(run_id, None)
                self._cancel_flags.pop(run_id, None)


pipeline_runner_service = PipelineRunnerService()
