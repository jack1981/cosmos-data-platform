"use client";

import { useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";

import { useAuth } from "@/components/auth/auth-provider";
import { DashboardShell } from "@/components/layout/dashboard-shell";
import { RunStatusBadge } from "@/components/runs/status-badge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { runLogStreamUrl } from "@/lib/api";
import type { PipelineRun, RunEvent } from "@/types/api";

export default function RunDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { apiCall, accessToken, hasRole } = useAuth();
  const [run, setRun] = useState<PipelineRun | null>(null);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [metrics, setMetrics] = useState<Record<string, unknown>>({});
  const [logs, setLogs] = useState<string[]>([]);

  const canOperate = hasRole("INFRA_ADMIN") || hasRole("AIOPS_ENGINEER") || hasRole("PIPELINE_DEV");

  const reload = () => {
    void apiCall((client) => client.getRun(id)).then(setRun);
    void apiCall((client) => client.listRunEvents(id)).then(setEvents);
    void apiCall((client) => client.getMetricsSummary(id)).then((summary) => setMetrics(summary.metrics));
  };

  useEffect(() => {
    reload();
  }, [apiCall, id]);

  useEffect(() => {
    if (!accessToken) {
      return;
    }

    const source = new EventSource(runLogStreamUrl(id, accessToken));

    source.addEventListener("log", (event) => {
      const data = (event as MessageEvent<string>).data;
      setLogs((prev) => [...prev, data]);
    });

    source.addEventListener("end", () => {
      source.close();
      reload();
    });

    source.onerror = () => {
      source.close();
    };

    return () => source.close();
  }, [accessToken, id]);

  const timeline = useMemo(
    () =>
      events
        .filter((event) => event.event_type === "stage_completed")
        .map((event) => ({
          stageId: event.stage_id || "-",
          duration: Number(event.payload.duration_seconds || 0),
          outputCount: Number(event.payload.output_count || 0),
        })),
    [events],
  );

  return (
    <DashboardShell>
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <h1 className="text-2xl font-semibold">Run {id.slice(0, 8)}</h1>
                <p className="text-sm text-[var(--color-muted)]">Pipeline run detail, logs, events, and control actions.</p>
              </div>
              <div className="flex items-center gap-2">
                {run ? <RunStatusBadge status={run.status} /> : null}
                {canOperate ? (
                  <>
                    <Button
                      variant="secondary"
                      onClick={async () => {
                        await apiCall((client) => client.stopRun(id));
                        reload();
                      }}
                    >
                      Stop
                    </Button>
                    <Button
                      variant="secondary"
                      onClick={async () => {
                        await apiCall((client) => client.rerun(id));
                        reload();
                      }}
                    >
                      Rerun
                    </Button>
                  </>
                ) : null}
              </div>
            </div>
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-4">
            <div className="rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] p-3">
              <p className="text-xs text-[var(--color-muted)]">Trigger</p>
              <p className="font-medium">{run?.trigger_type ?? "-"}</p>
            </div>
            <div className="rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] p-3">
              <p className="text-xs text-[var(--color-muted)]">Duration</p>
              <p className="font-medium">{run?.duration_seconds ? `${run.duration_seconds.toFixed(2)}s` : "-"}</p>
            </div>
            <div className="rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] p-3">
              <p className="text-xs text-[var(--color-muted)]">Execution Mode</p>
              <p className="font-medium uppercase">{run?.execution_mode ?? "-"}</p>
            </div>
            <div className="rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] p-3">
              <p className="text-xs text-[var(--color-muted)]">Output Artifacts</p>
              <p className="font-medium">{String(run?.artifact_pointers?.kind ?? "none")}</p>
            </div>
          </CardContent>
        </Card>

        <div className="grid gap-4 xl:grid-cols-[1fr_1fr]">
          <Card>
            <CardHeader>
              <h3 className="text-sm font-semibold">Stage Timeline (Gantt-ish)</h3>
            </CardHeader>
            <CardContent className="space-y-2">
              {timeline.length === 0 ? (
                <p className="text-sm text-[var(--color-muted)]">No completed stages yet.</p>
              ) : (
                timeline.map((item) => (
                  <div key={`${item.stageId}-${item.duration}`}>
                    <div className="flex items-center justify-between text-xs text-[var(--color-muted)]">
                      <span>{item.stageId}</span>
                      <span>{item.duration.toFixed(2)}s</span>
                    </div>
                    <div className="h-2 rounded bg-[var(--color-surface)]">
                      <div className="h-2 rounded bg-[var(--color-accent)]" style={{ width: `${Math.min(item.duration * 25, 100)}%` }} />
                    </div>
                    <p className="text-[11px] text-[var(--color-muted)]">Output count: {item.outputCount}</p>
                  </div>
                ))
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <h3 className="text-sm font-semibold">Metrics Widgets</h3>
            </CardHeader>
            <CardContent className="space-y-2">
              <div className="grid grid-cols-2 gap-2">
                <div className="rounded border border-[var(--color-card-border)] bg-[var(--color-surface)] p-2 text-xs">
                  <p className="text-[var(--color-muted)]">Throughput</p>
                  <p className="font-semibold">{String(metrics.output_count ?? "n/a")}</p>
                </div>
                <div className="rounded border border-[var(--color-card-border)] bg-[var(--color-surface)] p-2 text-xs">
                  <p className="text-[var(--color-muted)]">Queue Depth</p>
                  <p className="font-semibold">{String(metrics.queue_depth ?? "n/a")}</p>
                </div>
                <div className="rounded border border-[var(--color-card-border)] bg-[var(--color-surface)] p-2 text-xs">
                  <p className="text-[var(--color-muted)]">Workers</p>
                  <p className="font-semibold">{String(metrics.worker_count ?? "n/a")}</p>
                </div>
                <div className="rounded border border-[var(--color-card-border)] bg-[var(--color-surface)] p-2 text-xs">
                  <p className="text-[var(--color-muted)]">Errors</p>
                  <p className="font-semibold">{String(metrics.error_count ?? 0)}</p>
                </div>
              </div>
              <pre className="max-h-48 overflow-auto rounded bg-[#0f172a] p-3 text-xs text-slate-100">
                {JSON.stringify(metrics, null, 2)}
              </pre>
            </CardContent>
          </Card>
        </div>

        <div className="grid gap-4 xl:grid-cols-[1fr_1fr]">
          <Card>
            <CardHeader>
              <h3 className="text-sm font-semibold">Live Logs (SSE)</h3>
            </CardHeader>
            <CardContent>
              <pre className="max-h-[280px] overflow-auto rounded bg-[#0b1220] p-3 text-xs text-slate-100">
                {logs.length > 0 ? logs.join("\n") : "Waiting for logs..."}
              </pre>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <h3 className="text-sm font-semibold">Events Stream</h3>
            </CardHeader>
            <CardContent className="space-y-2">
              <div className="max-h-[280px] space-y-2 overflow-auto">
                {events.map((event) => (
                  <div key={event.id} className="rounded border border-[var(--color-card-border)] bg-[var(--color-surface)] p-2 text-xs">
                    <div className="flex items-center justify-between">
                      <Badge label={event.event_type} />
                      <span className="text-[var(--color-muted)]">{new Date(event.created_at).toLocaleTimeString()}</span>
                    </div>
                    <p className="mt-1">{event.message}</p>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>

      </div>
    </DashboardShell>
  );
}
