"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { useAuth } from "@/components/auth/auth-provider";
import { DashboardShell } from "@/components/layout/dashboard-shell";
import { RunStatusBadge } from "@/components/runs/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import type { Pipeline, PipelineRun } from "@/types/api";

export default function RunsPage() {
  const { apiCall } = useAuth();
  const [runs, setRuns] = useState<PipelineRun[]>([]);
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [selectedPipeline, setSelectedPipeline] = useState<string>("");

  const reload = () => {
    void apiCall((client) => client.listRuns()).then(setRuns);
    void apiCall((client) => client.listPipelines()).then(setPipelines);
  };

  useEffect(() => {
    reload();
  }, [apiCall]);

  return (
    <DashboardShell>
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <h1 className="text-2xl font-semibold">Runs</h1>
                <p className="text-sm text-[var(--color-muted)]">Track run status, trigger new runs, and inspect details.</p>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <select
                  className="h-9 rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] px-3 text-sm"
                  value={selectedPipeline}
                  onChange={(e) => setSelectedPipeline(e.target.value)}
                >
                  <option value="">Choose pipeline</option>
                  {pipelines.map((pipeline) => (
                    <option value={pipeline.id} key={pipeline.id}>
                      {pipeline.name}
                    </option>
                  ))}
                </select>
                <Button
                  disabled={!selectedPipeline}
                  onClick={async () => {
                    if (!selectedPipeline) return;
                    await apiCall((client) => client.triggerRun(selectedPipeline));
                    reload();
                  }}
                >
                  Trigger Run
                </Button>
              </div>
            </div>
          </CardHeader>
        </Card>

        <Card>
          <CardContent className="overflow-x-auto p-0">
            <table className="min-w-full text-sm">
              <thead className="bg-[var(--color-surface)] text-left text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">
                <tr>
                  <th className="px-4 py-3">Run ID</th>
                  <th className="px-4 py-3">Pipeline</th>
                  <th className="px-4 py-3">Status</th>
                  <th className="px-4 py-3">Duration</th>
                  <th className="px-4 py-3">Trigger</th>
                  <th className="px-4 py-3">Action</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run) => (
                  <tr key={run.id} className="border-t border-[var(--color-card-border)]">
                    <td className="px-4 py-3 font-mono text-xs">{run.id}</td>
                    <td className="px-4 py-3">{pipelines.find((pipeline) => pipeline.id === run.pipeline_id)?.name ?? run.pipeline_id}</td>
                    <td className="px-4 py-3">
                      <RunStatusBadge status={run.status} />
                    </td>
                    <td className="px-4 py-3">{run.duration_seconds ? `${run.duration_seconds.toFixed(2)}s` : "-"}</td>
                    <td className="px-4 py-3">{run.trigger_type}</td>
                    <td className="px-4 py-3">
                      <Link href={`/runs/${run.id}`} className="text-[var(--color-accent)] underline underline-offset-2">
                        Open
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>
      </div>
    </DashboardShell>
  );
}
