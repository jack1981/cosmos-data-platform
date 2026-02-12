"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { Plus } from "lucide-react";

import { DashboardShell } from "@/components/layout/dashboard-shell";
import { RunStatusBadge } from "@/components/runs/status-badge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { useAuth } from "@/components/auth/auth-provider";
import type { Pipeline, PipelineRun } from "@/types/api";

export default function PipelinesPage() {
  const { apiCall, hasRole } = useAuth();
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [runs, setRuns] = useState<PipelineRun[]>([]);
  const [search, setSearch] = useState("");
  const [tagFilter, setTagFilter] = useState("");

  const canCreate = hasRole("PIPELINE_DEV") || hasRole("INFRA_ADMIN");

  useEffect(() => {
    void apiCall((client) => client.listPipelines()).then(setPipelines);
    void apiCall((client) => client.listRuns()).then(setRuns);
  }, [apiCall]);

  const lastRunByPipeline = useMemo(() => {
    const map = new Map<string, PipelineRun>();
    for (const run of runs) {
      if (!map.has(run.pipeline_id)) {
        map.set(run.pipeline_id, run);
      }
    }
    return map;
  }, [runs]);

  const filtered = pipelines.filter((pipeline) => {
    if (search && !`${pipeline.name} ${pipeline.description}`.toLowerCase().includes(search.toLowerCase())) {
      return false;
    }
    if (tagFilter && !pipeline.tags.includes(tagFilter)) {
      return false;
    }
    return true;
  });

  const allTags = Array.from(new Set(pipelines.flatMap((pipeline) => pipeline.tags))).sort();
  const templates = pipelines.filter((pipeline) => pipeline.tags.includes("template"));

  return (
    <DashboardShell>
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold">Starter Templates</h2>
                <p className="text-sm text-[var(--color-muted)]">
                  Learn from these runnable examples. Trigger a run to see logs, events, and metrics.
                </p>
              </div>
              <Badge label={`${templates.length} templates`} />
            </div>
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-3">
            {templates.map((template) => (
              <div key={template.id} className="rounded-lg border border-[var(--color-card-border)] bg-[var(--color-surface)] p-3">
                <p className="font-medium">{template.name}</p>
                <p className="mt-1 text-xs text-[var(--color-muted)]">{template.description}</p>
                <div className="mt-2 flex flex-wrap gap-1">
                  {template.tags.map((tag) => (
                    <Badge key={tag} label={tag} />
                  ))}
                </div>
                <div className="mt-3 flex gap-2">
                  <Button
                    className="h-8"
                    onClick={async () => {
                      await apiCall((client) => client.triggerRun(template.id));
                      setRuns(await apiCall((client) => client.listRuns()));
                    }}
                  >
                    Run Demo
                  </Button>
                  <Link href={`/pipelines/${template.id}/builder`}>
                    <Button variant="secondary" className="h-8">
                      Open Builder
                    </Button>
                  </Link>
                </div>
              </div>
            ))}
            {templates.length === 0 ? (
              <p className="text-sm text-[var(--color-muted)]">No templates available.</p>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <h1 className="text-2xl font-semibold">Pipelines</h1>
                <p className="text-sm text-[var(--color-muted)]">
                  Create, review, publish, and monitor Cosmos-Xenna pipeline definitions.
                </p>
              </div>
              {canCreate ? (
                <Link href="/pipelines/new">
                  <Button>
                    <Plus className="mr-1 h-4 w-4" />
                    Create Pipeline
                  </Button>
                </Link>
              ) : null}
            </div>
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-3">
            <label className="space-y-1 md:col-span-2">
              <span className="text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">Search</span>
              <Input placeholder="Search by name or description" value={search} onChange={(e) => setSearch(e.target.value)} />
            </label>
            <label className="space-y-1">
              <span className="text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">Tag Filter</span>
              <select
                className="h-9 w-full rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] px-3 text-sm"
                value={tagFilter}
                onChange={(e) => setTagFilter(e.target.value)}
              >
                <option value="">All tags</option>
                {allTags.map((tag) => (
                  <option value={tag} key={tag}>
                    {tag}
                  </option>
                ))}
              </select>
            </label>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="overflow-x-auto p-0">
            <table className="min-w-full text-sm">
              <thead className="bg-[var(--color-surface)] text-left text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">
                <tr>
                  <th className="px-4 py-3">Name</th>
                  <th className="px-4 py-3">Mode</th>
                  <th className="px-4 py-3">Tags</th>
                  <th className="px-4 py-3">Latest Run</th>
                  <th className="px-4 py-3">Health</th>
                  <th className="px-4 py-3">Actions</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((pipeline) => {
                  const run = lastRunByPipeline.get(pipeline.id);
                  return (
                    <tr key={pipeline.id} className="border-t border-[var(--color-card-border)]">
                      <td className="px-4 py-3">
                        <p className="font-medium">{pipeline.name}</p>
                        <p className="text-xs text-[var(--color-muted)]">{pipeline.external_id}</p>
                      </td>
                      <td className="px-4 py-3 uppercase">{pipeline.execution_mode}</td>
                      <td className="px-4 py-3">
                        <div className="flex flex-wrap gap-1">
                          {pipeline.tags.map((tag) => (
                            <Badge key={tag} label={tag} />
                          ))}
                        </div>
                      </td>
                      <td className="px-4 py-3">{run ? <RunStatusBadge status={run.status} /> : <Badge label="No runs" />}</td>
                      <td className="px-4 py-3">
                        {run?.status === "SUCCEEDED" ? (
                          <Badge label="Healthy" variant="success" />
                        ) : run ? (
                          <Badge label="Needs attention" variant="warning" />
                        ) : (
                          <Badge label="Unknown" />
                        )}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex flex-wrap gap-2">
                          <Link href={`/pipelines/${pipeline.id}`} className="text-[var(--color-accent)] underline underline-offset-2">
                            Detail
                          </Link>
                          <Link
                            href={`/pipelines/${pipeline.id}/builder`}
                            className="text-[var(--color-accent)] underline underline-offset-2"
                          >
                            Builder
                          </Link>
                          {canCreate ? (
                            <button
                              className="text-[var(--color-danger)] underline underline-offset-2"
                              onClick={async () => {
                                if (!confirm(`Delete pipeline ${pipeline.name}?`)) {
                                  return;
                                }
                                await apiCall((client) => client.deletePipeline(pipeline.id));
                                setPipelines((prev) => prev.filter((item) => item.id !== pipeline.id));
                              }}
                            >
                              Delete
                            </button>
                          ) : null}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {filtered.length === 0 ? (
              <div className="px-4 py-8 text-center text-sm text-[var(--color-muted)]">No pipelines match the current filters.</div>
            ) : null}
          </CardContent>
        </Card>
      </div>
    </DashboardShell>
  );
}
