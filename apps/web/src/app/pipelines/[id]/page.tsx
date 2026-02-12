"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";

import { useAuth } from "@/components/auth/auth-provider";
import { DashboardShell } from "@/components/layout/dashboard-shell";
import { RunStatusBadge } from "@/components/runs/status-badge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import type { Pipeline, PipelineRun, PipelineVersion } from "@/types/api";

export default function PipelineDetailPage() {
  const { id } = useParams<{ id: string }>();
  const { apiCall, hasRole } = useAuth();
  const [pipeline, setPipeline] = useState<Pipeline | null>(null);
  const [versions, setVersions] = useState<PipelineVersion[]>([]);
  const [runs, setRuns] = useState<PipelineRun[]>([]);
  const [tab, setTab] = useState<"overview" | "versions" | "runs" | "settings">("overview");
  const [shares, setShares] = useState<Array<{ id: string; team_id: string; access_level: string }>>([]);
  const [shareTeamId, setShareTeamId] = useState("");
  const [shareAccess, setShareAccess] = useState("READ");

  useEffect(() => {
    void apiCall((client) => client.listPipelines()).then((items) => {
      setPipeline(items.find((item) => item.id === id) ?? null);
    });
    void apiCall((client) => client.listVersions(id)).then(setVersions);
    void apiCall((client) => client.listRuns()).then((items) => setRuns(items.filter((run) => run.pipeline_id === id)));
    void apiCall((client) => client.listPipelineShares(id)).then(setShares);
  }, [apiCall, id]);

  const latestRun = runs[0];
  const canEdit = hasRole("PIPELINE_DEV") || hasRole("INFRA_ADMIN");

  const health = useMemo(() => {
    if (!runs.length) {
      return "No runs";
    }
    const success = runs.filter((run) => run.status === "SUCCEEDED").length;
    return `${Math.round((success / runs.length) * 100)}% success`;
  }, [runs]);

  if (!pipeline) {
    return (
      <DashboardShell>
        <Card>
          <CardContent className="py-8 text-sm text-[var(--color-muted)]">Pipeline not found.</CardContent>
        </Card>
      </DashboardShell>
    );
  }

  return (
    <DashboardShell>
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <h1 className="text-2xl font-semibold">{pipeline.name}</h1>
                <p className="text-sm text-[var(--color-muted)]">{pipeline.description || "No description"}</p>
              </div>
              <div className="flex items-center gap-2">
                <Badge label={pipeline.execution_mode.toUpperCase()} />
                {latestRun ? <RunStatusBadge status={latestRun.status} /> : <Badge label="No runs" />}
                <Link href={`/pipelines/${pipeline.id}/builder`}>
                  <Button>Open Builder</Button>
                </Link>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-2">
              {pipeline.tags.map((tag) => (
                <Badge key={tag} label={tag} />
              ))}
            </div>
            <p className="mt-2 text-sm text-[var(--color-muted)]">Health: {health}</p>
          </CardContent>
        </Card>

        <div className="flex flex-wrap gap-2">
          <Button variant={tab === "overview" ? "default" : "secondary"} onClick={() => setTab("overview")}>Overview</Button>
          <Button variant={tab === "versions" ? "default" : "secondary"} onClick={() => setTab("versions")}>Versions</Button>
          <Button variant={tab === "runs" ? "default" : "secondary"} onClick={() => setTab("runs")}>Runs</Button>
          <Button variant={tab === "settings" ? "default" : "secondary"} onClick={() => setTab("settings")}>Settings</Button>
        </div>

        {tab === "overview" ? (
          <Card>
            <CardContent className="grid gap-3 py-4 md:grid-cols-3">
              <div className="rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] p-3">
                <p className="text-xs text-[var(--color-muted)]">External ID</p>
                <p className="font-medium">{pipeline.external_id}</p>
              </div>
              <div className="rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] p-3">
                <p className="text-xs text-[var(--color-muted)]">Published Version</p>
                <p className="font-medium">{versions.find((v) => v.is_active)?.version_number ?? "n/a"}</p>
              </div>
              <div className="rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] p-3">
                <p className="text-xs text-[var(--color-muted)]">Total Runs</p>
                <p className="font-medium">{runs.length}</p>
              </div>
            </CardContent>
          </Card>
        ) : null}

        {tab === "versions" ? (
          <Card>
            <CardContent className="space-y-2 py-4">
              {versions.map((version) => (
                <div key={version.id} className="rounded-md border border-[var(--color-card-border)] p-3">
                  <div className="flex items-center justify-between">
                    <p className="font-medium">Version {version.version_number}</p>
                    <div className="flex items-center gap-2">
                      <Badge label={version.status} />
                      {version.is_active ? <Badge label="ACTIVE" variant="success" /> : null}
                    </div>
                  </div>
                  <p className="mt-1 text-xs text-[var(--color-muted)]">{version.change_summary || "No summary"}</p>
                  <pre className="mt-2 max-h-40 overflow-auto rounded bg-[#0f172a] p-2 text-xs text-slate-100">
                    {JSON.stringify(version.spec, null, 2)}
                  </pre>
                </div>
              ))}
            </CardContent>
          </Card>
        ) : null}

        {tab === "runs" ? (
          <Card>
            <CardContent className="overflow-x-auto p-0">
              <table className="min-w-full text-sm">
                <thead className="bg-[var(--color-surface)] text-left text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">
                  <tr>
                    <th className="px-4 py-3">Run</th>
                    <th className="px-4 py-3">Status</th>
                    <th className="px-4 py-3">Duration</th>
                    <th className="px-4 py-3">Trigger</th>
                    <th className="px-4 py-3">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((run) => (
                    <tr key={run.id} className="border-t border-[var(--color-card-border)]">
                      <td className="px-4 py-3">{run.id.slice(0, 8)}</td>
                      <td className="px-4 py-3">
                        <RunStatusBadge status={run.status} />
                      </td>
                      <td className="px-4 py-3">{run.duration_seconds?.toFixed(2) ?? "-"}s</td>
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
        ) : null}

        {tab === "settings" ? (
          <Card>
            <CardHeader>
              <h3 className="text-sm font-semibold">Pipeline Access Settings</h3>
            </CardHeader>
            <CardContent className="space-y-3">
              <p className="text-sm text-[var(--color-muted)]">
                Share this pipeline with a team using READ/WRITE/OWNER access.
              </p>
              <div className="grid gap-2 md:grid-cols-[1fr_180px_120px]">
                <Input placeholder="Team ID" value={shareTeamId} onChange={(e) => setShareTeamId(e.target.value)} />
                <select
                  className="h-9 rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] px-3 text-sm"
                  value={shareAccess}
                  onChange={(e) => setShareAccess(e.target.value)}
                >
                  <option value="READ">READ</option>
                  <option value="WRITE">WRITE</option>
                  <option value="OWNER">OWNER</option>
                </select>
                <Button
                  disabled={!canEdit || !shareTeamId}
                  onClick={async () => {
                    await apiCall((client) =>
                      client.upsertPipelineShare(pipeline.id, {
                        team_id: shareTeamId,
                        access_level: shareAccess as "READ" | "WRITE" | "OWNER",
                      }),
                    );
                    setShares(await apiCall((client) => client.listPipelineShares(pipeline.id)));
                    setShareTeamId("");
                  }}
                >
                  Save
                </Button>
              </div>
              <div className="space-y-1 text-xs text-[var(--color-muted)]">
                {shares.map((share) => (
                  <p key={share.id}>
                    team {share.team_id}: {share.access_level}
                  </p>
                ))}
                {shares.length === 0 ? <p>No team shares configured.</p> : null}
              </div>
            </CardContent>
          </Card>
        ) : null}
      </div>
    </DashboardShell>
  );
}
