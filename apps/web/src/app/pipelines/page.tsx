"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { ArrowUpRight, Clock3, Plus, Search, Sparkles, X } from "lucide-react";

import { DashboardShell } from "@/components/layout/dashboard-shell";
import { RunStatusBadge } from "@/components/runs/status-badge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { useAuth } from "@/components/auth/auth-provider";
import {
  buildTemplateCatalogEntries,
  buildTemplateThemeOptions,
  filterTemplateEntries,
  sortTemplateEntries,
  suggestTemplateThemes,
  type TemplateQuickFilter,
  type TemplateSort,
} from "@/components/pipelines/pipeline-catalog";
import type { Pipeline, PipelineRun } from "@/types/api";

const RECENT_TEMPLATES_KEY = "pipelineforge.starter_templates.recent";
const MAX_RECENT_TEMPLATES = 12;

export default function PipelinesPage() {
  const { apiCall, hasRole } = useAuth();
  const router = useRouter();
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [runs, setRuns] = useState<PipelineRun[]>([]);
  const [search, setSearch] = useState("");
  const [tagFilter, setTagFilter] = useState("");
  const [templateSearch, setTemplateSearch] = useState("");
  const [debouncedTemplateSearch, setDebouncedTemplateSearch] = useState("");
  const [activeThemeId, setActiveThemeId] = useState("all");
  const [quickFilter, setQuickFilter] = useState<TemplateQuickFilter>("all");
  const [templateSort, setTemplateSort] = useState<TemplateSort>("recommended");
  const [selectedTemplateId, setSelectedTemplateId] = useState<string | null>(null);
  const [recentTemplateIds, setRecentTemplateIds] = useState<string[]>([]);
  const [templateBusyId, setTemplateBusyId] = useState<string | null>(null);
  const [templateMessage, setTemplateMessage] = useState<string | null>(null);

  const canCreate = hasRole("PIPELINE_DEV") || hasRole("INFRA_ADMIN");

  useEffect(() => {
    void apiCall((client) => client.listPipelines()).then(setPipelines).catch(() => setPipelines([]));
    void apiCall((client) => client.listRuns()).then(setRuns).catch(() => setRuns([]));
  }, [apiCall]);

  useEffect(() => {
    try {
      const stored = localStorage.getItem(RECENT_TEMPLATES_KEY);
      if (!stored) {
        return;
      }
      const parsed = JSON.parse(stored) as string[];
      if (Array.isArray(parsed)) {
        setRecentTemplateIds(parsed.filter((item) => typeof item === "string"));
      }
    } catch {
      setRecentTemplateIds([]);
    }
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setDebouncedTemplateSearch(templateSearch.trim().toLowerCase());
    }, 180);
    return () => window.clearTimeout(timer);
  }, [templateSearch]);

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
  const templateEntries = useMemo(() => buildTemplateCatalogEntries(templates), [templates]);
  const themeOptions = useMemo(
    () => buildTemplateThemeOptions(templateEntries, recentTemplateIds),
    [templateEntries, recentTemplateIds],
  );
  const filteredTemplateEntries = useMemo(
    () =>
      filterTemplateEntries(templateEntries, {
        query: debouncedTemplateSearch,
        themeId: activeThemeId,
        quickFilter,
        recentlyUsedIds: recentTemplateIds,
      }),
    [activeThemeId, debouncedTemplateSearch, quickFilter, recentTemplateIds, templateEntries],
  );
  const sortedTemplateEntries = useMemo(
    () => sortTemplateEntries(filteredTemplateEntries, templateSort, recentTemplateIds),
    [filteredTemplateEntries, recentTemplateIds, templateSort],
  );
  const suggestedThemes = useMemo(() => suggestTemplateThemes(templateEntries), [templateEntries]);

  useEffect(() => {
    if (sortedTemplateEntries.length === 0) {
      setSelectedTemplateId(null);
      return;
    }
    if (!selectedTemplateId || !sortedTemplateEntries.some((entry) => entry.pipeline.id === selectedTemplateId)) {
      setSelectedTemplateId(sortedTemplateEntries[0]?.pipeline.id ?? null);
    }
  }, [selectedTemplateId, sortedTemplateEntries]);

  const selectedTemplateEntry = useMemo(
    () => sortedTemplateEntries.find((entry) => entry.pipeline.id === selectedTemplateId) ?? null,
    [selectedTemplateId, sortedTemplateEntries],
  );

  const markTemplateUsed = useCallback((templateId: string) => {
    setRecentTemplateIds((previous) => {
      const next = [templateId, ...previous.filter((item) => item !== templateId)].slice(0, MAX_RECENT_TEMPLATES);
      try {
        localStorage.setItem(RECENT_TEMPLATES_KEY, JSON.stringify(next));
      } catch {
        // Ignore localStorage failures.
      }
      return next;
    });
  }, []);

  const runTemplateDemo = useCallback(
    async (templateId: string) => {
      setTemplateBusyId(templateId);
      setTemplateMessage(null);
      try {
        await apiCall((client) => client.triggerRun(templateId));
        setRuns(await apiCall((client) => client.listRuns()));
        markTemplateUsed(templateId);
        setTemplateMessage("Demo run triggered.");
      } catch (error) {
        setTemplateMessage(`Run failed: ${String(error)}`);
      } finally {
        setTemplateBusyId(null);
      }
    },
    [apiCall, markTemplateUsed],
  );

  const useTemplate = useCallback(
    async (template: Pipeline) => {
      setTemplateBusyId(template.id);
      setTemplateMessage(null);
      try {
        const versions = await apiCall((client) => client.listVersions(template.id));
        const sourceVersion = versions[0];
        if (!sourceVersion) {
          throw new Error("Template has no versions");
        }

        const baseName = template.name.replace(/^Template:\\s*/i, "").trim() || template.name;
        const cloneName = `${baseName} Copy`;
        const cloneExternalId = `${template.external_id.replace(/[^a-zA-Z0-9_]+/g, "_").toLowerCase()}_${Date.now()}`;
        const cloneTags = template.tags.filter((tag) => tag !== "template" && tag !== "starter");

        const createdPipeline = await apiCall((client) =>
          client.createPipeline({
            external_id: cloneExternalId,
            name: cloneName,
            description: template.description,
            tags: cloneTags,
            execution_mode: sourceVersion.spec.execution_mode,
          }),
        );

        const clonedSpec = {
          ...sourceVersion.spec,
          pipeline_id: createdPipeline.id,
          name: cloneName,
          tags: cloneTags,
        };

        await apiCall((client) =>
          client.createVersion(createdPipeline.id, clonedSpec, `Created from starter template ${template.external_id}`),
        );

        markTemplateUsed(template.id);
        setTemplateMessage(`Created ${cloneName}. Redirecting to builder...`);
        router.push(`/pipelines/${createdPipeline.id}/builder`);
      } catch (error) {
        setTemplateMessage(`Use template failed: ${String(error)}`);
      } finally {
        setTemplateBusyId(null);
      }
    },
    [apiCall, markTemplateUsed, router],
  );

  return (
    <DashboardShell>
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <h2 className="text-lg font-semibold">Starter Templates</h2>
                <p className="text-sm text-[var(--color-muted)]">
                  Browse themed starter templates and bootstrap a new pipeline in one click.
                </p>
              </div>
              <div className="flex items-center gap-2">
                <Badge label={`${templates.length} templates`} />
                <Badge label={`${sortedTemplateEntries.length} shown`} />
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-3 lg:grid-cols-[1fr_220px]">
              <div className="relative">
                <Search className="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-[var(--color-muted)]" />
                <Input
                  value={templateSearch}
                  onChange={(event) => setTemplateSearch(event.target.value)}
                  className="h-9 bg-white pl-9 pr-9"
                  placeholder="Search templates by name, description, or tags"
                  aria-label="Search starter templates"
                />
                {templateSearch ? (
                  <button
                    type="button"
                    onClick={() => setTemplateSearch("")}
                    className="absolute right-2 top-2 rounded p-0.5 text-[var(--color-muted)] transition hover:bg-slate-100"
                    aria-label="Clear starter template search"
                    title="Clear search"
                  >
                    <X className="h-4 w-4" />
                  </button>
                ) : null}
              </div>
              <select
                className="h-9 w-full rounded-md border border-[var(--color-card-border)] bg-white px-3 text-sm"
                value={templateSort}
                onChange={(event) => setTemplateSort(event.target.value as TemplateSort)}
                aria-label="Sort starter templates"
              >
                <option value="recommended">Recommended</option>
                <option value="a_to_z">A-Z</option>
                <option value="recently_added">Recently Added</option>
                <option value="recently_used">Recently Used</option>
              </select>
            </div>

            <div className="flex flex-wrap gap-2">
              {themeOptions.map((theme) => {
                const active = activeThemeId === theme.id;
                return (
                  <button
                    key={theme.id}
                    type="button"
                    onClick={() => setActiveThemeId(theme.id)}
                    className={`rounded-full border px-3 py-1 text-xs transition ${
                      active
                        ? "border-[var(--color-accent)] bg-[color-mix(in_oklab,var(--color-accent),white_90%)] text-[var(--color-text)]"
                        : "border-[var(--color-card-border)] bg-white text-[var(--color-muted)] hover:text-[var(--color-text)]"
                    }`}
                  >
                    {theme.label} ({theme.count})
                  </button>
                );
              })}
            </div>

            <div className="flex flex-wrap gap-2">
              {[
                { id: "all", label: "All Modes" },
                { id: "streaming", label: "Streaming" },
                { id: "batch", label: "Batch" },
                { id: "etl", label: "ETL" },
                { id: "ml", label: "ML" },
                { id: "ops", label: "Ops/Monitoring" },
              ].map((filterOption) => {
                const active = quickFilter === filterOption.id;
                return (
                  <button
                    key={filterOption.id}
                    type="button"
                    onClick={() => setQuickFilter(filterOption.id as TemplateQuickFilter)}
                    className={`rounded-full border px-2.5 py-1 text-xs transition ${
                      active
                        ? "border-[var(--flow-orange)] bg-orange-50 text-[var(--color-text)]"
                        : "border-[var(--color-card-border)] bg-white text-[var(--color-muted)] hover:text-[var(--color-text)]"
                    }`}
                  >
                    {filterOption.label}
                  </button>
                );
              })}
            </div>

            {templateMessage ? (
              <div className="rounded-lg border border-[var(--flow-panel-border)] bg-[var(--color-surface)] px-3 py-2 text-xs text-[var(--color-muted)]">
                {templateMessage}
              </div>
            ) : null}

            {sortedTemplateEntries.length === 0 ? (
              <div className="rounded-xl border border-dashed border-[var(--flow-panel-border)] bg-[var(--color-surface)] px-4 py-8 text-center">
                <p className="text-sm font-medium">No templates match your current filters.</p>
                <p className="mt-1 text-xs text-[var(--color-muted)]">
                  Suggested themes: {suggestedThemes.length > 0 ? suggestedThemes.join(", ") : "All"}
                </p>
                <Button
                  variant="secondary"
                  className="mt-3 h-8"
                  onClick={() => {
                    setActiveThemeId("all");
                    setQuickFilter("all");
                    setTemplateSearch("");
                    setTemplateSort("recommended");
                  }}
                >
                  Clear Filters
                </Button>
              </div>
            ) : (
              <div className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_320px]">
                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
                  {sortedTemplateEntries.map((entry) => {
                    const selected = selectedTemplateId === entry.pipeline.id;
                    const description = entry.pipeline.description || "No description provided.";
                    return (
                      <button
                        key={entry.pipeline.id}
                        type="button"
                        onClick={() => setSelectedTemplateId(entry.pipeline.id)}
                        className={`rounded-xl border p-3 text-left transition ${
                          selected
                            ? "border-[var(--flow-orange)] bg-orange-50/65 shadow-[0_8px_20px_rgba(249,115,22,0.12)]"
                            : "border-[var(--color-card-border)] bg-[var(--color-surface)] hover:border-[var(--flow-orange)]/60"
                        }`}
                        title={entry.pipeline.name}
                      >
                        <div className="flex items-center justify-between gap-2">
                          <span className="inline-flex h-7 min-w-7 items-center justify-center rounded-md border border-[var(--flow-panel-border)] bg-white px-1.5 text-xs font-semibold">
                            {entry.theme.slice(0, 1).toUpperCase()}
                          </span>
                          <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] uppercase tracking-[0.1em] text-[var(--color-muted)]">
                            {entry.pipeline.execution_mode}
                          </span>
                        </div>
                        <p className="mt-2 truncate text-sm font-semibold text-[var(--color-text)]" title={entry.pipeline.name}>
                          {entry.pipeline.name}
                        </p>
                        <p className="mt-1 line-clamp-2 text-xs text-[var(--color-muted)]">{description}</p>
                        <div className="mt-2 flex flex-wrap gap-1">
                          <Badge label={entry.theme} />
                          {entry.pipeline.tags.slice(0, 3).map((tag) => (
                            <Badge key={`${entry.pipeline.id}-${tag}`} label={tag} />
                          ))}
                        </div>
                      </button>
                    );
                  })}
                </div>

                <aside className="rounded-xl border border-[var(--flow-panel-border)] bg-white/90 p-3">
                  {!selectedTemplateEntry ? (
                    <p className="text-sm text-[var(--color-muted)]">Select any template card to preview details.</p>
                  ) : (
                    <div className="space-y-3">
                      <div>
                        <p className="text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">Preview</p>
                        <h3 className="mt-1 text-base font-semibold" title={selectedTemplateEntry.pipeline.name}>
                          {selectedTemplateEntry.pipeline.name}
                        </h3>
                        <p className="mt-1 text-xs text-[var(--color-muted)]">
                          {selectedTemplateEntry.pipeline.description || "No description provided."}
                        </p>
                      </div>

                      <div className="flex flex-wrap gap-1">
                        <Badge label={selectedTemplateEntry.theme} />
                        <Badge label={selectedTemplateEntry.pipeline.execution_mode.toUpperCase()} />
                        {selectedTemplateEntry.pipeline.tags.slice(0, 5).map((tag) => (
                          <Badge key={`${selectedTemplateEntry.pipeline.id}-detail-${tag}`} label={tag} />
                        ))}
                      </div>

                      <div className="rounded-md border border-[var(--flow-panel-border)] bg-[var(--color-surface)] px-2 py-2 text-xs text-[var(--color-muted)]">
                        <p className="flex items-center gap-1">
                          <Sparkles className="h-3.5 w-3.5" />
                          Recommended for {selectedTemplateEntry.theme}
                        </p>
                        <p className="mt-1 flex items-center gap-1">
                          <Clock3 className="h-3.5 w-3.5" />
                          Last updated: {new Date(selectedTemplateEntry.pipeline.updated_at).toLocaleDateString()}
                        </p>
                      </div>

                      <div className="space-y-2">
                        <Button
                          className="h-8 w-full"
                          onClick={() => void useTemplate(selectedTemplateEntry.pipeline)}
                          disabled={!canCreate || templateBusyId !== null}
                        >
                          {templateBusyId === selectedTemplateEntry.pipeline.id ? "Working..." : "Use Template"}
                        </Button>
                        <Button
                          variant="secondary"
                          className="h-8 w-full"
                          onClick={() => void runTemplateDemo(selectedTemplateEntry.pipeline.id)}
                          disabled={templateBusyId !== null}
                        >
                          Run Demo
                        </Button>
                        <Button
                          variant="secondary"
                          className="h-8 w-full"
                          onClick={() => {
                            markTemplateUsed(selectedTemplateEntry.pipeline.id);
                            router.push(`/pipelines/${selectedTemplateEntry.pipeline.id}/builder`);
                          }}
                        >
                          Open Template
                          <ArrowUpRight className="ml-1 h-4 w-4" />
                        </Button>
                        {!canCreate ? (
                          <p className="text-[11px] text-[var(--color-muted)]">
                            You need `PIPELINE_DEV` or `INFRA_ADMIN` role to create a pipeline from template.
                          </p>
                        ) : null}
                      </div>
                    </div>
                  )}
                </aside>
              </div>
            )}
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
