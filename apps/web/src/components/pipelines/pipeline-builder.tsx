"use client";

import { useEffect, useMemo, useState } from "react";
import { ArrowRight, GripVertical, Plus, Save, Send, UploadCloud } from "lucide-react";

import { useAuth } from "@/components/auth/auth-provider";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import type { Pipeline, PipelineSpecDocument, PipelineVersion, StageDefinition } from "@/types/api";

const defaultSpec = (name = "Untitled Pipeline"): PipelineSpecDocument => ({
  name,
  description: "",
  tags: [],
  owners: [],
  team_ids: [],
  execution_mode: "streaming",
  stages: [],
  edges: [],
  io: {
    source: { kind: "inline", static_data: [] },
    sink: { kind: "none", uri: "" },
  },
  runtime: {
    ray_address: "auto",
    autoscaling: {},
    retry_policy: {},
  },
  observability: {
    log_level: "INFO",
    metrics_enabled: true,
    tracing_enabled: false,
  },
  metadata_links: {
    datasets: [],
    models: [],
  },
});

function normalizeLinearEdges(stages: StageDefinition[]) {
  return stages.slice(0, -1).map((stage, index) => ({
    source: stage.stage_id,
    target: stages[index + 1]?.stage_id,
  }));
}

function slug(value: string) {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
  return normalized || "pipeline";
}

export function PipelineBuilder({
  pipeline,
  initialVersion,
  onSaved,
}: {
  pipeline?: Pipeline | null;
  initialVersion?: PipelineVersion | null;
  onSaved?: (pipelineId: string) => void;
}) {
  const { apiCall, hasRole } = useAuth();
  const [spec, setSpec] = useState<PipelineSpecDocument>(initialVersion?.spec ?? defaultSpec(pipeline?.name));
  const [changeSummary, setChangeSummary] = useState("UI edit");
  const [templates, setTemplates] = useState<Array<{ id: string; name: string; description: string }>>([]);
  const [versions, setVersions] = useState<PipelineVersion[]>([]);
  const [activeVersionId, setActiveVersionId] = useState<string | null>(initialVersion?.id ?? null);
  const [selectedStageId, setSelectedStageId] = useState<string | null>(spec.stages[0]?.stage_id ?? null);
  const [message, setMessage] = useState<string | null>(null);
  const [diffPreview, setDiffPreview] = useState<Record<string, unknown> | null>(null);

  const canEdit = hasRole("PIPELINE_DEV") || hasRole("INFRA_ADMIN");
  const canPublish = hasRole("INFRA_ADMIN");

  useEffect(() => {
    setSpec(initialVersion?.spec ?? defaultSpec(pipeline?.name));
    setActiveVersionId(initialVersion?.id ?? null);
    setSelectedStageId(initialVersion?.spec.stages[0]?.stage_id ?? null);
  }, [initialVersion, pipeline?.name]);

  useEffect(() => {
    void apiCall((client) => client.listStageTemplates()).then(setTemplates).catch(() => setTemplates([]));
  }, [apiCall]);

  useEffect(() => {
    if (!pipeline?.id) {
      return;
    }
    void apiCall((client) => client.listVersions(pipeline.id)).then(setVersions).catch(() => setVersions([]));
  }, [apiCall, pipeline?.id]);

  const selectedStage = useMemo(
    () => spec.stages.find((stage) => stage.stage_id === selectedStageId) ?? null,
    [spec.stages, selectedStageId],
  );

  const updateStage = (stageId: string, patch: Partial<StageDefinition>) => {
    setSpec((prev) => {
      const nextStages = prev.stages.map((stage) => (stage.stage_id === stageId ? { ...stage, ...patch } : stage));
      return {
        ...prev,
        stages: nextStages,
        edges: normalizeLinearEdges(nextStages),
      };
    });
  };

  const reorderStage = (dragStageId: string, dropStageId: string) => {
    if (dragStageId === dropStageId) {
      return;
    }

    setSpec((prev) => {
      const fromIndex = prev.stages.findIndex((s) => s.stage_id === dragStageId);
      const toIndex = prev.stages.findIndex((s) => s.stage_id === dropStageId);
      if (fromIndex < 0 || toIndex < 0) {
        return prev;
      }

      const stages = [...prev.stages];
      const [moved] = stages.splice(fromIndex, 1);
      stages.splice(toIndex, 0, moved);
      return {
        ...prev,
        stages,
        edges: normalizeLinearEdges(stages),
      };
    });
  };

  const addTemplateStage = (templateId: string) => {
    const nextId = `stage_${spec.stages.length + 1}`;
    const stage: StageDefinition = {
      stage_id: nextId,
      name: templateId.split(".").pop()?.replace("_", " ") ?? nextId,
      stage_template: templateId,
      batch_size: 1,
      concurrency_hint: 1,
      retries: 0,
      resources: { cpus: 1, gpus: 0 },
      params: {},
    };

    setSpec((prev) => {
      const nextStages = [...prev.stages, stage];
      return {
        ...prev,
        stages: nextStages,
        edges: normalizeLinearEdges(nextStages),
      };
    });
    setSelectedStageId(nextId);
  };

  const removeStage = (stageId: string) => {
    setSpec((prev) => {
      const nextStages = prev.stages.filter((stage) => stage.stage_id !== stageId);
      return {
        ...prev,
        stages: nextStages,
        edges: normalizeLinearEdges(nextStages),
      };
    });
    if (selectedStageId === stageId) {
      setSelectedStageId(null);
    }
  };

  const saveDraft = async () => {
    if (!canEdit) {
      return;
    }

    try {
      let pipelineId = pipeline?.id;
      if (!pipelineId) {
        const created = await apiCall((client) =>
          client.createPipeline({
            external_id: `${slug(spec.name)}-${Date.now()}`,
            name: spec.name,
            description: spec.description,
            tags: spec.tags,
            execution_mode: spec.execution_mode,
          }),
        );
        pipelineId = created.id;
      }

      const version = await apiCall((client) => client.createVersion(pipelineId!, spec, changeSummary));
      setActiveVersionId(version.id);
      setMessage(`Saved draft v${version.version_number}`);
      if (pipelineId) {
        const list = await apiCall((client) => client.listVersions(pipelineId));
        setVersions(list);
        onSaved?.(pipelineId);
      }
    } catch (error) {
      setMessage(`Save failed: ${String(error)}`);
    }
  };

  const submitReview = async () => {
    if (!pipeline?.id || !activeVersionId) {
      setMessage("Save a draft first");
      return;
    }
    try {
      const version = await apiCall((client) => client.submitReview(pipeline.id, activeVersionId, "Submitted from UI"));
      setMessage(`Version ${version.version_number} submitted for review`);
      setVersions(await apiCall((client) => client.listVersions(pipeline.id)));
    } catch (error) {
      setMessage(`Submit failed: ${String(error)}`);
    }
  };

  const publish = async () => {
    if (!pipeline?.id || !activeVersionId) {
      setMessage("No draft version selected");
      return;
    }
    try {
      await apiCall((client) => client.publishVersion(pipeline.id, activeVersionId, "Published from UI"));
      setMessage("Published successfully");
      setVersions(await apiCall((client) => client.listVersions(pipeline.id)));
    } catch (error) {
      setMessage(`Publish failed: ${String(error)}`);
    }
  };

  const previewDiff = async () => {
    if (!pipeline?.id || versions.length < 2) {
      setMessage("Need at least two versions for diff");
      return;
    }
    const [latest, previous] = versions.slice(0, 2);
    const diff = await apiCall((client) => client.getDiff(pipeline.id, previous.id, latest.id));
    setDiffPreview(diff);
  };

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div>
              <h2 className="text-lg font-semibold">Pipeline Builder</h2>
              <p className="text-sm text-[var(--color-muted)]">DAG-like canvas with linear-chain enforcement in v1</p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button onClick={saveDraft} disabled={!canEdit}>
                <Save className="mr-1 h-4 w-4" />
                Save Draft
              </Button>
              <Button variant="secondary" onClick={submitReview} disabled={!canEdit || !activeVersionId}>
                <Send className="mr-1 h-4 w-4" />
                Submit Review
              </Button>
              <Button variant="secondary" onClick={publish} disabled={!canPublish || !activeVersionId}>
                <UploadCloud className="mr-1 h-4 w-4" />
                Publish
              </Button>
            </div>
          </div>
          {message ? <p className="mt-2 text-xs text-[var(--color-muted)]">{message}</p> : null}
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-2">
          <label className="space-y-1">
            <span className="text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">Name</span>
            <Input value={spec.name} onChange={(e) => setSpec({ ...spec, name: e.target.value })} />
          </label>
          <label className="space-y-1">
            <span className="text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">Execution Mode</span>
            <select
              className="h-9 w-full rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] px-3 text-sm"
              value={spec.execution_mode}
              onChange={(e) =>
                setSpec({
                  ...spec,
                  execution_mode: e.target.value as PipelineSpecDocument["execution_mode"],
                })
              }
            >
              <option value="streaming">Streaming</option>
              <option value="batch">Batch</option>
              <option value="serving">Serving</option>
            </select>
          </label>
          <label className="space-y-1 md:col-span-2">
            <span className="text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">Description</span>
            <Textarea value={spec.description} onChange={(e) => setSpec({ ...spec, description: e.target.value })} rows={2} />
          </label>
        </CardContent>
      </Card>

      <div className="grid gap-4 lg:grid-cols-[260px_1fr_320px]">
        <Card>
          <CardHeader>
            <p className="text-sm font-semibold">Stage Palette</p>
          </CardHeader>
          <CardContent className="space-y-2">
            {templates.map((template) => (
              <button
                key={template.id}
                draggable
                onDragStart={(event) => {
                  event.dataTransfer.setData("xenna/template", template.id);
                }}
                onDoubleClick={() => addTemplateStage(template.id)}
                className="w-full rounded-md border border-dashed border-[var(--color-card-border)] bg-[var(--color-surface)] px-3 py-2 text-left text-sm hover:border-[var(--color-accent)]"
              >
                <p className="font-medium">{template.name}</p>
                <p className="text-xs text-[var(--color-muted)]">{template.id}</p>
              </button>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <p className="text-sm font-semibold">Canvas (Linear DAG)</p>
              <div className="flex items-center gap-2">
                <Badge label={`${spec.stages.length} stage${spec.stages.length === 1 ? "" : "s"}`} />
                <Button variant="secondary" onClick={previewDiff} disabled={versions.length < 2}>
                  Preview Diff
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div
              className="min-h-[360px] rounded-lg border border-dashed border-[var(--color-card-border)] bg-[var(--color-surface)] p-4"
              onDragOver={(event) => event.preventDefault()}
              onDrop={(event) => {
                event.preventDefault();
                const templateId = event.dataTransfer.getData("xenna/template");
                if (templateId) {
                  addTemplateStage(templateId);
                }
              }}
            >
              {spec.stages.length === 0 ? (
                <div className="flex h-full min-h-[300px] items-center justify-center rounded-md border border-dashed border-[var(--color-card-border)] text-sm text-[var(--color-muted)]">
                  Drag stage templates here or double click from palette
                </div>
              ) : (
                <ol className="space-y-2">
                  {spec.stages.map((stage, index) => (
                    <li
                      key={stage.stage_id}
                      draggable
                      onDragStart={(event) => event.dataTransfer.setData("xenna/stage", stage.stage_id)}
                      onDragOver={(event) => event.preventDefault()}
                      onDrop={(event) => {
                        event.preventDefault();
                        const dragged = event.dataTransfer.getData("xenna/stage");
                        if (dragged) {
                          reorderStage(dragged, stage.stage_id);
                        }
                      }}
                      className={`rounded-md border p-3 ${
                        selectedStageId === stage.stage_id
                          ? "border-[var(--color-accent)] bg-[color-mix(in_oklab,var(--color-accent),white_92%)]"
                          : "border-[var(--color-card-border)] bg-white"
                      }`}
                      onClick={() => setSelectedStageId(stage.stage_id)}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                          <GripVertical className="h-4 w-4 text-[var(--color-muted)]" />
                          <span className="text-sm font-semibold">
                            {index + 1}. {stage.name}
                          </span>
                        </div>
                        <div className="flex items-center gap-1">
                          <Badge label={stage.stage_template || "import"} />
                          <Button
                            variant="ghost"
                            className="h-7 px-2 text-xs"
                            onClick={(event) => {
                              event.stopPropagation();
                              removeStage(stage.stage_id);
                            }}
                          >
                            Remove
                          </Button>
                        </div>
                      </div>
                      {index < spec.stages.length - 1 ? (
                        <div className="mt-2 flex items-center text-[var(--color-muted)]">
                          <ArrowRight className="h-4 w-4" />
                          <span className="ml-1 text-xs">Linear edge</span>
                        </div>
                      ) : null}
                    </li>
                  ))}
                </ol>
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <p className="text-sm font-semibold">Stage Config</p>
          </CardHeader>
          <CardContent className="space-y-3">
            {!selectedStage ? (
              <p className="text-sm text-[var(--color-muted)]">Select a stage from the canvas.</p>
            ) : (
              <>
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">Stage Name</span>
                  <Input value={selectedStage.name} onChange={(e) => updateStage(selectedStage.stage_id, { name: e.target.value })} />
                </label>
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">Batch Size</span>
                  <Input
                    type="number"
                    value={selectedStage.batch_size ?? 1}
                    onChange={(e) => updateStage(selectedStage.stage_id, { batch_size: Number(e.target.value) })}
                  />
                </label>
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">Concurrency Hint</span>
                  <Input
                    type="number"
                    value={selectedStage.concurrency_hint ?? 1}
                    onChange={(e) => updateStage(selectedStage.stage_id, { concurrency_hint: Number(e.target.value) })}
                  />
                </label>
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">CPU</span>
                  <Input
                    type="number"
                    step="0.1"
                    value={selectedStage.resources?.cpus ?? 1}
                    onChange={(e) =>
                      updateStage(selectedStage.stage_id, {
                        resources: {
                          ...(selectedStage.resources ?? { cpus: 1, gpus: 0 }),
                          cpus: Number(e.target.value),
                        },
                      })
                    }
                  />
                </label>
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">GPU</span>
                  <Input
                    type="number"
                    step="0.1"
                    value={selectedStage.resources?.gpus ?? 0}
                    onChange={(e) =>
                      updateStage(selectedStage.stage_id, {
                        resources: {
                          ...(selectedStage.resources ?? { cpus: 1, gpus: 0 }),
                          gpus: Number(e.target.value),
                        },
                      })
                    }
                  />
                </label>
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">Params (JSON)</span>
                  <Textarea
                    rows={6}
                    value={JSON.stringify(selectedStage.params ?? {}, null, 2)}
                    onChange={(e) => {
                      try {
                        const parsed = JSON.parse(e.target.value);
                        updateStage(selectedStage.stage_id, { params: parsed });
                      } catch {
                        // Avoid interrupting user typing invalid transient JSON.
                      }
                    }}
                  />
                </label>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold">Code View</h3>
            <Button
              variant="secondary"
              onClick={() => navigator.clipboard.writeText(JSON.stringify(spec, null, 2))}
              className="h-8"
            >
              Copy JSON
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <pre className="max-h-[320px] overflow-auto rounded-md bg-[#0f172a] p-3 text-xs text-slate-100">
            {JSON.stringify(spec, null, 2)}
          </pre>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold">Version Timeline</h3>
            {activeVersionId ? <Badge label={`Active: ${activeVersionId.slice(0, 8)}`} /> : null}
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {versions.length === 0 ? (
            <p className="text-sm text-[var(--color-muted)]">No versions yet.</p>
          ) : (
            <div className="space-y-2">
              {versions.map((version) => (
                <button
                  key={version.id}
                  onClick={() => {
                    setActiveVersionId(version.id);
                    setSpec(version.spec);
                  }}
                  className={`w-full rounded-md border px-3 py-2 text-left text-sm ${
                    version.id === activeVersionId
                      ? "border-[var(--color-accent)] bg-[color-mix(in_oklab,var(--color-accent),white_92%)]"
                      : "border-[var(--color-card-border)] bg-[var(--color-surface)]"
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span>
                      v{version.version_number} {version.is_active ? "(published)" : ""}
                    </span>
                    <Badge label={version.status} />
                  </div>
                  {canPublish && version.status === "IN_REVIEW" ? (
                    <div className="mt-2 flex gap-2">
                      <Button
                        variant="secondary"
                        className="h-7 px-2 text-xs"
                        onClick={async (event) => {
                          event.stopPropagation();
                          if (!pipeline?.id) return;
                          await apiCall((client) => client.approveVersion(pipeline.id, version.id, "Approved from UI"));
                          setVersions(await apiCall((client) => client.listVersions(pipeline.id)));
                        }}
                      >
                        Approve
                      </Button>
                      <Button
                        variant="danger"
                        className="h-7 px-2 text-xs"
                        onClick={async (event) => {
                          event.stopPropagation();
                          if (!pipeline?.id) return;
                          await apiCall((client) => client.rejectVersion(pipeline.id, version.id, "Rejected from UI"));
                          setVersions(await apiCall((client) => client.listVersions(pipeline.id)));
                        }}
                      >
                        Reject
                      </Button>
                    </div>
                  ) : null}
                </button>
              ))}
            </div>
          )}

          {diffPreview ? (
            <div>
              <p className="mb-1 text-xs uppercase tracking-[0.16em] text-[var(--color-muted)]">Latest Diff</p>
              <pre className="max-h-[240px] overflow-auto rounded-md bg-[#0f172a] p-3 text-xs text-slate-100">
                {JSON.stringify(diffPreview, null, 2)}
              </pre>
            </div>
          ) : null}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <h3 className="text-sm font-semibold">Generated Python Skeleton</h3>
        </CardHeader>
        <CardContent>
          <pre className="max-h-[280px] overflow-auto rounded-md bg-[#111827] p-3 text-xs text-slate-100">
{`from cosmos_xenna.pipelines import v1 as pipelines_v1

# Fill stage implementations and imports
stages = [
${spec.stages
  .map(
    (stage) =>
      `    pipelines_v1.StageSpec(${stage.python_import_path ? `${stage.python_import_path.split(":").at(-1)}()` : "MyStage()"}),`
  )
  .join("\n")}
]

pipeline_spec = pipelines_v1.PipelineSpec(
    input_data=[],
    stages=stages,
    config=pipelines_v1.PipelineConfig(execution_mode=pipelines_v1.ExecutionMode.${spec.execution_mode.toUpperCase()}),
)

outputs = pipelines_v1.run_pipeline(pipeline_spec)
`}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}

export function NewStageButton({ onClick }: { onClick: () => void }) {
  return (
    <Button variant="secondary" onClick={onClick}>
      <Plus className="mr-1 h-4 w-4" />
      Add Stage
    </Button>
  );
}
