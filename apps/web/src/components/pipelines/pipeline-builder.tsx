"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowRight,
  ChevronRight,
  Database,
  Search,
  GripVertical,
  Plus,
  Save,
  Send,
  Trash2,
  UploadCloud,
  WandSparkles,
  X,
} from "lucide-react";

import { useAuth } from "@/components/auth/auth-provider";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import type { StageTemplate } from "@/lib/api";
import {
  buildStageCatalogEntries,
  buildStageTree,
  filterStageEntries,
  highlightMatch,
  type StageCatalogEntry,
} from "@/components/pipelines/pipeline-catalog";
import type { Pipeline, PipelineSpecDocument, PipelineVersion, StageDefinition } from "@/types/api";

const defaultSpec = (name = "Untitled Pipeline"): PipelineSpecDocument => ({
  name,
  description: "",
  tags: [],
  owners: [],
  team_ids: [],
  data_model: "samples",
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

type GraphPoint = { x: number; y: number };
type StageAnchors = { input: GraphPoint; output: GraphPoint };
type GraphEdge = PipelineSpecDocument["edges"][number];

type ConnectionDraft =
  | { kind: "new"; sourceId: string; pointer: GraphPoint }
  | { kind: "reconnect-source"; edgeId: string; targetId: string; pointer: GraphPoint }
  | { kind: "reconnect-target"; edgeId: string; sourceId: string; pointer: GraphPoint };

const PALETTE_TREE_STATE_KEY = "pipelineforge.stage_palette.expanded";
const SEARCH_ROW_HEIGHT = 78;
const SEARCH_OVERSCAN = 5;

function edgeId(edge: GraphEdge) {
  return `${edge.source}->${edge.target}`;
}

function sanitizeEdges(edges: GraphEdge[], stageIds: Set<string>) {
  const seen = new Set<string>();
  const cleaned: GraphEdge[] = [];
  for (const edge of edges) {
    if (!stageIds.has(edge.source) || !stageIds.has(edge.target)) {
      continue;
    }
    if (edge.source === edge.target) {
      continue;
    }
    const key = edgeId(edge);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    cleaned.push(edge);
  }
  return cleaned;
}

function normalizeSpecEdges(spec: PipelineSpecDocument): PipelineSpecDocument {
  const stageIds = new Set(spec.stages.map((stage) => stage.stage_id));
  return {
    ...spec,
    edges: sanitizeEdges(spec.edges ?? [], stageIds),
  };
}

function wouldCreateCycle(edges: GraphEdge[], source: string, target: string) {
  if (source === target) {
    return true;
  }

  const adjacency = new Map<string, string[]>();
  for (const edge of edges) {
    const list = adjacency.get(edge.source);
    if (list) {
      list.push(edge.target);
    } else {
      adjacency.set(edge.source, [edge.target]);
    }
  }

  const queue = [target];
  const visited = new Set<string>();

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || visited.has(current)) {
      continue;
    }
    if (current === source) {
      return true;
    }
    visited.add(current);
    const next = adjacency.get(current);
    if (next) {
      queue.push(...next);
    }
  }

  return false;
}

function buildEdgePath(start: GraphPoint, end: GraphPoint) {
  const bend = Math.max(56, Math.abs(end.x - start.x) * 0.48);
  return `M ${start.x} ${start.y} C ${start.x + bend} ${start.y}, ${end.x - bend} ${end.y}, ${end.x} ${end.y}`;
}

function slug(value: string) {
  const normalized = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
  return normalized || "pipeline";
}

function inferStageKind(stage: StageDefinition, index: number) {
  const value = `${stage.name} ${stage.stage_template ?? ""} ${stage.python_import_path ?? ""}`.toLowerCase();
  if (index === 0 || value.includes("dataset") || value.includes("source")) {
    return {
      label: "Dataset",
      chipClass: "border-violet-200 bg-violet-100 text-violet-700",
      iconClass: "bg-violet-500",
    };
  }
  if (value.includes("filter") || value.includes("judge") || value.includes("guard")) {
    return {
      label: "Filter",
      chipClass: "border-amber-200 bg-amber-100 text-amber-700",
      iconClass: "bg-amber-500",
    };
  }
  if (value.includes("generate") || value.includes("llm") || value.includes("model")) {
    return {
      label: "Generate",
      chipClass: "border-orange-200 bg-orange-100 text-orange-700",
      iconClass: "bg-orange-500",
    };
  }
  return {
    label: "Operator",
    chipClass: "border-cyan-200 bg-cyan-100 text-cyan-700",
    iconClass: "bg-cyan-500",
  };
}

function summarizeParams(params: Record<string, unknown> | undefined) {
  if (!params || Object.keys(params).length === 0) {
    return "No parameters configured yet.";
  }
  const entries = Object.entries(params)
    .slice(0, 2)
    .map(([key, value]) => {
      if (typeof value === "string") {
        return `${key}=${value.slice(0, 18)}`;
      }
      if (typeof value === "number" || typeof value === "boolean") {
        return `${key}=${String(value)}`;
      }
      return `${key}=...`;
    });
  return entries.join("  •  ");
}

function stageLogPreview(stage: StageDefinition) {
  const stageKey = stage.stage_template ?? stage.stage_id;
  return [
    `[queue] ${stageKey} accepted payload`,
    `[runtime] cpus=${stage.resources?.cpus ?? 1} gpus=${stage.resources?.gpus ?? 0}`,
    `[state] retries=${stage.retries ?? 0} concurrency=${stage.concurrency_hint ?? 1}`,
  ];
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
  const [spec, setSpec] = useState<PipelineSpecDocument>(normalizeSpecEdges(initialVersion?.spec ?? defaultSpec(pipeline?.name)));
  const [changeSummary, setChangeSummary] = useState("UI edit");
  const [templates, setTemplates] = useState<StageTemplate[]>([]);
  const [versions, setVersions] = useState<PipelineVersion[]>([]);
  const [activeVersionId, setActiveVersionId] = useState<string | null>(initialVersion?.id ?? null);
  const [selectedStageId, setSelectedStageId] = useState<string | null>(spec.stages[0]?.stage_id ?? null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [connectionDraft, setConnectionDraft] = useState<ConnectionDraft | null>(null);
  const [anchors, setAnchors] = useState<Record<string, StageAnchors>>({});
  const [message, setMessage] = useState<string | null>(null);
  const [diffPreview, setDiffPreview] = useState<Record<string, unknown> | null>(null);
  const [paletteQuery, setPaletteQuery] = useState("");
  const [debouncedPaletteQuery, setDebouncedPaletteQuery] = useState("");
  const [expandedPaletteTree, setExpandedPaletteTree] = useState<Record<string, boolean>>({});
  const [searchScrollTop, setSearchScrollTop] = useState(0);

  const graphRef = useRef<HTMLDivElement | null>(null);
  const stageRefs = useRef<Record<string, HTMLElement | null>>({});
  const searchScrollRef = useRef<HTMLDivElement | null>(null);

  const canEdit = hasRole("PIPELINE_DEV") || hasRole("INFRA_ADMIN");
  const canPublish = hasRole("INFRA_ADMIN");

  useEffect(() => {
    const nextSpec = normalizeSpecEdges(initialVersion?.spec ?? defaultSpec(pipeline?.name));
    setSpec(nextSpec);
    setActiveVersionId(initialVersion?.id ?? null);
    setSelectedStageId(nextSpec.stages[0]?.stage_id ?? null);
    setSelectedEdgeId(null);
    setConnectionDraft(null);
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

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setDebouncedPaletteQuery(paletteQuery.trim().toLowerCase());
    }, 180);
    return () => window.clearTimeout(timer);
  }, [paletteQuery]);

  useEffect(() => {
    try {
      const stored = sessionStorage.getItem(PALETTE_TREE_STATE_KEY);
      if (stored) {
        const parsed = JSON.parse(stored) as Record<string, boolean>;
        setExpandedPaletteTree(parsed);
      }
    } catch {
      // Ignore malformed session state.
    }
  }, []);

  useEffect(() => {
    try {
      sessionStorage.setItem(PALETTE_TREE_STATE_KEY, JSON.stringify(expandedPaletteTree));
    } catch {
      // Ignore storage failures.
    }
  }, [expandedPaletteTree]);

  useEffect(() => {
    setSearchScrollTop(0);
    if (searchScrollRef.current) {
      searchScrollRef.current.scrollTop = 0;
    }
  }, [debouncedPaletteQuery]);

  const stageCatalogEntries = useMemo(() => buildStageCatalogEntries(templates), [templates]);
  const stageTree = useMemo(() => buildStageTree(stageCatalogEntries), [stageCatalogEntries]);
  const stageSearchResults = useMemo(
    () => filterStageEntries(stageCatalogEntries, debouncedPaletteQuery),
    [stageCatalogEntries, debouncedPaletteQuery],
  );

  useEffect(() => {
    if (stageTree.length === 0) {
      return;
    }
    setExpandedPaletteTree((previous) => {
      if (Object.keys(previous).length > 0) {
        return previous;
      }
      const defaults: Record<string, boolean> = {};
      for (const category of stageTree) {
        defaults[category.id] = true;
        for (const subcategory of category.subcategories) {
          defaults[subcategory.id] = true;
        }
      }
      return defaults;
    });
  }, [stageTree]);

  const selectedStage = useMemo(
    () => spec.stages.find((stage) => stage.stage_id === selectedStageId) ?? null,
    [spec.stages, selectedStageId],
  );

  const selectedEdge = useMemo(
    () => spec.edges.find((edge) => edgeId(edge) === selectedEdgeId) ?? null,
    [spec.edges, selectedEdgeId],
  );

  const stageNames = useMemo(() => {
    const map: Record<string, string> = {};
    for (const stage of spec.stages) {
      map[stage.stage_id] = stage.name;
    }
    return map;
  }, [spec.stages]);

  const edgeEntries = useMemo(
    () => spec.edges.map((edge) => ({ edge, id: edgeId(edge) })),
    [spec.edges],
  );

  const toGraphPoint = useCallback((clientX: number, clientY: number) => {
    const graph = graphRef.current;
    if (!graph) {
      return null;
    }
    const rect = graph.getBoundingClientRect();
    return {
      x: clientX - rect.left,
      y: clientY - rect.top,
    };
  }, []);

  const measureAnchors = useCallback(() => {
    const graph = graphRef.current;
    if (!graph) {
      return;
    }

    const graphRect = graph.getBoundingClientRect();
    const next: Record<string, StageAnchors> = {};
    for (const stage of spec.stages) {
      const node = stageRefs.current[stage.stage_id];
      if (!node) {
        continue;
      }
      const rect = node.getBoundingClientRect();
      const y = rect.top - graphRect.top + rect.height / 2;
      next[stage.stage_id] = {
        input: { x: rect.left - graphRect.left, y },
        output: { x: rect.right - graphRect.left, y },
      };
    }
    setAnchors(next);
  }, [spec.stages]);

  useEffect(() => {
    const graph = graphRef.current;
    const raf = requestAnimationFrame(measureAnchors);
    window.addEventListener("resize", measureAnchors);
    graph?.addEventListener("scroll", measureAnchors);

    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", measureAnchors);
      graph?.removeEventListener("scroll", measureAnchors);
    };
  }, [measureAnchors, spec.stages.length, spec.edges.length, selectedStageId]);

  useEffect(() => {
    if (!connectionDraft) {
      return;
    }

    const handleMove = (event: MouseEvent) => {
      const point = toGraphPoint(event.clientX, event.clientY);
      if (!point) {
        return;
      }
      setConnectionDraft((prev) => (prev ? { ...prev, pointer: point } : prev));
    };

    const handleUp = () => {
      setConnectionDraft(null);
    };

    window.addEventListener("mousemove", handleMove);
    window.addEventListener("mouseup", handleUp);

    return () => {
      window.removeEventListener("mousemove", handleMove);
      window.removeEventListener("mouseup", handleUp);
    };
  }, [connectionDraft, toGraphPoint]);

  useEffect(() => {
    if (selectedEdgeId && !spec.edges.some((edge) => edgeId(edge) === selectedEdgeId)) {
      setSelectedEdgeId(null);
    }
  }, [selectedEdgeId, spec.edges]);

  const updateStage = (stageId: string, patch: Partial<StageDefinition>) => {
    setSpec((prev) => {
      const nextStages = prev.stages.map((stage) => (stage.stage_id === stageId ? { ...stage, ...patch } : stage));
      return {
        ...prev,
        stages: nextStages,
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
      if (!moved) {
        return prev;
      }
      stages.splice(toIndex, 0, moved);
      return {
        ...prev,
        stages,
      };
    });
  };

  const addTemplateStage = (templateId: string) => {
    const existingIds = new Set(spec.stages.map((stage) => stage.stage_id));
    let count = spec.stages.length + 1;
    let nextId = `stage_${count}`;
    while (existingIds.has(nextId)) {
      count += 1;
      nextId = `stage_${count}`;
    }

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

    const isDatasetTemplate = templateId.startsWith("builtin.dataset_");
    setSpec((prev) => ({
      ...prev,
      data_model: isDatasetTemplate ? "dataset" : (prev.data_model ?? "samples"),
      execution_mode:
        isDatasetTemplate && prev.execution_mode === "streaming"
          ? "batch"
          : prev.execution_mode,
      stages: [...prev.stages, stage],
    }));
    setSelectedStageId(nextId);
  };

  const togglePaletteBranch = (branchId: string) => {
    setExpandedPaletteTree((previous) => ({
      ...previous,
      [branchId]: !(previous[branchId] ?? true),
    }));
  };

  const isSearchMode = debouncedPaletteQuery.length > 0;
  const totalSearchRows = stageSearchResults.length;

  const searchWindow = useMemo(() => {
    const viewportHeight = searchScrollRef.current?.clientHeight ?? 340;
    const visibleRows = Math.ceil(viewportHeight / SEARCH_ROW_HEIGHT) + SEARCH_OVERSCAN * 2;
    const startIndex = Math.max(0, Math.floor(searchScrollTop / SEARCH_ROW_HEIGHT) - SEARCH_OVERSCAN);
    const endIndex = Math.min(totalSearchRows, startIndex + visibleRows);
    const offsetTop = startIndex * SEARCH_ROW_HEIGHT;
    const offsetBottom = Math.max(0, (totalSearchRows - endIndex) * SEARCH_ROW_HEIGHT);
    return {
      startIndex,
      endIndex,
      offsetTop,
      offsetBottom,
      rows: stageSearchResults.slice(startIndex, endIndex),
    };
  }, [searchScrollTop, stageSearchResults, totalSearchRows]);

  const renderStageName = (value: string) => {
    if (!isSearchMode) {
      return value;
    }
    const highlighted = highlightMatch(value, debouncedPaletteQuery);
    if (highlighted.length === 1 && !highlighted[0]?.matched) {
      return value;
    }
    return highlighted.map((part, index) =>
      part.matched ? (
        <mark key={`${value}-${index}`} className="rounded bg-amber-100 px-0.5 text-[var(--color-text)]">
          {part.text}
        </mark>
      ) : (
        <span key={`${value}-${index}`}>{part.text}</span>
      ),
    );
  };

  const renderPaletteStageItem = (entry: StageCatalogEntry, nested: boolean) => (
    <button
      key={`${entry.id}-${nested ? "nested" : "result"}`}
      draggable
      onDragStart={(event) => {
        event.dataTransfer.effectAllowed = "copy";
        event.dataTransfer.setData("pipelineforge/template", entry.template.id);
      }}
      onDoubleClick={() => addTemplateStage(entry.template.id)}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          addTemplateStage(entry.template.id);
        }
      }}
      className={`w-full rounded-lg border border-dashed border-[var(--color-card-border)] bg-white px-3 py-2 text-left transition hover:border-[var(--flow-orange)] hover:bg-orange-50 ${
        nested ? "pl-4" : ""
      }`}
      title={`${entry.template.name} • ${entry.pathLabel}`}
      aria-label={`Drag ${entry.template.name} from ${entry.pathLabel} to canvas or press enter to add`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="truncate text-sm font-medium text-[var(--color-text)]">{renderStageName(entry.template.name)}</p>
          <p className="truncate text-[11px] text-[var(--color-muted)]">{entry.template.id}</p>
        </div>
        <Plus className="mt-0.5 h-4 w-4 shrink-0 text-[var(--color-muted)]" />
      </div>
      {entry.template.description ? (
        <p className="mt-1 line-clamp-2 text-xs text-[var(--color-muted)]">{entry.template.description}</p>
      ) : null}
      {!nested ? <p className="mt-1 text-[11px] text-[var(--color-muted)]">{entry.pathLabel}</p> : null}
    </button>
  );

  const removeStage = (stageId: string) => {
    setSpec((prev) => ({
      ...prev,
      stages: prev.stages.filter((stage) => stage.stage_id !== stageId),
      edges: prev.edges.filter((edge) => edge.source !== stageId && edge.target !== stageId),
    }));
    if (selectedStageId === stageId) {
      setSelectedStageId(null);
    }
  };

  const connectStages = (sourceId: string, targetId: string) => {
    let rejectReason: string | null = null;
    setSpec((prev) => {
      if (sourceId === targetId) {
        rejectReason = "Source and target cannot be the same stage.";
        return prev;
      }
      if (prev.edges.some((edge) => edge.source === sourceId && edge.target === targetId)) {
        return prev;
      }
      if (wouldCreateCycle(prev.edges, sourceId, targetId)) {
        rejectReason = "Cannot connect nodes: relation would create a cycle.";
        return prev;
      }

      return {
        ...prev,
        edges: [...prev.edges, { source: sourceId, target: targetId }],
      };
    });

    if (rejectReason) {
      setMessage(rejectReason);
    }
  };

  const reconnectEdge = (currentEdgeId: string, patch: Partial<GraphEdge>) => {
    let rejectReason: string | null = null;

    setSpec((prev) => {
      const current = prev.edges.find((edge) => edgeId(edge) === currentEdgeId);
      if (!current) {
        return prev;
      }

      const next = { ...current, ...patch };
      if (next.source === next.target) {
        rejectReason = "Source and target cannot be the same stage.";
        return prev;
      }

      const remaining = prev.edges.filter((edge) => edgeId(edge) !== currentEdgeId);
      if (remaining.some((edge) => edge.source === next.source && edge.target === next.target)) {
        rejectReason = "Relation already exists.";
        return prev;
      }
      if (wouldCreateCycle(remaining, next.source, next.target)) {
        rejectReason = "Cannot reconnect: relation would create a cycle.";
        return prev;
      }

      return {
        ...prev,
        edges: [...remaining, next],
      };
    });

    if (rejectReason) {
      setMessage(rejectReason);
    }
  };

  const deleteEdge = (id: string) => {
    setSpec((prev) => ({
      ...prev,
      edges: prev.edges.filter((edge) => edgeId(edge) !== id),
    }));
    if (selectedEdgeId === id) {
      setSelectedEdgeId(null);
    }
  };

  const startNewConnection = (stageId: string, event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();
    const point = toGraphPoint(event.clientX, event.clientY);
    if (!point) {
      return;
    }
    setConnectionDraft({ kind: "new", sourceId: stageId, pointer: point });
  };

  const startReconnectSource = (edge: GraphEdge, event: React.MouseEvent<SVGCircleElement>) => {
    event.preventDefault();
    event.stopPropagation();
    const point = toGraphPoint(event.clientX, event.clientY);
    if (!point) {
      return;
    }
    const id = edgeId(edge);
    setSelectedEdgeId(id);
    setConnectionDraft({ kind: "reconnect-source", edgeId: id, targetId: edge.target, pointer: point });
  };

  const startReconnectTarget = (edge: GraphEdge, event: React.MouseEvent<SVGCircleElement>) => {
    event.preventDefault();
    event.stopPropagation();
    const point = toGraphPoint(event.clientX, event.clientY);
    if (!point) {
      return;
    }
    const id = edgeId(edge);
    setSelectedEdgeId(id);
    setConnectionDraft({ kind: "reconnect-target", edgeId: id, sourceId: edge.source, pointer: point });
  };

  const onInputPortMouseUp = (stageId: string, event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();
    if (!connectionDraft) {
      return;
    }

    if (connectionDraft.kind === "new") {
      connectStages(connectionDraft.sourceId, stageId);
    } else if (connectionDraft.kind === "reconnect-target") {
      reconnectEdge(connectionDraft.edgeId, { target: stageId });
    }

    setConnectionDraft(null);
  };

  const onOutputPortMouseUp = (stageId: string, event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    event.stopPropagation();
    if (!connectionDraft) {
      return;
    }

    if (connectionDraft.kind === "reconnect-source") {
      reconnectEdge(connectionDraft.edgeId, { source: stageId });
    }

    setConnectionDraft(null);
  };

  const previewPath = useMemo(() => {
    if (!connectionDraft) {
      return null;
    }

    if (connectionDraft.kind === "new") {
      const source = anchors[connectionDraft.sourceId]?.output;
      if (!source) {
        return null;
      }
      return buildEdgePath(source, connectionDraft.pointer);
    }

    if (connectionDraft.kind === "reconnect-source") {
      const target = anchors[connectionDraft.targetId]?.input;
      if (!target) {
        return null;
      }
      return buildEdgePath(connectionDraft.pointer, target);
    }

    const source = anchors[connectionDraft.sourceId]?.output;
    if (!source) {
      return null;
    }
    return buildEdgePath(source, connectionDraft.pointer);
  }, [anchors, connectionDraft]);

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
    if (!latest || !previous) {
      return;
    }
    const diff = await apiCall((client) => client.getDiff(pipeline.id, previous.id, latest.id));
    setDiffPreview(diff);
  };

  return (
    <div className="space-y-4">
      <section className="rounded-[26px] border border-[var(--flow-shell-border)] bg-[var(--flow-shell-bg)] p-3 shadow-[0_24px_60px_rgba(93,61,117,0.08)]">
        <div className="grid gap-3 xl:grid-cols-[260px_1fr_320px]">
          <aside className="rounded-2xl border border-[var(--flow-panel-border)] bg-white/90 p-3">
            <div className="rounded-xl border border-[var(--flow-panel-border)] bg-[var(--color-surface)]/75 p-3">
              <p className="text-xs uppercase tracking-[0.15em] text-[var(--color-muted)]">Pipeline</p>
              <h2 className="mt-1 text-base font-semibold text-[var(--color-text)]">{spec.name || "Untitled Pipeline"}</h2>
              <div className="mt-2 flex flex-wrap items-center gap-2">
                <Badge label={(spec.data_model ?? "samples").toUpperCase()} />
                <Badge label={spec.execution_mode.toUpperCase()} />
                <Badge label={`${spec.stages.length} stage${spec.stages.length === 1 ? "" : "s"}`} />
                <Badge label={`${spec.edges.length} relation${spec.edges.length === 1 ? "" : "s"}`} />
              </div>
            </div>

            <div className="mt-3 space-y-2">
              <label className="space-y-1">
                <span className="text-[11px] uppercase tracking-[0.14em] text-[var(--color-muted)]">Name</span>
                <Input value={spec.name} onChange={(e) => setSpec({ ...spec, name: e.target.value })} className="h-8 bg-white" />
              </label>
              <label className="space-y-1">
                <span className="text-[11px] uppercase tracking-[0.14em] text-[var(--color-muted)]">Data Model</span>
                <select
                  className="h-8 w-full rounded-md border border-[var(--color-card-border)] bg-white px-3 text-sm"
                  value={spec.data_model ?? "samples"}
                  onChange={(e) =>
                    setSpec({
                      ...spec,
                      data_model: e.target.value as PipelineSpecDocument["data_model"],
                    })
                  }
                >
                  <option value="samples">Samples</option>
                  <option value="dataset">Dataset</option>
                </select>
              </label>
              <label className="space-y-1">
                <span className="text-[11px] uppercase tracking-[0.14em] text-[var(--color-muted)]">Execution</span>
                <select
                  className="h-8 w-full rounded-md border border-[var(--color-card-border)] bg-white px-3 text-sm"
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
              <label className="space-y-1">
                <span className="text-[11px] uppercase tracking-[0.14em] text-[var(--color-muted)]">Change Summary</span>
                <Input value={changeSummary} onChange={(e) => setChangeSummary(e.target.value)} className="h-8 bg-white" />
              </label>
              <label className="space-y-1">
                <span className="text-[11px] uppercase tracking-[0.14em] text-[var(--color-muted)]">Description</span>
                <Textarea rows={3} value={spec.description} onChange={(e) => setSpec({ ...spec, description: e.target.value })} className="bg-white" />
              </label>
            </div>

            <div className="mt-4">
              <div className="mb-2 flex items-center justify-between">
                <p className="text-xs font-semibold uppercase tracking-[0.14em] text-[var(--color-muted)]">Stage Palette</p>
                <span className="text-xs text-[var(--color-muted)]">drag / double click</span>
              </div>
              <div className="relative">
                <Search className="pointer-events-none absolute left-2 top-2.5 h-4 w-4 text-[var(--color-muted)]" />
                <Input
                  value={paletteQuery}
                  onChange={(event) => setPaletteQuery(event.target.value)}
                  placeholder="Search stages, tags, or category path"
                  className="h-9 bg-white pl-8 pr-8"
                  aria-label="Search stage templates"
                />
                {paletteQuery ? (
                  <button
                    type="button"
                    onClick={() => setPaletteQuery("")}
                    className="absolute right-2 top-2 rounded p-0.5 text-[var(--color-muted)] transition hover:bg-slate-100"
                    aria-label="Clear stage search"
                    title="Clear search"
                  >
                    <X className="h-4 w-4" />
                  </button>
                ) : null}
              </div>

              {isSearchMode ? (
                <div className="mt-2 rounded-xl border border-[var(--flow-panel-border)] bg-white/80 p-2">
                  <div className="mb-2 flex items-center justify-between">
                    <p className="text-[11px] uppercase tracking-[0.14em] text-[var(--color-muted)]">Search Results</p>
                    <Badge label={`${stageSearchResults.length}`} />
                  </div>
                  <div
                    ref={searchScrollRef}
                    onScroll={(event) => {
                      setSearchScrollTop(event.currentTarget.scrollTop);
                    }}
                    className="max-h-[340px] overflow-auto pr-1"
                  >
                    {stageSearchResults.length === 0 ? (
                      <p className="rounded-lg border border-dashed border-[var(--color-card-border)] bg-[var(--color-surface)] px-3 py-4 text-sm text-[var(--color-muted)]">
                        No stages match this search. Try another keyword or clear the filter.
                      </p>
                    ) : (
                      <div className="space-y-2" style={{ paddingTop: searchWindow.offsetTop, paddingBottom: searchWindow.offsetBottom }}>
                        {searchWindow.rows.map((entry) => renderPaletteStageItem(entry, false))}
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <div role="tree" aria-label="Stage templates by category" className="mt-2 max-h-[370px] space-y-2 overflow-auto pr-1">
                  {stageTree.map((category) => {
                    const categoryExpanded = expandedPaletteTree[category.id] ?? true;
                    return (
                      <section key={category.id} className="rounded-lg border border-[var(--flow-panel-border)] bg-white/85">
                        <button
                          type="button"
                          onClick={() => togglePaletteBranch(category.id)}
                          className="flex w-full items-center justify-between px-2 py-2"
                          aria-expanded={categoryExpanded}
                          aria-label={`${categoryExpanded ? "Collapse" : "Expand"} ${category.name}`}
                        >
                          <span className="flex min-w-0 items-center gap-1.5">
                            <ChevronRight
                              className={`h-4 w-4 text-[var(--color-muted)] transition ${categoryExpanded ? "rotate-90" : ""}`}
                            />
                            <span className="truncate text-sm font-semibold text-[var(--color-text)]" title={category.name}>
                              {category.name}
                            </span>
                          </span>
                          <Badge label={`${category.count}`} />
                        </button>

                        {categoryExpanded ? (
                          <div className="space-y-1 px-2 pb-2">
                            {category.subcategories.map((subcategory) => {
                              const subcategoryExpanded = expandedPaletteTree[subcategory.id] ?? true;
                              return (
                                <div key={subcategory.id} className="rounded-md border border-[var(--flow-panel-border)] bg-[var(--color-surface)]/70">
                                  <button
                                    type="button"
                                    onClick={() => togglePaletteBranch(subcategory.id)}
                                    className="flex w-full items-center justify-between px-2 py-1.5"
                                    aria-expanded={subcategoryExpanded}
                                    aria-label={`${subcategoryExpanded ? "Collapse" : "Expand"} ${subcategory.name}`}
                                  >
                                    <span className="flex min-w-0 items-center gap-1.5">
                                      <ChevronRight
                                        className={`h-3.5 w-3.5 text-[var(--color-muted)] transition ${
                                          subcategoryExpanded ? "rotate-90" : ""
                                        }`}
                                      />
                                      <span className="truncate text-xs font-medium text-[var(--color-text)]" title={subcategory.name}>
                                        {subcategory.name}
                                      </span>
                                    </span>
                                    <span className="text-[11px] text-[var(--color-muted)]">{subcategory.count}</span>
                                  </button>

                                  {subcategoryExpanded ? (
                                    <div className="space-y-1 px-1 pb-1">
                                      {subcategory.stages.map((entry) => renderPaletteStageItem(entry, true))}
                                    </div>
                                  ) : null}
                                </div>
                              );
                            })}
                          </div>
                        ) : null}
                      </section>
                    );
                  })}
                  {stageTree.length === 0 ? (
                    <p className="text-sm text-[var(--color-muted)]">No stage templates available.</p>
                  ) : null}
                </div>
              )}
            </div>
          </aside>

          <div className="min-w-0 space-y-3">
            <div className="rounded-full border border-[var(--flow-panel-border)] bg-white/88 px-4 py-2 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.4)]">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="inline-flex items-center gap-1 rounded-full border border-[var(--flow-panel-border)] bg-white px-3 py-1 text-xs font-medium text-[var(--color-text)]">
                    <Database className="h-3.5 w-3.5" />
                    Dataset
                  </span>
                  <ArrowRight className="h-3.5 w-3.5 text-[var(--color-muted)]" />
                  <span className="inline-flex items-center rounded-full border border-[var(--flow-panel-border)] bg-white px-3 py-1 text-xs font-medium text-[var(--color-text)]">
                    Pipeline
                  </span>
                  <ArrowRight className="h-3.5 w-3.5 text-[var(--color-muted)]" />
                  <span className="inline-flex items-center rounded-full border border-[var(--flow-panel-border)] bg-white px-3 py-1 text-xs font-medium text-[var(--color-text)]">
                    Execution
                  </span>
                  {activeVersionId ? <Badge label={`Active ${activeVersionId.slice(0, 8)}`} /> : null}
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  {selectedEdge ? (
                    <Button variant="danger" className="h-8 rounded-full px-4" onClick={() => deleteEdge(edgeId(selectedEdge))}>
                      <Trash2 className="mr-1 h-4 w-4" />
                      Delete Relation
                    </Button>
                  ) : null}
                  <Button variant="secondary" className="h-8" onClick={previewDiff} disabled={versions.length < 2}>
                    <WandSparkles className="mr-1 h-4 w-4" />
                    Preview Diff
                  </Button>
                  <Button onClick={saveDraft} disabled={!canEdit} className="h-8 rounded-full px-4">
                    <Save className="mr-1 h-4 w-4" />
                    Save Draft
                  </Button>
                  <Button
                    variant="secondary"
                    onClick={submitReview}
                    disabled={!canEdit || !activeVersionId}
                    className="h-8 rounded-full px-4"
                  >
                    <Send className="mr-1 h-4 w-4" />
                    Submit
                  </Button>
                  <Button
                    variant="secondary"
                    onClick={publish}
                    disabled={!canPublish || !activeVersionId}
                    className="h-8 rounded-full px-4"
                  >
                    <UploadCloud className="mr-1 h-4 w-4" />
                    Publish
                  </Button>
                </div>
              </div>
            </div>

            {message ? (
              <div className="rounded-xl border border-[var(--flow-panel-border)] bg-white/90 px-3 py-2 text-xs text-[var(--color-muted)]">{message}</div>
            ) : null}

            <div
              className="relative overflow-x-auto rounded-2xl border border-[var(--flow-panel-border)] bg-white/82 p-4"
              onDragOver={(event) => event.preventDefault()}
              onDrop={(event) => {
                event.preventDefault();
                const templateId = event.dataTransfer.getData("pipelineforge/template");
                if (templateId) {
                  addTemplateStage(templateId);
                }
              }}
            >
              <div className="pipeline-canvas-grid pointer-events-none absolute inset-0 rounded-2xl" />
              {spec.stages.length === 0 ? (
                <div className="relative z-10 flex min-h-[460px] items-center justify-center rounded-xl border border-dashed border-[var(--flow-panel-border)] bg-white/70 text-sm text-[var(--color-muted)]">
                  Drag a stage from the left palette or double click any template to build your flow.
                </div>
              ) : (
                <div ref={graphRef} className="relative z-10 min-h-[460px] min-w-max pb-6">
                  <svg className="pointer-events-none absolute inset-0 h-full w-full overflow-visible">
                    {edgeEntries.map(({ edge, id }) => {
                      const source = anchors[edge.source]?.output;
                      const target = anchors[edge.target]?.input;
                      if (!source || !target) {
                        return null;
                      }
                      const selected = id === selectedEdgeId;
                      return (
                        <g key={id}>
                          <path
                            d={buildEdgePath(source, target)}
                            fill="none"
                            stroke={selected ? "var(--color-accent)" : "var(--flow-orange)"}
                            strokeWidth={selected ? 4 : 3}
                            strokeLinecap="round"
                            style={{ pointerEvents: "stroke", cursor: "pointer" }}
                            onMouseDown={(event) => {
                              event.stopPropagation();
                              setSelectedEdgeId(id);
                            }}
                          />
                          <circle
                            cx={source.x}
                            cy={source.y}
                            r={selected ? 5 : 4}
                            fill={selected ? "var(--color-accent)" : "var(--flow-orange)"}
                            style={{ pointerEvents: "all", cursor: "grab" }}
                            onMouseDown={(event) => startReconnectSource(edge, event)}
                          />
                          <circle
                            cx={target.x}
                            cy={target.y}
                            r={selected ? 5 : 4}
                            fill={selected ? "var(--color-accent)" : "var(--flow-orange)"}
                            style={{ pointerEvents: "all", cursor: "grab" }}
                            onMouseDown={(event) => startReconnectTarget(edge, event)}
                          />
                        </g>
                      );
                    })}

                    {previewPath ? (
                      <path
                        d={previewPath}
                        fill="none"
                        stroke="var(--flow-orange)"
                        strokeWidth={3}
                        strokeDasharray="6 6"
                        strokeLinecap="round"
                        style={{ pointerEvents: "none" }}
                      />
                    ) : null}
                  </svg>

                  <ol className="relative z-10 flex min-w-full items-start gap-14 px-4 pt-14">
                    {spec.stages.map((stage, index) => {
                      const kind = inferStageKind(stage, index);
                      const paramsPreview = summarizeParams(stage.params);
                      const logs = stageLogPreview(stage);
                      const selected = selectedStageId === stage.stage_id;
                      const inputDropEnabled = connectionDraft?.kind === "new" || connectionDraft?.kind === "reconnect-target";
                      const outputDropEnabled = connectionDraft?.kind === "reconnect-source";

                      return (
                        <li key={stage.stage_id} className="relative shrink-0">
                          <article
                            ref={(node) => {
                              stageRefs.current[stage.stage_id] = node;
                            }}
                            onDragOver={(event) => event.preventDefault()}
                            onDrop={(event) => {
                              event.preventDefault();
                              const dragged = event.dataTransfer.getData("pipelineforge/stage");
                              if (dragged) {
                                reorderStage(dragged, stage.stage_id);
                              }
                            }}
                            onClick={() => {
                              setSelectedStageId(stage.stage_id);
                              setSelectedEdgeId(null);
                            }}
                            className={`relative w-[288px] cursor-pointer rounded-xl border p-3 shadow-[0_16px_34px_rgba(63,31,90,0.07)] transition ${
                              selected
                                ? "border-[var(--flow-orange)] bg-[linear-gradient(180deg,#fff6ef_0%,#ffffff_36%)]"
                                : "border-[var(--flow-node-border)] bg-white"
                            }`}
                          >
                            <button
                              type="button"
                              className={`absolute -left-[10px] top-1/2 h-5 w-5 -translate-y-1/2 rounded-full border-2 border-white bg-[var(--flow-orange-soft)] transition ${
                                inputDropEnabled ? "ring-2 ring-orange-200" : ""
                              }`}
                              onMouseUp={(event) => onInputPortMouseUp(stage.stage_id, event)}
                              onMouseDown={(event) => {
                                event.preventDefault();
                                event.stopPropagation();
                              }}
                              title="Drop connection target"
                              aria-label={`Connect into ${stage.name}`}
                            />
                            <button
                              type="button"
                              className={`absolute -right-[10px] top-1/2 h-5 w-5 -translate-y-1/2 rounded-full border-2 border-white bg-[var(--flow-orange-soft)] transition ${
                                outputDropEnabled ? "ring-2 ring-orange-200" : ""
                              }`}
                              onMouseDown={(event) => startNewConnection(stage.stage_id, event)}
                              onMouseUp={(event) => onOutputPortMouseUp(stage.stage_id, event)}
                              title="Start or drop source connection"
                              aria-label={`Connect from ${stage.name}`}
                            />

                            <div className="flex items-start justify-between gap-2">
                              <div>
                                <div
                                  className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-semibold ${kind.chipClass}`}
                                >
                                  <span className={`h-2 w-2 rounded-full ${kind.iconClass}`} />
                                  {kind.label}
                                </div>
                                <p className="mt-2 text-sm font-semibold leading-snug text-[var(--color-text)]">{stage.name}</p>
                                <p className="mt-0.5 text-[11px] text-[var(--color-muted)]">{stage.stage_template ?? stage.stage_id}</p>
                              </div>

                              <div className="flex items-center gap-1">
                                <button
                                  type="button"
                                  draggable
                                  onDragStart={(event) => {
                                    event.stopPropagation();
                                    event.dataTransfer.setData("pipelineforge/stage", stage.stage_id);
                                  }}
                                  className="rounded-md p-1 text-[var(--color-muted)] transition hover:bg-slate-100"
                                  title="Drag to reorder"
                                  aria-label={`Reorder ${stage.name}`}
                                >
                                  <GripVertical className="h-4 w-4" />
                                </button>
                                <button
                                  className="rounded-md p-1 text-[var(--color-muted)] transition hover:bg-red-50 hover:text-red-500"
                                  onClick={(event) => {
                                    event.stopPropagation();
                                    removeStage(stage.stage_id);
                                  }}
                                  aria-label={`Remove ${stage.name}`}
                                >
                                  <X className="h-3.5 w-3.5" />
                                </button>
                              </div>
                            </div>

                            <div className="mt-3 space-y-2">
                              <div className="rounded-md border border-[var(--flow-node-border)] bg-[var(--flow-node-soft)] px-2 py-1.5">
                                <p className="text-[10px] uppercase tracking-[0.12em] text-[var(--color-muted)]">Init Parameters</p>
                                <p className="mt-1 line-clamp-2 text-xs text-[var(--color-text)]">{paramsPreview}</p>
                              </div>

                              <div className="rounded-md border border-[#1c2f4a] bg-[#0a1221] px-2 py-1.5 font-mono text-[10px] leading-relaxed text-emerald-200">
                                {logs.map((line) => (
                                  <p key={`${stage.stage_id}-${line}`}>{line}</p>
                                ))}
                              </div>
                            </div>

                            <div className="mt-2 flex flex-wrap items-center gap-1.5 text-[11px]">
                              <span className="rounded bg-orange-50 px-1.5 py-0.5 text-orange-700">cpu {stage.resources?.cpus ?? 1}</span>
                              <span className="rounded bg-orange-50 px-1.5 py-0.5 text-orange-700">gpu {stage.resources?.gpus ?? 0}</span>
                              <span className="rounded bg-slate-100 px-1.5 py-0.5 text-slate-600">x{stage.concurrency_hint ?? 1}</span>
                            </div>
                          </article>
                        </li>
                      );
                    })}
                  </ol>
                </div>
              )}
            </div>

            <p className="text-xs text-[var(--color-muted)]">
              Drag from a node&apos;s right dot to another node&apos;s left dot to create a relation. Drag edge endpoints to reconnect,
              and delete any selected relation.
            </p>
          </div>

          <aside className="rounded-2xl border border-[var(--flow-panel-border)] bg-white/90 p-3">
            <div className="mb-3 flex items-center justify-between">
              <p className="text-sm font-semibold">Stage Inspector</p>
              {selectedStage ? <Badge label={selectedStage.stage_id} /> : null}
            </div>

            {!selectedStage ? (
              <p className="rounded-lg border border-dashed border-[var(--flow-panel-border)] bg-[var(--flow-node-soft)] px-3 py-4 text-sm text-[var(--color-muted)]">
                Select any node in the flow canvas to edit runtime settings and params.
              </p>
            ) : (
              <div className="space-y-2">
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">Stage Name</span>
                  <Input value={selectedStage.name} onChange={(e) => updateStage(selectedStage.stage_id, { name: e.target.value })} className="h-8 bg-white" />
                </label>
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">Batch Size</span>
                  <Input
                    type="number"
                    value={selectedStage.batch_size ?? 1}
                    onChange={(e) => updateStage(selectedStage.stage_id, { batch_size: Number(e.target.value) })}
                    className="h-8 bg-white"
                  />
                </label>
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">Concurrency Hint</span>
                  <Input
                    type="number"
                    value={selectedStage.concurrency_hint ?? 1}
                    onChange={(e) => updateStage(selectedStage.stage_id, { concurrency_hint: Number(e.target.value) })}
                    className="h-8 bg-white"
                  />
                </label>
                <div className="grid grid-cols-2 gap-2">
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
                      className="h-8 bg-white"
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
                      className="h-8 bg-white"
                    />
                  </label>
                </div>
                <label className="space-y-1">
                  <span className="text-xs text-[var(--color-muted)]">Params (JSON)</span>
                  <Textarea
                    rows={9}
                    value={JSON.stringify(selectedStage.params ?? {}, null, 2)}
                    onChange={(e) => {
                      try {
                        const parsed = JSON.parse(e.target.value);
                        updateStage(selectedStage.stage_id, { params: parsed });
                      } catch {
                        // Keep transient JSON edit errors local while user types.
                      }
                    }}
                    className="bg-white font-mono text-xs"
                  />
                </label>
              </div>
            )}

            <div className="mt-4 border-t border-[var(--flow-panel-border)] pt-3">
              <div className="mb-2 flex items-center justify-between">
                <p className="text-sm font-semibold">Relations</p>
                <Badge label={`${spec.edges.length}`} />
              </div>

              {spec.edges.length === 0 ? (
                <p className="rounded-lg border border-dashed border-[var(--flow-panel-border)] bg-[var(--flow-node-soft)] px-3 py-3 text-xs text-[var(--color-muted)]">
                  No relations yet. Add isolated nodes first, then connect them later by dragging between ports.
                </p>
              ) : (
                <div className="max-h-[220px] space-y-1 overflow-auto pr-1">
                  {edgeEntries.map(({ edge, id }) => {
                    const selected = id === selectedEdgeId;
                    return (
                      <div
                        key={id}
                        className={`flex items-center justify-between gap-2 rounded-md border px-2 py-1.5 ${
                          selected
                            ? "border-[var(--color-accent)] bg-[color-mix(in_oklab,var(--color-accent),white_92%)]"
                            : "border-[var(--flow-panel-border)] bg-white"
                        }`}
                      >
                        <button
                          className="flex min-w-0 items-center gap-1 text-left text-xs"
                          onClick={() => {
                            setSelectedEdgeId(id);
                            setSelectedStageId(null);
                          }}
                        >
                          <span className="truncate">{stageNames[edge.source] ?? edge.source}</span>
                          <ArrowRight className="h-3 w-3 shrink-0" />
                          <span className="truncate">{stageNames[edge.target] ?? edge.target}</span>
                        </button>
                        <button
                          className="rounded p-1 text-[var(--color-muted)] transition hover:bg-red-50 hover:text-red-500"
                          onClick={() => deleteEdge(id)}
                          aria-label={`Delete relation ${id}`}
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </button>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </aside>
        </div>
      </section>

      <div className="grid gap-4 xl:grid-cols-2">
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
                      const normalized = normalizeSpecEdges(version.spec);
                      setActiveVersionId(version.id);
                      setSpec(normalized);
                      setSelectedStageId(normalized.stages[0]?.stage_id ?? null);
                      setSelectedEdgeId(null);
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
      </div>

      <Card>
        <CardHeader>
          <h3 className="text-sm font-semibold">Generated Python Skeleton</h3>
        </CardHeader>
        <CardContent>
          <pre className="max-h-[280px] overflow-auto rounded-md bg-[#111827] p-3 text-xs text-slate-100">
{`# Submit this payload to POST /api/v1/pipelines/{pipeline_id}/versions
spec = {
  "execution_mode": "${spec.execution_mode}",
  "stages": [
${spec.stages
  .map(
    (stage) =>
      `    {"stage_id": "${stage.stage_id}", "name": "${stage.name}", "stage_template": "${stage.stage_template ?? "builtin.identity"}"},`
  )
  .join("\n")}
  ],
  "edges": ${JSON.stringify(spec.edges, null, 2)},
}
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
