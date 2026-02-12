export type RoleName = "INFRA_ADMIN" | "PIPELINE_DEV" | "AIOPS_ENGINEER";

export type UserInfo = {
  id: string;
  email: string;
  full_name: string;
  roles: RoleName[];
  is_active: boolean;
  created_at: string;
};

export type Pipeline = {
  id: string;
  external_id: string;
  name: string;
  description: string;
  tags: string[];
  execution_mode: "streaming" | "batch" | "serving";
  owner_user_id: string;
  owner_team_id: string | null;
  metadata_links: Record<string, unknown>;
  created_at: string;
  updated_at: string;
};

export type StageDefinition = {
  stage_id: string;
  name: string;
  python_import_path?: string;
  stage_template?: string;
  resources?: {
    cpus: number;
    gpus: number;
    memory_mb?: number;
  };
  batch_size?: number;
  concurrency_hint?: number;
  retries?: number;
  params?: Record<string, unknown>;
};

export type PipelineSpecDocument = {
  pipeline_id?: string;
  name: string;
  description: string;
  tags: string[];
  owners: string[];
  team_ids: string[];
  execution_mode: "streaming" | "batch" | "serving";
  stages: StageDefinition[];
  edges: { source: string; target: string }[];
  io: {
    source: { kind: "inline" | "queue" | "dataset_uri"; static_data: unknown[]; uri?: string | null };
    sink: { kind: "none" | "queue" | "artifact_uri"; uri?: string | null };
  };
  runtime: {
    ray_address?: string | null;
    autoscaling: Record<string, unknown>;
    retry_policy: Record<string, unknown>;
  };
  observability: {
    log_level: "DEBUG" | "INFO" | "WARN" | "ERROR";
    metrics_enabled: boolean;
    tracing_enabled: boolean;
  };
  metadata_links: {
    datasets: string[];
    models: string[];
  };
};

export type PipelineVersion = {
  id: string;
  pipeline_id: string;
  version_number: number;
  status: "DRAFT" | "IN_REVIEW" | "PUBLISHED" | "REJECTED";
  is_active: boolean;
  spec: PipelineSpecDocument;
  change_summary: string;
  created_by: string;
  created_at: string;
  review_requested_at?: string | null;
  published_at?: string | null;
};

export type PipelineRun = {
  id: string;
  pipeline_id: string;
  pipeline_version_id: string;
  status: "QUEUED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "STOPPED";
  execution_mode: string;
  trigger_type: string;
  initiated_by: string;
  start_time?: string | null;
  end_time?: string | null;
  duration_seconds?: number | null;
  error_message?: string | null;
  artifact_pointers: Record<string, unknown>;
  metrics_summary: Record<string, unknown>;
  stop_requested: boolean;
  created_at: string;
};

export type RunEvent = {
  id: string;
  run_id: string;
  event_type: string;
  stage_id?: string | null;
  message: string;
  payload: Record<string, unknown>;
  created_at: string;
};
