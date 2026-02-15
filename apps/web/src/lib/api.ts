import type {
  Pipeline,
  PipelineRun,
  PipelineSpecDocument,
  PipelineVersion,
  RoleName,
  RunEvent,
  UserInfo,
} from "@/types/api";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000/api/v1";

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

export async function apiRequest<T>(
  path: string,
  options: RequestInit & { token?: string } = {},
): Promise<T> {
  const headers = new Headers(options.headers || {});
  headers.set("Content-Type", "application/json");
  if (options.token) {
    headers.set("Authorization", `Bearer ${options.token}`);
  }

  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const body = await response.text();
    throw new ApiError(body || "Request failed", response.status);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export type LoginResponse = {
  access_token: string;
  refresh_token: string;
  token_type: string;
};

export type StageTemplate = {
  id: string;
  name: string;
  description: string;
  display_name?: string;
  category?: string;
  params_schema?: Array<Record<string, unknown>>;
  inputs?: Array<Record<string, unknown>>;
  outputs?: Array<Record<string, unknown>>;
};

export type MetricsSummary = {
  run_id: string;
  metrics: Record<string, unknown>;
};

export type RoleItem = {
  id: string;
  name: RoleName;
  description: string;
};

export type AdminUser = {
  id: string;
  email: string;
  full_name: string;
  roles: RoleName[];
  is_active: boolean;
  created_at: string;
};

export type AdminAuditEntry = {
  id: string;
  actor_user_id: string;
  action: string;
  resource_type: string;
  resource_id: string;
  details: Record<string, unknown>;
  created_at: string;
};

export type PipelineShare = {
  id: string;
  team_id: string;
  access_level: "READ" | "WRITE" | "OWNER";
};

export type ApiClient = {
  getMe: () => Promise<UserInfo>;
  listPipelines: () => Promise<Pipeline[]>;
  createPipeline: (body: Record<string, unknown>) => Promise<Pipeline>;
  updatePipeline: (id: string, body: Record<string, unknown>) => Promise<Pipeline>;
  deletePipeline: (id: string) => Promise<void>;
  listPipelineShares: (pipelineId: string) => Promise<PipelineShare[]>;
  upsertPipelineShare: (
    pipelineId: string,
    body: { team_id: string; access_level: "READ" | "WRITE" | "OWNER" },
  ) => Promise<PipelineShare>;
  listVersions: (pipelineId: string) => Promise<PipelineVersion[]>;
  createVersion: (pipelineId: string, spec: PipelineSpecDocument, changeSummary: string) => Promise<PipelineVersion>;
  submitReview: (pipelineId: string, versionId: string, comments: string) => Promise<PipelineVersion>;
  approveVersion: (pipelineId: string, versionId: string, comments: string) => Promise<unknown>;
  rejectVersion: (pipelineId: string, versionId: string, comments: string) => Promise<unknown>;
  publishVersion: (pipelineId: string, versionId: string, comments: string) => Promise<PipelineVersion>;
  getDiff: (pipelineId: string, fromVersionId: string, toVersionId: string) => Promise<Record<string, unknown>>;
  listStageTemplates: () => Promise<StageTemplate[]>;
  importSourceYaml: (body: {
    source_yaml_path: string;
    external_id?: string;
    name?: string;
    description?: string;
    tags?: string[];
    publish?: boolean;
  }) => Promise<{ pipeline: Pipeline; version: PipelineVersion }>;
  listRuns: () => Promise<PipelineRun[]>;
  triggerRun: (pipelineId: string, versionId?: string) => Promise<PipelineRun>;
  getRun: (runId: string) => Promise<PipelineRun>;
  stopRun: (runId: string) => Promise<PipelineRun>;
  rerun: (runId: string) => Promise<PipelineRun>;
  listRunEvents: (runId: string) => Promise<RunEvent[]>;
  getMetricsSummary: (runId: string) => Promise<MetricsSummary>;
  listRoles: () => Promise<RoleItem[]>;
  listUsers: () => Promise<AdminUser[]>;
  createUser: (body: Record<string, unknown>) => Promise<AdminUser>;
  listAuditLog: (limit?: number) => Promise<AdminAuditEntry[]>;
};

export function makeClient(token: string): ApiClient {
  return {
    getMe: () => apiRequest<UserInfo>("/auth/me", { token }),
    listPipelines: () => apiRequest<Pipeline[]>("/pipelines", { token }),
    createPipeline: (body) => apiRequest<Pipeline>("/pipelines", { method: "POST", body: JSON.stringify(body), token }),
    updatePipeline: (id, body) =>
      apiRequest<Pipeline>(`/pipelines/${id}`, { method: "PATCH", body: JSON.stringify(body), token }),
    deletePipeline: (id) => apiRequest<void>(`/pipelines/${id}`, { method: "DELETE", token }),
    listPipelineShares: (pipelineId) => apiRequest<PipelineShare[]>(`/pipelines/${pipelineId}/shares`, { token }),
    upsertPipelineShare: (pipelineId, body) =>
      apiRequest<PipelineShare>(`/pipelines/${pipelineId}/shares`, {
        method: "POST",
        body: JSON.stringify(body),
        token,
      }),
    listVersions: (pipelineId) => apiRequest<PipelineVersion[]>(`/pipelines/${pipelineId}/versions`, { token }),
    createVersion: (pipelineId, spec, changeSummary) =>
      apiRequest<PipelineVersion>(`/pipelines/${pipelineId}/versions`, {
        method: "POST",
        body: JSON.stringify({ spec, change_summary: changeSummary }),
        token,
      }),
    submitReview: (pipelineId, versionId, comments) =>
      apiRequest<PipelineVersion>(`/pipelines/${pipelineId}/versions/${versionId}/submit-review`, {
        method: "POST",
        body: JSON.stringify({ comments }),
        token,
      }),
    approveVersion: (pipelineId, versionId, comments) =>
      apiRequest(`/pipelines/${pipelineId}/versions/${versionId}/approve`, {
        method: "POST",
        body: JSON.stringify({ comments }),
        token,
      }),
    rejectVersion: (pipelineId, versionId, comments) =>
      apiRequest(`/pipelines/${pipelineId}/versions/${versionId}/reject`, {
        method: "POST",
        body: JSON.stringify({ comments }),
        token,
      }),
    publishVersion: (pipelineId, versionId, comments) =>
      apiRequest<PipelineVersion>(`/pipelines/${pipelineId}/versions/${versionId}/publish`, {
        method: "POST",
        body: JSON.stringify({ comments }),
        token,
      }),
    getDiff: (pipelineId, fromVersionId, toVersionId) =>
      apiRequest<Record<string, unknown>>(
        `/pipelines/${pipelineId}/diff?from_version_id=${fromVersionId}&to_version_id=${toVersionId}`,
        { token },
      ),
    listStageTemplates: () => apiRequest<StageTemplate[]>("/pipelines/stage-templates", { token }),
    importSourceYaml: (body) =>
      apiRequest<{ pipeline: Pipeline; version: PipelineVersion }>("/pipelines/import-source-yaml", {
        method: "POST",
        body: JSON.stringify(body),
        token,
      }),
    listRuns: () => apiRequest<PipelineRun[]>("/runs", { token }),
    triggerRun: (pipelineId, versionId) =>
      apiRequest<PipelineRun>("/runs/trigger", {
        method: "POST",
        body: JSON.stringify({ pipeline_id: pipelineId, pipeline_version_id: versionId, trigger_type: "manual" }),
        token,
      }),
    getRun: (runId) => apiRequest<PipelineRun>(`/runs/${runId}`, { token }),
    stopRun: (runId) => apiRequest<PipelineRun>(`/runs/${runId}/stop`, { method: "POST", token }),
    rerun: (runId) => apiRequest<PipelineRun>(`/runs/${runId}/rerun`, { method: "POST", token }),
    listRunEvents: (runId) => apiRequest<RunEvent[]>(`/runs/${runId}/events`, { token }),
    getMetricsSummary: (runId) => apiRequest<MetricsSummary>(`/runs/${runId}/metrics-summary`, { token }),
    listRoles: () => apiRequest<RoleItem[]>("/admin/roles", { token }),
    listUsers: () => apiRequest<AdminUser[]>("/admin/users", { token }),
    createUser: (body) => apiRequest<AdminUser>("/admin/users", { method: "POST", body: JSON.stringify(body), token }),
    listAuditLog: (limit = 100) => apiRequest<AdminAuditEntry[]>(`/admin/audit-log?limit=${limit}`, { token }),
  };
}

export async function login(email: string, password: string): Promise<LoginResponse> {
  return apiRequest<LoginResponse>("/auth/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
}

export async function refresh(refreshToken: string): Promise<LoginResponse> {
  return apiRequest<LoginResponse>("/auth/refresh", {
    method: "POST",
    body: JSON.stringify({ refresh_token: refreshToken }),
  });
}

export function runLogStreamUrl(runId: string, token: string): string {
  const base = `${API_BASE}/runs/${runId}/logs/stream`;
  const url = new URL(base);
  url.searchParams.set("access_token", token);
  return url.toString();
}
