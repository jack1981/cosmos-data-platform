"use client";

import { useParams, useRouter } from "next/navigation";
import { useEffect, useState } from "react";

import { useAuth } from "@/components/auth/auth-provider";
import { DashboardShell } from "@/components/layout/dashboard-shell";
import { PipelineBuilder } from "@/components/pipelines/pipeline-builder";
import { Card, CardContent } from "@/components/ui/card";
import type { Pipeline, PipelineVersion } from "@/types/api";

export default function PipelineBuilderPage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();
  const { apiCall } = useAuth();
  const [pipeline, setPipeline] = useState<Pipeline | null>(null);
  const [version, setVersion] = useState<PipelineVersion | null>(null);

  useEffect(() => {
    void apiCall((client) => client.listPipelines()).then((items) => {
      setPipeline(items.find((item) => item.id === id) ?? null);
    });
    void apiCall((client) => client.listVersions(id)).then((items) => {
      const active = items.find((item) => item.is_active) ?? items[0] ?? null;
      setVersion(active);
    });
  }, [apiCall, id]);

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
      <PipelineBuilder
        pipeline={pipeline}
        initialVersion={version}
        onSaved={(pipelineId) => {
          router.replace(`/pipelines/${pipelineId}/builder`);
        }}
      />
    </DashboardShell>
  );
}
