"use client";

import { useRouter } from "next/navigation";

import { DashboardShell } from "@/components/layout/dashboard-shell";
import { PipelineBuilder } from "@/components/pipelines/pipeline-builder";

export default function NewPipelinePage() {
  const router = useRouter();

  return (
    <DashboardShell>
      <PipelineBuilder
        onSaved={(pipelineId) => {
          router.replace(`/pipelines/${pipelineId}/builder`);
        }}
      />
    </DashboardShell>
  );
}
