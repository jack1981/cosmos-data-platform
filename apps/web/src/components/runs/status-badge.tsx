import { Badge } from "@/components/ui/badge";

export function RunStatusBadge({ status }: { status: string }) {
  if (status === "SUCCEEDED") {
    return <Badge label={status} variant="success" />;
  }
  if (status === "RUNNING" || status === "QUEUED") {
    return <Badge label={status} variant="warning" />;
  }
  if (status === "FAILED" || status === "STOPPED") {
    return <Badge label={status} variant="danger" />;
  }
  return <Badge label={status} />;
}
