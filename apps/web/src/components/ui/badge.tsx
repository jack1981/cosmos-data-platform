import { cn } from "@/lib/utils";

export function Badge({
  label,
  variant = "default",
}: {
  label: string;
  variant?: "default" | "success" | "warning" | "danger";
}) {
  const classes = {
    default: "bg-[var(--color-surface)] text-[var(--color-text)] border-[var(--color-card-border)]",
    success: "bg-[color-mix(in_oklab,var(--color-success),white_80%)] text-[var(--color-success-strong)] border-[color-mix(in_oklab,var(--color-success),black_10%)]",
    warning: "bg-[color-mix(in_oklab,var(--color-warn),white_85%)] text-[var(--color-warn-strong)] border-[color-mix(in_oklab,var(--color-warn),black_10%)]",
    danger: "bg-[color-mix(in_oklab,var(--color-danger),white_85%)] text-[var(--color-danger)] border-[color-mix(in_oklab,var(--color-danger),black_10%)]",
  };

  return (
    <span className={cn("inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-medium", classes[variant])}>
      {label}
    </span>
  );
}
