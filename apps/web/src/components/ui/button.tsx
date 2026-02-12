import * as React from "react";

import { cn } from "@/lib/utils";

type ButtonVariant = "default" | "secondary" | "ghost" | "danger";

type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
};

const variantClasses: Record<ButtonVariant, string> = {
  default:
    "bg-[var(--color-accent)] text-white hover:bg-[color-mix(in_oklab,var(--color-accent),black_10%)] border border-transparent",
  secondary:
    "bg-[var(--color-surface)] text-[var(--color-text)] hover:bg-[var(--color-surface-hover)] border border-[var(--color-card-border)]",
  ghost: "bg-transparent text-[var(--color-text)] hover:bg-[var(--color-surface-hover)] border border-transparent",
  danger: "bg-[var(--color-danger)] text-white hover:opacity-90 border border-transparent",
};

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { className, variant = "default", ...props },
  ref,
) {
  return (
    <button
      ref={ref}
      className={cn(
        "inline-flex h-9 items-center justify-center rounded-md px-3 text-sm font-medium transition-colors disabled:pointer-events-none disabled:opacity-40",
        variantClasses[variant],
        className,
      )}
      {...props}
    />
  );
});
