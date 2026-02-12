import * as React from "react";

import { cn } from "@/lib/utils";

export const Input = React.forwardRef<HTMLInputElement, React.InputHTMLAttributes<HTMLInputElement>>(function Input(
  { className, ...props },
  ref,
) {
  return (
    <input
      ref={ref}
      className={cn(
        "h-9 w-full rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] px-3 text-sm text-[var(--color-text)] outline-none placeholder:text-[var(--color-muted)] focus:border-[var(--color-accent)]",
        className,
      )}
      {...props}
    />
  );
});
