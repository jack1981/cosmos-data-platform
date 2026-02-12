import type { ReactNode } from "react";

import { cn } from "@/lib/utils";

export function Card({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <section className={cn("rounded-xl border border-[var(--color-card-border)] bg-[var(--color-card)]", className)}>
      {children}
    </section>
  );
}

export function CardHeader({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("border-b border-[var(--color-card-border)] px-4 py-3", className)}>{children}</div>;
}

export function CardContent({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("px-4 py-3", className)}>{children}</div>;
}
