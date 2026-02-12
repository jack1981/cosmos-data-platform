"use client";

import { AppShell } from "@/components/layout/app-shell";
import { Protected } from "@/components/auth/protected";

export function DashboardShell({ children }: { children: React.ReactNode }) {
  return (
    <Protected>
      <AppShell>{children}</AppShell>
    </Protected>
  );
}
