"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

import { useAuth } from "@/components/auth/auth-provider";

export function Protected({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, loading } = useAuth();
  const router = useRouter();

  useEffect(() => {
    if (!loading && !isAuthenticated) {
      router.replace("/login");
    }
  }, [isAuthenticated, loading, router]);

  if (loading || !isAuthenticated) {
    return (
      <div className="flex h-screen items-center justify-center bg-[var(--color-bg)] text-[var(--color-text)]">
        <div className="rounded-2xl border border-[var(--color-card-border)] bg-[var(--color-card)] px-6 py-4 text-sm">
          Loading session...
        </div>
      </div>
    );
  }

  return <>{children}</>;
}
