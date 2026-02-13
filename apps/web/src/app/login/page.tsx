"use client";

import { FormEvent, useEffect, useState } from "react";
import { useRouter } from "next/navigation";

import { useAuth } from "@/components/auth/auth-provider";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";

export default function LoginPage() {
  const { signIn, isAuthenticated } = useAuth();
  const router = useRouter();
  const [email, setEmail] = useState("dev@pipelineforge.local");
  const [password, setPassword] = useState("Dev123!");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (isAuthenticated) {
      router.replace("/pipelines");
    }
  }, [isAuthenticated, router]);

  const onSubmit = async (event: FormEvent) => {
    event.preventDefault();
    setError(null);
    setLoading(true);
    try {
      await signIn(email, password);
      router.replace("/pipelines");
    } catch {
      setError("Login failed. Check credentials.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center px-4">
      <Card className="w-full max-w-md">
        <CardHeader>
          <p className="text-xs uppercase tracking-[0.2em] text-[var(--color-muted)]">Management Plane</p>
          <h1 className="text-xl font-semibold">Login</h1>
        </CardHeader>
        <CardContent>
          <form className="space-y-3" onSubmit={onSubmit}>
            <label className="block space-y-1">
              <span className="text-xs text-[var(--color-muted)]">Email</span>
              <Input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required />
            </label>
            <label className="block space-y-1">
              <span className="text-xs text-[var(--color-muted)]">Password</span>
              <Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} required />
            </label>
            {error ? <p className="text-sm text-[var(--color-danger)]">{error}</p> : null}
            <Button className="w-full" type="submit" disabled={loading}>
              {loading ? "Signing in..." : "Sign in"}
            </Button>
          </form>
          <div className="mt-4 rounded-md bg-[var(--color-surface)] p-3 text-xs text-[var(--color-muted)]">
            <p className="font-semibold">Default dev users</p>
            <p>Admin: admin@pipelineforge.local / Admin123!</p>
            <p>Developer: dev@pipelineforge.local / Dev123!</p>
            <p>AIOps: aiops@pipelineforge.local / Aiops123!</p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
