"use client";

import { useState } from "react";

import { useAuth } from "@/components/auth/auth-provider";
import { DashboardShell } from "@/components/layout/dashboard-shell";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

export default function ProfilePage() {
  const { user, apiCall } = useAuth();
  const [result, setResult] = useState<string>("");

  return (
    <DashboardShell>
      <Card>
        <CardHeader>
          <h1 className="text-2xl font-semibold">Profile</h1>
          <p className="text-sm text-[var(--color-muted)]">Session details and token-refresh validation.</p>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] p-3 text-sm">
            <p className="font-medium">{user?.full_name}</p>
            <p className="text-[var(--color-muted)]">{user?.email}</p>
            <div className="mt-2 flex flex-wrap gap-1">
              {user?.roles.map((role) => (
                <Badge key={role} label={role} />
              ))}
            </div>
          </div>

          <Button
            variant="secondary"
            onClick={async () => {
              try {
                const me = await apiCall((client) => client.getMe());
                setResult(`Session valid for ${me.email}. Token refresh handled automatically on 401.`);
              } catch (error) {
                setResult(`Session check failed: ${String(error)}`);
              }
            }}
          >
            Validate Session
          </Button>

          {result ? <p className="text-sm text-[var(--color-muted)]">{result}</p> : null}
        </CardContent>
      </Card>
    </DashboardShell>
  );
}
