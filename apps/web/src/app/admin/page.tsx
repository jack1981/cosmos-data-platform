"use client";

import { useEffect, useState } from "react";

import { useAuth } from "@/components/auth/auth-provider";
import { DashboardShell } from "@/components/layout/dashboard-shell";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import type { AdminAuditEntry, AdminUser, RoleItem } from "@/lib/api";

export default function AdminPage() {
  const { apiCall, hasRole } = useAuth();
  const isAdmin = hasRole("INFRA_ADMIN");

  const [roles, setRoles] = useState<RoleItem[]>([]);
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [audit, setAudit] = useState<AdminAuditEntry[]>([]);

  const [newUserEmail, setNewUserEmail] = useState("new.user@pipelineforge.local");
  const [newUserName, setNewUserName] = useState("New User");
  const [newUserPassword, setNewUserPassword] = useState("User123!");
  const [newUserRole, setNewUserRole] = useState("PIPELINE_DEV");

  const reload = () => {
    void apiCall((client) => client.listRoles()).then(setRoles);
    void apiCall((client) => client.listUsers()).then(setUsers);
    void apiCall((client) => client.listAuditLog(50)).then(setAudit);
  };

  useEffect(() => {
    if (!isAdmin) {
      return;
    }
    reload();
  }, [apiCall, isAdmin]);

  if (!isAdmin) {
    return (
      <DashboardShell>
        <Card>
          <CardContent className="py-8 text-sm text-[var(--color-muted)]">
            Admin panel is restricted to INFRA_ADMIN.
          </CardContent>
        </Card>
      </DashboardShell>
    );
  }

  return (
    <DashboardShell>
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <h1 className="text-2xl font-semibold">Admin</h1>
            <p className="text-sm text-[var(--color-muted)]">
              Minimal management controls for users, roles, and audit trail.
            </p>
          </CardHeader>
        </Card>

        <div className="grid gap-4 xl:grid-cols-2">
          <Card>
            <CardHeader>
              <h3 className="text-sm font-semibold">Users & Roles</h3>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="grid gap-2 md:grid-cols-2">
                <Input value={newUserEmail} onChange={(e) => setNewUserEmail(e.target.value)} placeholder="Email" />
                <Input value={newUserName} onChange={(e) => setNewUserName(e.target.value)} placeholder="Full name" />
                <Input value={newUserPassword} onChange={(e) => setNewUserPassword(e.target.value)} placeholder="Password" />
                <select
                  className="h-9 rounded-md border border-[var(--color-card-border)] bg-[var(--color-surface)] px-3 text-sm"
                  value={newUserRole}
                  onChange={(e) => setNewUserRole(e.target.value)}
                >
                  {(roles.length > 0
                    ? roles
                    : [
                        { id: "infra", name: "INFRA_ADMIN", description: "" },
                        { id: "dev", name: "PIPELINE_DEV", description: "" },
                        { id: "aiops", name: "AIOPS_ENGINEER", description: "" },
                      ]
                  ).map((role) => (
                    <option key={role.id} value={role.name}>
                      {role.name}
                    </option>
                  ))}
                </select>
              </div>
              <Button
                onClick={async () => {
                  await apiCall((client) =>
                    client.createUser({
                      email: newUserEmail,
                      full_name: newUserName,
                      password: newUserPassword,
                      roles: [newUserRole],
                    }),
                  );
                  reload();
                }}
              >
                Create User
              </Button>
              <div className="space-y-2">
                {users.map((user) => (
                  <div key={user.id} className="rounded border border-[var(--color-card-border)] bg-[var(--color-surface)] p-2 text-xs">
                    <p className="font-semibold">{user.email}</p>
                    <p className="text-[var(--color-muted)]">{user.full_name}</p>
                    <div className="mt-1 flex gap-1">
                      {user.roles.map((role) => (
                        <Badge key={role} label={role} />
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <h3 className="text-sm font-semibold">Audit Trail (Latest 50)</h3>
            </CardHeader>
            <CardContent>
              <div className="max-h-[420px] space-y-2 overflow-auto">
                {audit.map((entry) => (
                  <div key={entry.id} className="rounded border border-[var(--color-card-border)] bg-[var(--color-surface)] p-2 text-xs">
                    <div className="flex items-center justify-between gap-2">
                      <Badge label={entry.action} />
                      <span className="text-[var(--color-muted)]">
                        {new Date(entry.created_at).toLocaleString()}
                      </span>
                    </div>
                    <p className="mt-1 text-[var(--color-muted)]">
                      {entry.resource_type}:{entry.resource_id}
                    </p>
                  </div>
                ))}
                {audit.length === 0 ? (
                  <p className="text-sm text-[var(--color-muted)]">No audit events found.</p>
                ) : null}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </DashboardShell>
  );
}
