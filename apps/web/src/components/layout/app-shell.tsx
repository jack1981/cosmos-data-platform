"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Cable, LayoutList, PanelLeftClose, Shield } from "lucide-react";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useAuth } from "@/components/auth/auth-provider";

const navItems = [
  { href: "/pipelines", label: "Pipelines", icon: Cable },
  { href: "/runs", label: "Runs", icon: LayoutList },
  { href: "/admin", label: "Admin", icon: Shield, adminOnly: true },
];

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { user, hasRole, signOut } = useAuth();

  return (
    <div className="min-h-screen bg-[var(--color-bg)] text-[var(--color-text)]">
      <div className="mx-auto grid min-h-screen w-full max-w-[1700px] grid-cols-1 lg:grid-cols-[260px_1fr]">
        <aside className="border-r border-[var(--color-card-border)] bg-[linear-gradient(165deg,var(--color-nav)_0%,#0f2831_65%,#10242a_100%)] px-4 py-5 text-white">
          <div className="mb-6 flex items-center justify-between rounded-lg border border-white/15 bg-black/20 px-3 py-2">
            <div>
              <p className="text-xs uppercase tracking-[0.2em] text-cyan-200">Cosmos-Xenna</p>
              <p className="text-sm font-semibold">Management Plane</p>
            </div>
            <PanelLeftClose className="h-4 w-4 text-cyan-200" />
          </div>

          <nav className="space-y-1">
            {navItems
              .filter((item) => !item.adminOnly || hasRole("INFRA_ADMIN"))
              .map((item) => {
                const Icon = item.icon;
                const isActive = pathname === item.href || pathname.startsWith(`${item.href}/`);
                return (
                  <Link
                    href={item.href}
                    key={item.href}
                    className={cn(
                      "flex items-center gap-2 rounded-md px-3 py-2 text-sm transition",
                      isActive ? "bg-white/20 text-white" : "text-cyan-100 hover:bg-white/10",
                    )}
                  >
                    <Icon className="h-4 w-4" />
                    {item.label}
                  </Link>
                );
              })}
          </nav>

          <div className="mt-8 rounded-lg border border-white/15 bg-black/20 p-3 text-xs">
            <p className="font-medium">Signed in</p>
            <p className="mt-1 truncate text-cyan-100">{user?.email}</p>
            <div className="mt-2 flex flex-wrap gap-1">
              {user?.roles.map((role) => (
                <span key={role} className="rounded border border-white/20 px-1.5 py-0.5 text-[10px]">
                  {role}
                </span>
              ))}
            </div>
            <div className="mt-3 flex items-center gap-2">
              <Link href="/profile" className="text-cyan-200 underline underline-offset-2">
                Profile
              </Link>
              <Button variant="ghost" className="h-7 px-2 text-xs text-cyan-100" onClick={signOut}>
                Sign out
              </Button>
            </div>
          </div>
        </aside>

        <main className="p-4 lg:p-6">{children}</main>
      </div>
    </div>
  );
}
