import type { Metadata } from "next";
import { IBM_Plex_Sans, Space_Grotesk } from "next/font/google";

import { AuthProvider } from "@/components/auth/auth-provider";

import "./globals.css";

const bodyFont = IBM_Plex_Sans({ subsets: ["latin"], variable: "--font-body" });
const displayFont = Space_Grotesk({ subsets: ["latin"], variable: "--font-display" });

export const metadata: Metadata = {
  title: "Management Plane",
  description: "Airflow-like control plane for Cosmos-Xenna Ray pipelines",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className={`${bodyFont.variable} ${displayFont.variable}`}>
        <AuthProvider>{children}</AuthProvider>
      </body>
    </html>
  );
}
