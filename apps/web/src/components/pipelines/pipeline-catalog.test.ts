import { describe, expect, it } from "vitest";

import {
  buildStageCatalogEntries,
  buildStageTree,
  buildTemplateCatalogEntries,
  buildTemplateThemeOptions,
  filterStageEntries,
  filterTemplateEntries,
  highlightMatch,
  sortTemplateEntries,
} from "./pipeline-catalog";

import type { StageTemplate } from "../../lib/api";
import type { Pipeline } from "../../types/api";

function makePipeline(overrides: Partial<Pipeline>): Pipeline {
  return {
    id: overrides.id ?? "pipeline-1",
    external_id: overrides.external_id ?? "template_default",
    name: overrides.name ?? "Template Default",
    description: overrides.description ?? "default",
    tags: overrides.tags ?? ["template", "starter"],
    execution_mode: overrides.execution_mode ?? "batch",
    owner_user_id: overrides.owner_user_id ?? "user-1",
    owner_team_id: overrides.owner_team_id ?? null,
    metadata_links: overrides.metadata_links ?? {},
    created_at: overrides.created_at ?? "2026-01-10T00:00:00Z",
    updated_at: overrides.updated_at ?? "2026-01-11T00:00:00Z",
  };
}

describe("stage palette catalog", () => {
  it("groups stage templates into category tree and supports category path search", () => {
    const templates: StageTemplate[] = [
      { id: "builtin.dataset_filter", name: "Dataset Filter", description: "Filter rows" },
      { id: "builtin.video_caption", name: "Video Caption", description: "Caption videos" },
    ];

    const entries = buildStageCatalogEntries(templates);
    const tree = buildStageTree(entries);

    const datasetCategory = tree.find((category) => category.name === "Dataset");
    expect(datasetCategory).toBeTruthy();
    expect(datasetCategory?.subcategories.some((subcategory) => subcategory.name === "Transform")).toBe(true);

    const searchByPath = filterStageEntries(entries, "dataset / transform");
    expect(searchByPath.map((entry) => entry.id)).toContain("builtin.dataset_filter");
  });

  it("highlights matched substrings", () => {
    const parts = highlightMatch("Dataset Lance Writer", "lance");
    expect(parts.some((part) => part.matched && part.text.toLowerCase() === "lance")).toBe(true);
  });
});

describe("template gallery catalog", () => {
  it("filters by theme, quick filter, and query", () => {
    const templates = [
      makePipeline({
        id: "p1",
        external_id: "template_video_incident_triage",
        name: "Template: Video Incident Triage",
        description: "Detect operational incidents from video captions",
        tags: ["template", "starter", "video", "ops"],
      }),
      makePipeline({
        id: "p2",
        external_id: "template_video_caption_batch",
        name: "Template: Video Caption Batch",
        description: "Caption model stage",
        tags: ["template", "starter", "video", "ml"],
      }),
    ];

    const entries = buildTemplateCatalogEntries(templates);
    const filtered = filterTemplateEntries(entries, {
      query: "incident",
      themeId: "Ops/Monitoring",
      quickFilter: "ops",
      recentlyUsedIds: [],
    });

    expect(filtered).toHaveLength(1);
    expect(filtered[0]?.pipeline.external_id).toBe("template_video_incident_triage");
  });

  it("orders recently used templates first", () => {
    const templates = [
      makePipeline({ id: "p1", external_id: "template_video_caption_batch", name: "Caption Batch" }),
      makePipeline({ id: "p2", external_id: "template_video_quality_review", name: "Quality Review" }),
      makePipeline({ id: "p3", external_id: "template_video_incident_triage", name: "Incident Triage" }),
    ];

    const entries = buildTemplateCatalogEntries(templates);
    const sorted = sortTemplateEntries(entries, "recently_used", ["p3", "p1"]);

    expect(sorted[0]?.pipeline.id).toBe("p3");
    expect(sorted[1]?.pipeline.id).toBe("p1");

    const themes = buildTemplateThemeOptions(entries, ["p3", "p1"]);
    const recent = themes.find((item) => item.id === "recent");
    expect(recent?.count).toBe(2);
  });
});
