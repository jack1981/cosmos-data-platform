import type { StageTemplate } from "@/lib/api";
import type { Pipeline } from "@/types/api";

export type StageCatalogEntry = {
  id: string;
  template: StageTemplate;
  category: string;
  subcategory: string;
  pathLabel: string;
  keywords: string[];
  searchText: string;
};

export type StageTreeSubcategory = {
  id: string;
  name: string;
  count: number;
  stages: StageCatalogEntry[];
};

export type StageTreeCategory = {
  id: string;
  name: string;
  count: number;
  subcategories: StageTreeSubcategory[];
};

export type HighlightPart = {
  text: string;
  matched: boolean;
};

type StageCategoryOverride = {
  category: string;
  subcategory: string;
  keywords?: string[];
};

const STAGE_CATEGORY_OVERRIDES: Record<string, StageCategoryOverride> = {
  "builtin.identity": { category: "Core", subcategory: "Utility", keywords: ["debug", "pass-through"] },
  "builtin.uppercase": { category: "Core", subcategory: "Transform", keywords: ["text", "normalize"] },
  "builtin.sleep": { category: "Core", subcategory: "Control", keywords: ["delay", "throttle"] },
  "builtin.filter_null": { category: "Core", subcategory: "Filter", keywords: ["null", "cleaning"] },
  "builtin.video_download": { category: "Video AI", subcategory: "Ingestion", keywords: ["http", "s3", "download"] },
  "builtin.video_caption": { category: "Video AI", subcategory: "ML", keywords: ["caption", "vlm", "model"] },
  "builtin.video_quality_gate": { category: "Video AI", subcategory: "Quality", keywords: ["gate", "quality", "monitoring"] },
  "builtin.video_incident_enrich": { category: "Video AI", subcategory: "Enrichment", keywords: ["incident", "severity"] },
  "builtin.video_writer": { category: "Video AI", subcategory: "Output", keywords: ["writer", "jsonl", "artifact"] },
  "builtin.dataset_lance_reader": {
    category: "Dataset",
    subcategory: "I/O",
    keywords: ["lance", "reader", "source", "daft"],
  },
  "builtin.dataset_filter": { category: "Dataset", subcategory: "Transform", keywords: ["filter", "predicate"] },
  "builtin.dataset_column_select": {
    category: "Dataset",
    subcategory: "Transform",
    keywords: ["projection", "columns", "select"],
  },
  "builtin.dataset_shuffle": {
    category: "Dataset",
    subcategory: "Transform",
    keywords: ["shuffle", "sample", "random"],
  },
  "builtin.dataset_union_by_name": {
    category: "Dataset",
    subcategory: "Merge",
    keywords: ["union", "fan-in", "combine"],
  },
  "builtin.dataset_join": {
    category: "Dataset",
    subcategory: "Merge",
    keywords: ["join", "fan-in", "merge"],
  },
  "builtin.dataset_lance_writer": {
    category: "Dataset",
    subcategory: "I/O",
    keywords: ["lance", "writer", "sink", "materialize"],
  },
};

function toTitleCase(value: string): string {
  const words = value
    .replace(/[^a-zA-Z0-9/_> -]+/g, " ")
    .split(/[\s_-]+/)
    .filter(Boolean);

  if (words.length === 0) {
    return "General";
  }

  return words
    .map((word) => {
      if (word.length <= 3) {
        return word.toUpperCase();
      }
      return `${word.slice(0, 1).toUpperCase()}${word.slice(1).toLowerCase()}`;
    })
    .join(" ");
}

function parseCategoryPath(category?: string): [string, string] | null {
  if (!category) {
    return null;
  }

  const normalized = category.replace(/::/g, "/").replace(/\s*>\s*/g, "/").trim();
  if (!normalized) {
    return null;
  }

  const parts = normalized
    .split("/")
    .map((part) => toTitleCase(part.trim()))
    .filter(Boolean);

  if (parts.length === 0) {
    return null;
  }
  if (parts.length === 1) {
    return [parts[0], "General"];
  }
  return [parts[0], parts[1]];
}

function inferCategoryFromTemplateId(templateId: string): [string, string] {
  if (templateId.startsWith("builtin.dataset_")) {
    return ["Dataset", "Operators"];
  }
  if (templateId.startsWith("builtin.video_")) {
    return ["Video AI", "Operators"];
  }
  if (templateId.startsWith("builtin.")) {
    return ["Core", "Operators"];
  }
  return ["Custom", "Operators"];
}

function normalizeSearch(value: string): string {
  return value.trim().toLowerCase();
}

export function buildStageCatalogEntries(templates: StageTemplate[]): StageCatalogEntry[] {
  const entries = templates.map((template) => {
    const override = STAGE_CATEGORY_OVERRIDES[template.id];
    const metadataCategory = parseCategoryPath(template.category);
    const [category, subcategory] = metadataCategory
      ? metadataCategory
      : override
        ? [override.category, override.subcategory]
        : inferCategoryFromTemplateId(template.id);

    const keywords = Array.from(
      new Set(
        [
          ...(override?.keywords ?? []),
          ...template.id.split(/[._-]+/).filter(Boolean),
          ...(template.display_name ? template.display_name.split(/\s+/).filter(Boolean) : []),
        ].map((item) => item.toLowerCase()),
      ),
    );

    const pathLabel = `${category} / ${subcategory}`;
    const searchText = normalizeSearch(
      [template.name, template.description, template.id, template.display_name ?? "", pathLabel, keywords.join(" ")].join(" "),
    );

    return {
      id: template.id,
      template,
      category,
      subcategory,
      pathLabel,
      keywords,
      searchText,
    } satisfies StageCatalogEntry;
  });

  return entries.sort((left, right) => left.template.name.localeCompare(right.template.name));
}

export function filterStageEntries(entries: StageCatalogEntry[], query: string): StageCatalogEntry[] {
  const normalized = normalizeSearch(query);
  if (!normalized) {
    return entries;
  }
  return entries.filter((entry) => entry.searchText.includes(normalized));
}

export function buildStageTree(entries: StageCatalogEntry[]): StageTreeCategory[] {
  const categoryMap = new Map<string, Map<string, StageCatalogEntry[]>>();

  for (const entry of entries) {
    if (!categoryMap.has(entry.category)) {
      categoryMap.set(entry.category, new Map());
    }
    const subcategoryMap = categoryMap.get(entry.category);
    if (!subcategoryMap) {
      continue;
    }
    if (!subcategoryMap.has(entry.subcategory)) {
      subcategoryMap.set(entry.subcategory, []);
    }
    const list = subcategoryMap.get(entry.subcategory);
    if (!list) {
      continue;
    }
    list.push(entry);
  }

  const categories: StageTreeCategory[] = [];
  for (const [categoryName, subcategoryMap] of categoryMap.entries()) {
    const subcategories: StageTreeSubcategory[] = [];
    for (const [subcategoryName, stages] of subcategoryMap.entries()) {
      const sortedStages = [...stages].sort((left, right) => left.template.name.localeCompare(right.template.name));
      subcategories.push({
        id: `${categoryName}::${subcategoryName}`,
        name: subcategoryName,
        count: sortedStages.length,
        stages: sortedStages,
      });
    }

    subcategories.sort((left, right) => right.count - left.count || left.name.localeCompare(right.name));

    categories.push({
      id: categoryName,
      name: categoryName,
      count: subcategories.reduce((sum, subcategory) => sum + subcategory.count, 0),
      subcategories,
    });
  }

  categories.sort((left, right) => right.count - left.count || left.name.localeCompare(right.name));
  return categories;
}

export function highlightMatch(value: string, query: string): HighlightPart[] {
  const normalizedQuery = query.trim();
  if (!normalizedQuery) {
    return [{ text: value, matched: false }];
  }

  const lowerValue = value.toLowerCase();
  const lowerQuery = normalizedQuery.toLowerCase();

  const parts: HighlightPart[] = [];
  let cursor = 0;

  while (cursor < value.length) {
    const foundAt = lowerValue.indexOf(lowerQuery, cursor);
    if (foundAt === -1) {
      parts.push({ text: value.slice(cursor), matched: false });
      break;
    }

    if (foundAt > cursor) {
      parts.push({ text: value.slice(cursor, foundAt), matched: false });
    }

    parts.push({ text: value.slice(foundAt, foundAt + lowerQuery.length), matched: true });
    cursor = foundAt + lowerQuery.length;
  }

  return parts.filter((part) => part.text.length > 0);
}

export type TemplateQuickFilter = "all" | "streaming" | "batch" | "etl" | "ml" | "ops";
export type TemplateSort = "recommended" | "a_to_z" | "recently_added" | "recently_used";

export type TemplateCatalogEntry = {
  pipeline: Pipeline;
  theme: string;
  searchText: string;
  quickFilters: TemplateQuickFilter[];
  recommendedRank: number;
};

export type TemplateThemeOption = {
  id: string;
  label: string;
  count: number;
};

const TEMPLATE_THEME_ORDER = ["Data Prep", "ML", "Video AI", "Ops/Monitoring", "General"];

type TemplateOverride = {
  theme: string;
  quickFilters: TemplateQuickFilter[];
  recommendedRank: number;
};

const TEMPLATE_OVERRIDES: Record<string, TemplateOverride> = {
  template_video_caption_batch: { theme: "Video AI", quickFilters: ["batch", "ml"], recommendedRank: 10 },
  template_video_quality_review: { theme: "Ops/Monitoring", quickFilters: ["batch", "ops"], recommendedRank: 11 },
  template_video_incident_triage: { theme: "Ops/Monitoring", quickFilters: ["batch", "ops"], recommendedRank: 12 },
};

function inferTemplateTheme(template: Pipeline): string {
  const override = TEMPLATE_OVERRIDES[template.external_id];
  if (override) {
    return override.theme;
  }

  const tags = new Set(template.tags.map((tag) => tag.toLowerCase()));
  const context = `${template.name} ${template.description} ${template.tags.join(" ")}`.toLowerCase();

  if (tags.has("video") || context.includes("caption") || context.includes("incident")) {
    return "Video AI";
  }
  if (tags.has("dataset") || tags.has("daft") || tags.has("lance") || context.includes("etl")) {
    return "Data Prep";
  }
  if (context.includes("model") || context.includes("classifier") || context.includes("score") || tags.has("ml")) {
    return "ML";
  }
  if (context.includes("quality") || context.includes("monitor") || context.includes("ops") || tags.has("quality")) {
    return "Ops/Monitoring";
  }
  return "General";
}

function inferTemplateQuickFilters(template: Pipeline, theme: string): TemplateQuickFilter[] {
  const quickFilters = new Set<TemplateQuickFilter>();
  const tags = template.tags.map((tag) => tag.toLowerCase());
  const context = `${template.name} ${template.description} ${template.tags.join(" ")}`.toLowerCase();

  if (template.execution_mode === "streaming") {
    quickFilters.add("streaming");
  }
  if (template.execution_mode === "batch") {
    quickFilters.add("batch");
  }

  if (tags.some((tag) => ["dataset", "daft", "lance", "etl"].includes(tag)) || context.includes("dataset") || context.includes("etl")) {
    quickFilters.add("etl");
  }

  if (theme === "ML" || tags.includes("ml") || context.includes("model") || context.includes("classifier") || context.includes("inference")) {
    quickFilters.add("ml");
  }

  if (
    theme === "Ops/Monitoring" ||
    tags.some((tag) => ["ops", "monitoring", "quality", "incident", "video"].includes(tag)) ||
    context.includes("monitor") ||
    context.includes("quality")
  ) {
    quickFilters.add("ops");
  }

  if (quickFilters.size === 0) {
    quickFilters.add("batch");
  }

  return Array.from(quickFilters);
}

export function buildTemplateCatalogEntries(templates: Pipeline[]): TemplateCatalogEntry[] {
  return templates.map((template, index) => {
    const override = TEMPLATE_OVERRIDES[template.external_id];
    const theme = inferTemplateTheme(template);
    const quickFilters = Array.from(new Set([...(override?.quickFilters ?? []), ...inferTemplateQuickFilters(template, theme)]));
    const searchText = normalizeSearch(
      [template.name, template.description, template.external_id, template.execution_mode, template.tags.join(" "), theme].join(" "),
    );

    return {
      pipeline: template,
      theme,
      searchText,
      quickFilters,
      recommendedRank: override?.recommendedRank ?? 100 + index,
    } satisfies TemplateCatalogEntry;
  });
}

export function buildTemplateThemeOptions(
  entries: TemplateCatalogEntry[],
  recentlyUsedIds: string[],
): TemplateThemeOption[] {
  const counts = new Map<string, number>();
  for (const entry of entries) {
    counts.set(entry.theme, (counts.get(entry.theme) ?? 0) + 1);
  }

  const recentlyUsedSet = new Set(recentlyUsedIds);
  const recentlyUsedCount = entries.filter((entry) => recentlyUsedSet.has(entry.pipeline.id)).length;

  const themes = Array.from(counts.entries())
    .sort((left, right) => {
      const leftOrder = TEMPLATE_THEME_ORDER.indexOf(left[0]);
      const rightOrder = TEMPLATE_THEME_ORDER.indexOf(right[0]);
      if (leftOrder >= 0 || rightOrder >= 0) {
        return (leftOrder < 0 ? Number.MAX_SAFE_INTEGER : leftOrder) - (rightOrder < 0 ? Number.MAX_SAFE_INTEGER : rightOrder);
      }
      return left[0].localeCompare(right[0]);
    })
    .map(([theme, count]) => ({ id: theme, label: theme, count }));

  return [
    { id: "all", label: "All", count: entries.length },
    { id: "recent", label: "Recently Used", count: recentlyUsedCount },
    ...themes,
  ];
}

export function filterTemplateEntries(
  entries: TemplateCatalogEntry[],
  options: {
    query: string;
    themeId: string;
    quickFilter: TemplateQuickFilter;
    recentlyUsedIds: string[];
  },
): TemplateCatalogEntry[] {
  const normalizedQuery = normalizeSearch(options.query);
  const recentSet = new Set(options.recentlyUsedIds);

  return entries.filter((entry) => {
    if (options.themeId === "recent" && !recentSet.has(entry.pipeline.id)) {
      return false;
    }
    if (options.themeId !== "all" && options.themeId !== "recent" && entry.theme !== options.themeId) {
      return false;
    }
    if (options.quickFilter !== "all" && !entry.quickFilters.includes(options.quickFilter)) {
      return false;
    }
    if (normalizedQuery && !entry.searchText.includes(normalizedQuery)) {
      return false;
    }
    return true;
  });
}

export function sortTemplateEntries(
  entries: TemplateCatalogEntry[],
  sortBy: TemplateSort,
  recentlyUsedIds: string[],
): TemplateCatalogEntry[] {
  const recentOrder = new Map<string, number>();
  recentlyUsedIds.forEach((id, index) => {
    recentOrder.set(id, index);
  });

  const copy = [...entries];

  if (sortBy === "a_to_z") {
    return copy.sort((left, right) => left.pipeline.name.localeCompare(right.pipeline.name));
  }

  if (sortBy === "recently_added") {
    return copy.sort((left, right) => {
      const leftTime = Date.parse(left.pipeline.created_at || "");
      const rightTime = Date.parse(right.pipeline.created_at || "");
      return rightTime - leftTime;
    });
  }

  if (sortBy === "recently_used") {
    return copy.sort((left, right) => {
      const leftIdx = recentOrder.get(left.pipeline.id);
      const rightIdx = recentOrder.get(right.pipeline.id);
      if (leftIdx === undefined && rightIdx === undefined) {
        return left.recommendedRank - right.recommendedRank || left.pipeline.name.localeCompare(right.pipeline.name);
      }
      if (leftIdx === undefined) {
        return 1;
      }
      if (rightIdx === undefined) {
        return -1;
      }
      return leftIdx - rightIdx;
    });
  }

  return copy.sort((left, right) => {
    if (left.recommendedRank !== right.recommendedRank) {
      return left.recommendedRank - right.recommendedRank;
    }
    return left.pipeline.name.localeCompare(right.pipeline.name);
  });
}

export function suggestTemplateThemes(entries: TemplateCatalogEntry[], limit = 3): string[] {
  const themeCounts = new Map<string, number>();
  for (const entry of entries) {
    themeCounts.set(entry.theme, (themeCounts.get(entry.theme) ?? 0) + 1);
  }

  return Array.from(themeCounts.entries())
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, limit)
    .map(([theme]) => theme);
}
