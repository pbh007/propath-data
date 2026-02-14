import fs from "fs";
import path from "path";
import Papa from "papaparse";

import { writeEventsCsv } from "./lib/writeCsv.js";
import { runJsonApi } from "./connectors/json_api.js";
import { runHtmlTable } from "./connectors/html_table.js";
import { runHtmlBlocks } from "./connectors/html_blocks.js";

type Source = {
  id: string;
  name: string;
  connector: "json_api" | "html_table" | "html_blocks";
  url: string;

  /**
   * IMPORTANT:
   * This should be a stable filename (same every run), e.g.
   * "data/ProPath-MiniTour2026-MasterEvents.csv"
   * or "data/ProPathEvents2026-MasterEvents.csv"
   */
  output: string;

  defaults?: Record<string, string>;
  tableSelector?: string;
};

function isValidUrl(url?: string) {
  if (!url) return false;
  if (url.includes("PASTE_URL_HERE")) return false;

  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
}

/** Ensure every row has a stable id (needed for merge) */
function ensureId(row: any, fallbackPrefix: string) {
  const id = String(row?.id ?? "").trim();
  if (id) return id;

  const parts = [
    row?.tour ?? "",
    row?.title ?? "",
    row?.start ?? "",
    row?.city ?? "",
    row?.state_country ?? "",
  ]
    .map((x: any) => String(x || "").trim())
    .filter(Boolean);

  const combo = parts.join("|");
  if (!combo) return `${fallbackPrefix}-${Math.random().toString(36).slice(2)}`;

  // lightweight slug
  const slug = combo
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");

  return `${fallbackPrefix}-${slug}`;
}

function readExistingCsvRows(outPath: string): any[] {
  try {
    if (!fs.existsSync(outPath)) return [];
    const csv = fs.readFileSync(outPath, "utf8");
    const parsed = Papa.parse(csv, { header: true, skipEmptyLines: "greedy" });
    const rows = (parsed.data as any[]).filter((r) => r && Object.keys(r).length);
    return rows;
  } catch {
    return [];
  }
}

/**
 * Merge rule:
 * - keep everything already in the file (including past events)
 * - overwrite rows with same id using fresh scrape (updates)
 * - add brand new ids (new events)
 */
function mergeById(existing: any[], fresh: any[]): any[] {
  const map = new Map<string, any>();

  for (const r of existing) {
    const id = String(r?.id ?? "").trim();
    if (id) map.set(id, r);
  }
  for (const r of fresh) {
    const id = String(r?.id ?? "").trim();
    if (id) map.set(id, r); // overwrite/update
  }

  return Array.from(map.values());
}

async function runSource(s: Source): Promise<boolean> {
  console.log(`\n=== ${s.name} (${s.connector}) ===`);

  // Skip invalid URLs
  if (!isValidUrl(s.url)) {
    console.log(`⚠ Skipping ${s.name}: invalid or placeholder URL.`);
    return false;
  }

  try {
    let events: any[] = [];

    if (s.connector === "json_api") {
      events = await runJsonApi(s);
    } else if (s.connector === "html_table") {
      events = await runHtmlTable(s);
    } else if (s.connector === "html_blocks") {
      events = await runHtmlBlocks(s);
    } else {
      console.log(`⚠ Unknown connector for ${s.name}, skipping.`);
      return false;
    }

    if (!events || !events.length) {
      console.log(`⚠ ${s.name}: 0 events returned. Skipping write.`);
      return false;
    }

    // Normalize output path (most of your sources.json uses "data/<file>.csv")
    const outPath = path.resolve(s.output);

    // Ensure ids exist (critical for merge)
    const fresh = events.map((r) => ({
      ...r,
      id: ensureId(r, s.id || "src"),
    }));

    // ✅ NEW BEHAVIOR: merge into existing file instead of overwriting
    const existing = readExistingCsvRows(outPath);
    const merged = mergeById(existing, fresh);

    writeEventsCsv(s.output, merged);

    console.log(
      `✓ ${s.name}: scraped=${fresh.length}, existing=${existing.length}, written=${merged.length}`
    );
    return true;
  } catch (err) {
    console.error(`✖ ${s.name} failed:`);
    console.error(err);
    return false;
  }
}

async function main() {
  const sourcesPath = path.resolve("data", "sources.json");

  if (!fs.existsSync(sourcesPath)) {
    throw new Error("sources.json not found in data folder.");
  }

  const sources = JSON.parse(fs.readFileSync(sourcesPath, "utf8")) as Source[];

  if (!Array.isArray(sources) || !sources.length) {
    throw new Error("sources.json is empty or invalid.");
  }

  let successCount = 0;

  for (const s of sources) {
    const ok = await runSource(s);
    if (ok) successCount++;
  }

  if (successCount === 0) {
    throw new Error("All sources failed.");
  }

  console.log(`\nComplete. ${successCount}/${sources.length} sources succeeded.`);
}

main().catch((err) => {
  console.error("\nScraper failed:");
  console.error(err);
  process.exit(1);
});
