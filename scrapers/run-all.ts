import fs from "fs";
import path from "path";
import Papa from "papaparse";

import { writeEventsCsv } from "./lib/writeCsv.js";
import { runJsonApi } from "./connectors/json_api.js";
import { runHtmlTable } from "./connectors/html_table.js";
import { runHtmlBlocks } from "./connectors/html_blocks.js";

type ProPathEvent = {
  id?: string;
  tour?: string;
  gender?: string;
  type?: string;
  stage?: string;
  title?: string;
  start?: string | null;
  end?: string | null;
  city?: string;
  state_country?: string;
  tourUrl?: string;
  signupUrl?: string;
  mondayUrl?: string;
  mondayDate?: string | null;
};

type Source = {
  id: string;
  name: string;
  connector: "json_api" | "html_table" | "html_blocks";
  url: string;
  output: string; // IMPORTANT: should be under data/
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

function safeRead(filePath: string): string {
  if (!fs.existsSync(filePath)) return "";
  return fs.readFileSync(filePath, "utf8");
}

function slug(s: string) {
  return (s || "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function makeStableId(e: ProPathEvent): string {
  // stable enough: tour + start + title + city
  return [
    slug(e.tour || "unknown-tour"),
    e.start || "tbd",
    slug(e.title || "event"),
    slug(e.city || ""),
  ].filter(Boolean).join("-");
}

function normalizeRow(r: any): ProPathEvent {
  const out: ProPathEvent = {
    id: (r.id ?? "").toString().trim() || undefined,
    tour: (r.tour ?? "").toString().trim() || undefined,
    gender: (r.gender ?? "").toString().trim() || undefined,
    type: (r.type ?? "").toString().trim() || undefined,
    stage: (r.stage ?? "").toString().trim() || undefined,
    title: (r.title ?? "").toString().trim() || undefined,
    start: (r.start ?? "").toString().trim() || null,
    end: (r.end ?? "").toString().trim() || null,
    city: (r.city ?? "").toString().trim() || undefined,
    state_country: (r.state_country ?? "").toString().trim() || undefined,
    tourUrl: (r.tourUrl ?? "").toString().trim() || undefined,
    signupUrl: (r.signupUrl ?? "").toString().trim() || undefined,
    mondayUrl: (r.mondayUrl ?? "").toString().trim() || undefined,
    mondayDate: (r.mondayDate ?? "").toString().trim() || null,
  };

  if (!out.id) out.id = makeStableId(out);
  return out;
}

function parseCsvToEvents(csvText: string): ProPathEvent[] {
  if (!csvText.trim()) return [];
  const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: "greedy" });
  if (parsed.errors?.length) {
    console.log("CSV parse warnings:", parsed.errors.slice(0, 3));
  }
  const rows = (parsed.data as any[]).filter(Boolean);
  return rows.map(normalizeRow);
}

function mergeById(existing: ProPathEvent[], incoming: ProPathEvent[]): ProPathEvent[] {
  const map = new Map<string, ProPathEvent>();

  // Existing first (preserve past events)
  for (const e of existing) {
    const id = e.id || makeStableId(e);
    map.set(id, { ...e, id });
  }

  // Incoming overrides/updates by id
  for (const e of incoming) {
    const id = e.id || makeStableId(e);
    const prev = map.get(id) || {};
    map.set(id, { ...prev, ...e, id });
  }

  // Sort by start date (nulls at end)
  const out = Array.from(map.values());
  out.sort((a, b) => {
    const as = a.start || "9999-12-31";
    const bs = b.start || "9999-12-31";
    return as.localeCompare(bs);
  });
  return out;
}

async function runSource(s: Source): Promise<boolean> {
  console.log(`\n=== ${s.name} (${s.connector}) ===`);

  if (!isValidUrl(s.url)) {
    console.log(`⚠ Skipping ${s.name}: invalid or placeholder URL.`);
    return false;
  }

  try {
    let events: ProPathEvent[] = [];

    if (s.connector === "json_api") {
      events = await runJsonApi(s as any);
    } else if (s.connector === "html_table") {
      events = await runHtmlTable(s as any);
    } else if (s.connector === "html_blocks") {
      events = await runHtmlBlocks(s as any);
    } else {
      console.log(`⚠ Unknown connector for ${s.name}, skipping.`);
      return false;
    }

    if (!events || !events.length) {
      console.log(`⚠ ${s.name}: 0 events returned. Skipping write.`);
      return false;
    }

    // Ensure output is under data/
    if (!s.output.startsWith("data/")) {
      console.log(`⚠ ${s.name}: output MUST start with "data/". Currently: ${s.output}`);
      return false;
    }

    writeEventsCsv(s.output, events as any);
    console.log(`✓ ${s.name}: ${events.length} events written to ${s.output}.`);
    return true;

  } catch (err) {
    console.error(`✖ ${s.name} failed:`);
    console.error(err);
    return false;
  }
}

function mergeIncomingMiniTourMaster() {
  const dataDir = path.resolve("data");
  const masterPath = path.join(dataDir, "ProPath-MiniTour2026-MasterEvents.csv");

  // Read existing master
  const existingCsv = safeRead(masterPath);
  const existing = parseCsvToEvents(existingCsv);

  // Read all incoming staging files
  const incomingFiles = fs.existsSync(dataDir)
    ? fs.readdirSync(dataDir).filter((f) => f.startsWith("_incoming_") && f.endsWith(".csv"))
    : [];

  const incomingAll: ProPathEvent[] = [];
  for (const f of incomingFiles) {
    const p = path.join(dataDir, f);
    const csv = safeRead(p);
    const events = parseCsvToEvents(csv);
    incomingAll.push(...events);
  }

  if (!incomingAll.length) {
    console.log("⚠ No incoming mini-tour events found (data/_incoming_*.csv). Merge skipped.");
    return;
  }

  const merged = mergeById(existing, incomingAll);

  // Write merged back to master using your existing writer (keeps headers consistent)
  writeEventsCsv("data/ProPath-MiniTour2026-MasterEvents.csv", merged as any);

  console.log(`✓ MiniTour master updated: ${merged.length} total rows`);
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

  // ✅ merge staging mini tour updates into the real master file your app reads
  mergeIncomingMiniTourMaster();

  console.log(`\nComplete. ${successCount}/${sources.length} sources succeeded.`);
}

main().catch((err) => {
  console.error("\nScraper failed:");
  console.error(err);
  process.exit(1);
});
