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
  return [
    slug(e.tour || "unknown-tour"),
    e.start || "tbd",
    slug(e.title || "event"),
    slug(e.city || ""),
  ]
    .filter(Boolean)
    .join("-");
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

function todayISO(): string {
  // Use LOCAL date for “past vs upcoming” split
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function isUpcoming(e: ProPathEvent): boolean {
  const t = todayISO();
  const s = (e.start || "").toString().slice(0, 10);
  if (!s) return true; // treat unknown start as upcoming so it gets cleaned out
  return s >= t;
}

function mergeDedupeById(events: ProPathEvent[]): ProPathEvent[] {
  const map = new Map<string, ProPathEvent>();
  for (const e of events) {
    const id = e.id || makeStableId(e);
    // last write wins (fine after we’ve removed upcoming for a tour)
    map.set(id, { ...e, id });
  }

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

/**
 * Merge rule:
 * - Keep ALL past events forever
 * - For each tour that appears in incoming:
 *     replace UPCOMING rows for that tour with the incoming rows
 *   (this deletes junk future rows)
 * - If incoming is empty, do nothing
 */
function mergeIncomingMiniTourMaster() {
  const dataDir = path.resolve("data");
  const masterPath = path.join(dataDir, "ProPath-MiniTour2026-MasterEvents.csv");

  const existingCsv = safeRead(masterPath);
  const existing = parseCsvToEvents(existingCsv);

  const incomingFiles = fs.existsSync(dataDir)
    ? fs
        .readdirSync(dataDir)
        .filter((f) => f.startsWith("_incoming_") && f.endsWith(".csv"))
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

  // Group incoming by tour (so each tour can be authoritative for UPCOMING)
  const incomingByTour = new Map<string, ProPathEvent[]>();
  for (const e of incomingAll) {
    const tour = (e.tour || "").trim();
    if (!tour) continue;
    if (!incomingByTour.has(tour)) incomingByTour.set(tour, []);
    incomingByTour.get(tour)!.push(e);
  }

  let merged = existing;

  for (const [tour, incomingForTour] of incomingByTour.entries()) {
    if (!incomingForTour.length) continue;

    // Remove UPCOMING rows in master for that tour (wipes junk, keeps past)
    merged = merged.filter((e) => {
      const sameTour = (e.tour || "").trim() === tour;
      if (!sameTour) return true;
      // keep past rows; remove upcoming rows
      return !isUpcoming(e);
    });

    // Add the new incoming rows for that tour
    merged = merged.concat(incomingForTour);
  }

  // Dedupe and sort
  const finalRows = mergeDedupeById(merged);

  writeEventsCsv("data/ProPath-MiniTour2026-MasterEvents.csv", finalRows as any);
  console.log(`✓ MiniTour master updated: ${finalRows.length} total rows`);
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

  mergeIncomingMiniTourMaster();

  console.log(`\nComplete. ${successCount}/${sources.length} sources succeeded.`);
}

main().catch((err) => {
  console.error("\nScraper failed:");
  console.error(err);
  process.exit(1);
});