// scrapers/run-all.ts
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
  output: string; // should be data/_incoming_<something>.csv
  defaults?: Record<string, string>;
  tableSelector?: string;
};

/* ----------------------------
   Config
---------------------------- */
const DATA_DIR = path.resolve("data");
const MASTER_MINI = path.join(DATA_DIR, "ProPath-MiniTour2026-MasterEvents.csv");

// Safety guard: if a scrape returns too few rows for a tour,
// we assume it failed and DO NOT overwrite that tour’s upcoming block.
const MIN_ROWS_TO_TRUST_PER_TOUR = 5;

// Junk filters (these are the “bad rows” you don’t want living in master)
const JUNK_TITLE_RE = /^\s*event\s*info\s*$/i;
const JUNK_ID_RE = /-event-info-/i;

/* ----------------------------
   Helpers
---------------------------- */
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
    (e.start || "tbd").slice(0, 10),
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
  const rows = (parsed.data as any[]).filter(Boolean);
  return rows.map(normalizeRow);
}

function todayISO(): string {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function isUpcoming(start?: string | null): boolean {
  const s = (start || "").slice(0, 10);
  if (!s) return true;
  return s >= todayISO();
}

function keyTour(t?: string) {
  return (t || "").trim().toLowerCase();
}

/**
 * Decide if a row is junk / unsafe to keep.
 * This will clean BOTH:
 * - existing master (fixes current junk next run)
 * - incoming files (prevents reintroducing junk)
 */
function isJunkRow(e: ProPathEvent): boolean {
  const id = (e.id || "").trim();
  const title = (e.title || "").trim();
  const tour = (e.tour || "").trim();
  const start = (e.start || "").toString().trim();

  // Required fields (if missing, treat as junk)
  if (!tour) return true;
  if (!title) return true;
  if (!start) return true;

  // Explicit junk patterns you flagged
  if (JUNK_TITLE_RE.test(title)) return true;
  if (JUNK_ID_RE.test(id)) return true;

  // Common garbage
  if (/^register$/i.test(title)) return true;

  return false;
}

function cleanEvents(rows: ProPathEvent[]): ProPathEvent[] {
  return rows
    .map((r) => {
      // normalize “nullish” strings
      const start = (r.start ?? "").toString().trim();
      const end = (r.end ?? "").toString().trim();
      return {
        ...r,
        start: start ? start : null,
        end: end ? end : null,
      };
    })
    .filter((r) => !isJunkRow(r));
}

function dedupeAndSort(rows: ProPathEvent[]) {
  const map = new Map<string, ProPathEvent>();

  for (const r of rows) {
    const id = (r.id || makeStableId(r)).trim();
    map.set(id, { ...r, id });
  }

  const out = Array.from(map.values());

  out.sort((a, b) => {
    const as = (a.start || "9999-12-31").slice(0, 10);
    const bs = (b.start || "9999-12-31").slice(0, 10);
    return as.localeCompare(bs);
  });

  return out;
}

/**
 * Replace UPCOMING rows for each tour found in incoming.
 * - Keeps PAST rows for that tour.
 * - Leaves all other tours untouched.
 * - Safety:
 *    - incoming must have >= MIN_ROWS_TO_TRUST_PER_TOUR rows
 *    - AND must include at least 1 UPCOMING valid row
 */
function mergeMasterWithIncoming(master: ProPathEvent[], incoming: ProPathEvent[]) {
  const incomingByTour = new Map<string, ProPathEvent[]>();

  for (const r of incoming) {
    const tourKey = keyTour(r.tour);
    if (!tourKey) continue;
    if (!incomingByTour.has(tourKey)) incomingByTour.set(tourKey, []);
    incomingByTour.get(tourKey)!.push(r);
  }

  if (incomingByTour.size === 0) return master;

  let out = [...master];

  for (const [tourKey, rawRows] of incomingByTour.entries()) {
    const rows = cleanEvents(rawRows);
    const count = rows.length;

    const upcomingCount = rows.filter((r) => isUpcoming(r.start)).length;

    // Safety: skip if looks like a failed scrape
    if (count < MIN_ROWS_TO_TRUST_PER_TOUR) {
      console.log(
        `Skipping overwrite for tour "${tourKey}" (only ${count} clean incoming rows; min=${MIN_ROWS_TO_TRUST_PER_TOUR}).`
      );
      continue;
    }

    // Safety: do not overwrite if there are ZERO upcoming rows
    if (upcomingCount === 0) {
      console.log(
        `Skipping overwrite for tour "${tourKey}" (0 upcoming rows in incoming; would risk deleting upcoming master rows).`
      );
      continue;
    }

    // Remove UPCOMING master rows for this tourKey (keep past)
    out = out.filter((m) => {
      const sameTour = keyTour(m.tour) === tourKey;
      if (!sameTour) return true;
      return !isUpcoming(m.start); // keep past
    });

    // Add incoming (authoritative upcoming)
    out.push(...rows);
  }

  return out;
}

/* ----------------------------
   Running sources
---------------------------- */
async function runSource(s: Source): Promise<boolean> {
  if (!s.url || s.url.includes("PASTE_URL_HERE")) return false;

  let events: ProPathEvent[] = [];

  if (s.connector === "json_api") {
    events = await runJsonApi(s as any);
  } else if (s.connector === "html_table") {
    events = await runHtmlTable(s as any);
  } else if (s.connector === "html_blocks") {
    events = await runHtmlBlocks(s as any);
  }

  // Clean before writing incoming file (prevents junk from being produced)
  events = cleanEvents(events);

  if (!events.length) {
    console.log(`No clean rows for source "${s.id}"`);
    return false;
  }

  writeEventsCsv(s.output, events as any);
  console.log(`Wrote ${events.length} rows -> ${s.output}`);
  return true;
}

/* ----------------------------
   Merge step
---------------------------- */
function mergeIncomingIntoMiniMaster() {
  // Clean existing master too (this is what fixes your CURRENT junk rows)
  const existing = cleanEvents(parseCsvToEvents(safeRead(MASTER_MINI)));

  const incomingFiles = fs.existsSync(DATA_DIR)
    ? fs
        .readdirSync(DATA_DIR)
        .filter((f) => f.startsWith("_incoming_") && f.endsWith(".csv"))
    : [];

  const incomingAll: ProPathEvent[] = [];

  for (const f of incomingFiles) {
    const full = path.join(DATA_DIR, f);
    const csv = safeRead(full);
    const events = cleanEvents(parseCsvToEvents(csv));
    incomingAll.push(...events);
  }

  if (!incomingAll.length) {
    console.log("No incoming rows found — merge skipped.");
    return;
  }

  const merged = mergeMasterWithIncoming(existing, incomingAll);
  const finalRows = dedupeAndSort(cleanEvents(merged));

  writeEventsCsv("data/ProPath-MiniTour2026-MasterEvents.csv", finalRows as any);
  console.log(`✅ Master updated: ${finalRows.length} rows`);
}

/* ----------------------------
   Main
---------------------------- */
async function main() {
  const sourcesPath = path.resolve("data", "sources.json");
  const sources = JSON.parse(fs.readFileSync(sourcesPath, "utf8")) as Source[];

  for (const s of sources) {
    await runSource(s);
  }

  mergeIncomingIntoMiniMaster();
}

main().catch((err) => {
  console.error("Scraper failed:", err);
  process.exit(1);
});