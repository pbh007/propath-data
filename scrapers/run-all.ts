// scrapers/run-all.ts
import fs from "fs";
import path from "path";
import Papa from "papaparse";

import { writeEventsCsv } from "./lib/writeCsv.js";
import { coerceISO } from "./lib/normalize.js";
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

// Safety guard: if a scrape returns too few "valid" rows for a tour,
// we assume it failed and DO NOT overwrite that tour’s upcoming block.
const MIN_VALID_ROWS_TO_TRUST_PER_TOUR = 5;

// Optional: treat "Event Info" placeholder rows as junk and never keep them
const DROP_EVENT_INFO_PLACEHOLDERS = true;

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

function isEventInfoPlaceholder(e: ProPathEvent): boolean {
  const t = (e.title || "").trim().toLowerCase();
  return t === "event info";
}

function normalizeRow(r: any): ProPathEvent {
  const startISO = coerceISO(r.start ?? null);
  const endISO = coerceISO(r.end ?? null);
  const mondayISO = coerceISO(r.mondayDate ?? null);

  const out: ProPathEvent = {
    id: (r.id ?? "").toString().trim() || undefined,
    tour: (r.tour ?? "").toString().trim() || undefined,
    gender: (r.gender ?? "").toString().trim() || undefined,
    type: (r.type ?? "").toString().trim() || undefined,
    stage: (r.stage ?? "").toString().trim() || undefined,
    title: (r.title ?? "").toString().trim() || undefined,
    start: startISO,
    end: endISO ?? startISO,
    city: (r.city ?? "").toString().trim() || undefined,
    state_country: (r.state_country ?? "").toString().trim() || undefined,
    tourUrl: (r.tourUrl ?? "").toString().trim() || undefined,
    signupUrl: (r.signupUrl ?? "").toString().trim() || undefined,
    mondayUrl: (r.mondayUrl ?? "").toString().trim() || undefined,
    mondayDate: mondayISO,
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
  if (!s) return true; // unknown date => treat as upcoming
  return s >= todayISO();
}

function keyTour(t?: string) {
  return (t || "").trim().toLowerCase();
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

function countValidRows(rows: ProPathEvent[]) {
  // "valid" = has tour + title + start ISO
  return rows.filter((r) => {
    const tourOk = !!keyTour(r.tour);
    const titleOk = !!(r.title || "").trim();
    const startOk = !!(r.start || "").trim();
    if (!tourOk || !titleOk || !startOk) return false;
    if (DROP_EVENT_INFO_PLACEHOLDERS && isEventInfoPlaceholder(r)) return false;
    return true;
  }).length;
}

/**
 * Replace UPCOMING rows for each tour found in incoming.
 * - Keeps PAST rows for that tour.
 * - Leaves all other tours untouched.
 * - Safety: only overwrites a tour if incoming has >= MIN_VALID_ROWS_TO_TRUST_PER_TOUR "valid" rows.
 */
function mergeMasterWithIncoming(master: ProPathEvent[], incoming: ProPathEvent[]) {
  const incomingByTour = new Map<string, ProPathEvent[]>();

  for (const r of incoming) {
    const tourKey = keyTour(r.tour);
    if (!tourKey) continue;

    if (DROP_EVENT_INFO_PLACEHOLDERS && isEventInfoPlaceholder(r)) continue;

    if (!incomingByTour.has(tourKey)) incomingByTour.set(tourKey, []);
    incomingByTour.get(tourKey)!.push(r);
  }

  if (incomingByTour.size === 0) return master;

  let out = DROP_EVENT_INFO_PLACEHOLDERS
    ? master.filter((m) => !isEventInfoPlaceholder(m))
    : [...master];

  for (const [tourKey, rows] of incomingByTour.entries()) {
    const validCount = countValidRows(rows);

    // Safety: skip if looks like a failed scrape
    if (validCount < MIN_VALID_ROWS_TO_TRUST_PER_TOUR) {
      console.log(
        `Skipping overwrite for tour "${tourKey}" (only ${validCount} valid incoming rows; min=${MIN_VALID_ROWS_TO_TRUST_PER_TOUR}).`
      );
      continue;
    }

    // Remove UPCOMING master rows for this tourKey (keep past)
    out = out.filter((m) => {
      const sameTour = keyTour(m.tour) === tourKey;
      if (!sameTour) return true;
      return !isUpcoming(m.start);
    });

    // Add incoming (assumed authoritative upcoming)
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

  if (!events.length) {
    console.log(`No rows for source "${s.id}"`);
    return false;
  }

  // Normalize/clean before writing incoming
  const cleaned = events
    .map((e: any) => normalizeRow(e))
    .filter((e) => !DROP_EVENT_INFO_PLACEHOLDERS || !isEventInfoPlaceholder(e));

  if (!cleaned.length) {
    console.log(`All rows filtered for source "${s.id}"`);
    return false;
  }

  writeEventsCsv(s.output, cleaned as any);
  console.log(`Wrote ${cleaned.length} rows -> ${s.output}`);
  return true;
}

/* ----------------------------
   Merge step
---------------------------- */
function mergeIncomingIntoMiniMaster() {
  const existing = parseCsvToEvents(safeRead(MASTER_MINI));

  const incomingFiles = fs.existsSync(DATA_DIR)
    ? fs
        .readdirSync(DATA_DIR)
        .filter((f) => f.startsWith("_incoming_") && f.endsWith(".csv"))
    : [];

  const incomingAll: ProPathEvent[] = [];

  for (const f of incomingFiles) {
    const full = path.join(DATA_DIR, f);
    const csv = safeRead(full);
    const events = parseCsvToEvents(csv);
    incomingAll.push(...events);
  }

  if (!incomingAll.length) {
    console.log("No incoming rows found — merge skipped.");
    return;
  }

  const merged = mergeMasterWithIncoming(existing, incomingAll);
  const finalRows = dedupeAndSort(merged);

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