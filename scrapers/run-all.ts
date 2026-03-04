// scrapers/run-all.ts
import fs from "fs";
import path from "path";
import Papa from "papaparse";

import { writeEventsCsv } from "./lib/writeCsv.js";
import { coerceISO } from "./lib/normalize.js";
import { runJsonApi } from "./connectors/json_api.js";
import { runHtmlTable } from "./connectors/html_table.js";
import { runHtmlBlocks } from "./connectors/html_blocks.js";
import { runHtmlCards } from "./connectors/html_cards.js";

/**
 * IMPORTANT:
 * Your scrapers pipeline has strict TS settings, so we allow nulls in the event shape.
 * We normalize aggressively to keep stored CSV values consistent.
 */
type ProPathEvent = {
  id?: string | null;
  tour?: string | null;
  gender?: string | null;
  type?: string | null;
  stage?: string | null;
  title?: string | null;

  start?: string | null;
  end?: string | null;

  city?: string | null;
  state_country?: string | null;

  tourUrl?: string | null;
  signupUrl?: string | null;

  mondayUrl?: string | null;
  mondayDate?: string | null;
};

type MergeMode = "replace_upcoming" | "patch_only" | "append_only";

type Source = {
  id: string;
  name: string;
  connector: "json_api" | "html_table" | "html_blocks" | "html_cards";
  url: string;
  output: string;
  defaults?: Record<string, string>;
  tableSelector?: string;
  cardSelector?: string;

  minValidRows?: number;
  mergeMode?: MergeMode;

  // Default policy: BlueGolf is link-only unless explicitly "allow"
  policy?: "allow" | "deny" | "link_only";
};

/* ----------------------------
   Config
---------------------------- */
const DATA_DIR = path.resolve("data");
const MASTER_MINI = path.join(DATA_DIR, "ProPath-MiniTour2026-MasterEvents.csv");

const MIN_VALID_ROWS_TO_TRUST_PER_TOUR = 5;

// runtime maps from sources.json
const TOUR_MIN_ROWS = new Map<string, number>();
const TOUR_MERGE_MODE = new Map<string, MergeMode>();

/* ----------------------------
   Utilities
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
  const tour = (e.tour ?? "unknown-tour") || "unknown-tour";
  const start = ((e.start ?? "tbd") || "tbd").slice(0, 10);
  const title = (e.title ?? "event") || "event";
  const city = (e.city ?? "") || "";

  return [slug(tour), start, slug(title), slug(city)].filter(Boolean).join("-");
}

function keyTour(t?: string | null) {
  return (t || "").trim().toLowerCase();
}

function nonEmpty(v?: string | null) {
  const s = (v ?? "").toString().trim();
  return s ? s : "";
}

function normalizeRow(r: any): ProPathEvent {
  const startISO = coerceISO(r.start ?? null);
  const endISO = coerceISO(r.end ?? null) ?? startISO;
  const mondayISO = coerceISO(r.mondayDate ?? null);

  const out: ProPathEvent = {
    id: nonEmpty(r.id) || null,
    tour: nonEmpty(r.tour) || null,
    gender: nonEmpty(r.gender) || null,
    type: nonEmpty(r.type) || null,
    stage: nonEmpty(r.stage) || null,
    title: nonEmpty(r.title) || null,

    start: startISO ?? null,
    end: endISO ?? null,

    city: nonEmpty(r.city) || null,
    state_country: nonEmpty(r.state_country) || null,

    tourUrl: nonEmpty(r.tourUrl) || null,
    signupUrl: nonEmpty(r.signupUrl) || null,

    mondayUrl: nonEmpty(r.mondayUrl) || null,
    mondayDate: mondayISO ?? null,
  };

  if (!out.id) out.id = makeStableId(out);
  return out;
}

/* ----------------------------
   Placeholder Detection (SAFE)
---------------------------- */
function isEventInfoPlaceholder(e: ProPathEvent): boolean {
  const title = (e.title || "").trim().toLowerCase();
  if (title !== "event info") return false;

  const type = (e.type || "").trim().toLowerCase();
  const stage = (e.stage || "").trim().toLowerCase();
  const signup = (e.signupUrl || "").trim().toLowerCase();

  const typeLooksPlaceholder = type === "" || type === "training division";
  const stageEmpty = stage === "";
  const hasEventId = signup.includes("event_id=");

  return typeLooksPlaceholder && stageEmpty && hasEventId;
}

/* ----------------------------
   Parsing
---------------------------- */
function parseCsvToEvents(csvText: string): ProPathEvent[] {
  if (!csvText.trim()) return [];
  const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: "greedy" });
  const rows = (parsed.data as any[]).filter(Boolean);
  return rows.map(normalizeRow);
}

/* ----------------------------
   Date helpers
---------------------------- */
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

/* ----------------------------
   Validation
---------------------------- */
function countValidRows(rows: ProPathEvent[]) {
  return rows.filter((r) => {
    if (!r.tour || !r.title || !r.start) return false;
    if (isEventInfoPlaceholder(r)) return false;
    return true;
  }).length;
}

/* ----------------------------
   Dedupe + Sort
---------------------------- */
function dedupeAndSort(rows: ProPathEvent[]) {
  const map = new Map<string, ProPathEvent>();

  for (const r of rows) {
    const id = (r.id || makeStableId(r)).toString().trim();
    map.set(id, { ...r, id });
  }

  const out = Array.from(map.values());

  out.sort((a, b) => {
    const as = ((a.start || "9999-12-31") as string).slice(0, 10);
    const bs = ((b.start || "9999-12-31") as string).slice(0, 10);
    return as.localeCompare(bs);
  });

  return out;
}

/* ----------------------------
   Patch helpers
---------------------------- */
function patchEvent(existing: ProPathEvent, incoming: ProPathEvent): ProPathEvent {
  const pick = (a?: string | null, b?: string | null) => {
    const bb = nonEmpty(b);
    if (bb) return bb;
    const aa = nonEmpty(a);
    return aa ? aa : null;
  };

  return {
    ...existing,
    id: existing.id || incoming.id || makeStableId(existing),

    tour: pick(existing.tour, incoming.tour),
    title: pick(existing.title, incoming.title),
    type: pick(existing.type, incoming.type),
    stage: pick(existing.stage, incoming.stage),
    gender: pick(existing.gender, incoming.gender),

    start: pick(existing.start, incoming.start),
    end: pick(existing.end, incoming.end),

    city: pick(existing.city, incoming.city),
    state_country: pick(existing.state_country, incoming.state_country),

    // Patch URLs (never erase)
    signupUrl: pick(existing.signupUrl, incoming.signupUrl),
    tourUrl: pick(existing.tourUrl, incoming.tourUrl),

    mondayUrl: pick(existing.mondayUrl, incoming.mondayUrl),
    mondayDate: pick(existing.mondayDate, incoming.mondayDate),
  };
}

function matchKey(e: ProPathEvent) {
  // Stable match key independent of id changes:
  // tour + start date + normalized title
  const t = keyTour(e.tour);
  const s = (e.start || "").slice(0, 10);
  const title = (e.title || "").trim().toLowerCase();
  return `${t}|${s}|${title}`;
}

/* ----------------------------
   Merge Logic (SAFE + MODES)
---------------------------- */
function mergeMasterWithIncoming(master: ProPathEvent[], incoming: ProPathEvent[]) {
  const incomingByTour = new Map<string, ProPathEvent[]>();

  for (const r of incoming) {
    if (isEventInfoPlaceholder(r)) continue;

    const tourKey = keyTour(r.tour);
    if (!tourKey) continue;

    if (!incomingByTour.has(tourKey)) incomingByTour.set(tourKey, []);
    incomingByTour.get(tourKey)!.push(r);
  }

  if (incomingByTour.size === 0) return master;

  let out = master.filter((m) => !isEventInfoPlaceholder(m));

  for (const [tourKey, rows] of incomingByTour.entries()) {
    const validCount = countValidRows(rows);
    const minRows = TOUR_MIN_ROWS.get(tourKey) ?? MIN_VALID_ROWS_TO_TRUST_PER_TOUR;
    const mode = TOUR_MERGE_MODE.get(tourKey) ?? "replace_upcoming";

    if (validCount < minRows) {
      console.log(
        `Skipping merge for tour "${tourKey}" (only ${validCount} valid rows; min=${minRows}; mode=${mode})`
      );
      continue;
    }

    if (mode === "append_only") {
      out.push(...rows);
      continue;
    }

    if (mode === "patch_only") {
      // Patch existing matching events; append if not found.
      const indexByKey = new Map<string, number>();

      for (let i = 0; i < out.length; i++) {
        const e = out[i];
        if (keyTour(e.tour) !== tourKey) continue;
        indexByKey.set(matchKey(e), i);
      }

      for (const inc of rows) {
        const k = matchKey(inc);
        const idx = indexByKey.get(k);

        if (typeof idx === "number") {
          out[idx] = patchEvent(out[idx], inc);
        } else {
          out.push(inc);
        }
      }

      continue;
    }

    // Default: replace_upcoming
    // Remove upcoming events for that tour, keep past events
    out = out.filter((m) => {
      const sameTour = keyTour(m.tour) === tourKey;
      if (!sameTour) return true;
      return !isUpcoming(m.start);
    });

    out.push(...rows);
  }

  return out;
}

/* ----------------------------
   Run Sources
---------------------------- */
async function runSource(s: Source): Promise<boolean> {
  if (!s.url || s.url.includes("PASTE_URL_HERE")) return false;

  // Safety: treat BlueGolf as link-only by default
  try {
    const host = new URL(s.url).hostname.toLowerCase();
    if (host.includes("bluegolf.com") && s.policy !== "allow") {
      console.log(`Skipping source "${s.id}" (BlueGolf link-only policy)`);
      return false;
    }
  } catch {
    // ignore URL parsing failures
  }

  let events: ProPathEvent[] = [];

  if (s.connector === "json_api") {
    events = await runJsonApi(s as any);
  } else if (s.connector === "html_table") {
    events = await runHtmlTable(s as any);
  } else if (s.connector === "html_blocks") {
    events = await runHtmlBlocks(s as any);
  } else if (s.connector === "html_cards") {
    events = await runHtmlCards(s as any);
  }

  if (!events.length) {
    console.log(`No rows for source "${s.id}"`);
    return false;
  }

  const cleaned = events.map((e: any) => normalizeRow(e)).filter((e) => !isEventInfoPlaceholder(e));

  if (!cleaned.length) {
    console.log(`All rows filtered for source "${s.id}"`);
    return false;
  }

  writeEventsCsv(s.output, cleaned as any);
  console.log(`Wrote ${cleaned.length} rows -> ${s.output}`);
  return true;
}

/* ----------------------------
   Merge Step
---------------------------- */
function mergeIncomingIntoMiniMaster() {
  const existing = parseCsvToEvents(safeRead(MASTER_MINI));

  const incomingFiles = fs.existsSync(DATA_DIR)
    ? fs.readdirSync(DATA_DIR).filter((f) => f.startsWith("_incoming_") && f.endsWith(".csv"))
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

  TOUR_MIN_ROWS.clear();
  TOUR_MERGE_MODE.clear();

  // Build per-tour override maps from sources.json
  for (const s of sources) {
    const tourName = (s.defaults?.tour || "").toString().trim();
    if (!tourName) continue;

    const tk = keyTour(tourName);
    if (!tk) continue;

    if (typeof s.minValidRows === "number" && Number.isFinite(s.minValidRows)) {
      TOUR_MIN_ROWS.set(tk, s.minValidRows);
    }

    const next = (s.mergeMode || "replace_upcoming") as MergeMode;

    // Prefer safer mode if multiple sources exist for same tour:
    // patch_only (best) > replace_upcoming > append_only
    const current = TOUR_MERGE_MODE.get(tk);
    const rank = (m: MergeMode) => (m === "patch_only" ? 3 : m === "replace_upcoming" ? 2 : 1);

    if (!current || rank(next) > rank(current)) {
      TOUR_MERGE_MODE.set(tk, next);
    }
  }

  for (const s of sources) {
    await runSource(s);
  }

  mergeIncomingIntoMiniMaster();
}

main().catch((err) => {
  console.error("Scraper failed:", err);
  process.exit(1);
});