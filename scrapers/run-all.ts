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

function replaceUpcomingByTour(masterRows: ProPathEvent[], incomingRows: ProPathEvent[]) {
  const incomingByTour = new Map<string, ProPathEvent[]>();

  for (const r of incomingRows) {
    const tour = String(r.tour || "").trim();
    if (!tour) continue;
    if (!incomingByTour.has(tour)) incomingByTour.set(tour, []);
    incomingByTour.get(tour)!.push(r);
  }

  if (incomingByTour.size === 0) return masterRows;

  let out = masterRows;

  for (const [tour, rows] of incomingByTour.entries()) {
    if (!rows.length) continue;

    // Remove UPCOMING rows for that tour
    out = out.filter((m) => {
      const sameTour = String(m.tour || "").trim() === tour;
      if (!sameTour) return true;
      return !isUpcoming(m.start);
    });

    // Add new upcoming rows
    out = out.concat(rows);
  }

  return out;
}

function dedupeAndSort(rows: ProPathEvent[]) {
  const map = new Map<string, ProPathEvent>();

  for (const r of rows) {
    const id = r.id || makeStableId(r);
    map.set(id, { ...r, id });
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
  if (!isValidUrl(s.url)) return false;

  let events: ProPathEvent[] = [];

  if (s.connector === "json_api") {
    events = await runJsonApi(s as any);
  } else if (s.connector === "html_table") {
    events = await runHtmlTable(s as any);
  } else if (s.connector === "html_blocks") {
    events = await runHtmlBlocks(s as any);
  }

  if (!events.length) return false;

  writeEventsCsv(s.output, events as any);
  return true;
}

function mergeIncomingMiniTourMaster() {
  const dataDir = path.resolve("data");
  const masterPath = path.join(dataDir, "ProPath-MiniTour2026-MasterEvents.csv");

  const existingCsv = safeRead(masterPath);
  const existing = parseCsvToEvents(existingCsv);

  const incomingFiles = fs.existsSync(dataDir)
    ? fs.readdirSync(dataDir).filter((f) => f.startsWith("_incoming_") && f.endsWith(".csv"))
    : [];

  const incomingAll: ProPathEvent[] = [];

  for (const f of incomingFiles) {
    const csv = safeRead(path.join(dataDir, f));
    const events = parseCsvToEvents(csv);
    incomingAll.push(...events);
  }

  if (!incomingAll.length) {
    console.log("No incoming files — merge skipped.");
    return;
  }

  const replaced = replaceUpcomingByTour(existing, incomingAll);
  const finalRows = dedupeAndSort(replaced);

  writeEventsCsv("data/ProPath-MiniTour2026-MasterEvents.csv", finalRows as any);
  console.log(`MiniTour master updated: ${finalRows.length} rows`);
}

async function main() {
  const sourcesPath = path.resolve("data", "sources.json");
  const sources = JSON.parse(fs.readFileSync(sourcesPath, "utf8")) as Source[];

  for (const s of sources) {
    await runSource(s);
  }

  mergeIncomingMiniTourMaster();
}

main().catch((err) => {
  console.error("Scraper failed:", err);
  process.exit(1);
});