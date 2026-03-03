import * as cheerio from "cheerio";
import { coerceISO } from "../lib/normalize.js";

/**
 * Matches your existing ProPathEvent shape used in run-all.ts
 */
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
  url: string;
  defaults?: Record<string, string>;
  tableSelector?: string; // optional override
};

function clean(s: string) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function slug(s: string) {
  return clean(s)
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

/**
 * BlueGolf date cell examples:
 *  - "Mar 25-27"
 *  - "Apr 8-10"
 *  - sometimes "May 5" (pro-am)
 *
 * We infer year with rollover:
 * - Default to current year
 * - If we are late in the year (Nov/Dec) and the event month is earlier (Jan/Feb/Mar),
 *   treat it as next year.
 */
function parseBlueGolfDateRange(dateText: string): { start: string | null; end: string | null } {
  const raw = clean(dateText);
  if (!raw) return { start: null, end: null };

  const m = raw.match(/^([A-Za-z]{3,})\s+(\d{1,2})(?:\s*[-–—]\s*(\d{1,2}))?$/);
  if (!m) return { start: null, end: null };

  const monName = m[1];
  const d1 = Number(m[2]);
  const d2 = m[3] ? Number(m[3]) : null;

  const month = monthToNumber(monName);
  if (!month || !d1) return { start: null, end: null };

  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth() + 1;

  // Rollover rule: if it's Nov/Dec and event month is Jan/Feb/Mar, assume next year
  let year = currentYear;
  if (currentMonth >= 11 && month <= 3) year = currentYear + 1;

  const start = coerceISO(`${year}-${pad2(month)}-${pad2(d1)}`);
  const end = d2 ? coerceISO(`${year}-${pad2(month)}-${pad2(d2)}`) : null;

  return { start, end };
}

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function monthToNumber(name: string) {
  const k = name.slice(0, 3).toLowerCase();
  const map: Record<string, number> = {
    jan: 1,
    feb: 2,
    mar: 3,
    apr: 4,
    may: 5,
    jun: 6,
    jul: 7,
    aug: 8,
    sep: 9,
    oct: 10,
    nov: 11,
    dec: 12
  };
  return map[k] ?? 0;
}

/**
 * Pull city + state from strings like:
 * "Heritage Oaks GC · Brunswick, GA"
 */
function parseCityStateFromTournamentText(text: string): { city?: string; state_country?: string } {
  const t = clean(text);

  // Prefer dot separator pattern
  const dot = t.match(/·\s*([^,]+),\s*([A-Z]{2})\b/);
  if (dot) {
    const city = clean(dot[1]);
    const st = clean(dot[2]);
    return { city, state_country: st };
  }

  // Fallback: last "City, ST"
  const fallback = t.match(/([^,]+),\s*([A-Z]{2})\b/);
  if (fallback) {
    return { city: clean(fallback[1]), state_country: clean(fallback[2]) };
  }

  return {};
}

/**
 * Find the event details URL in the tournament cell.
 * BlueGolf event pages look like: /.../event/gprotour252/index.htm
 */
function findEventUrl($cell: cheerio.Cheerio<cheerio.Element>, baseUrl: string): string | undefined {
  const links = $cell.find("a[href]").toArray();
  for (const a of links) {
    const href = (a.attribs?.href || "").trim();
    if (!href) continue;
    if (/\/event\/[^/]+\/index\.htm/i.test(href) || /\/event\/[^/]+\/index\.html/i.test(href)) {
      return new URL(href, baseUrl).toString();
    }
  }
  // If none matched, fall back to first link
  const first = $cell.find("a[href]").first().attr("href");
  if (first) return new URL(first, baseUrl).toString();
  return undefined;
}

/**
 * Find signup/register URL in the register cell.
 * Usually contains /util/gosecure.htm?... secure=...
 */
function findSignupUrl($cell: cheerio.Cheerio<cheerio.Element>, baseUrl: string): string | undefined {
  const links = $cell.find("a[href]").toArray();
  for (const a of links) {
    const href = (a.attribs?.href || "").trim();
    if (!href) continue;
    if (/gosecure/i.test(href)) {
      return new URL(href, baseUrl).toString();
    }
  }
  return undefined;
}

/**
 * Title extraction:
 * Prefer <b>/<strong> inside the tournament cell, else first link text, else cell text (trimmed).
 */
function extractTitle($tournamentCell: cheerio.Cheerio<cheerio.Element>): string {
  const strong = clean($tournamentCell.find("b,strong").first().text());
  if (strong) return strong;

  const linkText = clean($tournamentCell.find("a").first().text());
  if (linkText) return linkText;

  // last fallback – but keep it short
  return clean($tournamentCell.clone().children().remove().end().text()) || clean($tournamentCell.text());
}

function makeId(e: ProPathEvent): string {
  return [
    slug(e.tour || "tour"),
    e.start || "tbd",
    slug(e.title || "event"),
    slug(e.city || "")
  ].filter(Boolean).join("-");
}

/**
 * Main runner
 */
export async function runHtmlTable(source: Source): Promise<ProPathEvent[]> {
  const res = await fetch(source.url, { headers: { "user-agent": "propath-bot" } });
  if (!res.ok) throw new Error(`html_table fetch failed: ${res.status} ${res.statusText}`);

  const html = await res.text();
  const $ = cheerio.load(html);

  const table = source.tableSelector ? $(source.tableSelector).first() : $("table").first();
  if (!table.length) throw new Error("html_table: no table found");

  const rows: ProPathEvent[] = [];

  table.find("tr").each((i, tr) => {
    const $tr = $(tr);
    const tds = $tr.find("td").toArray();
    if (tds.length < 2) return;

    const dateCellText = clean($(tds[0]).text());
    const $tournamentCell = $(tds[1]);
    const $registerCell = tds[2] ? $(tds[2]) : $("<div></div>");

    // Skip header-ish rows
    if (i === 0 && /date/i.test(dateCellText)) return;

    // ✅ Rule #1: must look like "Mon dd" etc, otherwise skip (prevents Register/Closes junk)
    const { start, end } = parseBlueGolfDateRange(dateCellText);
    if (!start) return;

    const title = extractTitle($tournamentCell);
    if (!title) return;
    if (/^register$/i.test(title)) return;

    const tourUrl = findEventUrl($tournamentCell, source.url) || source.url;
    const signupUrl = findSignupUrl($registerCell, source.url);

    const locText = clean($tournamentCell.text());
    const { city, state_country } = parseCityStateFromTournamentText(locText);

    const event: ProPathEvent = {
      ...(source.defaults ?? {}),
      title,
      start,
      end,
      city: city || "",
      state_country: state_country ? `${city ? city + ", " : ""}${state_country}` : "",
      tourUrl,
      signupUrl: signupUrl || ""
    };

    // Stable-ish id so merge overwrites instead of duplicating
    event.id = makeId(event);

    rows.push(event);
  });

  if (!rows.length) throw new Error("html_table: 0 events parsed");
  return rows;
}