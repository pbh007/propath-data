import * as cheerio from "cheerio";
import { coerceISO } from "../lib/normalize.js";

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
  tableSelector?: string;
};

/** Month name to number helper */
const MON = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
  jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12
} as const;

function normalizeSpace(s: string) {
  return (s || "").replace(/\s+/g, " ").trim();
}

/** Detects cells like "Mar 25-27" / "Aug 11-13" / "Apr 6" etc */
function looksLikeBlueGolfDateCell(s: string) {
  const t = normalizeSpace(s);
  if (!t) return false;
  // matches "Mar 25-27" or "Mar 25 – 27" or "Mar 25" etc
  return /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t);
}

/** Pulls year from page (dropdown like "2026 GPro" or any '20xx' present) */
function extractYearFromPage($: any, html: string): number {
  const txt = normalizeSpace($("body").text() || "");
  const m1 = txt.match(/\b(20\d{2})\b/);
  if (m1) return Number(m1[1]);

  const m2 = (html || "").match(/\b(20\d{2})\b/);
  if (m2) return Number(m2[1]);

  // fallback to current year
  return new Date().getFullYear();
}

function parseBlueGolfMonthDay(monthDay: string, year: number): string | null {
  const t = normalizeSpace(monthDay);
  const m = t.match(/^([A-Za-z]{3})\s+(\d{1,2})$/);
  if (!m) return coerceISO(`${t} ${year}`); // try general
  const mon = m[1].toLowerCase() as keyof typeof MON;
  const day = Number(m[2]);
  const mm = MON[mon];
  if (!mm || !day) return null;
  const iso = `${year}-${String(mm).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
  return coerceISO(iso);
}

/**
 * Date range formats on BlueGolf:
 * - "Mar 25-27"
 * - "Aug 11-13"
 * - "Apr 6" (single day)
 * - occasionally "Apr 8-10"
 */
function parseBlueGolfDateRange(dateCell: string, year: number): { start: string | null; end: string | null } {
  const raw = normalizeSpace(dateCell);
  if (!raw) return { start: null, end: null };

  // handle en dash/em dash too
  const dash = raw.includes("–") ? "–" : raw.includes("—") ? "—" : "-";

  // "Mar 25-27"
  const m = raw.match(/^([A-Za-z]{3})\s+(\d{1,2})\s*[-–—]\s*(\d{1,2})$/);
  if (m) {
    const mon = m[1];
    const d1 = `${mon} ${m[2]}`;
    const d2 = `${mon} ${m[3]}`;
    return {
      start: parseBlueGolfMonthDay(d1, year),
      end: parseBlueGolfMonthDay(d2, year)
    };
  }

  // "Mar 25 - Apr 1" (rare but possible)
  if (raw.includes(dash) && /[A-Za-z]{3}/.test(raw.split(dash)[1] || "")) {
    const parts = raw.split(new RegExp(`\\s*\\${dash}\\s*`)).map(p => normalizeSpace(p));
    const a = parts[0] || "";
    const b = parts[1] || "";
    return {
      start: parseBlueGolfMonthDay(a, year),
      end: parseBlueGolfMonthDay(b, year)
    };
  }

  // single day "Apr 6"
  const s = parseBlueGolfMonthDay(raw, year);
  return { start: s, end: null };
}

/** Choose best table if selector not supplied */
function pickBestTable($: any) {
  let best = $("table").first();
  let bestScore = -1;

  $("table").each((_: any, tbl: any) => {
    const table = $(tbl);
    const trs = table.find("tr").toArray();
    let score = 0;

    for (const tr of trs.slice(0, 25)) {
      const tds = $(tr).find("th,td");
      if (tds.length < 2) continue;

      const first = normalizeSpace($(tds[0]).text());
      if (looksLikeBlueGolfDateCell(first)) score += 3;
      if ($(tr).find("a[href]").length) score += 1;
      if (/tournament|tournaments|events/i.test(normalizeSpace($(tr).text()))) score += 1;
    }

    if (score > bestScore) {
      bestScore = score;
      best = table;
    }
  });

  return best;
}

/** Extract title from the tournament column cell */
function extractTitleFromTournamentCell($td: any) {
  // Prefer strong/bold title if present
  const strong = normalizeSpace($td.find("strong").first().text());
  if (strong) return strong;

  // Otherwise first link text that isn't "Register"
  const links = $td.find("a").toArray();
  for (const a of links) {
    const t = normalizeSpace($td.find(a).text());
    if (t && !/^register$/i.test(t)) return t;
  }

  // Fallback: first line of text
  const txt = normalizeSpace($td.text());
  return txt;
}

/** Extract location-ish text from tournament cell: club + city/state often sit under title */
function extractLocationFromTournamentCell($td: any) {
  // The cell often contains:
  // TITLE
  // Club Name
  // City, ST
  // $price or closes etc
  // We'll take the first "City, ST" line if present, else combine club+city.
  const lines = normalizeSpace($td.text()).split(" ").filter(Boolean);

  // We can't reliably preserve line breaks from mobile HTML, so instead:
  // try to locate "City, ST" pattern
  const m = normalizeSpace($td.text()).match(/([A-Za-z .'-]+,\s*[A-Z]{2})/);
  if (m) return normalizeSpace(m[1]);

  // fallback: just entire text minus title
  const title = extractTitleFromTournamentCell($td);
  const all = normalizeSpace($td.text());
  const cleaned = normalizeSpace(all.replace(title, ""));
  return cleaned;
}

export async function runHtmlTable(source: Source): Promise<ProPathEvent[]> {
  const res = await fetch(source.url, { headers: { "user-agent": "propath-bot" } });
  if (!res.ok) throw new Error(`html_table fetch failed: ${res.status} ${res.statusText}`);

  const html = await res.text();
  const $ = cheerio.load(html);

  const year = extractYearFromPage($ as any, html);

  const table = source.tableSelector
    ? $(source.tableSelector).first()
    : pickBestTable($ as any);

  if (!table.length) throw new Error("html_table: no table found");

  const rows: ProPathEvent[] = [];

  table.find("tr").each((i: number, tr: any) => {
    const $tr = $(tr);
    const tds = $tr.find("th,td").toArray();
    if (tds.length < 2) return;

    const dateCell = normalizeSpace($(tds[0]).text());
    if (i === 0 && /date/i.test(dateCell)) return;
    if (!looksLikeBlueGolfDateCell(dateCell)) return;

    // Tournament column (usually 2nd col)
    const $tourTd = $(tds[1]);

    const title = extractTitleFromTournamentCell($tourTd);
    if (!title || /^register$/i.test(title)) return;

    // Event detail page link (best available)
    // Prefer first link that isn't "Register"
    let eventHref: string | undefined;
    const links = $tourTd.find("a[href]").toArray();
    for (const a of links) {
      const t = normalizeSpace($tourTd.find(a).text());
      const href = $tourTd.find(a).attr("href") || undefined;
      if (!href) continue;
      if (/^register$/i.test(t)) continue;
      eventHref = href;
      break;
    }

    const eventUrl = eventHref ? new URL(eventHref, source.url).toString() : source.url;

    // Signup/Register link (often in first column or inside tour cell)
    let signupHref: string | undefined;
    const regLink = $tr.find('a[href*="register"], a[href*="Register"], a:contains("Register")').first();
    const regHref = regLink.attr("href");
    if (regHref) signupHref = regHref;

    const signupUrl = signupHref ? new URL(signupHref, source.url).toString() : undefined;

    // Dates
    const { start, end } = parseBlueGolfDateRange(dateCell, year);

    // Location: sometimes in col 3+, sometimes embedded in tournament cell
    const col2Text = extractLocationFromTournamentCell($tourTd);
    const locCell = normalizeSpace($(tds[2] ? tds[2] : tds[1]).text());
    const location = locCell && locCell.length > 2 ? locCell : col2Text;

    rows.push({
      ...(source.defaults ?? {}),
      title,
      start,
      end,
      state_country: location || "",
      tourUrl: eventUrl,
      signupUrl
    });
  });

  if (!rows.length) throw new Error("html_table: 0 events parsed");

  // Make IDs deterministic-ish (title + start) so master merge doesn't churn
  return rows.map((r) => {
    const slugBase = `${r.tour || ""}-${r.title || ""}-${r.start || ""}`.toLowerCase();
    const slug = slugBase
      .replace(/https?:\/\/\S+/g, "")
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 80);

    return { ...r, id: r.id || slug };
  });
}