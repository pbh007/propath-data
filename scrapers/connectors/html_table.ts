// scrapers/connectors/html_table.ts
import * as cheerio from "cheerio";

/**
 * Generic "HTML table -> event rows" extractor.
 * Includes special handling for BlueGolf/GPro schedule tables.
 *
 * Key goals:
 * - Only emit REAL events (must have a start date + real event page URL)
 * - Skip junk rows (Register-only, gosecure-only, headers)
 * - Extract clean city/state (avoid "NCAlbemarle..." concatenation)
 */

export type EventRow = {
  id: string;
  tour: string;
  gender?: string;
  type?: string;
  stage?: string;
  title: string;
  start?: string; // YYYY-MM-DD
  end?: string; // YYYY-MM-DD
  city?: string;
  state_country?: string;
  tourUrl?: string;
  signupUrl?: string;
  mondayUrl?: string;
  mondayDate?: string;
};

export type HtmlTableOptions = {
  /** Name of tour to stamp on rows */
  tourName: string;

  /** Optional explicit year override (e.g., 2026). If omitted, we try to detect from the page. */
  year?: number;

  /** For sites with multiple tables, you can narrow selection */
  tableSelector?: string;

  /** Optional baseUrl for resolving relative links */
  baseUrl?: string;

  /** Optional: force "type" (Event/QSchool/etc) */
  defaultType?: string;

  /** Optional: gender */
  gender?: string;
};

/** Backwards-compatible aliases (in case other code imports these names) */
export const htmlTableToEvents = extractEventsFromHtmlTable;
export const parseHtmlTable = extractEventsFromHtmlTable;
export default extractEventsFromHtmlTable;

/* =========================
   Public API
   ========================= */

export function extractEventsFromHtmlTable(
  html: string,
  opts: HtmlTableOptions
): EventRow[] {
  const $ = cheerio.load(html);

  const year = opts.year ?? detectYear($) ?? new Date().getFullYear();

  // Prefer a specific table if provided; else choose the "best" one.
  const table =
    (opts.tableSelector ? $(opts.tableSelector).first() : null) ||
    chooseBestTable($);

  if (!table || table.length === 0) return [];

  const headerMap = buildHeaderMap($, table);

  const rows: EventRow[] = [];

  table.find("tr").each((_, tr) => {
    const tds = $(tr).find("th,td");
    if (tds.length === 0) return;

    // Skip header-ish rows
    const rowText = norm($(tr).text());
    if (!rowText || /^date\s+tournaments/i.test(rowText)) return;

    const dateCell = getCellText($, tds, headerMap, ["date"]);
    const tourCell = getCellText($, tds, headerMap, ["tournaments", "tournament"]);
    const registerCell = getCellText($, tds, headerMap, ["register", "signup"]);

    const tourCellEl = getCellEl($, tds, headerMap, ["tournaments", "tournament"]);
    const registerCellEl = getCellEl($, tds, headerMap, ["register", "signup"]);

    const tourCellFlat = norm(
      [tourCell, registerCell].filter(Boolean).join(" ")
    );

    // Title extraction (BlueGolf: first bold/strong/anchor text in Tournaments cell)
    let title =
      norm(tourCellEl.find("b,strong").first().text()) ||
      norm(tourCellEl.find("a").first().text()) ||
      guessTitleFromCell(tourCell);

    // If title still empty, try overall row
    if (!title) title = guessTitleFromCell(rowText);

    // Extract URLs
    const eventUrl =
      pickBestEventUrl($, tourCellEl, registerCellEl, opts.baseUrl) || "";
    const signupUrl =
      pickBestSignupUrl($, tourCellEl, registerCellEl, opts.baseUrl) || "";

    // Date parse
    const { start, end } = parseDateRange(dateCell, year);

    // City/state parse (from "Club · City, ST" or any "City, ST" pattern)
    const { city, state_country } = parseCityState(tourCell, tourCellFlat);

    // ========= Surgical filters (prevents "-tbd" + junk rows) =========

    // Skip junk rows that BlueGolf includes (register-only rows, etc.)
    const badTitle =
      !title || /^register$/i.test(title) || title.length < 3;
    if (badTitle) return;

    // Must have a real event page (not just gosecure)
    if (!eventUrl || /\/util\/gosecure\.htm/i.test(eventUrl)) return;

    // If we couldn’t parse a start date, don’t emit the row (prevents "-tbd" duplicates)
    if (!start) return;

    // ================================================================

    const id = makeId(opts.tourName, title, start);

    rows.push({
      id,
      tour: opts.tourName,
      gender: opts.gender ?? "",
      type: opts.defaultType ?? "Event",
      stage: "",
      title,
      start,
      end: end || "",
      city: city || "",
      state_country: state_country || "",
      tourUrl: eventUrl,
      signupUrl: signupUrl || "",
      mondayUrl: "",
      mondayDate: "",
    });
  });

  return rows;
}

/* =========================
   Helpers
   ========================= */

function norm(s: string | undefined | null): string {
  return String(s ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function resolveUrl(url: string, baseUrl?: string): string {
  const u = norm(url);
  if (!u) return "";
  if (/^https?:\/\//i.test(u)) return u;
  if (!baseUrl) return u;
  try {
    return new URL(u, baseUrl).toString();
  } catch {
    return u;
  }
}

function chooseBestTable($: cheerio.CheerioAPI): cheerio.Cheerio<cheerio.Element> {
  // Pick the table with the most rows and containing "Date" + "Tournaments" in header.
  let best: cheerio.Cheerio<cheerio.Element> = $("table").first();
  let bestScore = -1;

  $("table").each((_, tbl) => {
    const t = $(tbl);
    const headerText = norm(t.find("tr").first().text()).toLowerCase();
    const rowCount = t.find("tr").length;

    let score = rowCount;
    if (headerText.includes("date")) score += 50;
    if (headerText.includes("tournament")) score += 50;
    if (headerText.includes("tournaments")) score += 50;
    if (headerText.includes("register")) score += 20;

    if (score > bestScore) {
      bestScore = score;
      best = t;
    }
  });

  return best;
}

function buildHeaderMap(
  $: cheerio.CheerioAPI,
  table: cheerio.Cheerio<cheerio.Element>
): Record<string, number> {
  const map: Record<string, number> = {};
  const headerRow = table.find("tr").first();
  const cells = headerRow.find("th,td");

  cells.each((i, cell) => {
    const text = norm($(cell).text()).toLowerCase();
    if (!text) return;

    if (text.includes("date")) map["date"] = i;
    if (text.includes("tournament")) map["tournaments"] = i;
    if (text.includes("register")) map["register"] = i;
    if (text.includes("signup")) map["signup"] = i;
  });

  return map;
}

function getCellEl(
  $: cheerio.CheerioAPI,
  tds: cheerio.Cheerio<cheerio.Element>,
  headerMap: Record<string, number>,
  keys: string[]
): cheerio.Cheerio<cheerio.Element> {
  for (const k of keys) {
    const idx = headerMap[k];
    if (typeof idx === "number" && idx >= 0 && idx < tds.length) {
      return tds.eq(idx);
    }
  }
  // Fallback: return first td
  return tds.eq(0);
}

function getCellText(
  $: cheerio.CheerioAPI,
  tds: cheerio.Cheerio<cheerio.Element>,
  headerMap: Record<string, number>,
  keys: string[]
): string {
  return norm(getCellEl($, tds, headerMap, keys).text());
}

function detectYear($: cheerio.CheerioAPI): number | null {
  // BlueGolf schedule pages often have a dropdown showing "2026 GPro" etc.
  // We look for a strong "20xx" near a select option or a visible label.
  const textBlob = norm($("body").text());
  const m = textBlob.match(/\b(20\d{2})\b/);
  if (m) {
    const y = Number(m[1]);
    if (y >= 2000 && y <= 2100) return y;
  }

  // Try specific select option patterns
  const opt = $("select option:selected").first();
  const optText = norm(opt.text());
  const m2 = optText.match(/\b(20\d{2})\b/);
  if (m2) {
    const y = Number(m2[1]);
    if (y >= 2000 && y <= 2100) return y;
  }

  return null;
}

function guessTitleFromCell(cellText: string): string {
  const s = norm(cellText);
  if (!s) return "";

  // BlueGolf often repeats title + club + city. Take the first "chunk" before " · " or newline-like separators
  const parts = s.split("·").map(norm).filter(Boolean);
  if (parts.length > 0) {
    // The left side often starts with TITLE (sometimes repeated). Take first 1-6 words if very long
    const t = parts[0];
    if (t.length <= 60) return t;
    return t.split(" ").slice(0, 8).join(" ");
  }

  // Fallback: first 8 words
  return s.split(" ").slice(0, 8).join(" ");
}

function pickBestEventUrl(
  $: cheerio.CheerioAPI,
  tourCellEl: cheerio.Cheerio<cheerio.Element>,
  registerCellEl: cheerio.Cheerio<cheerio.Element>,
  baseUrl?: string
): string | null {
  const links = [
    ...tourCellEl.find("a").toArray(),
    ...registerCellEl.find("a").toArray(),
  ];

  // Prefer actual event page links containing "/event/" and ending in .htm/.html
  for (const a of links) {
    const href = norm($(a).attr("href"));
    if (!href) continue;
    if (/\/event\/.+\.(htm|html)\b/i.test(href)) return resolveUrl(href, baseUrl);
  }

  // Next best: any link that looks like a BlueGolf event index
  for (const a of links) {
    const href = norm($(a).attr("href"));
    if (!href) continue;
    if (/bluegolf\/.+\/event\/.+\/index\.(htm|html)\b/i.test(href))
      return resolveUrl(href, baseUrl);
  }

  return null;
}

function pickBestSignupUrl(
  $: cheerio.CheerioAPI,
  tourCellEl: cheerio.Cheerio<cheerio.Element>,
  registerCellEl: cheerio.Cheerio<cheerio.Element>,
  baseUrl?: string
): string | null {
  const links = [
    ...registerCellEl.find("a").toArray(),
    ...tourCellEl.find("a").toArray(),
  ];

  // Prefer gosecure links (registration flow)
  for (const a of links) {
    const href = norm($(a).attr("href"));
    if (!href) continue;
    if (/\/util\/gosecure\.htm/i.test(href)) return resolveUrl(href, baseUrl);
  }

  // Fallback: any link with "/secure/" or "start?"
  for (const a of links) {
    const href = norm($(a).attr("href"));
    if (!href) continue;
    if (/\/secure\//i.test(href) || /\bstart\?/i.test(href)) {
      return resolveUrl(href, baseUrl);
    }
  }

  return null;
}

function parseDateRange(dateText: string, year: number): { start?: string; end?: string } {
  const s = norm(dateText);
  if (!s) return {};

  // If already ISO-ish
  const iso = s.match(/\b(20\d{2})-(\d{2})-(\d{2})\b/);
  if (iso) {
    const start = `${iso[1]}-${iso[2]}-${iso[3]}`;
    return { start };
  }

  // BlueGolf common: "Mar 25-27" or "Apr 8-10" or "Apr 6"
  const m = s.match(
    /\b([A-Za-z]{3,})\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?\b/
  );
  if (!m) return {};

  const monthName = m[1].toLowerCase();
  const day1 = Number(m[2]);
  const day2 = m[3] ? Number(m[3]) : null;

  const month = monthToNumber(monthName);
  if (!month) return {};

  const start = toISO(year, month, day1);
  const end = day2 ? toISO(year, month, day2) : "";

  return { start, end };
}

function monthToNumber(m: string): number | null {
  const k = m.slice(0, 3).toLowerCase();
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
    dec: 12,
  };
  return map[k] ?? null;
}

function toISO(year: number, month: number, day: number): string {
  const yyyy = String(year);
  const mm = String(month).padStart(2, "0");
  const dd = String(day).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function parseCityState(tourCell: string, tourCellFlat: string): { city?: string; state_country?: string } {
  const text = norm([tourCell, tourCellFlat].filter(Boolean).join(" "));

  // Prefer " · City, ST" pattern
  const dotSplit = text.split("·").map(norm);
  for (const part of dotSplit) {
    const m = part.match(/(.+?),\s*([A-Z]{2})\b/);
    if (m) {
      return { city: norm(m[1]), state_country: m[2] };
    }
  }

  // Fallback: any "City, ST" occurrence
  const m2 = text.match(/(.+?),\s*([A-Z]{2})\b/);
  if (m2) {
    return { city: norm(m2[1]), state_country: m2[2] };
  }

  return {};
}

function makeId(tourName: string, title: string, startISO: string): string {
  const base = `${tourName}-${title}-${startISO}`
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  // Shorten if absurdly long
  return base.length > 120 ? base.slice(0, 120).replace(/-$/g, "") : base;
}