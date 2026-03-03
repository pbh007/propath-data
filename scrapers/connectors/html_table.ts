// scrapers/connectors/html_table.ts
import { load } from "cheerio";
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

function norm(s: string) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function looksLikeDateCell(s: string) {
  const t = norm(s);
  // BlueGolf often uses: "Mar 25-27", "Apr 6", "Apr 8-10", "Sep 22-24"
  return /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t);
}

function slugify(s: string) {
  return norm(s)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * Try to infer the season year from the page header/dropdown.
 * Screenshot shows "2026 GPro" on the page.
 */
function inferSeasonYearFromPage(htmlText: string): number | null {
  // Be intentionally broad; BlueGolf markup varies.
  // Prefer "2026 GPro" / "2026" near the top.
  const m = htmlText.match(/\b(20\d{2})\b\s*GPro\b/i) || htmlText.match(/\b(20\d{2})\b/);
  if (!m) return null;
  const yr = Number(m[1]);
  return Number.isFinite(yr) ? yr : null;
}

/**
 * Parse BlueGolf date cell text like:
 * - "Mar 25-27"
 * - "Apr 6"
 * - "Apr 8-10"
 * - (occasionally) "Nov 30-Dec 2"
 *
 * Uses seasonYear because the table usually omits the year.
 */
function parseBlueGolfDateCell(dateText: string, seasonYear: number | null): { start: string | null; end: string | null } {
  const raw = norm(dateText);
  if (!raw) return { start: null, end: null };

  // Handle "Nov 30-Dec 2"
  const cross = raw.match(
    /^([A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*([A-Za-z]+)\s+(\d{1,2})$/i
  );
  if (cross && seasonYear) {
    const mon1 = cross[1];
    const d1 = cross[2];
    const mon2 = cross[3];
    const d2 = cross[4];
    return {
      start: coerceISO(`${mon1} ${d1}, ${seasonYear}`),
      end: coerceISO(`${mon2} ${d2}, ${seasonYear}`)
    };
  }

  // Handle "Mar 25-27" OR "Apr 8-10"
  const range = raw.match(/^([A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*(\d{1,2})$/i);
  if (range && seasonYear) {
    const mon = range[1];
    const d1 = range[2];
    const d2 = range[3];
    return {
      start: coerceISO(`${mon} ${d1}, ${seasonYear}`),
      end: coerceISO(`${mon} ${d2}, ${seasonYear}`)
    };
  }

  // Handle "Apr 6"
  const single = raw.match(/^([A-Za-z]+)\s+(\d{1,2})$/i);
  if (single && seasonYear) {
    const mon = single[1];
    const d = single[2];
    return {
      start: coerceISO(`${mon} ${d}, ${seasonYear}`),
      end: null
    };
  }

  // Fallback: if the text includes a year already, coerce it
  const maybe = coerceISO(raw);
  return { start: maybe, end: null };
}

/**
 * Extract "City, ST" from the tournament cell text.
 */
function extractCityState(s: string): { city: string | null; st: string | null } {
  const t = norm(s);
  // Find last-ish "Something, XX" pattern
  const m = t.match(/([A-Za-z .'-]+),\s*([A-Z]{2})\b/);
  if (!m) return { city: null, st: null };
  return { city: norm(m[1]), st: m[2] };
}

function pickBestTable($: ReturnType<typeof load>) {
  let best = $("table").first();
  let bestScore = -1;

  $("table").each((_, tbl) => {
    const table = $(tbl);
    const trs = table.find("tr").toArray();
    let score = 0;

    for (const tr of trs.slice(0, 25)) {
      const tds = $(tr).find("th,td");
      if (tds.length < 2) continue;

      const first = norm($(tds[0]).text());
      if (looksLikeDateCell(first)) score += 3;

      // BlueGolf rows almost always have event links
      const hasLinks = $(tr).find("a[href]").length;
      if (hasLinks) score += 1;

      // If we see "Register" links, it’s almost certainly the schedule table
      const hasRegister = $(tr).find("a").toArray().some(a => /register/i.test(norm($(a).text())));
      if (hasRegister) score += 2;
    }

    if (score > bestScore) {
      bestScore = score;
      best = table;
    }
  });

  return best;
}

export async function runHtmlTable(source: {
  url: string;
  defaults?: Record<string, string>;
  tableSelector?: string;

  /**
   * Optional: for sites that omit year and you want to force it (e.g., 2027)
   * You can set defaults: { seasonYear: "2027" } in sources.json later.
   */
}): Promise<ProPathEvent[]> {
  const res = await fetch(source.url, {
    headers: { "user-agent": "propath-bot" }
  });

  if (!res.ok) {
    throw new Error(`html_table fetch failed: ${res.status} ${res.statusText}`);
  }

  const html = await res.text();
  const $ = load(html);

  // Determine season year (used to convert "Apr 6" → 2026-04-06)
  const forcedSeasonYear = source.defaults?.seasonYear ? Number(source.defaults.seasonYear) : null;
  const inferredSeasonYear = inferSeasonYearFromPage(html);
  const seasonYear = forcedSeasonYear || inferredSeasonYear;

  const table = source.tableSelector
    ? $(source.tableSelector).first()
    : pickBestTable($);

  if (!table.length) {
    throw new Error("html_table: no table found");
  }

  const rows: ProPathEvent[] = [];

  table.find("tr").each((i, tr) => {
    const $tr = $(tr);
    const tds = $tr.find("th,td").toArray();
    if (tds.length < 2) return;

    const dateCellText = norm($(tds[0]).text());
    const tournamentCellText = norm($(tds[1]).text());

    // header row guard
    if (i === 0 && /date/i.test(dateCellText)) return;
    if (!looksLikeDateCell(dateCellText)) return;

    // Event page link: from the tournament cell (not the register link)
    const eventLink = $(tds[1]).find("a[href]").first();
    const eventTitleFromLink = norm(eventLink.text());
    const eventHref = eventLink.attr("href");
    const eventUrl = eventHref ? new URL(eventHref, source.url).toString() : source.url;

    // Signup/Register link: usually lives in the date cell
    // Prefer link whose text contains "Register"; else choose a /secure/ link
    let signupUrl: string | undefined;
    const allLinks = $tr.find("a[href]").toArray().map(a => ({
      text: norm($(a).text()),
      href: $(a).attr("href") || ""
    }));

    const reg = allLinks.find(l => /register/i.test(l.text)) || allLinks.find(l => /secure|gosecure/i.test(l.href));
    if (reg?.href) signupUrl = new URL(reg.href, source.url).toString();

    // Title: prefer link text; otherwise fall back to tournament cell text
    const title = eventTitleFromLink || tournamentCellText;
    if (!title) return;

    const { start, end } = parseBlueGolfDateCell(dateCellText, seasonYear);

    // Extract city/state from tournament cell
    const { city, st } = extractCityState(tournamentCellText);

    // Build stable ID
    const idDate = start || "tbd";
    const id = `gpro-tour-${slugify(title)}-${idDate}`;

    rows.push({
      ...(source.defaults ?? {}),
      id,
      title,
      start,
      end,
      city: city || undefined,
      state_country: st || undefined,
      tourUrl: eventUrl,
      signupUrl
    });
  });

  if (!rows.length) {
    throw new Error("html_table: 0 events parsed");
  }

  return rows;
}