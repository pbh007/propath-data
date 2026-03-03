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
  return (s || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
}

function cellLines($: ReturnType<typeof load>, el: any): string[] {
  // Cheerio keeps line breaks from <br> etc in many tables; splitting gives us clean “rows”
  const raw = $(el).text() || "";
  return raw
    .replace(/\u00a0/g, " ")
    .split(/\r?\n+/)
    .map(s => s.trim())
    .filter(Boolean);
}

function looksLikeDateToken(s: string) {
  const t = norm(s);
  return /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t);
}

function extractDateTokenFromCell(lines: string[], fallbackText: string): string {
  // Prefer a line that looks like "Mar 25-27" / "Apr 6" / "Apr 8-10"
  const pick = lines.find(looksLikeDateToken);
  if (pick) return norm(pick);

  // Fallback: search inside the flattened cell text for a month/day token
  const t = norm(fallbackText);
  const m = t.match(
    /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b\s+\d{1,2}(?:\s*[-–—]\s*(?:\d{1,2}|\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b\s+\d{1,2}))?/i
  );
  return m ? norm(m[0]) : t;
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
 * Try to infer the season year from the page UI ("2026 GPro").
 * If not found, you can force it via source.defaults.seasonYear.
 */
function inferSeasonYearFromHtml(htmlText: string): number | null {
  // Prefer the explicit selector text "2026 GPro" if present
  const m1 = htmlText.match(/\b(20\d{2})\b\s*GPro\b/i);
  if (m1) {
    const yr = Number(m1[1]);
    return Number.isFinite(yr) ? yr : null;
  }
  // Otherwise, look for a "2026" near "Upcoming" chunk (best-effort)
  const m2 = htmlText.match(/\b(20\d{2})\b/);
  if (!m2) return null;
  const yr = Number(m2[1]);
  return Number.isFinite(yr) ? yr : null;
}

/**
 * Parse BlueGolf date token like:
 * - "Mar 25-27"
 * - "Apr 6"
 * - "Apr 8-10"
 * - "Nov 30-Dec 2"
 */
function parseBlueGolfDateToken(token: string, seasonYear: number | null): { start: string | null; end: string | null } {
  const raw = norm(token);
  if (!raw || !seasonYear) return { start: null, end: null };

  // "Nov 30-Dec 2"
  const cross = raw.match(/^([A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*([A-Za-z]+)\s+(\d{1,2})$/i);
  if (cross) {
    return {
      start: coerceISO(`${cross[1]} ${cross[2]}, ${seasonYear}`),
      end: coerceISO(`${cross[3]} ${cross[4]}, ${seasonYear}`)
    };
  }

  // "Mar 25-27"
  const range = raw.match(/^([A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*(\d{1,2})$/i);
  if (range) {
    return {
      start: coerceISO(`${range[1]} ${range[2]}, ${seasonYear}`),
      end: coerceISO(`${range[1]} ${range[3]}, ${seasonYear}`)
    };
  }

  // "Apr 6"
  const single = raw.match(/^([A-Za-z]+)\s+(\d{1,2})$/i);
  if (single) {
    return {
      start: coerceISO(`${single[1]} ${single[2]}, ${seasonYear}`),
      end: null
    };
  }

  // last resort
  return { start: coerceISO(raw), end: null };
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

      const firstText = norm($(tds[0]).text());
      if (looksLikeDateToken(firstText)) score += 3;

      const hasRegister = $(tr).find("a").toArray().some(a => /register/i.test(norm($(a).text())));
      if (hasRegister) score += 3;

      if ($(tr).find("a[href]").length) score += 1;
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
}): Promise<ProPathEvent[]> {
  const res = await fetch(source.url, {
    headers: { "user-agent": "propath-bot" }
  });

  if (!res.ok) {
    throw new Error(`html_table fetch failed: ${res.status} ${res.statusText}`);
  }

  const html = await res.text();
  const $ = load(html);

  const forcedSeasonYear = source.defaults?.seasonYear ? Number(source.defaults.seasonYear) : null;
  const inferredSeasonYear = inferSeasonYearFromHtml(html);
  const seasonYear = forcedSeasonYear || inferredSeasonYear;

  const table = source.tableSelector ? $(source.tableSelector).first() : pickBestTable($);
  if (!table.length) throw new Error("html_table: no table found");

  const rows: ProPathEvent[] = [];

  table.find("tr").each((i, tr) => {
    const $tr = $(tr);
    const tds = $tr.find("th,td").toArray();
    if (tds.length < 2) return;

    const dateLines = cellLines($, tds[0]);
    const tourLines = cellLines($, tds[1]);

    const dateCellFlat = norm($(tds[0]).text());
    const tourCellFlat = norm($(tds[1]).text());

    if (i === 0 && /date/i.test(dateCellFlat)) return;

    const dateToken = extractDateTokenFromCell(dateLines, dateCellFlat);
    if (!looksLikeDateToken(dateToken)) return;

    const { start, end } = parseBlueGolfDateToken(dateToken, seasonYear);

    // Event link + title live in the tournament cell
    const eventLink = $(tds[1]).find("a[href]").first();
    const eventTitleFromLink = norm(eventLink.text());
    const eventHref = eventLink.attr("href");
    const eventUrl = eventHref ? new URL(eventHref, source.url).toString() : source.url;

    const title = eventTitleFromLink || tourLines[0] || tourCellFlat;
    if (!title) return;

    // City/state line is typically its own line ("Brunswick, GA")
    const cityStateLine = tourLines.find(l => /,\s*[A-Z]{2}\b/.test(l)) || "";
    let city: string | undefined;
    let state_country: string | undefined;
    const m = cityStateLine.match(/^(.+?),\s*([A-Z]{2})\b/);
    if (m) {
      city = norm(m[1]);
      state_country = m[2];
    }

    // Register link (signup) is in the date cell on BlueGolf schedule
    let signupUrl: string | undefined;
    const allLinks = $tr.find("a[href]").toArray().map(a => ({
      text: norm($(a).text()),
      href: $(a).attr("href") || ""
    }));
    const reg =
      allLinks.find(l => /register/i.test(l.text)) ||
      allLinks.find(l => /secure|gosecure/i.test(l.href));
    if (reg?.href) signupUrl = new URL(reg.href, source.url).toString();

    const id = `gpro-tour-${slugify(title)}-${start || "tbd"}`;

    rows.push({
      ...(source.defaults ?? {}),
      id,
      title,
      start,
      end,
      city,
      state_country,
      tourUrl: eventUrl,
      signupUrl
    });
  });

  if (!rows.length) throw new Error("html_table: 0 events parsed");
  return rows;
}