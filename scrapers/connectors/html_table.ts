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
  return (s || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function looksLikeDateToken(s: string) {
  const t = norm(s);
  return /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t);
}

/**
 * Pull a clean BlueGolf date token out of messy text.
 * Handles:
 *  - "Mar 25-27 Register"
 *  - "Apr 8-10"
 *  - "Apr 6"
 *  - "Nov 30-Dec 2"
 */
function extractCleanDateToken(text: string): string {
  const t = norm(text);

  // "Nov 30-Dec 2"
  const cross = t.match(
    /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b\s+\d{1,2}\s*[-–—]\s*\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b\s+\d{1,2}\b/i
  );
  if (cross) return norm(cross[0]);

  // "Mar 25-27" OR "Apr 8-10"
  const range = t.match(
    /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b\s+\d{1,2}\s*[-–—]\s*\d{1,2}\b/i
  );
  if (range) return norm(range[0]);

  // "Apr 6"
  const single = t.match(
    /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b\s+\d{1,2}\b/i
  );
  if (single) return norm(single[0]);

  return t;
}

function cellLines($: ReturnType<typeof load>, el: any): string[] {
  // BlueGolf uses a middle dot "·" between course and city/state, so split on that too.
  const raw = ($(el).text() || "")
    .replace(/\u00a0/g, " ")
    .replace(/·/g, "\n");

  return raw
    .split(/\r?\n+/)
    .map(s => s.trim())
    .filter(Boolean);
}

function slugify(s: string) {
  return norm(s)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function inferSeasonYearFromHtml(htmlText: string): number | null {
  // Prefer "2026 GPro" in the UI
  const m1 = htmlText.match(/\b(20\d{2})\b\s*GPro\b/i);
  if (m1) {
    const yr = Number(m1[1]);
    return Number.isFinite(yr) ? yr : null;
  }
  return null;
}

function parseBlueGolfDateToken(token: string, seasonYear: number | null): { start: string | null; end: string | null } {
  if (!seasonYear) return { start: null, end: null };

  const raw = extractCleanDateToken(token);
  if (!raw || !looksLikeDateToken(raw)) return { start: null, end: null };

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

  // last resort (rare)
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

    const dateCellFlat = norm($(tds[0]).text());
    const tourCellFlat = norm($(tds[1]).text());

    if (i === 0 && /date/i.test(dateCellFlat)) return;

    // Date token (cleaned)
    const dateToken = extractCleanDateToken(dateCellFlat);
    if (!looksLikeDateToken(dateToken)) return;

    const { start, end } = parseBlueGolfDateToken(dateToken, seasonYear);

    // Tournament lines (split on "·" as well)
    const tourLines = cellLines($, tds[1]);

    // Event link + title
    const eventLink = $(tds[1]).find("a[href]").first();
    const eventTitleFromLink = norm(eventLink.text());
    const eventHref = eventLink.attr("href");
    const eventUrl = eventHref ? new URL(eventHref, source.url).toString() : source.url;

    const title = eventTitleFromLink || tourLines[0] || tourCellFlat;
    if (!title) return;

    // City/state line: prefer a clean line like "Brunswick, GA"
    const cityStateLine =
      tourLines.find(l => /,\s*[A-Z]{2}\b/.test(l)) ||
      (tourCellFlat.replace(/·/g, " ").match(/([A-Za-z .'-]+),\s*([A-Z]{2})\b/)?.[0] ?? "");

    let city: string | undefined;
    let state_country: string | undefined;
    const m = cityStateLine.match(/^(.+?),\s*([A-Z]{2})\b/);
    if (m) {
      city = norm(m[1]);
      state_country = m[2];
    }

    // Register link (signup) usually in the date cell
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