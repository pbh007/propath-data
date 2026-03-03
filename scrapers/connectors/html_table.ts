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

/* ------------------------------------------------ */
/* Detect if a cell looks like a date              */
/* ------------------------------------------------ */
function looksLikeDateCell(s: string) {
  const t = (s || "").replace(/\s+/g, " ").trim();
  return (
    /\b20\d{2}\b/.test(t) ||
    /^\d{4}-\d{2}-\d{2}$/.test(t) ||
    /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t)
  );
}

/* ------------------------------------------------ */
/* Extract season year (BlueGolf shows 2026 GPro)  */
/* ------------------------------------------------ */
function getSeasonYear($: cheerio.CheerioAPI): number {
  const text = $("body").text().replace(/\s+/g, " ");

  const match =
    text.match(/\b(20\d{2})\s*GPro\b/i) ||
    text.match(/\bSeasons:\s*(20\d{2})\b/i);

  return match ? Number(match[1]) : new Date().getFullYear();
}

/* ------------------------------------------------ */
/* Parse BlueGolf date like "Mar 25-27" or "Apr 6" */
/* ------------------------------------------------ */
function parseBlueGolfDate(
  rawDate: string,
  year: number
): { start: string | null; end: string | null } {
  if (!rawDate) return { start: null, end: null };

  const cleaned = rawDate
    .replace(/\s+/g, " ")
    .split("Register")[0]
    .split("Opens")[0]
    .trim();

  if (!cleaned) return { start: null, end: null };

  // Match single day: Apr 6
  const single = cleaned.match(/^([A-Za-z]+)\s+(\d{1,2})$/);
  if (single) {
    return {
      start: coerceISO(`${single[1]} ${single[2]}, ${year}`),
      end: null
    };
  }

  // Match range: Mar 25-27
  const range = cleaned.match(
    /^([A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*(\d{1,2})$/
  );

  if (range) {
    const mon = range[1];
    const d1 = range[2];
    const d2 = range[3];

    return {
      start: coerceISO(`${mon} ${d1}, ${year}`),
      end: coerceISO(`${mon} ${d2}, ${year}`)
    };
  }

  // Fallback: try appending year
  return {
    start: coerceISO(`${cleaned}, ${year}`),
    end: null
  };
}

/* ------------------------------------------------ */
/* Smart table picker                              */
/* ------------------------------------------------ */
function pickBestTable($: cheerio.CheerioAPI) {
  let best = $("table").first();
  let bestScore = -1;

  $("table").each((_, tbl) => {
    const table = $(tbl);
    const trs = table.find("tr").toArray();
    let score = 0;

    for (const tr of trs.slice(0, 20)) {
      const tds = $(tr).find("th,td");
      if (tds.length < 2) continue;

      const first = $(tds[0]).text().trim();
      if (looksLikeDateCell(first)) score += 2;
      if ($(tr).find("a[href]").length) score += 1;
    }

    if (score > bestScore) {
      bestScore = score;
      best = table;
    }
  });

  return best;
}

/* ------------------------------------------------ */
/* MAIN CONNECTOR                                  */
/* ------------------------------------------------ */
export async function runHtmlTable(source: {
  url: string;
  defaults?: Record<string, string>;
  tableSelector?: string;
}): Promise<ProPathEvent[]> {

  const res = await fetch(source.url, {
    headers: { "user-agent": "propath-bot" }
  });

  if (!res.ok) {
    throw new Error(
      `html_table fetch failed: ${res.status} ${res.statusText}`
    );
  }

  const html = await res.text();
  const $ = cheerio.load(html);

  const seasonYear = getSeasonYear($);

  const table = source.tableSelector
    ? $(source.tableSelector).first()
    : pickBestTable($);

  if (!table.length) {
    throw new Error("html_table: no table found");
  }

  const rows: ProPathEvent[] = [];

  table.find("tr").each((_, tr) => {
    const $tr = $(tr);
    const tds = $tr.find("td");

    if (tds.length < 2) return;

    const dateCell = $(tds[0])
      .text()
      .replace(/\s+/g, " ")
      .trim();

    if (!looksLikeDateCell(dateCell)) return;

    // Remove nested elements for clean title text
    const titleCell = $(tds[1])
      .clone()
      .children()
      .remove()
      .end()
      .text()
      .replace(/\s+/g, " ")
      .trim();

    // Skip pure "Register" rows
    if (!titleCell || /^register$/i.test(titleCell)) return;

    const { start, end } = parseBlueGolfDate(dateCell, seasonYear);
    if (!start) return;

    // Extract city/state like "Brunswick, GA"
    const infoText = $(tds[1]).text();
    const cityMatch = infoText.match(/([A-Za-z\s]+,\s*[A-Z]{2})/);

    const cityState = cityMatch ? cityMatch[1] : "";
    const city = cityState ? cityState.split(",")[0].trim() : "";
    const state = cityState ? cityState.split(",")[1].trim() : "";

    const state_country = state ? `${state}, USA` : "";

    const href = $(tds[1]).find("a[href]").first().attr("href");
    const eventUrl = href
      ? new URL(href, source.url).toString()
      : source.url;

    rows.push({
      ...(source.defaults ?? {}),
      title: titleCell,
      start,
      end,
      city,
      state_country,
      tourUrl: eventUrl
    });
  });

  if (!rows.length) {
    throw new Error("html_table: 0 events parsed");
  }

  return rows;
}