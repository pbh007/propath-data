// scrapers/connectors/html_table.ts
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
  defaults?: Record<string, any>;
  tableSelector?: string; // optional override
};

function slug(s: string) {
  return (s || "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 90);
}

function looksLikeDateCell(s: string) {
  const t = (s || "").replace(/\s+/g, " ").trim();
  // BlueGolf upcoming examples: "Mar 25-27", "Apr 6", "Sep 22-24"
  return (
    /^\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t) ||
    /^\d{4}-\d{2}-\d{2}$/.test(t) ||
    /\b20\d{2}\b/.test(t)
  );
}

function extractYearFromPage($: cheerio.CheerioAPI, html: string): number | null {
  // Try common patterns: selected season "2026", visible "2026 GPro", option selected, etc.
  const bodyText = $("body").text().replace(/\s+/g, " ");

  // Prefer a "2026" near "GPro" if present
  const m1 = bodyText.match(/\b(20\d{2})\s+GPro\b/i);
  if (m1) return Number(m1[1]);

  // Any year in the page at all (pick the first reasonable one)
  const m2 = bodyText.match(/\b(20\d{2})\b/);
  if (m2) return Number(m2[1]);

  // Last resort: raw html scan
  const m3 = html.match(/\b(20\d{2})\b/);
  if (m3) return Number(m3[1]);

  return null;
}

function parseBlueGolfDateRange(raw: string, year: number): { start: string | null; end: string | null } {
  const t = (raw || "").replace(/\s+/g, " ").trim();
  if (!t) return { start: null, end: null };

  // If it already includes a year, just coerce directly (and handle ranges)
  if (/\b20\d{2}\b/.test(t)) {
    // Examples: "March 25-27, 2026" or "Mar 25, 2026"
    const m = t.match(/^([A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*(\d{1,2}),\s*(20\d{2})$/);
    if (m) {
      const mon = m[1];
      const d1 = m[2];
      const d2 = m[3];
      const yr = m[4];
      return {
        start: coerceISO(`${mon} ${d1}, ${yr}`),
        end: coerceISO(`${mon} ${d2}, ${yr}`)
      };
    }
    return { start: coerceISO(t), end: null };
  }

  // BlueGolf common: "Mar 25-27" OR "Apr 6" OR "Sep 22-24"
  const m = t.match(/^([A-Za-z]{3,})\s+(\d{1,2})(?:\s*[-–—]\s*(\d{1,2}))?$/);
  if (m) {
    const mon = m[1];
    const d1 = m[2];
    const d2 = m[3];
    const start = coerceISO(`${mon} ${d1}, ${year}`);
    const end = d2 ? coerceISO(`${mon} ${d2}, ${year}`) : start;
    return { start, end };
  }

  // fallback
  return { start: coerceISO(`${t}, ${year}`), end: null };
}

function pickBestTable($: cheerio.CheerioAPI) {
  let best = $("table").first();
  let bestScore = -1;

  $("table").each((_, tbl) => {
    const table = $(tbl);
    const trs = table.find("tr").toArray();
    let score = 0;

    for (const tr of trs.slice(0, 25)) {
      const tds = $(tr).find("th,td");
      if (tds.length < 2) continue;

      const first = $(tds[0]).text().trim();
      if (looksLikeDateCell(first)) score += 3;

      // register links / event links are common on BlueGolf
      if ($(tr).find("a[href]").length) score += 1;

      // rows with icons etc still have anchors
    }

    if (score > bestScore) {
      bestScore = score;
      best = table;
    }
  });

  return best;
}

function extractTitleFromTournamentCell($td: cheerio.Cheerio<cheerio.Element>) {
  // BlueGolf tends to have title in strong/b/a at top of the cell
  const strong = $td.find("strong, b").first().text().replace(/\s+/g, " ").trim();
  if (strong) return strong;

  const a = $td.find("a").first().text().replace(/\s+/g, " ").trim();
  if (a && !/^register$/i.test(a)) return a;

  // fallback: take first line of text
  const full = $td.text().replace(/\s+/g, " ").trim();
  if (!full) return "";
  // sometimes starts with title then course etc
  return full.split("  ")[0].trim();
}

function extractCityStateFromTournamentCell(text: string): { city?: string; state_country?: string } {
  const t = (text || "").replace(/\s+/g, " ").trim();

  // Find LAST occurrence of "City, ST"
  const matches = Array.from(t.matchAll(/([A-Za-z.'-]+(?:\s+[A-Za-z.'-]+)*)\s*,\s*([A-Z]{2})\b/g));
  if (!matches.length) return {};

  const last = matches[matches.length - 1];
  const city = last[1].trim();
  const st = last[2].trim();
  return { city, state_country: `${st}, USA` };
}

export async function runHtmlTable(source: Source): Promise<ProPathEvent[]> {
  const res = await fetch(source.url, { headers: { "user-agent": "propath-bot" } });
  if (!res.ok) throw new Error(`html_table fetch failed: ${res.status} ${res.statusText}`);

  const html = await res.text();
  const $ = cheerio.load(html);

  // Year inference (BlueGolf upcoming dates usually omit year)
  const defaultYearFromSource =
    typeof source.defaults?.year === "number"
      ? source.defaults.year
      : typeof source.defaults?.year === "string"
        ? Number(source.defaults.year)
        : null;

  const inferredYear = defaultYearFromSource || extractYearFromPage($, html) || new Date().getFullYear();

  const table = source.tableSelector ? $(source.tableSelector).first() : pickBestTable($);
  if (!table.length) throw new Error("html_table: no table found");

  const rows: ProPathEvent[] = [];

  table.find("tr").each((i, tr) => {
    const $tr = $(tr);
    const tds = $tr.find("th,td").toArray();
    if (tds.length < 2) return;

    const $dateTd = $(tds[0]);
    const $tournamentTd = $(tds[1]);

    const dateCell = $dateTd.text().replace(/\s+/g, " ").trim();

    // skip header-ish rows
    if (i === 0 && /date/i.test(dateCell)) return;
    if (!looksLikeDateCell(dateCell)) return;

    const { start, end } = parseBlueGolfDateRange(dateCell, inferredYear);
    if (!start) return;

    const title = extractTitleFromTournamentCell($tournamentTd);
    if (!title || /^register$/i.test(title)) return;

    // event page link (usually in the tournament cell)
    const eventLink = $tournamentTd.find("a[href]").first();
    const eventHref = eventLink.attr("href");
    const tourUrl = eventHref ? new URL(eventHref, source.url).toString() : source.url;

    // register link is usually in the date/left column
    const regLink = $tr.find("a[href]").toArray().find(a => {
      const txt = $(a).text().replace(/\s+/g, " ").trim();
      return /register/i.test(txt);
    });
    const signupUrl = regLink ? new URL($(regLink).attr("href")!, source.url).toString() : "";

    const tournamentCellText = $tournamentTd.text().replace(/\s+/g, " ").trim();
    const loc = extractCityStateFromTournamentCell(tournamentCellText);

    const id = [
      slug(source.defaults?.tour || ""),
      start,
      slug(title),
      slug(loc.city || "")
    ].filter(Boolean).join("-");

    rows.push({
      ...(source.defaults ?? {}),
      id,
      title,
      start,
      end,
      city: loc.city,
      state_country: loc.state_country,
      tourUrl,
      signupUrl
    });
  });

  if (!rows.length) throw new Error("html_table: 0 events parsed");
  return rows;
}