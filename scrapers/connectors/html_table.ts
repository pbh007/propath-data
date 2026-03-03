// scrapers/connectors/html_table.ts
import * as cheerio from "cheerio";

export type EventRow = {
  id: string;
  tour: string;
  gender?: string;
  type?: string;
  stage?: string;
  title: string;
  start?: string;
  end?: string;
  city?: string;
  state_country?: string;
  tourUrl?: string;
  signupUrl?: string;
  mondayUrl?: string;
  mondayDate?: string;
};

export type HtmlTableOptions = {
  tourName: string;
  year?: number;
  tableSelector?: string;
  baseUrl?: string;
  defaultType?: string;
  gender?: string;
};

// This is the extractor (kept as default export)
export default function extractEventsFromHtmlTable(
  html: string,
  opts: HtmlTableOptions
): EventRow[] {
  const $ = cheerio.load(html);

  const detectedYear =
    opts.year ||
    (() => {
      const body = $("body").text();
      const match = body.match(/\b(20\d{2})\b/);
      return match ? Number(match[1]) : new Date().getFullYear();
    })();

  const table = opts.tableSelector ? $(opts.tableSelector).first() : $("table").first();
  if (!table || table.length === 0) return [];

  const rows: EventRow[] = [];

  table.find("tr").each((_, tr) => {
    const tds = $(tr).find("td");
    if (tds.length < 2) return;

    const dateText = clean(tds.eq(0).text());
    const tournamentCell = tds.eq(1);
    const registerCell = tds.eq(2);

    const title =
      clean(tournamentCell.find("b,strong").first().text()) ||
      clean(tournamentCell.find("a").first().text());

    // Skip empty / junk rows
    if (!title) return;
    if (/^register$/i.test(title)) return;

    const eventUrl = findEventUrl(tournamentCell, opts.baseUrl);
    if (!eventUrl) return;

    const signupUrl = findSignupUrl(registerCell, opts.baseUrl);

    const { start, end } = parseDateRange(dateText, detectedYear);
    if (!start) return; // prevents "tbd" junk getting into the CSV

    const { city, state_country } = parseCityState(clean(tournamentCell.text()));
    const id = makeId(opts.tourName, title, start);

    rows.push({
      id,
      tour: opts.tourName,
      gender: opts.gender || "",
      type: opts.defaultType || "Event",
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

/**
 * ✅ This is what run-all.ts expects:
 * import { runHtmlTable } from "./connectors/html_table.js";
 *
 * It fetches the HTML, runs the extractor, and returns rows.
 */
export async function runHtmlTable(source: any): Promise<EventRow[]> {
  const url = source?.url;
  if (!url) return [];

  const res = await fetch(url, {
    headers: {
      // Some sites behave better with a UA
      "User-Agent": "propath-data-scraper/1.0",
      "Accept": "text/html,application/xhtml+xml",
    },
  });

  if (!res.ok) {
    throw new Error(`HTML table fetch failed: ${res.status} ${res.statusText} (${url})`);
  }

  const html = await res.text();

  const defaults = source?.defaults || {};

  const opts: HtmlTableOptions = {
    tourName: defaults.tour || defaults.tourName || "Unknown Tour",
    year: defaults.year ? Number(defaults.year) : undefined,
    tableSelector: source?.tableSelector || "table",
    baseUrl: source?.url,
    defaultType: defaults.type || "Event",
    gender: defaults.gender || "",
  };

  return extractEventsFromHtmlTable(html, opts);
}

function clean(s: string) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function resolve(url: string, base?: string) {
  if (!url) return "";
  if (/^https?:\/\//i.test(url)) return url;
  if (!base) return url;
  try {
    return new URL(url, base).toString();
  } catch {
    return url;
  }
}

function findEventUrl(cell: any, base?: string) {
  const links = cell.find("a").toArray();
  for (const link of links) {
    const href = link.attribs?.href;
    if (!href) continue;
    if (/\/event\/.+\.(htm|html)/i.test(href)) return resolve(href, base);
  }
  return null;
}

function findSignupUrl(cell: any, base?: string) {
  if (!cell) return "";
  const links = cell.find("a").toArray();
  for (const link of links) {
    const href = link.attribs?.href;
    if (!href) continue;
    if (/gosecure/i.test(href)) return resolve(href, base);
  }
  return "";
}

function parseDateRange(text: string, year: number) {
  if (!text) return {};

  const m = text.match(/\b([A-Za-z]{3,})\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?\b/);
  if (!m) return {};

  const month = monthToNumber(m[1]);
  if (!month) return {};

  const start = toISO(year, month, Number(m[2]));
  const end = m[3] ? toISO(year, month, Number(m[3])) : "";

  return { start, end };
}

function monthToNumber(name: string) {
  const m = name.slice(0, 3).toLowerCase();
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
  return map[m] || 0;
}

function toISO(year: number, month: number, day: number) {
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function parseCityState(text: string) {
  // Typical GPro pattern: "Event Name · City, ST"
  const m = text.match(/ · (.+?),\s*([A-Z]{2})\b/);
  if (m) {
    return {
      city: clean(m[1]),
      state_country: m[2],
    };
  }

  const fallback = text.match(/(.+?),\s*([A-Z]{2})\b/);
  if (fallback) {
    return {
      city: clean(fallback[1]),
      state_country: fallback[2],
    };
  }

  return {};
}

function makeId(tour: string, title: string, start: string) {
  return `${tour}-${title}-${start}`
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}