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

/**
 * Connector: fetches HTML from a Source-like object and extracts events.
 * This is what run-all.ts expects to call.
 */
export async function runHtmlTable(source: any): Promise<EventRow[]> {
  const url = String(source?.url || "").trim();
  if (!url) return [];

  const baseUrl =
    (source?.defaults?.baseUrl && String(source.defaults.baseUrl).trim()) || url;

  const tourName =
    (source?.defaults?.tour && String(source.defaults.tour).trim()) ||
    (source?.defaults?.tourName && String(source.defaults.tourName).trim()) ||
    (source?.name && String(source.name).trim()) ||
    "Unknown Tour";

  const defaultType =
    (source?.defaults?.type && String(source.defaults.type).trim()) ||
    (source?.defaults?.defaultType && String(source.defaults.defaultType).trim()) ||
    "Event";

  const gender =
    (source?.defaults?.gender && String(source.defaults.gender).trim()) || "";

  const yearRaw =
    source?.defaults?.year ?? source?.year ?? source?.defaults?.detectedYear;
  const year =
    yearRaw !== undefined && yearRaw !== null && String(yearRaw).trim() !== ""
      ? Number(yearRaw)
      : undefined;

  const tableSelector =
    (source?.tableSelector && String(source.tableSelector).trim()) || undefined;

  const html = await fetchHtml(url);

  const rows = extractEventsFromHtmlTable(html, {
    tourName,
    year: Number.isFinite(year as any) ? (year as number) : undefined,
    tableSelector,
    baseUrl,
    defaultType,
    gender,
  });

  // Apply any last-mile overrides from defaults (optional)
  // (e.g. if you want to force a specific "tour" label)
  const forcedTour =
    (source?.defaults?.tour && String(source.defaults.tour).trim()) || "";

  return rows.map((r) => ({
    ...r,
    tour: forcedTour || r.tour,
  }));
}

async function fetchHtml(url: string): Promise<string> {
  // Node 18+ has global fetch in GitHub Actions runners.
  // Add a UA to reduce basic bot blocking.
  const res = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (compatible; ProPathScraper/1.0; +https://github.com/pbh007)",
      Accept: "text/html,application/xhtml+xml",
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`fetch failed ${res.status} ${res.statusText} for ${url}\n${text.slice(0, 200)}`);
  }

  return await res.text();
}

/**
 * Pure extractor (your existing logic), kept as default export.
 * Useful for testing and reuse.
 */
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

  const table = opts.tableSelector
    ? $(opts.tableSelector).first()
    : $("table").first();

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

    if (!title || /^register$/i.test(title)) return;

    const eventUrl = findEventUrl(tournamentCell, opts.baseUrl);
    if (!eventUrl) return;

    const signupUrl = findSignupUrl(registerCell, opts.baseUrl);

    const { start, end } = parseDateRange(dateText, detectedYear);
    if (!start) return; // prevents -tbd junk

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
  const m = text.match(/(.+?) · (.+?),\s*([A-Z]{2})/);
  if (m) {
    return {
      city: clean(m[2]),
      state_country: m[3],
    };
  }

  const fallback = text.match(/(.+?),\s*([A-Z]{2})/);
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