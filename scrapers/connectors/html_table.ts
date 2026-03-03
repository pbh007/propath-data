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

function looksLikeDateCell(s: string) {
  const t = (s || "").replace(/\s+/g, " ").trim();
  return (
    /\b20\d{2}\b/.test(t) ||
    /^\d{4}-\d{2}-\d{2}$/.test(t) ||
    /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t)
  );
}

function parseDateRange(dateText: string): { start: string | null; end: string | null } {
  const raw = (dateText || "").replace(/\s+/g, " ").trim();
  if (!raw) return { start: null, end: null };

  const dash = raw.includes("–") ? "–" : raw.includes("—") ? "—" : "-";

  const m = raw.match(/^([A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*(\d{1,2}),\s*(20\d{2})$/);
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

  if (raw.includes(dash)) {
    const parts = raw.split(new RegExp(`\\s*\\${dash}\\s*`));
    return {
      start: coerceISO(parts[0]?.trim() || ""),
      end: coerceISO(parts.slice(1).join(dash).trim()) || null
    };
  }

  return { start: coerceISO(raw), end: null };
}

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
  const $ = cheerio.load(html);

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

    const cellText = tds.map(td =>
      $(td).text().replace(/\s+/g, " ").trim()
    );

    const dateCell = cellText[0] || "";
    const titleCell = cellText[1] || "";

    if (i === 0 && /date/i.test(dateCell)) return;
    if (!looksLikeDateCell(dateCell)) return;

    const link = $tr.find("a[href]").first();
    const linkTitle = link.text().replace(/\s+/g, " ").trim();
    const href = link.attr("href");

    const eventUrl = href
      ? new URL(href, source.url).toString()
      : source.url;

    const title = linkTitle || titleCell;
    if (!title) return;

    const { start, end } = parseDateRange(dateCell);

    const locCell = cellText[2] ?? cellText[3] ?? "";

    rows.push({
      ...(source.defaults ?? {}),
      title,
      start,
      end,
      state_country: locCell,
      tourUrl: eventUrl
    });
  });

  if (!rows.length) {
    throw new Error("html_table: 0 events parsed");
  }

  return rows;
}