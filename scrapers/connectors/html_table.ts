import * as cheerio from "cheerio";
import type { ProPathEvent } from "../lib/types.js";
import { coerceISO } from "../lib/normalize.js";

type Source = {
  url: string;
  defaults?: Record<string, string>;
  tableSelector?: string; // optional override
};

function looksLikeDateCell(s: string) {
  const t = (s || "").replace(/\s+/g, " ").trim();
  // handles: "Mar 12-14, 2026", "March 12, 2026", "2026-03-12"
  return (
    /\b20\d{2}\b/.test(t) ||
    /^\d{4}-\d{2}-\d{2}$/.test(t) ||
    /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t)
  );
}

function parseDateRange(dateText: string): { start: string | null; end: string | null } {
  const raw = (dateText || "").replace(/\s+/g, " ").trim();
  if (!raw) return { start: null, end: null };

  // If your coerceISO already handles lots of formats, try simplest first.
  // But for ranges, we split on dash and coerce each side.
  // Examples:
  // "Mar 12-14, 2026"
  // "Mar 12 – 14, 2026"
  // "Mar 12, 2026"
  const dash = raw.includes("–") ? "–" : raw.includes("—") ? "—" : "-";

  // Range with a dash
  if (raw.includes(dash)) {
    // Try to keep year/month context on both sides
    // "Mar 12-14, 2026" => left="Mar 12, 2026", right="Mar 14, 2026"
    const m = raw.match(/^([A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*(\d{1,2}),\s*(20\d{2})$/);
    if (m) {
      const mon = m[1];
      const d1 = m[2];
      const d2 = m[3];
      const yr = m[4];
      const s = coerceISO(`${mon} ${d1}, ${yr}`);
      const e = coerceISO(`${mon} ${d2}, ${yr}`);
      return { start: s, end: e };
    }

    // Fallback: split and coerce each part (less reliable but generic)
    const parts = raw.split(new RegExp(`\\s*\\${dash}\\s*`));
    const left = parts[0]?.trim() || "";
    const right = parts.slice(1).join(dash).trim();

    const start = coerceISO(left);
    const end = coerceISO(right) || null;
    return { start, end };
  }

  // Single date
  return { start: coerceISO(raw), end: null };
}

function pickBestTable($: cheerio.CheerioAPI): cheerio.Cheerio<cheerio.Element> {
  let best = $("table").first();
  let bestScore = -1;

  $("table").each((_, tbl) => {
    const table = $(tbl);
    const trs = table.find("tr").toArray();

    // Score: count rows where first td/th looks like a date AND there's a link somewhere
    let score = 0;
    for (const tr of trs.slice(0, 25)) {
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

export async function runHtmlTable(source: Source): Promise<ProPathEvent[]> {
  const res = await fetch(source.url, { headers: { "user-agent": "propath-bot" } });
  if (!res.ok) throw new Error(`html_table fetch failed: ${res.status} ${res.statusText}`);
  const html = await res.text();
  const $ = cheerio.load(html);

  const table = source.tableSelector ? $(source.tableSelector).first() : pickBestTable($);
  if (!table.length) throw new Error("html_table: no table found");

  const rows: ProPathEvent[] = [];

  table.find("tr").each((i, tr) => {
    const $tr = $(tr);
    const tds = $tr.find("th,td").toArray();
    if (tds.length < 2) return;

    const cellText = tds.map((td) => $(td).text().replace(/\s+/g, " ").trim());
    const dateCell = cellText[0] || "";
    const titleCell = cellText[1] || "";

    // Skip obvious header rows
    if (
      i === 0 &&
      /date/i.test(dateCell) &&
      /(event|tournament|name)/i.test(titleCell)
    ) return;

    // Must look like a date row
    if (!looksLikeDateCell(dateCell)) return;

    // Prefer link text for title + capture event url
    const a = $tr.find("a[href]").first();
    const linkTitle = a.text().replace(/\s+/g, " ").trim();
    const href = a.attr("href");
    const eventUrl = href ? new URL(href, source.url).toString() : "";

    const title = (linkTitle && linkTitle.length >= 2) ? linkTitle : titleCell;
    if (!title || title.length < 2) return;

    const { start, end } = parseDateRange(dateCell);

    // Location/city/state is usually later columns; keep it simple and store in state_country for now
    const locCell = cellText[2] ?? cellText[3] ?? "";
    const state_country = locCell || "";

    rows.push({
      // leave id undefined; your runAll makes stable ids
      ...(source.defaults ?? {}),
      title,
      start,
      end,
      state_country,
      tourUrl: eventUrl || source.url
    });
  });

  const out = rows.filter(r => r.title && r.title.length > 1);

  if (!out.length) {
    throw new Error("html_table: 0 events parsed (tableSelector may need tightening)");
  }

  return out;
}