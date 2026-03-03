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
    /^\d{4}-\d{2}-\d{2}$/.test(t) ||
    /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t)
  );
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

  /* ================= DEBUG ================= */
  console.log("========== GPRO DEBUG ==========");
  console.log("HTML length:", html.length);
  console.log("Contains HERITAGE:", html.toUpperCase().includes("HERITAGE"));
  console.log("Table count:", $("table").length);
  console.log("================================");
  /* ========================================= */

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

    const titleCell = $(tds[1])
      .text()
      .replace(/\s+/g, " ")
      .trim();

    if (!titleCell || /^register$/i.test(titleCell)) return;

    rows.push({
      ...(source.defaults ?? {}),
      title: titleCell,
      start: null,
      end: null
    });
  });

  if (!rows.length) {
    throw new Error("html_table: 0 events parsed");
  }

  return rows;
}