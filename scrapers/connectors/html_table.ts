import * as cheerio from "cheerio";
import type { ProPathEvent } from "../lib/types.js";
import { coerceISO } from "../lib/normalize.js";

type Source = {
  url: string;
  defaults?: Record<string, string>;
  tableSelector?: string; // optional override
};

export async function runHtmlTable(source: Source): Promise<ProPathEvent[]> {
  const res = await fetch(source.url, { headers: { "user-agent": "propath-bot" } });
  if (!res.ok) throw new Error(`html_table fetch failed: ${res.status} ${res.statusText}`);
  const html = await res.text();
  const $ = cheerio.load(html);

  const table = source.tableSelector ? $(source.tableSelector) : $("table").first();
  if (!table.length) throw new Error("html_table: no table found");

  const rows: ProPathEvent[] = [];
  table.find("tr").each((i, tr) => {
    const cells = $(tr).find("th,td").toArray().map(td => $(td).text().trim());
    if (cells.length < 2) return;

    // Very generic guess: [date, title, location...]
    const dateCell = cells[0];
    const titleCell = cells[1];
    const locCell = cells[2] ?? "";

    // If header row, skip
    if (i === 0 && /date/i.test(dateCell) && /event|tournament/i.test(titleCell)) return;

    rows.push({
      id: String(i),
      ...(source.defaults ?? {}),
      title: titleCell,
      start: coerceISO(dateCell),
      end: null,
      state_country: locCell
    });
  });

  return rows.filter(r => r.title && r.title.length > 1);
}
