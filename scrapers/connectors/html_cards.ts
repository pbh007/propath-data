// scrapers/connectors/html_cards.ts
import * as cheerio from "cheerio";
import type { ProPathEvent } from "../lib/types.js";
import { coerceISO } from "../lib/normalize.js";

type Source = {
  url: string;
  defaults?: Record<string, any>;
  // Optional: allow overriding card selector if needed for other sites later
  cardSelector?: string;
};

function clean(s: string) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function resolve(href: string, base: string) {
  if (!href) return "";
  if (/^https?:\/\//i.test(href)) return href;
  try {
    return new URL(href, base).toString();
  } catch {
    return href;
  }
}

function monthToNumber(mon: string) {
  const m = mon.slice(0, 3).toLowerCase();
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

/**
 * Parses date strings like:
 *  "Mar 25-27 · $1,049+"
 *  "Apr 8-10"
 *  "Apr 6 · $1,100"
 */
function parseCardDateRange(text: string, year: number): { start?: string; end?: string } {
  const t = clean(text).replace(/\u00a0/g, " ").split("·")[0].trim();
  const m = t.match(/\b([A-Za-z]{3,})\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?\b/);
  if (!m) return {};

  const month = monthToNumber(m[1]);
  if (!month) return {};

  const d1 = Number(m[2]);
  const d2 = m[3] ? Number(m[3]) : d1;

  return {
    start: toISO(year, month, d1),
    end: toISO(year, month, d2),
  };
}

/**
 * Infer season year:
 * - If defaults.year is provided, use it.
 * - Else if page contains a gprotourNN token (like gprotour25), interpret as 20NN.
 * - Else fallback to current year.
 */
function inferYear(html: string, url: string, provided?: number) {
  if (provided && Number.isFinite(provided)) return provided;

  const blob = `${url}\n${html}`;
  const m = blob.match(/\bgprotour(\d{2})\b/i);
  if (m) {
    const yy = Number(m[1]);
    if (yy >= 0 && yy <= 99) return 2000 + yy;
  }
  return new Date().getFullYear();
}

/**
 * Extract events from "card" layouts.
 * Works on https://thegprotour.com/pro/programs/gprotour/index.html
 */
export async function runHtmlCards(source: Source): Promise<ProPathEvent[]> {
  const res = await fetch(source.url, {
    headers: {
      "User-Agent": "propath-data-scraper/1.0",
      Accept: "text/html,application/xhtml+xml,*/*",
    },
  });
  if (!res.ok) throw new Error(`html_cards fetch failed: ${res.status} ${res.statusText}`);

  const html = await res.text();
  const $ = cheerio.load(html);

  const year = inferYear(html, source.url, source.defaults?.year ? Number(source.defaults.year) : undefined);

  // GPro page uses: li.bg-event-linked > div.card
  const selector = source.cardSelector || "li.bg-event-linked, div.card";
  const nodes = $(selector).toArray();

  const events: ProPathEvent[] = [];

  for (const node of nodes) {
    const $node = $(node);

    // If we matched li.bg-event-linked, find its internal div.card
    const $card = $node.is("div.card") ? $node : $node.find("div.card").first();
    if (!$card || !$card.length) continue;

    // Title: <p class="card-title"> <a>HERITAGE OPEN</a>
    const title =
      clean($card.find("p.card-title a").first().text()) ||
      clean($card.find(".card-title a").first().text()) ||
      clean($card.find("a").first().text());

    if (!title) continue;

    // Course: second line in your snippet (best effort)
    // Keep it for now only if you later add a dedicated field — we won't store it in schema today.
    // const course = clean($card.find("p.bg-text-smaller").eq(0).text());

    // City/state: <span class="city">Brunswick</span>, GA
    const city = clean($card.find("span.city").first().text());
    const cityStateLine = clean($card.find("span.city").first().parent().text());
    const stMatch = cityStateLine.match(/,\s*([A-Z]{2})\b/);
    const state = stMatch ? stMatch[1] : "";
    const state_country = state ? `${state}, USA` : "";

    // Date line: find first <p> containing a month token
    const pTexts = $card
      .find("p")
      .toArray()
      .map((p) => clean($(p).text()))
      .filter(Boolean);

    const dateLine = pTexts.find((t) => /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t)) || "";

    const { start, end } = parseCardDateRange(dateLine, year);
    const startISO = coerceISO(start ?? null);
    const endISO = coerceISO(end ?? null) ?? startISO;
    if (!startISO) continue;

    // Links
    const infoHref =
      $card.find('a:contains("INFO")').attr("href") ||
      $card.find('a:contains("Info")').attr("href") ||
      $card.find("p.card-title a").attr("href") ||
      "";

    const registerHref =
      $card.find('a:contains("REGISTER")').attr("href") ||
      $card.find('a:contains("Register")').attr("href") ||
      "";

    const tourUrl = resolve(infoHref, source.url) || source.url;
    const signupUrl = resolve(registerHref, source.url);

    events.push({
      ...(source.defaults ?? {}),
      id: undefined,
      title,
      start: startISO,
      end: endISO,
      city: city || clean((cityStateLine.split(",")[0] || "").trim()),
      state_country,
      type: source.defaults?.type || "Event",
      tourUrl,
      signupUrl,
      mondayUrl: "",
      mondayDate: null,
    });
  }

  // Dedupe by title+start
  const seen = new Set<string>();
  const deduped: ProPathEvent[] = [];
  for (const e of events) {
    const k = `${(e.title || "").toLowerCase()}|${(e.start || "").slice(0, 10)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    deduped.push(e);
  }

  if (!deduped.length) throw new Error("html_cards: 0 events parsed (selectors need adjustment)");
  return deduped;
}