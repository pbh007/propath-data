import * as cheerio from "cheerio";
import type { ProPathEvent } from "../lib/types.js";
import { coerceISO } from "../lib/normalize.js";

type Source = {
  url: string;
  defaults?: Record<string, string>;
};

function addDays(iso: string, days: number) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return iso;

  const d = new Date(`${iso}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return iso;

  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function parseDayCount(text: string): number {
  const m = text.match(/(\d+)\s*-\s*Day/i);
  const n = m ? Number(m[1]) : 1;
  return Number.isFinite(n) && n >= 1 ? n : 1;
}

function parseFee(text: string): string {
  const m = text.match(/\$(\d[\d,]*)/);
  return m ? m[1].replace(/,/g, "") : "";
}

export async function runHtmlBlocks(source: Source): Promise<ProPathEvent[]> {
  const res = await fetch(source.url, { headers: { "user-agent": "propath-bot" } });
  if (!res.ok) throw new Error(`html_blocks fetch failed: ${res.status} ${res.statusText}`);

  const html = await res.text();
  const $ = cheerio.load(html);

  const events: ProPathEvent[] = [];

  const dateCandidates = $("h1,h2,h3,h4,h5,div,span,strong,b,td")
    .toArray()
    .filter((el) => {
      const t = $(el).text().trim();
      return /(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),/.test(t) && /\b20\d{2}\b/.test(t);
    });

  for (const dateEl of dateCandidates) {
    const dateText = $(dateEl).text().trim();
    const start = coerceISO(dateText);
    if (!start) continue;

    let cursor = $(dateEl).next();
    const windowText: string[] = [];
    const links: { text: string; href: string }[] = [];

    for (let i = 0; i < 25 && cursor.length; i++) {
      const t = cursor.text().replace(/\s+/g, " ").trim();
      if (t) windowText.push(t);

      cursor.find("a").each((_, a) => {
        const txt = $(a).text().trim();
        const href = $(a).attr("href");
        if (href) {
          try {
            links.push({ text: txt, href: new URL(href, source.url).toString() });
          } catch {
            // ignore bad urls
          }
        }
      });

      if (/(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),/.test(t) && /\b20\d{2}\b/.test(t)) break;

      cursor = cursor.next();
    }

    const joined = windowText.join(" | ");
    const cityStateMatch = joined.match(/([A-Za-z .'-]+,\s*[A-Z]{2})/);
    const state_country = cityStateMatch ? `${cityStateMatch[0]}, USA` : "";

    let title = "";

    const joinedClean = windowText
      .map((t) => t.replace(/\s+/g, " ").trim())
      .filter(Boolean);

    const junk = (s: string) => {
      const x = s.toLowerCase();
      if (x === "event info") return true;
      if (x === "register") return true;
      if (/\d+\s*-\s*day\s*\$\s*[\d,]+/i.test(s)) return true;
      if (/^[A-Za-z .'-]+,\s*[A-Z]{2}$/.test(s)) return true;
      if (x.includes("minor league golf tour")) return true;
      if (x.includes("training division")) return true;
      return false;
    };

    const looksLikeTitle = (s: string) => {
      const x = s.toLowerCase();
      return (
        s.length >= 6 &&
        !junk(s) &&
        (
          x.includes("classic") ||
          x.includes("open") ||
          x.includes("championship") ||
          x.includes("shootout") ||
          x.includes("invitational") ||
          x.includes("series") ||
          x.includes("qualifier") ||
          x.includes("club") ||
          x.includes("cup")
        )
      );
    };

    title =
      joinedClean.find(looksLikeTitle) ||
      joinedClean.find((s) => !junk(s) && s.length >= 6) ||
      "";

    if (/^event info$/i.test(title)) title = "";
    if (!title) continue;

    const feeMatch = joined.match(/\d+\s*-\s*Day\s*\$\s*[\d,]+/i);
    const feeText = feeMatch ? feeMatch[0] : "1-Day $0";
    const dayCount = parseDayCount(feeText);
    parseFee(feeText); // kept available if you later want fee field

    const end = dayCount > 1 ? addDays(start, dayCount - 1) : start;

    const register = links.find((l) => /Register/i.test(l.text));
    const isTD = /(^|[^A-Z])TD([^A-Z]|$)/.test(joined);
    const type = isTD ? "Training Division" : "Event";
    const city = cityStateMatch ? cityStateMatch[0].split(",")[0].trim() : "";

    events.push({
      ...(source.defaults ?? {}),
      id: undefined,
      title,
      start,
      end,
      city,
      state_country,
      type,
      signupUrl: register?.href ?? "",
      tourUrl: source.url,
      mondayUrl: "",
      mondayDate: null
    });
  }

  const seen = new Set<string>();
  const deduped: ProPathEvent[] = [];
  for (const e of events) {
    const key = `${e.title}|${e.start}|${e.type}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(e);
  }

  if (!deduped.length) {
    console.warn(`html_blocks: 0 events parsed for ${source.url}`);
    return [];
  }

  return deduped;
}