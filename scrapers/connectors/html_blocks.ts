import * as cheerio from "cheerio";
import type { ProPathEvent } from "../lib/types";
import { coerceISO } from "../lib/normalize";


type Source = {
  url: string;
  defaults?: Record<string, string>;
};

function addDays(iso: string, days: number) {
  const d = new Date(iso + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function parseDayCount(text: string): number {
  // examples: "1-Day $275", "2-Day $540", "3-Day $0"
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

  // The page uses a repeating pattern; easiest robust approach:
  // Find headings that look like "Monday, February 16, 2026" and walk forward.
  const events: ProPathEvent[] = [];

  // Pick elements that contain a day-of-week + year pattern
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

    // Walk the DOM forward until the next date heading.
    // We will read a small window of text and links to form one event.
    let cursor = $(dateEl).next();
    const windowText: string[] = [];
    const links: { text: string; href: string }[] = [];

    // Collect a bounded chunk (prevents grabbing the whole page)
    for (let i = 0; i < 25 && cursor.length; i++) {
      const t = cursor.text().replace(/\s+/g, " ").trim();
      if (t) windowText.push(t);

      cursor.find("a").each((_, a) => {
        const txt = $(a).text().trim();
        const href = $(a).attr("href");
        if (href) links.push({ text: txt, href: new URL(href, source.url).toString() });
      });

      // stop if we hit another date heading-like element
      if (/(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),/.test(t) && /\b20\d{2}\b/.test(t)) break;

      cursor = cursor.next();
    }

    // Heuristic extraction based on the known repeating pattern on this page:
    // [Course name], [City, ST], [Event title], [X-Day $Fee ... (MLGT/TD)]
    const joined = windowText.join(" | ");

    // Course is usually early; city/state is next line like "Stuart, FL"
    const cityStateMatch = joined.match(/([A-Za-z .'-]+,\s*[A-Z]{2})/);
    const state_country = cityStateMatch ? `${cityStateMatch[0]}, USA` : "";

    // Event title: usually the thing with "Classic" / "Contest" etc.
    // We can grab from the "Event Info" link if present (its anchor text is the title in your page)
    const eventInfo = links.find((l) => /Event Info/i.test(l.text));
    const title = eventInfo ? eventInfo.text : "";

    // Fee / days: usually contains "1-Day $275" etc
    const feeMatch = joined.match(/\d+\s*-\s*Day\s*\$\s*[\d,]+/i);
    const feeText = feeMatch ? feeMatch[0] : "1-Day $0";
    const dayCount = parseDayCount(feeText);
    const fee = parseFee(feeText);

    // End date = start + (dayCount-1)
    const end = dayCount > 1 ? addDays(start, dayCount - 1) : start;

    // Register link is perfect for signupUrl
    const register = links.find((l) => /Register/i.test(l.text));

    // Tour/type tags: MLGT vs TD appears in the block text; we map it to "type"
    const isTD = /(^|[^A-Z])TD([^A-Z]|$)/.test(joined);
    const type = isTD ? "Training Division" : "Event";

    // Course name heuristic: the first chunk often is course name. Try to pull something usable.
    // We'll set it as city field? No — better: set city as the city, and ignore course for now unless you add "location" field later.
    const city = cityStateMatch ? cityStateMatch[0].split(",")[0].trim() : "";

    // Only push if we have a usable title
    if (!title) continue;

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

    // NOTE: This will create duplicates because the page has MLGT + TD for the same date/course.
    // That’s OK — your app can filter by type, OR we can dedupe later if you want.
  }

  // Basic dedupe by title+start+type
  const seen = new Set<string>();
  const deduped: ProPathEvent[] = [];
  for (const e of events) {
    const key = `${e.title}|${e.start}|${e.type}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(e);
  }

  if (!deduped.length) throw new Error("html_blocks: 0 events parsed (selectors need adjustment)");
  return deduped;
}
