// scrapers/find-schedule.ts
import * as cheerio from "cheerio";

type LinkHit = {
  text: string;
  href: string;
  score: number;
  reason: string[];
};

function clean(s: string) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function absUrl(href: string, base: string) {
  try {
    return new URL(href, base).toString();
  } catch {
    return "";
  }
}

function scoreLink(textRaw: string, hrefRaw: string) {
  const text = clean(textRaw).toLowerCase();
  const href = clean(hrefRaw).toLowerCase();

  const reason: string[] = [];
  let score = 0;

  const boost = (n: number, r: string) => {
    score += n;
    reason.push(r);
  };

  // Strong keywords
  if (text.includes("schedule")) boost(8, "text:schedule");
  if (href.includes("schedule")) boost(8, "href:schedule");

  if (text.includes("tournaments")) boost(7, "text:tournaments");
  if (href.includes("tournament")) boost(6, "href:tournament");

  if (text.includes("events")) boost(6, "text:events");
  if (href.includes("events")) boost(6, "href:events");

  if (text.includes("calendar")) boost(6, "text:calendar");
  if (href.includes("calendar")) boost(6, "href:calendar");

  if (text.includes("upcoming")) boost(5, "text:upcoming");
  if (href.includes("upcoming")) boost(5, "href:upcoming");

  if (text.includes("registration") || text.includes("register")) boost(4, "text:register");
  if (href.includes("register") || href.includes("registration")) boost(4, "href:register");

  // Useful file types
  if (href.endsWith(".ics")) boost(10, "ics calendar");
  if (href.endsWith(".pdf")) boost(2, "pdf");

  // Avoid junk
  if (text.includes("results")) boost(-2, "results (less ideal)");
  if (href.includes("results")) boost(-2, "results (less ideal)");
  if (href.includes("leaderboard")) boost(-3, "leaderboard");
  if (href.includes("standings")) boost(-3, "standings");

  return { score, reason };
}

async function main() {
  const baseUrl = process.argv[2];
  if (!baseUrl) {
    console.error("Usage: tsx scrapers/find-schedule.ts <tour_homepage_url>");
    process.exit(1);
  }

  const res = await fetch(baseUrl, {
    headers: {
      "User-Agent": "propath-bot/1.0",
      Accept: "text/html,application/xhtml+xml",
    },
  });

  if (!res.ok) {
    console.error(`Fetch failed: ${res.status} ${res.statusText}`);
    process.exit(1);
  }

  const html = await res.text();
  const $ = cheerio.load(html);

  const hits: LinkHit[] = [];

  $("a").each((_, a) => {
    const text = $(a).text();
    const href = $(a).attr("href") || "";
    if (!href) return;

    const full = absUrl(href, baseUrl);
    if (!full) return;

    // Same-site preference (but allow external)
    const { score, reason } = scoreLink(text, full);
    if (score <= 0) return;

    hits.push({
      text: clean(text) || "(no text)",
      href: full,
      score,
      reason,
    });
  });

  hits.sort((a, b) => b.score - a.score);

  console.log("\n--- TOP SCHEDULE CANDIDATES ---");
  for (const h of hits.slice(0, 12)) {
    console.log(`\nScore ${h.score}: ${h.text}\n${h.href}\nReason: ${h.reason.join(", ")}`);
  }

  console.log("\nTip: Try the top 1–3 links with `npm run detect -- <url>`.");
}

main().catch((e) => {
  console.error("find-schedule failed:", e);
  process.exit(1);
});