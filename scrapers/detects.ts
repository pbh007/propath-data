// scrapers/detect.ts
import * as cheerio from "cheerio";

type Guess =
  | { connector: "json_api"; confidence: number; notes: string[] }
  | { connector: "html_table"; confidence: number; notes: string[]; tableSelector: string }
  | { connector: "html_cards"; confidence: number; notes: string[]; cardSelector: string }
  | { connector: "html_blocks"; confidence: number; notes: string[] };

function clean(s: string) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function slug(s: string) {
  return clean(s)
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 60);
}

async function fetchText(url: string) {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "propath-bot/1.0",
      Accept: "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    },
  });
  const contentType = res.headers.get("content-type") || "";
  const text = await res.text();
  return { ok: res.ok, status: res.status, contentType, text };
}

function guessConnector(url: string, contentType: string, body: string): Guess {
  const notes: string[] = [];

  // JSON detection
  if (contentType.includes("application/json") || body.trim().startsWith("{") || body.trim().startsWith("[")) {
    notes.push("Looks like JSON response.");
    return { connector: "json_api", confidence: 0.9, notes };
  }

  const $ = cheerio.load(body);

  // Table detection: look for a table with multiple rows and at least 2 columns
  let bestTableSel = "table";
  let bestScore = 0;

  $("table").each((i, el) => {
    const $t = $(el);
    const rows = $t.find("tr");
    let goodRows = 0;

    rows.each((_, tr) => {
      const tds = $(tr).find("td");
      if (tds.length >= 2) goodRows++;
    });

    const score = goodRows;
    if (score > bestScore) {
      bestScore = score;
      bestTableSel = i === 0 ? "table" : `table:nth-of-type(${i + 1})`;
    }
  });

  if (bestScore >= 6) {
    notes.push(`Found a strong table (${bestScore} usable rows).`);
    return { connector: "html_table", confidence: 0.85, notes, tableSelector: bestTableSel };
  }

  // Cards detection: repeated blocks with buttons/links like INFO/REGISTER
  // Heuristic: look for many "register" links and repeated container structure.
  const registerLinks = $("a")
    .toArray()
    .map((a) => clean($(a).text()).toLowerCase())
    .filter((t) => t === "register" || t.includes("register")).length;

  if (registerLinks >= 3) {
    notes.push(`Found ${registerLinks} register-ish links; likely cards/grid.`);
    // Try common containers
    const candidates = [
      ".card",
      ".event",
      ".tournament",
      ".grid > *",
      ".row > *",
      "section a:contains(Register)",
      "a:contains(Register)",
    ];

    // crude “best” selector guess:
    // If there are cards, often .card exists; otherwise fallback to "a:contains(Register)".
    let cardSelector = ".card";
    if ($(".card").length >= 3) cardSelector = ".card";
    else if ($(".event").length >= 3) cardSelector = ".event";
    else if ($(".tournament").length >= 3) cardSelector = ".tournament";
    else cardSelector = "a:contains(Register)";

    return { connector: "html_cards", confidence: 0.65, notes, cardSelector };
  }

  // Blocks detection: headings with dates etc
  const dayHeadings = $("h1,h2,h3,h4,h5,div,span,strong,b")
    .toArray()
    .map((el) => clean($(el).text()))
    .filter((t) => /(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),/.test(t) && /\b20\d{2}\b/.test(t)).length;

  if (dayHeadings >= 3) {
    notes.push(`Found ${dayHeadings} day-of-week date headings; likely blocks.`);
    return { connector: "html_blocks", confidence: 0.7, notes };
  }

  notes.push("No strong table/cards/date blocks detected. Defaulting to html_blocks.");
  return { connector: "html_blocks", confidence: 0.35, notes };
}

function makeSourcesEntry(url: string, guess: Guess) {
  const idBase = slug(new URL(url).hostname + "_" + new URL(url).pathname.split("/").pop());

  const base: any = {
    id: idBase || "new_source",
    name: `New Source (${new URL(url).hostname})`,
    connector: guess.connector,
    url,
    output: `data/_incoming_${idBase || "new_source"}.csv`,
    defaults: { tour: "TODO Tour Name", gender: "Men", type: "Event" },
    mergeMode: "replace_upcoming",
    minValidRows: 5,
  };

  // If it's BlueGolf, default to link_only unless you override
  if (new URL(url).hostname.toLowerCase().includes("bluegolf.com")) {
    base.policy = "link_only";
  }

  if (guess.connector === "html_table") base.tableSelector = guess.tableSelector;
  if (guess.connector === "html_cards") base.cardSelector = guess.cardSelector;

  // For cards, patch_only is often safer
  if (guess.connector === "html_cards") {
    base.mergeMode = "patch_only";
    base.minValidRows = 1;
  }

  return base;
}

async function main() {
  const url = process.argv[2];
  if (!url) {
    console.error("Usage: tsx scrapers/detect.ts <url>");
    process.exit(1);
  }

  const { ok, status, contentType, text } = await fetchText(url);
  if (!ok) {
    console.error(`Fetch failed: ${status}`);
    process.exit(1);
  }

  const guess = guessConnector(url, contentType, text);
  const entry = makeSourcesEntry(url, guess);

  console.log("\n--- DETECT RESULT ---");
  console.log(JSON.stringify({ ...guess, url }, null, 2));

  console.log("\n--- PASTE INTO data/sources.json ---");
  console.log(JSON.stringify(entry, null, 2));

  console.log("\n--- NOTES ---");
  for (const n of guess.notes) console.log(`- ${n}`);
}

main().catch((e) => {
  console.error("detect failed:", e);
  process.exit(1);
});