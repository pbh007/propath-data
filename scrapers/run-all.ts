import fs from "fs";
import path from "path";
import { writeEventsCsv } from "./lib/writeCsv.js";
import { runJsonApi } from "./connectors/json_api.js";
import { runHtmlTable } from "./connectors/html_table.js";
import { runHtmlBlocks } from "./connectors/html_blocks.js";

type Source = {
  id: string;
  name: string;
  connector: "json_api" | "html_table" | "html_blocks";
  url: string;
  output: string;
  defaults?: Record<string, string>;
  tableSelector?: string;
};

function isValidUrl(url?: string) {
  if (!url) return false;
  if (url.includes("PASTE_URL_HERE")) return false;

  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
}

async function runSource(s: Source): Promise<boolean> {
  console.log(`\n=== ${s.name} (${s.connector}) ===`);

  // Skip invalid URLs
  if (!isValidUrl(s.url)) {
    console.log(`⚠ Skipping ${s.name}: invalid or placeholder URL.`);
    return false;
  }

  try {
    let events = [];

    if (s.connector === "json_api") {
      events = await runJsonApi(s);
    } else if (s.connector === "html_table") {
      events = await runHtmlTable(s);
    } else if (s.connector === "html_blocks") {
      events = await runHtmlBlocks(s);
    } else {
      console.log(`⚠ Unknown connector for ${s.name}, skipping.`);
      return false;
    }

    if (!events || !events.length) {
      console.log(`⚠ ${s.name}: 0 events returned. Skipping write.`);
      return false;
    }

    writeEventsCsv(s.output, events);
    console.log(`✓ ${s.name}: ${events.length} events written.`);
    return true;

  } catch (err) {
    console.error(`✖ ${s.name} failed:`);
    console.error(err);
    return false;
  }
}

async function main() {
  const sourcesPath = path.resolve("data", "sources.json");

  if (!fs.existsSync(sourcesPath)) {
    throw new Error("sources.json not found in data folder.");
  }

  const sources = JSON.parse(
    fs.readFileSync(sourcesPath, "utf8")
  ) as Source[];

  if (!Array.isArray(sources) || !sources.length) {
    throw new Error("sources.json is empty or invalid.");
  }

  let successCount = 0;

  for (const s of sources) {
    const ok = await runSource(s);
    if (ok) successCount++;
  }

  if (successCount === 0) {
    throw new Error("All sources failed.");
  }

  console.log(`\nComplete. ${successCount}/${sources.length} sources succeeded.`);
}

main().catch((err) => {
  console.error("\nScraper failed:");
  console.error(err);
  process.exit(1);
});
