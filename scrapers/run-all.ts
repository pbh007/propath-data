import fs from "fs";
import path from "path";
import { writeEventsCsv } from "./lib/writeCsv";
import { runJsonApi } from "./connectors/json_api";
import { runHtmlTable } from "./connectors/html_table";
import { runHtmlBlocks } from "./connectors/html_blocks";


type Source = {
  id: string;
  name: string;
  connector: "json_api" | "html_table" | "html_blocks";
  url: string;
  output: string;
  defaults?: Record<string, string>;
  tableSelector?: string;
};

async function runSource(s: Source) {
  console.log(`\n=== ${s.name} (${s.connector}) ===`);

  let events = [];

  if (s.connector === "json_api") {
    events = await runJsonApi(s);
  } else if (s.connector === "html_table") {
    events = await runHtmlTable(s);
  } else if (s.connector === "html_blocks") {
    events = await runHtmlBlocks(s);
  } else {
    throw new Error(`Unknown connector: ${s.connector}`);
  }

  // Safety guard: prevents silent failure
  if (!events || !events.length) {
    throw new Error(`${s.id}: 0 events returned (likely parsing broke)`);
  }

  writeEventsCsv(s.output, events);

  console.log(`✓ ${s.name}: ${events.length} events written.`);
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

  for (const s of sources) {
    await runSource(s);
  }

  console.log("\nAll sources complete.");
}

main().catch((err) => {
  console.error("\nScraper failed:");
  console.error(err);
  process.exit(1);
});
