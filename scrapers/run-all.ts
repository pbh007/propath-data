import fs from "fs";
import path from "path";
import { writeEventsCsv } from "./lib/writeCsv.js";
import { runJsonApi } from "./connectors/json_api.js";
import { runHtmlTable } from "./connectors/html_table.js";

type Source = {
  id: string;
  name: string;
  connector: "json_api" | "html_table";
  url: string;
  output: string;
  defaults?: Record<string, string>;
  tableSelector?: string;
};

async function runSource(s: Source) {
  console.log(`\n=== ${s.name} (${s.connector}) ===`);

  let events = [];
  if (s.connector === "json_api") events = await runJsonApi(s);
  else if (s.connector === "html_table") events = await runHtmlTable(s);
  else throw new Error(`Unknown connector: ${s.connector}`);

  // Guardrails
  if (!events.length) throw new Error(`${s.id}: 0 events returned (likely parsing broke)`);

  writeEventsCsv(s.output, events);
}

async function main() {
  const sourcesPath = path.resolve("data", "sources.json");
  const sources = JSON.parse(fs.readFileSync(sourcesPath, "utf8")) as Source[];

  for (const s of sources) {
    await runSource(s);
  }

  console.log("\nAll sources complete.");
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
