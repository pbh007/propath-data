import { scrapePGATour } from "./scrape-pga-tour.js";

async function run() {
  console.log("Starting scrape...");
  await scrapePGATour();
  console.log("Scrape complete.");
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
