import fs from "fs";
import path from "path";

export async function scrapePGATour() {
  console.log("Generating test PGA TOUR CSV...");

  const rows = [
    {
      id: "1",
      tour: "PGA TOUR",
      gender: "Men",
      type: "Regular",
      stage: "Season",
      title: "Example Event",
      start: "2026-01-10",
      end: "2026-01-13",
      city: "Palm Beach",
      state_country: "FL, USA",
      tourUrl: "https://pgatour.com",
      signupUrl: "",
      mondayUrl: "",
      mondayDate: ""
    }
  ];

  const headers = Object.keys(rows[0]).join(",");
  const body = rows.map(r => Object.values(r).join(",")).join("\n");
  const csv = `${headers}\n${body}`;

  const outputPath = path.resolve("data/ProPathEvents2026-MasterEvents.csv");

  fs.writeFileSync(outputPath, csv);

  console.log("CSV written successfully.");
}
