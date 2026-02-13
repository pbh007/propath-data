import fs from "fs";
import path from "path";
import type { ProPathEvent } from "./types";



const COLS: (keyof ProPathEvent)[] = [
  "id","tour","gender","type","stage","title","start","end","city","state_country",
  "tourUrl","signupUrl","mondayUrl","mondayDate"
];

function csvEscape(v: unknown) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function writeEventsCsv(outFileName: string, rows: ProPathEvent[]) {
  const header = COLS.join(",");
  const body = rows
    .map(r => COLS.map(c => csvEscape((r as any)[c])).join(","))
    .join("\n");

  const csv = `${header}\n${body}\n`;
  const outPath = path.resolve("data", outFileName);

  fs.writeFileSync(outPath, csv, "utf8");
  console.log(`Wrote ${outPath} (${rows.length} rows)`);
}
