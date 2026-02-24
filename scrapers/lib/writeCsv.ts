import fs from "fs";
import path from "path";
import type { ProPathEvent } from "./types.js";

const COLS: (keyof ProPathEvent)[] = [
  "id",
  "tour",
  "gender",
  "type",
  "stage",
  "title",
  "start",
  "end",
  "city",
  "state_country",
  "tourUrl",
  "signupUrl",
  "mondayUrl",
  "mondayDate",
];

function csvEscape(v: unknown) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function writeEventsCsv(outFileName: string, rows: ProPathEvent[]) {
  const header = COLS.join(",");
  const body = rows.map((r) => COLS.map((c) => csvEscape((r as any)[c])).join(",")).join("\n");
  const csv = `${header}\n${body}\n`;

  // ✅ Allow either:
  // - "data/_incoming_x.csv" (already includes data/)
  // - "_incoming_x.csv" or "ProPath-Whatever.csv" (write under data/)
  const normalized = (outFileName || "").replace(/\\/g, "/").trim();
  const outPath = normalized.startsWith("data/")
    ? path.resolve(normalized)
    : path.resolve("data", normalized);

  // ✅ Ensure directory exists (prevents ENOENT)
  fs.mkdirSync(path.dirname(outPath), { recursive: true });

  fs.writeFileSync(outPath, csv, "utf8");
  console.log(`Wrote ${outPath} (${rows.length} rows)`);
}
