export function coerceISO(input?: unknown): string | null {
  if (!input) return null;

  let s = String(input).trim();
  if (!s) return null;

  // Already ISO format
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  // Strip time portion if present (e.g. "2026-02-01 00:00")
  if (s.includes(" ")) {
    const firstToken = s.split(" ")[0];
    if (/^\d{4}-\d{2}-\d{2}$/.test(firstToken)) return firstToken;
  }

  // YYYY/MM/DD
  const ymdSlash = s.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})$/);
  if (ymdSlash) {
    const [, y, m, d] = ymdSlash;
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }

  // MM/DD/YYYY
  const mdyyyy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mdyyyy) {
    const [, m, d, y] = mdyyyy;
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }

  // MM/DD/YY
  const mdyy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2})$/);
  if (mdyy) {
    const [, m, d, yy] = mdyy;
    const y = Number(yy) < 50 ? `20${yy}` : `19${yy}`;
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }

  // Remove leading weekday if present
  // Example: "Monday, February 16, 2026"
  s = s.replace(
    /^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s*/i,
    ""
  );

  // Handle long form: "February 16, 2026"
  const monthMap: Record<string, string> = {
    january: "01",
    february: "02",
    march: "03",
    april: "04",
    may: "05",
    june: "06",
    july: "07",
    august: "08",
    september: "09",
    october: "10",
    november: "11",
    december: "12"
  };

  const longForm = s.match(/^([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})$/);
  if (longForm) {
    const [, monthRaw, dayRaw, year] = longForm;
    const month = monthMap[monthRaw.toLowerCase()];
    if (!month) return null;
    const day = dayRaw.padStart(2, "0");
    return `${year}-${month}-${day}`;
  }

  // If nothing matched, return null (don't return garbage)
  return null;
}
