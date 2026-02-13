import type { ProPathEvent } from "../lib/types.js";
import { coerceISO } from "../lib/normalize.js";



type Source = {
  url: string;
  defaults?: Record<string, string>;
};

export async function runJsonApi(source: Source): Promise<ProPathEvent[]> {
  const res = await fetch(source.url);
  if (!res.ok) throw new Error(`json_api fetch failed: ${res.status} ${res.statusText}`);
  const data = await res.json();

  // NOTE: This part changes per API shape. For now we assume data.events exists.
  const rawEvents = Array.isArray((data as any).events) ? (data as any).events : [];

  return rawEvents.map((e: any, idx: number) => ({
    id: e.id ?? String(idx + 1),
    ...(source.defaults ?? {}),
    title: e.title ?? e.name ?? "",
    start: coerceISO(e.start ?? e.startDate) ?? null,
    end: coerceISO(e.end ?? e.endDate) ?? null,
    city: e.city ?? "",
    state_country: e.state_country ?? e.location ?? "",
    tourUrl: e.tourUrl ?? "",
    signupUrl: e.signupUrl ?? "",
    mondayUrl: e.mondayUrl ?? "",
    mondayDate: coerceISO(e.mondayDate) ?? null
  }));
}
