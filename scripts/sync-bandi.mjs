// Scarica i bandi pubblicati da ANAC (dati.anticorruzione.it, formato OCDS) per il mese
// corrente e i due precedenti, li normalizza e li carica su Supabase (tabella "bandi")
// tramite la funzione upsert_bandi(), che fa da sola l'upsert per CIG e segnala le righe
// nuove. Al termine avvisa la Edge Function notify-new-bandi.
//
// Variabili d'ambiente richieste:
//   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SYNC_SECRET, NOTIFY_FUNCTION_URL

import streamChainPkg from "stream-chain";
import streamJsonPkg from "stream-json";
import pickPkg from "stream-json/filters/Pick.js";
import streamArrayPkg from "stream-json/streamers/StreamArray.js";
import { Readable } from "node:stream";

const { chain } = streamChainPkg;
const { parser } = streamJsonPkg;
const { pick } = pickPkg;
const { streamArray } = streamArrayPkg;

const SUPABASE_URL = requireEnv("SUPABASE_URL");
const SERVICE_KEY = requireEnv("SUPABASE_SERVICE_ROLE_KEY");
const SYNC_SECRET = requireEnv("SYNC_SECRET");
const NOTIFY_FUNCTION_URL = requireEnv("NOTIFY_FUNCTION_URL");

const MONTHS_TO_CHECK = 6;
const BATCH_SIZE = 500;
const COMUNI_URL = "https://raw.githubusercontent.com/matteocontrini/comuni-json/master/comuni.json";
const ANAC_PROXY_URL = "https://bandopedia.netlify.app/anac-proxy-edge";
const CHUNK_SIZE = 50 * 1024 * 1024;

const ANAC_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "application/json,text/html;q=0.9,*/*;q=0.8",
  "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
};

const SETTORE_MAP = { works: "lavori", services: "servizi", goods: "forniture" };

const REGIONE_SLUG_OVERRIDES = {
  "Trentino-Alto Adige/Sudtirol": "trentino-alto-adige",
  "Valle d'Aosta/Vallee d'Aoste": "valle-d-aosta",
};

function requireEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error("Variabile d'ambiente mancante: " + name);
    process.exit(1);
  }
  return v;
}

function stripDiacritics(s) {
  return s.normalize("NFD").replace(/[̀-ͯ]/g, "");
}

function slugify(nome) {
  return stripDiacritics(nome)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-+|-+$)/g, "");
}

function toTitleCase(s) {
  return s.toLowerCase().replace(/(^|[\s'-])\p{L}/gu, (c) => c.toUpperCase());
}

function dateOnly(iso) {
  if (!iso || typeof iso !== "string") return null;
  const m = iso.match(/^\d{4}-\d{2}-\d{2}/);
  return m ? m[0] : null;
}

async function buildComuneRegioneMap() {
  const res = await fetch(COMUNI_URL);
  if (!res.ok) throw new Error("Impossibile scaricare comuni.json: HTTP " + res.status);
  const comuni = await res.json();
  const map = new Map();
  for (const c of comuni) {
    const regioneNomeRaw = c.regione && c.regione.nome;
    if (!regioneNomeRaw) continue;
    const regioneNome = stripDiacritics(regioneNomeRaw);
    const overrideKey = Object.keys(REGIONE_SLUG_OVERRIDES).find((k) => stripDiacritics(k) === regioneNome);
    const slug = overrideKey ? REGIONE_SLUG_OVERRIDES[overrideKey] : slugify(regioneNomeRaw);
    const key = c.nome.trim().toUpperCase();
    if (!map.has(key)) map.set(key, slug);
  }
  return map;
}

function monthsToCheck(n) {
  const now = new Date();
  const out = [];
  for (let i = 0; i < n; i++) {
    const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - i, 1));
    out.push({ year: d.getUTCFullYear(), month: String(d.getUTCMonth() + 1).padStart(2, "0") });
  }
  return out;
}

async function fetchRange(year, month, start, end, attempt) {
attempt = attempt || 1;
  const url = ANAC_PROXY_URL + "?year=" + year + "&month=" + month;
  try {
    const res = await fetch(url, {
      headers: Object.assign({}, ANAC_HEADERS, { Range: "bytes=" + start + "-" + end }),
    });
    if (res.status === 404) return null;
    if (res.status !== 206 && res.status !== 200) throw new Error("HTTP " + res.status);
    return res;
  } catch (err) {
    if (attempt >= 6) {
      throw new Error("ANAC " + year + "/" + month + " range " + start + "-" + end + " fallito dopo " + attempt + " tentativi: " + err.message);
    }
    console.warn("[" + year + "-" + month + "] range " + start + "-" + end + " tentativo " + attempt + " fallito (" + err.message + "), riprovo...");
    await new Promise((r) => setTimeout(r, 5000 * attempt));
    return fetchRange(year, month, start, end, attempt + 1);
  }
}
 

function buyerLocality(release) {
  const buyerId = release.buyer && release.buyer.id;
  const parties = release.parties || [];
  const party =
    parties.find((p) => p.id === buyerId && (p.roles || []).includes("buyer")) ||
    parties.find((p) => (p.roles || []).includes("buyer"));
  return (party && party.address && party.address.locality) || null;
}

function buildRows(release, comuneRegioneMap) {
  const tender = release.tender;
  if (!tender || !Array.isArray(tender.lots) || tender.lots.length === 0) return [];

  const localityRaw = buyerLocality(release);
  const comune = localityRaw ? toTitleCase(localityRaw) : null;
  const regione = localityRaw ? comuneRegioneMap.get(localityRaw.trim().toUpperCase()) || null : null;
  const settore = SETTORE_MAP[tender.mainProcurementCategory] || null;
  const dataPubblicazione = dateOnly(tender.tenderPeriod && tender.tenderPeriod.startDate);
  const dataScadenza = dateOnly(tender.tenderPeriod && tender.tenderPeriod.endDate);

  return tender.lots
    .filter((lot) => lot && lot.id)
    .map((lot) => ({
      cig: lot.id,
      titolo: lot.description || tender.description || null,
      comune: comune,
      regione: regione,
      settore: settore,
      importo: lot.value && lot.value.amount != null ? lot.value.amount : null,
      data_pubblicazione: dataPubblicazione,
      data_scadenza: dataScadenza,
      fonte: "ANAC",
    }));
}

async function upsertBatch(rows) {
  const res = await fetch(SUPABASE_URL + "/rest/v1/rpc/upsert_bandi", {
    method: "POST",
    headers: {
      apikey: SERVICE_KEY,
      Authorization: "Bearer " + SERVICE_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ payload: rows }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error("upsert_bandi HTTP " + res.status + ": " + text);
  }
  return res.json();
}

async function processMonth(year, month, comuneRegioneMap, stats) {
  const firstChunk = await fetchRange(year, month, 0, CHUNK_SIZE - 1);
  if (!firstChunk) {
    console.log("[" + year + "-" + month + "] non ancora pubblicato su ANAC, salto.");
    return;
  }

  const contentRange = firstChunk.headers.get("content-range");
  const totalSize = contentRange ? parseInt(contentRange.split("/")[1], 10) : null;

  console.log("[" + year + "-" + month + "] download a blocchi (" + (totalSize || "dimensione sconosciuta") + " byte) e parsing...");

  async function* fullBody() {
    for await (const chunk of firstChunk.body) yield chunk;
    if (!totalSize) return;
    let start = CHUNK_SIZE;
    while (start < totalSize) {
      const end = Math.min(start + CHUNK_SIZE - 1, totalSize - 1);
      const res = await fetchRange(year, month, start, end);
      for await (const chunk of res.body) yield chunk;
      start = end + 1;
    }
  }

  const pipeline = chain([
    Readable.from(fullBody()),
    parser(),
    pick({ filter: "releases" }),
    streamArray(),
  ]);

  let batch = [];
  let monthTotal = 0;
  let monthNew = 0;

  const flush = async () => {
    if (batch.length === 0) return;
    const rows = batch;
    batch = [];
    const result = await upsertBatch(rows);
    monthTotal += rows.length;
    monthNew += result.filter((r) => r.is_new).length;
  };

  for await (const { value: release } of pipeline) {
    for (const row of buildRows(release, comuneRegioneMap)) {
      batch.push(row);
      if (batch.length >= BATCH_SIZE) await flush();
    }
  }
  await flush();

  console.log("[" + year + "-" + month + "] righe processate: " + monthTotal + ", nuove: " + monthNew);
  stats.total += monthTotal;
  stats.new += monthNew;
}

async function notify() {
  try {
    const res = await fetch(NOTIFY_FUNCTION_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-sync-secret": SYNC_SECRET },
      body: "{}",
    });
    const text = await res.text();
    console.log("notify-new-bandi -> HTTP " + res.status + ": " + text);
  } catch (err) {
    console.error("Chiamata a notify-new-bandi fallita:", err.message);
  }
}

async function main() {
  console.log("Costruzione mappa comune -> regione...");
  const comuneRegioneMap = await buildComuneRegioneMap();
  console.log("Mappa pronta: " + comuneRegioneMap.size + " comuni.");

  const stats = { total: 0, new: 0 };
  for (const { year, month } of monthsToCheck(MONTHS_TO_CHECK)) {
    await processMonth(year, month, comuneRegioneMap, stats);
  }

  console.log("Sincronizzazione completata. Righe processate: " + stats.total + ", nuove: " + stats.new + ".");
  await notify();
}

main().catch((err) => {
  console.error("Errore fatale durante la sincronizzazione:", err);
  process.exit(1);
});;

