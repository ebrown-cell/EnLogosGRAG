// Snowflake → local SQLite snapshot loader for the Buildings entity.
//
// Source of truth: Kent's CE_HISTORICAL.BUILDING_IDENTITY in Snowflake.
//   - BUILDING_CANONICAL  ~9k rows  → buildings
//   - BUILDING_ADDRESS_XREF ~13k rows → building_addresses
//
// Authn: PAT bearer token in EnLogosMCP/.env (SNOWFLAKE_KENT_PAT). The
// REST API is the same one Snowflake's web UI uses; no driver dependency.
// Token type header is required (this is a programmatic access token, not
// an OAuth token, and the server rejects bearer-only requests).
//
// We fetch each table in one shot. At ~22k rows total this is a single
// HTTP partition; if the dataset grows past a few hundred thousand rows
// we'd need to paginate via partitionInfo[].

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SNOWFLAKE_HOST = "https://TLGHCHK-GM14311.snowflakecomputing.com";
const SNOWFLAKE_DB = "CE_HISTORICAL";
const SNOWFLAKE_SCHEMA = "BUILDING_IDENTITY";
const SNOWFLAKE_WAREHOUSE = "COMPUTE_WH";

// Reads SNOWFLAKE_KENT_PAT from EnLogosMCP/.env. We don't ship the PAT
// with EG; the user already has it stashed in the EnLogosMCP repo and
// reusing that file avoids a second copy. Throws if the file or token
// isn't there so the caller can surface a clear error.
function readPat() {
  const here = path.dirname(fileURLToPath(import.meta.url));
  // EG layout: c:/data/dev/GitHub/EnLogosGRAG/app/buildings_loader.js
  // Sibling repo: c:/data/dev/GitHub/EnLogosMCP/.env
  const envPath = path.join(here, "..", "..", "EnLogosMCP", ".env");
  let raw;
  try { raw = fs.readFileSync(envPath, "utf8"); }
  catch (e) {
    throw new Error("Could not read " + envPath + " — " + e.message +
      " (the snapshot loader expects the EnLogosMCP repo to be a sibling of EnLogosGRAG)");
  }
  const m = raw.match(/^SNOWFLAKE_KENT_PAT=(.+)$/m);
  if (!m) throw new Error("SNOWFLAKE_KENT_PAT not found in " + envPath);
  return m[1].trim();
}

// One logical query → Snowflake's REST SQL API, with partition pagination.
// The first POST returns metadata + partition 0; if there are more
// partitions we GET each one and concatenate. Returns { rowType, rows }.
async function executeSql(pat, statement) {
  const headers = {
    "Authorization": "Bearer " + pat,
    "Content-Type": "application/json",
    "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
  };
  const res = await fetch(SNOWFLAKE_HOST + "/api/v2/statements", {
    method: "POST",
    headers,
    body: JSON.stringify({
      statement,
      timeout: 60,
      warehouse: SNOWFLAKE_WAREHOUSE,
      database: SNOWFLAKE_DB,
      schema: SNOWFLAKE_SCHEMA,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error("Snowflake REST " + res.status + ": " + body.slice(0, 500));
  }
  const j = await res.json();
  const rowType = j.resultSetMetaData?.rowType || [];
  const partitions = j.resultSetMetaData?.partitionInfo || [];
  const handle = j.statementHandle;
  const rows = j.data || [];

  // Partition 0 came back inline. Fetch any remaining partitions.
  for (let p = 1; p < partitions.length; p++) {
    const purl = SNOWFLAKE_HOST + "/api/v2/statements/" + handle + "?partition=" + p;
    const pres = await fetch(purl, { method: "GET", headers });
    if (!pres.ok) {
      const body = await pres.text();
      throw new Error("Snowflake partition " + p + " " + pres.status + ": " + body.slice(0, 500));
    }
    const pj = await pres.json();
    if (Array.isArray(pj.data)) rows.push(...pj.data);
  }
  return { rowType, rows };
}

// Build a precomputed lowercase token string used by the matcher.
// Strategy: collect candidate tokens from canonical_address +
// delivery_line_1 + names_sample, then drop:
//   - structural stopwords (state codes, street suffixes, "the", etc.)
//   - tokens that appear in too many buildings (computed across the
//     full snapshot — see filterCorpusFrequency below)
//
// The matcher does set-membership checks on these tokens against the
// document's filename + first-page text, so a token that appears in
// hundreds of buildings would match every document containing that
// English word and drown out signal.
const STOPWORDS = new Set([
  "the", "and", "of", "for", "at", "in", "on", "by", "to", "with", "from",
  "ca", "tx", "ny", "fl", "wa", "or", "az", "nv", "co", "il", "pa", "ga", "nc", "sc", "va", "md",
  "st", "rd", "ave", "blvd", "ln", "dr", "way", "pkwy", "ct", "pl", "hwy", "rte",
  "n", "s", "e", "w", "ne", "nw", "se", "sw",
  "suite", "ste", "unit", "apt", "fl", "floor", "bldg",
  // Project/file-shape noise that frequently shows up in NAMES_SAMPLE
  // entries because they're project descriptions, not clean addresses.
  "renovation", "remodel", "upgrade", "replacement", "repair", "install",
  "installation", "addition", "expansion", "phase", "tenant", "improvement",
  "office", "offices", "warehouse", "lobby", "garage", "parking",
  "panel", "panels", "alarm", "fire", "system", "service", "test", "testing",
  "annual", "inspection", "report", "scope", "site", "main", "new",
  "company", "corp", "corporation", "inc", "llc", "ltd", "group",
  "com", "net", "org", "www",
  // Generic identity tokens — appear constantly in JobFiles project titles.
  "building", "buildings", "campus", "center", "centre", "tower", "plaza",
  "complex", "facility", "facilities", "store", "shop",
  // Common English short words that creep through NAMES_SAMPLE because
  // project titles are full sentences ("use this for the new...").
  "use", "see", "not", "job", "any", "all", "are", "was", "had", "has",
  "but", "can", "did", "you", "her", "his", "one", "two", "old", "out",
  "set", "fix", "etc", "via", "per", "yes", "now", "here", "there", "this",
  "that", "they", "them", "what", "when", "will", "have", "been", "were",
]);

function rawTokens(canonicalAddress, deliveryLine1, namesSample) {
  const parts = [canonicalAddress, deliveryLine1, namesSample]
    .filter(Boolean)
    .join(" | ")
    .toLowerCase();
  const cleaned = parts.replace(/['']/g, "").replace(/[^\w\s]/g, " ");
  const out = new Set();
  for (const tok of cleaned.split(/\s+/)) {
    if (!tok) continue;
    if (tok.length < 3) continue;
    if (STOPWORDS.has(tok)) continue;
    // Drop pure-numeric tokens entirely. They're either ZIPs (which fire
    // on company letterheads, not job locations), years, or street
    // numbers — and street numbers are matched separately by the address
    // strategy in the matcher. Alphanumeric tokens (b183, blk7) survive
    // because they're high-signal building-code references.
    if (/^\d+$/.test(tok)) continue;
    out.add(tok);
  }
  return out;
}

// Count token frequency across the full set of buildings, then drop any
// token that appears in more than `maxFrequency` buildings — those are
// effectively stopwords specific to this corpus (e.g. "diego" in 800
// San Diego buildings, "irvine" in 200, etc.).
function filterCorpusFrequency(perBuildingTokenSets, maxFrequency) {
  const counts = new Map();
  for (const set of perBuildingTokenSets) {
    for (const tok of set) {
      counts.set(tok, (counts.get(tok) || 0) + 1);
    }
  }
  const kept = perBuildingTokenSets.map((set) => {
    const out = new Set();
    for (const tok of set) {
      if ((counts.get(tok) || 0) <= maxFrequency) out.add(tok);
    }
    return out;
  });
  // Stats for diagnostics: how many tokens dropped at the corpus level.
  let dropped = 0;
  for (const [, n] of counts) if (n > maxFrequency) dropped++;
  return { kept, dropped, total: counts.size };
}

// Pull BUILDING_CANONICAL (full snapshot) and upsert into the local
// `buildings` table. Returns { fetched, inserted, updated, error? }.
export async function snapshotBuildings(openDb) {
  const pat = readPat();
  const ts = new Date().toISOString();
  const sql = `
    SELECT BUILDING_UID, CANONICAL_ADDRESS, DELIVERY_LINE_1, LAST_LINE,
           CANONICAL_CITY, CANONICAL_STATE, CANONICAL_ZIP, LAT, LNG,
           COUNTY_NAME, CAMPUS_GROUP_ID, SOURCES, SOURCE_COUNT,
           TOTAL_ACTIVITY, NAME_COUNT, NAMES_SAMPLE, SUITE_COUNT,
           RAW_ADDRESS_COUNT, DATA_QUALITY
      FROM CE_HISTORICAL.BUILDING_IDENTITY.BUILDING_CANONICAL
  `;
  const { rowType, rows } = await executeSql(pat, sql);
  // Map column name → array index so we don't depend on SELECT order.
  const idx = {};
  rowType.forEach((c, i) => { idx[c.name.toUpperCase()] = i; });

  // Pass 1: raw tokenization per row.
  const rawSets = rows.map((r) => rawTokens(
    r[idx.CANONICAL_ADDRESS],
    r[idx.DELIVERY_LINE_1],
    r[idx.NAMES_SAMPLE],
  ));
  // Pass 2: drop tokens that appear in too many buildings. 25 is a
  // conservative cutoff — a token shared across more than 25 buildings
  // out of 9,159 is too generic to disambiguate (~0.3% of the corpus).
  const { kept: filteredSets, dropped, total } =
    filterCorpusFrequency(rawSets, 25);
  console.log("[buildings_loader] token vocab:", total,
    "→ dropped", dropped, "common tokens (>", 25, "buildings)");

  const db = openDb();
  let inserted = 0, updated = 0;
  try {
    db.exec("BEGIN");
    const upsert = db.prepare(
      `INSERT INTO canonical_buildings (
         building_uid, canonical_address, canonical_city, canonical_state,
         canonical_zip, delivery_line_1, last_line, lat, lng,
         county_name, campus_group_id, sources, source_count,
         total_activity, name_count, names_sample, suite_count,
         raw_address_count, data_quality, match_tokens, snapshot_at
       ) VALUES (
         ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
       )
       ON CONFLICT(building_uid) DO UPDATE SET
         canonical_address = excluded.canonical_address,
         canonical_city    = excluded.canonical_city,
         canonical_state   = excluded.canonical_state,
         canonical_zip     = excluded.canonical_zip,
         delivery_line_1   = excluded.delivery_line_1,
         last_line         = excluded.last_line,
         lat               = excluded.lat,
         lng               = excluded.lng,
         county_name       = excluded.county_name,
         campus_group_id   = excluded.campus_group_id,
         sources           = excluded.sources,
         source_count      = excluded.source_count,
         total_activity    = excluded.total_activity,
         name_count        = excluded.name_count,
         names_sample      = excluded.names_sample,
         suite_count       = excluded.suite_count,
         raw_address_count = excluded.raw_address_count,
         data_quality      = excluded.data_quality,
         match_tokens      = excluded.match_tokens,
         snapshot_at       = excluded.snapshot_at`,
    );
    const exists = db.prepare("SELECT 1 FROM canonical_buildings WHERE building_uid = ?");

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const uid = r[idx.BUILDING_UID];
      if (!uid) continue;
      const canonAddr  = r[idx.CANONICAL_ADDRESS] || null;
      const delivery1  = r[idx.DELIVERY_LINE_1] || null;
      const namesSamp  = r[idx.NAMES_SAMPLE] || null;
      const matchTokens = Array.from(filteredSets[i]).sort().join(" ");
      const wasPresent = !!exists.get(uid);
      upsert.run(
        uid,
        canonAddr,
        r[idx.CANONICAL_CITY] || null,
        r[idx.CANONICAL_STATE] || null,
        r[idx.CANONICAL_ZIP] || null,
        delivery1,
        r[idx.LAST_LINE] || null,
        toFloat(r[idx.LAT]),
        toFloat(r[idx.LNG]),
        r[idx.COUNTY_NAME] || null,
        r[idx.CAMPUS_GROUP_ID] || null,
        r[idx.SOURCES] || null,
        toInt(r[idx.SOURCE_COUNT]),
        toInt(r[idx.TOTAL_ACTIVITY]),
        toInt(r[idx.NAME_COUNT]),
        namesSamp,
        toInt(r[idx.SUITE_COUNT]),
        toInt(r[idx.RAW_ADDRESS_COUNT]),
        r[idx.DATA_QUALITY] || null,
        matchTokens,
        ts,
      );
      if (wasPresent) updated++;
      else            inserted++;
    }
    db.exec("COMMIT");
  } catch (e) {
    try { db.exec("ROLLBACK"); } catch { /* ignore */ }
    db.close();
    throw e;
  }
  db.close();
  return { fetched: rows.length, inserted, updated, snapshot_at: ts };
}

// Pull BUILDING_ADDRESS_XREF (full snapshot) and upsert into local
// `building_addresses`. Same idempotent shape as snapshotBuildings.
export async function snapshotBuildingAddresses(openDb) {
  const pat = readPat();
  const ts = new Date().toISOString();
  const sql = `
    SELECT XREF_ID, BUILDING_UID, CANONICAL_ADDRESS, RAW_ADDRESS,
           RAW_STREET, RAW_CITY, RAW_STATE, SUITE_DESIGNATOR, SOURCE_SYSTEM
      FROM CE_HISTORICAL.BUILDING_IDENTITY.BUILDING_ADDRESS_XREF
  `;
  const { rowType, rows } = await executeSql(pat, sql);
  const idx = {};
  rowType.forEach((c, i) => { idx[c.name.toUpperCase()] = i; });

  const db = openDb();
  let inserted = 0, updated = 0, orphaned = 0;
  try {
    db.exec("BEGIN");
    const buildingExists = db.prepare(
      "SELECT 1 FROM canonical_buildings WHERE building_uid = ?",
    );
    const upsert = db.prepare(
      `INSERT INTO building_addresses (
         xref_id, building_uid, canonical_address, raw_address,
         raw_street, raw_city, raw_state, suite_designator, source_system,
         snapshot_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(xref_id) DO UPDATE SET
         building_uid      = excluded.building_uid,
         canonical_address = excluded.canonical_address,
         raw_address       = excluded.raw_address,
         raw_street        = excluded.raw_street,
         raw_city          = excluded.raw_city,
         raw_state         = excluded.raw_state,
         suite_designator  = excluded.suite_designator,
         source_system     = excluded.source_system,
         snapshot_at       = excluded.snapshot_at`,
    );
    const exists = db.prepare("SELECT 1 FROM building_addresses WHERE xref_id = ?");

    for (const r of rows) {
      const xrefId = toInt(r[idx.XREF_ID]);
      const uid    = r[idx.BUILDING_UID];
      if (xrefId == null || !uid) continue;
      // Skip xref rows whose building isn't in our buildings snapshot —
      // could happen if the buildings load was older than the xref load.
      // Caller is expected to load buildings first; we count and report.
      if (!buildingExists.get(uid)) {
        orphaned++;
        continue;
      }
      const wasPresent = !!exists.get(xrefId);
      upsert.run(
        xrefId,
        uid,
        r[idx.CANONICAL_ADDRESS] || null,
        r[idx.RAW_ADDRESS] || null,
        r[idx.RAW_STREET] || null,
        r[idx.RAW_CITY] || null,
        r[idx.RAW_STATE] || null,
        r[idx.SUITE_DESIGNATOR] || null,
        r[idx.SOURCE_SYSTEM] || null,
        ts,
      );
      if (wasPresent) updated++;
      else            inserted++;
    }
    db.exec("COMMIT");
  } catch (e) {
    try { db.exec("ROLLBACK"); } catch { /* ignore */ }
    db.close();
    throw e;
  }
  db.close();
  return { fetched: rows.length, inserted, updated, orphaned, snapshot_at: ts };
}

// Pyrocomm's own office addresses. These are the buildings whose
// letterhead lives on every PO / contract / invoice the company issues —
// without an explicit blocklist, every such doc would false-match against
// the warehouse and every branch office. Address-prefix patterns are
// resolved to building_uids by querying the local snapshot.
const PYROCOMM_OFFICE_ADDRESS_PREFIXES = [
  "15215 Alton",     // Irvine HQ
  "15531 Container", // Huntington Beach warehouse
  "5421 McFadden",   // Huntington Beach branch
  "6960 Koll",       // Pleasanton
  "2149 O",          // O'Toole — apostrophe handling varies
  "680 Fletcher",    // El Cajon
  "23502 Delford",   // Pyro-Lynx
  "10966 Bigge",     // San Leandro
];

// Seed ignored_buildings from the matched local-snapshot rows. Idempotent:
// existing rows are left alone, new matches are inserted with note='seed'.
// Called automatically after snapshotBuildings() so the FK target exists.
// Returns { resolved, inserted } where resolved is how many UIDs we found
// for the patterns and inserted is how many got newly added to the
// ignored_buildings table.
export function seedPyrocommIgnored(openDb) {
  const ts = new Date().toISOString();
  const db = openDb();
  let resolved = 0, inserted = 0;
  try {
    const findUid = db.prepare(
      `SELECT building_uid FROM canonical_buildings
        WHERE LOWER(names_sample) LIKE '%pyro%'
           OR ${PYROCOMM_OFFICE_ADDRESS_PREFIXES.map(() => "canonical_address LIKE ?").join(" OR ")}`,
    );
    const rows = findUid.all(...PYROCOMM_OFFICE_ADDRESS_PREFIXES.map((p) => p + "%"));
    resolved = rows.length;
    const ins = db.prepare(
      `INSERT OR IGNORE INTO ignored_buildings (building_uid, added_at, notes)
       VALUES (?, ?, 'seed: Pyrocomm office')`,
    );
    db.exec("BEGIN");
    for (const r of rows) {
      const result = ins.run(r.building_uid, ts);
      inserted += Number(result.changes || 0);
    }
    db.exec("COMMIT");
  } finally {
    db.close();
  }
  return { resolved, inserted };
}

function toInt(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}
function toFloat(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
