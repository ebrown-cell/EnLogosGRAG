// Buildings extractor: scans JobFiles documents (filename/path + extracted
// text) for building references, find-or-creates rows in the `buildings`
// table for each distinct reference, and links documents to those rows.
//
// Two complementary signals identify candidate buildings:
//   1. Project-name match — the filename or first-page text contains tokens
//      that appear in a canonical building's NAMES_SAMPLE (e.g. "JPL B183",
//      "Bordeaux Apts", "9190 Irvine Center"). Most JobFiles documents are
//      named for the project, not the address, so this is the primary signal.
//   2. Street-address match — the filename or text contains a raw street
//      address that maps via BUILDING_ADDRESS_XREF (e.g. "4800 Oak Grove"
//      → JPL). Useful when the project name is generic ("Proposal.pdf")
//      but the address is in the body.
//
// Outputs:
//   - One row in `buildings` per distinct canonical_uid observed (deduped
//     across documents) — carries the canonical metadata so unmatched
//     extractions can also live here as orphans.
//   - One row in `document_buildings` per (document, buildings.id) pair,
//     with confidence + source labelling why the link exists.
//
// Mirrors hasher.js / extractor.js: singleton worker, status polled via
// /api/buildings-match-status.

import { getBuildingExtractorRules, reloadBuildingExtractorRules } from "./classifier.js";

const state = {
  running: false,
  shouldStop: false,
  total: 0,
  done: 0,        // documents processed (with or without a match)
  matched: 0,     // documents that got at least one document_buildings row
  linksAdded: 0,  // total document_buildings rows added (1 doc may match many)
  failed: 0,
  currentDoc: null,
  startedAt: null,
  finishedAt: null,
  lastError: null,
};

export function getStatus() {
  return { ...state };
}

export function stop() {
  if (!state.running) return false;
  state.shouldStop = true;
  return true;
}

// Tokens with these properties are too noisy to use on their own. They
// would otherwise match thousands of buildings (every JPL address has
// "blvd", every campus has "center"). Same idea as buildings_loader's
// STOPWORDS but tighter — these are the tokens that make it through
// the loader's filter but are still unsafe at match time.
const MATCH_STOPWORDS = new Set([
  "building", "buildings", "campus", "center", "centre", "plaza", "tower",
  "north", "south", "east", "west", "phase", "block", "level",
  "fire", "alarm", "panel", "system", "service", "annual", "inspection",
  "test", "report", "proposal", "quote", "invoice", "permit", "drawing",
  "rfi", "rfc", "loi", "ntp", "co", "po", "coi", "bom", "ahj",
  "the", "and", "for", "with",
  // Common file-shaped fragments
  "scan", "copy", "rev", "draft", "final", "signed",
]);

// Tokens for matching. Lowercase, drop punctuation, drop stopwords.
// Keeps numeric tokens (street numbers, building numbers like "B183") —
// those are very high signal.
function tokenize(s) {
  if (!s) return [];
  const cleaned = String(s).toLowerCase().replace(/['']/g, "").replace(/[^\w\s]/g, " ");
  const out = [];
  for (const tok of cleaned.split(/\s+/)) {
    if (!tok) continue;
    if (tok.length < 3) continue;
    if (MATCH_STOPWORDS.has(tok)) continue;
    out.push(tok);
  }
  return out;
}

// Strict version: numeric tokens only, plus the next 1-2 word tokens
// after a numeric. Targets street-address fragments like "4800 oak grove"
// or "1 legoland dr". Used when matching against raw_street so we don't
// misfire on a building whose name happens to share an English word with
// the filename.
//
// Accepts 1-5 digit street numbers (JobFiles uses many "1 X St" / "2 Y Ave"
// folder names). For both 2-token and 3-token candidates the trailing tokens
// must be alphabetic — that prevents random "<digit> <year> <month>" date
// fragments from looking like addresses.
function streetCandidates(s) {
  if (!s) return [];
  const cleaned = String(s).toLowerCase().replace(/['']/g, "").replace(/[^\w\s]/g, " ");
  const toks = cleaned.split(/\s+/).filter(Boolean);
  const out = [];
  for (let i = 0; i < toks.length; i++) {
    if (!/^\d{1,5}$/.test(toks[i])) continue;
    const tail = [];
    for (let j = i + 1; j < Math.min(i + 3, toks.length); j++) {
      const t = toks[j];
      if (t.length < 2) break;
      if (!/^[a-z]+$/.test(t)) break;
      tail.push(t);
    }
    if (tail.length >= 1) out.push(toks[i] + " " + tail[0]);
    if (tail.length >= 2) out.push(toks[i] + " " + tail[0] + " " + tail[1]);
  }
  return out;
}

let pathRemapper = (p) => p;
export function setPathRemapper(fn) { pathRemapper = fn; }

// opts: { openDb, onlyMissing? = true, sdt? = ["JobFiles","Sales"], mode? = "all" }
//   onlyMissing: skip documents that already have a document_buildings row.
//   sdt: scope the run to one or more source-data-types. Defaults to
//        the two corpora that reference buildings — JobFiles + Sales.
//        Vendor docs almost never reference job sites and are excluded.
//        Pass a string for one SDT or an array for multiple.
//   mode controls which signals the extractor reads from each document:
//     "all"     — filename + extract text (default; full coverage)
//     "file"    — filename / path tokens only
//     "content" — extract text only (first ~2000 chars)
export function start(opts) {
  if (state.running) return false;
  state.running = true;
  state.shouldStop = false;
  state.total = 0;
  state.done = 0;
  state.matched = 0;
  state.linksAdded = 0;
  state.failed = 0;
  state.currentDoc = null;
  state.startedAt = new Date().toISOString();
  state.finishedAt = null;
  state.lastError = null;

  runWorker(opts).catch((e) => {
    state.lastError = String(e.message || e);
  }).finally(() => {
    state.running = false;
    state.shouldStop = false;
    state.currentDoc = null;
    state.finishedAt = new Date().toISOString();
  });
  return true;
}

async function runWorker(opts) {
  const onlyMissing = opts.onlyMissing !== false; // default true
  // Accept string or array. Default covers both building-bearing corpora.
  const sdts = Array.isArray(opts.sdt)
    ? opts.sdt
    : opts.sdt
      ? [opts.sdt]
      : ["JobFiles", "Sales"];
  const sdtPlaceholders = sdts.map(() => "?").join(",");
  const mode = opts.mode || "all";

  // Snapshot the queue + the candidate index up front so the loop is
  // pure CPU. canonical_buildings is only ~9k rows; we hold the entire
  // (uid, tokens, addr) tuple in memory and do per-token set lookups.
  const db = opts.openDb();
  let queue, canonicals, addressIndex, docFreqStopwords;
  try {
    const queueSql = onlyMissing
      ? `SELECT d.id AS doc_id, f.path AS file_path, f.name AS file_name,
                e.text AS extract_text
           FROM documents d
           JOIN files f ON f.id = d.file_id
           LEFT JOIN document_extracts e ON e.document_id = d.id
           LEFT JOIN document_buildings db ON db.document_id = d.id
          WHERE f.source_data_type IN (${sdtPlaceholders}) AND db.document_id IS NULL`
      : `SELECT d.id AS doc_id, f.path AS file_path, f.name AS file_name,
                e.text AS extract_text
           FROM documents d
           JOIN files f ON f.id = d.file_id
           LEFT JOIN document_extracts e ON e.document_id = d.id
          WHERE f.source_data_type IN (${sdtPlaceholders})`;
    queue = db.prepare(queueSql).all(...sdts);

    // Document-frequency stopwords. A token that appears in more than 5%
    // of the in-scope corpus is too common to be a building signal — it's
    // almost certainly an English word that happens to also appear in
    // some canonical row's NAMES_SAMPLE (e.g. "labor", "eng", "phase",
    // "renovation"). Computed across the canonical token vocabulary so
    // the cost is bounded.
    const candidateTokens = new Set();
    for (const r of db.prepare(
      "SELECT match_tokens FROM canonical_buildings WHERE match_tokens IS NOT NULL AND match_tokens != ''",
    ).all()) {
      for (const t of r.match_tokens.split(/\s+/)) if (t) candidateTokens.add(t);
    }
    const totalScope = db.prepare(
      `SELECT COUNT(*) AS n FROM documents d JOIN files f ON f.id = d.file_id
        WHERE f.source_data_type IN (${sdtPlaceholders})`,
    ).get(...sdts).n;
    const docFreqMax = Math.max(40, Math.floor(totalScope * 0.05));
    docFreqStopwords = new Set();
    if (totalScope > 0 && candidateTokens.size > 0) {
      // For each candidate token, count how many docs in scope contain it
      // anywhere in the filename or the first 2000 chars of extracted text.
      // This is one prepared statement reused per token; on 9k tokens × 4k
      // docs this still completes in a couple of seconds.
      const countStmt = db.prepare(
        `SELECT COUNT(*) AS n FROM documents d
         JOIN files f ON f.id = d.file_id
         LEFT JOIN document_extracts e ON e.document_id = d.id
         WHERE f.source_data_type IN (${sdtPlaceholders})
           AND (LOWER(f.name) LIKE ? OR LOWER(SUBSTR(e.text, 1, 2000)) LIKE ?)`,
      );
      for (const t of candidateTokens) {
        const like = "%" + t + "%";
        const n = countStmt.get(...sdts, like, like).n;
        if (n > docFreqMax) docFreqStopwords.add(t);
      }
    }
    console.log("[buildings_matcher] doc-freq stopwords (token in >",
      docFreqMax, "docs):", docFreqStopwords.size, "of", candidateTokens.size);

    // Canonical buildings index: for every canonical row, the set of
    // distinguishing tokens (doc-frequency stopwords removed) plus the
    // metadata we'll carry onto a buildings row when we find-or-create
    // it for this canonical_uid. Canonical rows whose token set was
    // entirely filtered are excluded from the index (they can't match).
    //
    // Self-blocklist: drop canonical rows in ignored_buildings. Seeded
    // with Pyrocomm offices; users can extend it to suppress any
    // building that's a frequent false-positive.
    const selfBlocklist = new Set(
      db.prepare("SELECT building_uid FROM ignored_buildings").all()
        .map((r) => r.building_uid),
    );
    if (selfBlocklist.size > 0) {
      console.log("[buildings_matcher] ignored_buildings size:", selfBlocklist.size);
    }
    const cbRows = db.prepare(
      `SELECT building_uid, match_tokens, canonical_address,
              canonical_city, canonical_state, canonical_zip, names_sample
         FROM canonical_buildings
        WHERE match_tokens IS NOT NULL AND match_tokens != ''`,
    ).all();
    canonicals = cbRows
      .filter((r) => !selfBlocklist.has(r.building_uid))
      .map((r) => {
        const toks = new Set();
        for (const t of r.match_tokens.split(/\s+/)) {
          if (t && !docFreqStopwords.has(t)) toks.add(t);
        }
        return {
          uid: r.building_uid,
          tokens: toks,
          // Metadata used when find-or-creating a buildings row.
          canonical_address: r.canonical_address,
          canonical_city: r.canonical_city,
          canonical_state: r.canonical_state,
          canonical_zip: r.canonical_zip,
          // Pull the first sample-name as a usable display name; the full
          // names_sample is on canonical_buildings if anyone needs it.
          raw_name: r.names_sample
            ? r.names_sample.split(",")[0].trim().slice(0, 200)
            : null,
        };
      })
      .filter((b) => b.tokens.size > 0);

    // Address index: street-prefix → canonical_uid. Same structure as
    // before but indexes into canonical_buildings via building_addresses.
    const aRows = db.prepare(
      "SELECT building_uid, raw_street FROM building_addresses WHERE raw_street IS NOT NULL",
    ).all();
    addressIndex = new Map();
    for (const a of aRows) {
      if (selfBlocklist.has(a.building_uid)) continue;
      const cleaned = a.raw_street.toLowerCase().replace(/[^\w\s]/g, " ").split(/\s+/).filter(Boolean);
      if (cleaned.length >= 2 && /^\d{2,5}$/.test(cleaned[0])) {
        const key = cleaned[0] + " " + cleaned[1];
        if (!addressIndex.has(key)) addressIndex.set(key, []);
        addressIndex.get(key).push(a.building_uid);
        if (cleaned.length >= 3) {
          const key3 = cleaned[0] + " " + cleaned[1] + " " + cleaned[2];
          if (!addressIndex.has(key3)) addressIndex.set(key3, []);
          addressIndex.get(key3).push(a.building_uid);
        }
      }
    }
  } finally {
    db.close();
  }
  state.total = queue.length;

  if (canonicals.length === 0) {
    state.lastError = "No canonical buildings in local snapshot. Run 'Snapshot buildings from Snowflake' first.";
    return;
  }
  // Index canonicals by uid for find-or-create lookups.
  const canonicalByUid = new Map(canonicals.map((c) => [c.uid, c]));

  // Load YAML rules — these run alongside the algorithmic strategies
  // above. Each rule that matches produces an additional hit with
  // source="rule:<id>" so the user can see why a link came from a rule
  // vs. the auto-matcher. Rules are also filtered by mode so file/content
  // splits work consistently with the rest of the extractor.
  reloadBuildingExtractorRules();
  const allRuleHits = getBuildingExtractorRules();
  const FILE_MATCHES    = new Set(["name", "path"]);
  const CONTENT_MATCHES = new Set(["first_page", "extract"]);
  const activeRules =
    mode === "file"    ? allRuleHits.filter((r) => FILE_MATCHES.has(r.match)) :
    mode === "content" ? allRuleHits.filter((r) => CONTENT_MATCHES.has(r.match)) :
    allRuleHits;

  // Single write connection held across the whole run, with periodic
  // transaction commits so progress is durable mid-batch. Per-doc work
  // is pure CPU (token set lookups), so the DB cost is dominated by the
  // INSERT volume — batched in BEGIN/COMMIT chunks of BATCH_SIZE docs.
  const BATCH_SIZE = 100;
  const wdb = opts.openDb();

  // Find-or-create on `buildings` keyed by canonical_uid. We seed the
  // cache with whatever already exists so re-runs upsert metadata
  // without creating duplicate rows. dedup_key for canonicalized rows
  // is "uid:<canonical_uid>".
  const findByDedup = wdb.prepare(
    "SELECT id FROM buildings WHERE dedup_key = ?",
  );
  const insertBuilding = wdb.prepare(
    `INSERT INTO buildings (
       canonical_uid, dedup_key, raw_name, raw_address,
       raw_street, raw_city, raw_state, raw_zip,
       match_confidence, match_source,
       first_seen_at, last_seen_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  const touchBuilding = wdb.prepare(
    `UPDATE buildings
        SET last_seen_at      = ?,
            match_confidence  = COALESCE(?, match_confidence),
            match_source      = COALESCE(?, match_source)
      WHERE id = ?`,
  );
  const linkUpsert = wdb.prepare(
    `INSERT OR IGNORE INTO document_buildings
       (document_id, building_id, confidence, source, matched_token)
     VALUES (?, ?, ?, ?, ?)`,
  );

  // uid → buildings.id, populated lazily during the run.
  const buildingIdByUid = new Map();
  function ensureBuildingRow(uid, confidence, source, ts) {
    const cached = buildingIdByUid.get(uid);
    if (cached) {
      // Already seen in this run; just update last_seen_at.
      touchBuilding.run(ts, null, null, cached);
      return cached;
    }
    const dedupKey = "uid:" + uid;
    const existing = findByDedup.get(dedupKey);
    if (existing) {
      buildingIdByUid.set(uid, existing.id);
      touchBuilding.run(ts, null, null, existing.id);
      return existing.id;
    }
    // Brand-new row — seed it with the canonical's address/name fields
    // so this row carries useful metadata even if the user never opens
    // the canonical_buildings view.
    const c = canonicalByUid.get(uid) || {};
    const result = insertBuilding.run(
      uid, dedupKey,
      c.raw_name || null,
      c.canonical_address || null,
      null, // raw_street: TBD when the address-extraction strategy fires
      c.canonical_city || null,
      c.canonical_state || null,
      c.canonical_zip || null,
      confidence,
      source,
      ts, ts,
    );
    const id = Number(result.lastInsertRowid);
    buildingIdByUid.set(uid, id);
    return id;
  }

  let inBatch = 0;
  function commitBatch() {
    if (inBatch > 0) {
      wdb.exec("COMMIT");
      inBatch = 0;
    }
  }
  function startBatch() {
    if (inBatch === 0) wdb.exec("BEGIN");
  }

  try {
    for (const row of queue) {
      if (state.shouldStop) break;
      state.currentDoc = { id: row.doc_id, path: row.file_path };
      try {
        const matches = findMatches(row, canonicals, addressIndex, mode);
        // Apply YAML rules in addition. Each rule whose target field
        // matches the rule's regex produces a hit at the rule's
        // confidence with source="rule:<id>". Rules and the auto-matcher
        // can both fire for the same canonical UID — dedup by uid (rule
        // hit wins by appearing later in the matches array, since it's
        // hand-curated; the linkUpsert is INSERT-OR-IGNORE so the first
        // hit per (doc, building_id) is preserved).
        if (activeRules.length > 0) {
          const firstPage = (row.extract_text || "").slice(0, 600);
          for (const rule of activeRules) {
            const target =
              rule.match === "name"       ? row.file_name :
              rule.match === "path"       ? row.file_path :
              rule.match === "first_page" ? firstPage :
              /* extract */                 row.extract_text;
            if (!target) continue;
            if (!rule.regex.test(target)) continue;
            matches.push({
              uid:        rule.building_uid,
              confidence: rule.confidence,
              source:     "rule:" + rule.id,
              token:      rule.match,
            });
          }
        }
        if (matches.length > 0) {
          startBatch();
          const ts = new Date().toISOString();
          let added = 0;
          for (const m of matches) {
            const buildingId = ensureBuildingRow(m.uid, m.confidence, m.source, ts);
            const r = linkUpsert.run(row.doc_id, buildingId, m.confidence, m.source, m.token);
            added += Number(r.changes || 0);
          }
          state.linksAdded += added;
          if (added > 0) state.matched++;
          inBatch++;
          if (inBatch >= BATCH_SIZE) commitBatch();
        }
        state.done++;
      } catch (e) {
        state.failed++;
        state.lastError = `${row.file_path}: ${e.message}`;
      }

      if ((state.done + state.failed) % 100 === 0) {
        await new Promise((resolve) => setImmediate(resolve));
      }
    }
    commitBatch();
  } finally {
    try { commitBatch(); } catch { /* already committed */ }
    wdb.close();
  }
}

// Apply both extraction strategies to a single document. Returns an
// array of { uid, confidence, source, token } describing each canonical
// building reference found. De-duplicated by uid so we don't emit two
// hits per (doc, canonical) pair when both strategies fire.
//
// mode controls which side of the document we read from:
//   "all"     — filename tokens AND extract-text tokens (default)
//   "file"    — filename tokens only (extract text ignored)
//   "content" — extract-text tokens only (filename ignored)
function findMatches(row, canonicals, addressIndex, mode) {
  // Empty token sets when a side is excluded so the existing logic
  // below ("found in fname / found in text?") naturally short-circuits.
  const useFile    = mode !== "content";
  const useContent = mode !== "file";
  const fnameTokens = useFile    ? new Set(tokenize(row.file_name))           : new Set();
  const textHead    = useContent ? (row.extract_text || "").slice(0, 2000)    : "";
  const textTokens  = useContent ? new Set(tokenize(textHead))                : new Set();

  const out = new Map();  // canonical_uid → match row

  // Strategy 1: project-name token matching against each canonical's
  // distinguishing-token set. Requires at least one "distinguishing"
  // token (digit-bearing OR length ≥ 5) plus the standard hit-count
  // gate so single-letter coincidences don't fire.
  for (const b of canonicals) {
    let fnHits = 0, textHits = 0;
    let firstHit = null;
    let numericHit = false;
    let longAlphaHit = null;
    let distinguishingHit = false;
    for (const t of b.tokens) {
      const inFn   = fnameTokens.has(t);
      const inText = textTokens.has(t);
      if (inFn)   { fnHits++;   firstHit = firstHit || t; }
      if (inText) { textHits++; firstHit = firstHit || t; }
      if (inFn || inText) {
        if (/\d/.test(t)) numericHit = true;
        else if (t.length >= 6) longAlphaHit = t;
        // A "distinguishing" token is one we trust as a real signal:
        // contains a digit (b183, suite-style codes) OR is at least 5
        // chars (long enough to be a vendor/site name fragment, not an
        // English filler word like "job" or "not"). At least ONE such
        // token must appear or we emit no link, even with multiple hits
        // on short generic tokens.
        if (/\d/.test(t) || t.length >= 5) distinguishingHit = true;
      }
    }
    const totalHits = fnHits + textHits;
    if (totalHits === 0) continue;
    if (!distinguishingHit) continue;

    let confidence = null;
    let source = null;
    let token = firstHit;
    // What counts as a "strong" numeric hit: alphanumeric mixes
    // (building codes like "b183", "blk7") are very distinctive even
    // alone. Pure all-digit tokens (ZIPs, years) are weak — they fire
    // on any document whose letterhead happens to share that ZIP. So:
    //   - 2+ token hits → confident match
    //   - alphanumeric numeric singleton → medium / high (filename)
    //   - all-digit singleton → ignore (too noisy)
    //   - pure alpha singleton → ignore (rare words still produce
    //     incidental matches across thousands of docs)
    let alnumNumericHit = false;
    for (const t of b.tokens) {
      if ((fnameTokens.has(t) || textTokens.has(t)) &&
          /\d/.test(t) && /[a-z]/i.test(t)) {
        alnumNumericHit = true;
        break;
      }
    }
    if (totalHits >= 2 && fnHits >= 1) {
      confidence = "high";
      source = "names_filename+text";
    } else if (totalHits >= 2) {
      confidence = "medium";
      source = "names_text";
    } else if (alnumNumericHit) {
      confidence = fnHits ? "high" : "medium";
      source = fnHits ? "names_filename_alnum" : "names_text_alnum";
    }
    if (confidence) {
      out.set(b.uid, { uid: b.uid, confidence, source, token });
    }
  }

  // Strategy 2: street-address candidates. Looks for "<number> <word>"
  // and "<number> <word> <word>" sequences and looks them up in the
  // addressIndex. Always at least medium confidence because a street-
  // number prefix is very distinctive. Reads from filename + path + text,
  // each side gated by the mode. The folder layout for JobFiles is
  // "<address>-<city>-<bldg-name>\..." so the path itself often carries
  // the address even when the filename does not.
  const allText = (useFile ? (row.file_name || "") + " " + (row.file_path || "") + " " : "") +
                  (useContent ? textHead : "");
  for (const cand of streetCandidates(allText)) {
    const uids = addressIndex.get(cand);
    if (!uids) continue;
    // If a 3-token candidate maps to a unique building, that's high
    // confidence. A 2-token candidate or a 3-token one with multiple
    // matches stays medium.
    const conf = uids.length === 1 && cand.split(" ").length >= 3 ? "high" : "medium";
    for (const uid of uids) {
      // Only upgrade — don't downgrade a match the names strategy
      // already produced at higher confidence.
      const existing = out.get(uid);
      if (!existing || rank(conf) > rank(existing.confidence)) {
        out.set(uid, {
          uid,
          confidence: conf,
          source: "address",
          token: cand,
        });
      }
    }
  }

  return Array.from(out.values());
}

function rank(c) { return c === "high" ? 3 : c === "medium" ? 2 : 1; }
