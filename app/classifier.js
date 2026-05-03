// Filename/path-based document_type classifier.
// Reads classifier.yaml at module load. First matching rule wins.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import YAML from "yaml";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Document-type rule files live at <repo>/classifiers/, sibling to
// <repo>/app/. Extraction rule files (products, buildings) live at
// <repo>/extractors/ — separate folder because they identify entities
// (products, buildings) rather than classifying documents.
const CLASSIFIERS_DIR = path.join(__dirname, "..", "classifiers");
const EXTRACTORS_DIR  = path.join(__dirname, "..", "extractors");
const RULES_PATH = path.join(CLASSIFIERS_DIR, "classifier.yaml");

const VALID_MATCH_FIELDS = new Set(["name", "parent", "path", "file_type"]);
const VALID_CONFIDENCE = new Set(["high", "medium", "low"]);
const VALID_SOURCE_DATA_TYPES = new Set(["Vendor", "JobFiles", "Sales", "Any"]);

// Normalize the source_data_type field on a rule. Missing/empty defaults
// to 'Vendor' for back-compat (the original rule files were authored
// against the Vendors corpus). Throws on unknown values.
function normalizeRuleSdt(raw, ruleId) {
  if (raw == null || raw === "") return "Vendor";
  if (!VALID_SOURCE_DATA_TYPES.has(raw)) {
    throw new Error(`rule ${ruleId} has invalid source_data_type ${raw} (must be Vendor, JobFiles, or Any)`);
  }
  return raw;
}

// True when a rule with sdt should run against a file with fileSdt.
// Any-tagged rules fire against any file. Otherwise an exact match is
// required. 'Other'-typed files are matched only by 'Any' rules.
function ruleAppliesToSdt(ruleSdt, fileSdt) {
  if (ruleSdt === "Any") return true;
  return ruleSdt === fileSdt;
}

let compiledRules = null;

function loadRules() {
  const raw = fs.readFileSync(RULES_PATH, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("classifier.yaml must have a top-level `rules:` list");
  }
  const out = [];
  for (const r of doc.rules) {
    const id = r.id || `rule_${out.length}`;
    if (!r.document_type) throw new Error(`rule ${id} missing document_type`);
    if (!VALID_CONFIDENCE.has(r.confidence)) {
      throw new Error(`rule ${id} has invalid confidence ${r.confidence}`);
    }
    if (!VALID_MATCH_FIELDS.has(r.match)) {
      throw new Error(`rule ${id} has invalid match ${r.match}`);
    }
    if (!r.pattern) throw new Error(`rule ${id} missing pattern`);
    out.push({
      id,
      documentType: r.document_type,
      confidence: r.confidence,
      match: r.match,
      regex: new RegExp(r.pattern, "i"),
      sourceDataType: normalizeRuleSdt(r.source_data_type, id),
    });
  }
  return out;
}

function getRules() {
  if (!compiledRules) compiledRules = loadRules();
  return compiledRules;
}

// Force a fresh load (used by /api/classify so editing the YAML doesn't
// require restarting the server).
export function reloadRules() {
  compiledRules = loadRules();
  return compiledRules.length;
}

// Returns { documentType, confidence, ruleId } or null if no rule matched.
// `source_data_type` filters which rules participate: a Vendor file only
// sees Vendor + Any rules; a JobFiles file only sees JobFiles + Any. An
// 'Other'-typed file sees only Any rules. Default for missing arg is Vendor
// for legacy callers — but ingest now always supplies it.
export function classify({ name, parent, path: fullPath, file_type, source_data_type }) {
  const fileSdt = source_data_type || "Vendor";
  const fields = { name, parent, path: fullPath, file_type };
  for (const rule of getRules()) {
    if (!ruleAppliesToSdt(rule.sourceDataType, fileSdt)) continue;
    const target = fields[rule.match] ?? "";
    if (rule.regex.test(target)) {
      return {
        documentType: rule.documentType,
        confidence: rule.confidence,
        ruleId: rule.id,
      };
    }
  }
  return null;
}

// Bulk-classify every non-dir file by joining files + documents.
// Updates the documents table — files stays untouched. Returns counts
// grouped by document_type and confidence.
export function classifyAll(db) {
  reloadRules();

  const docTypeIdByName = new Map(
    db
      .prepare("SELECT id, name FROM document_types")
      .all()
      .map((r) => [r.name, r.id]),
  );

  const update = db.prepare(
    "UPDATE documents SET document_type_id = ?, confidence = ? WHERE id = ?",
  );

  // For each non-dir file, pull what the classifier needs plus the parent's
  // name (for `match: parent` rules) and the existing classification so we
  // can skip rows that already have a stricter result. documents.id is the
  // row we update.
  const rows = db
    .prepare(`
      SELECT d.id                AS doc_id,
             d.confidence        AS prev_confidence,
             f.path              AS path,
             f.name              AS name,
             p.name              AS parent,
             f.file_type         AS file_type,
             f.source_data_type  AS source_data_type
      FROM documents d
      JOIN files f       ON f.id = d.file_id
      LEFT JOIN files p  ON p.id = f.parent_id
      WHERE f.is_dir = 0
    `)
    .all();

  const byType = {};   // doc_type name → count
  const byConfidence = { high: 0, medium: 0, low: 0, none: 0 };
  let unknownDocType = 0;
  let updated = 0;
  let kept = 0;        // existing classification beat or tied the new rule

  db.exec("BEGIN");
  try {
    for (const r of rows) {
      const result = classify(r);
      const prevRank = r.prev_confidence ? (CONFIDENCE_RANK[r.prev_confidence] || 0) : 0;

      if (!result) {
        // No filename rule fired. Only clear an existing classification if
        // it was unset or low — never wipe a medium/high that some other
        // pass (e.g. content classifier) wrote.
        if (prevRank <= CONFIDENCE_RANK.low) {
          update.run(null, null, r.doc_id);
        } else {
          kept++;
        }
        byConfidence.none++;
        continue;
      }
      const dtId = docTypeIdByName.get(result.documentType);
      if (dtId === undefined) {
        unknownDocType++;
        if (prevRank <= CONFIDENCE_RANK.low) update.run(null, null, r.doc_id);
        else kept++;
        byConfidence.none++;
        continue;
      }

      // Skip rows whose existing classification is at least as strong as
      // what this rule would produce. Re-running the filename pass should
      // not wipe out content-classifier wins.
      const newRank = CONFIDENCE_RANK[result.confidence] || 0;
      if (prevRank >= newRank && prevRank > 0) {
        kept++;
        continue;
      }
      update.run(dtId, result.confidence, r.doc_id);
      byType[result.documentType] = (byType[result.documentType] || 0) + 1;
      byConfidence[result.confidence]++;
      updated++;
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }

  return {
    totalFiles: rows.length,
    updated,
    kept,
    byType,
    byConfidence,
    unknownDocType,
    rulesLoaded: getRules().length,
  };
}

// --- Content-based classifier --------------------------------------------
// Same rule shape but `match` selects which extracted text to test:
//   first_page → first PAGE BREAK chunk
//   extract    → full extracted text
//
// Targets only documents where the existing classification is null OR low
// confidence (per user spec: "low confidence + unclassified"). Filename
// hits at high/medium are left alone.
//
// Result-write policy: only fill empty slots or upgrade strictly. Never
// overwrite a high/medium classification.

const CONTENT_RULES_PATH = path.join(CLASSIFIERS_DIR, "content_classifier.yaml");
const CONTENT_VALID_MATCH = new Set(["first_page", "extract"]);
const CONFIDENCE_RANK = { low: 1, medium: 2, high: 3 };

let contentRules = null;

function loadContentRules() {
  const raw = fs.readFileSync(CONTENT_RULES_PATH, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("content_classifier.yaml must have a top-level rules: list");
  }
  const out = [];
  for (const r of doc.rules) {
    const id = r.id || `content_rule_${out.length}`;
    if (!r.document_type) throw new Error(`rule ${id} missing document_type`);
    if (!VALID_CONFIDENCE.has(r.confidence)) {
      throw new Error(`rule ${id} has invalid confidence ${r.confidence}`);
    }
    if (!CONTENT_VALID_MATCH.has(r.match)) {
      throw new Error(`rule ${id} has invalid match ${r.match} (use first_page or extract)`);
    }
    if (!r.pattern) throw new Error(`rule ${id} missing pattern`);
    out.push({
      id,
      documentType: r.document_type,
      confidence: r.confidence,
      match: r.match,
      regex: new RegExp(r.pattern, "i"),
      sourceDataType: normalizeRuleSdt(r.source_data_type, id),
    });
  }
  return out;
}

function getContentRules() {
  if (!contentRules) contentRules = loadContentRules();
  return contentRules;
}

// Split the PAGE BREAK sentinel that extractor.js inserts between pages.
// Returns the body of the first page (or the whole text if there's no break).
function firstPageOf(text) {
  if (!text) return "";
  const idx = text.indexOf("\n\n=== PAGE BREAK ===\n\n");
  return idx >= 0 ? text.slice(0, idx) : text;
}

function classifyContent({ text, firstPage, source_data_type }) {
  const fileSdt = source_data_type || "Vendor";
  for (const rule of getContentRules()) {
    if (!ruleAppliesToSdt(rule.sourceDataType, fileSdt)) continue;
    const target = rule.match === "first_page" ? firstPage : text;
    if (!target) continue;
    if (rule.regex.test(target)) {
      return {
        documentType: rule.documentType,
        confidence: rule.confidence,
        ruleId: rule.id,
      };
    }
  }
  return null;
}

export function reloadContentRules() {
  contentRules = loadContentRules();
  return contentRules.length;
}

// --- Product classifier --------------------------------------------------
// Vendor-scoped rule engine. Each rule binds a (vendor, product) pair to
// a regex against a path/name/text field; only fires when the document's
// vendor matches the rule's vendor. Many-to-many results land in the
// document_products table.

const PRODUCT_RULES_PATH = path.join(EXTRACTORS_DIR, "product_extractor.yaml");
const PRODUCT_VALID_MATCH = new Set(["name", "path", "first_page", "extract"]);

let productRules = null;

function loadProductRules() {
  const raw = fs.readFileSync(PRODUCT_RULES_PATH, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("product_extractor.yaml must have a top-level rules: list");
  }
  const out = [];
  for (const r of doc.rules) {
    const id = r.id || `product_rule_${out.length}`;
    if (!r.vendor)  throw new Error(`rule ${id} missing vendor`);
    if (!r.product) throw new Error(`rule ${id} missing product`);
    if (!VALID_CONFIDENCE.has(r.confidence)) {
      throw new Error(`rule ${id} has invalid confidence ${r.confidence}`);
    }
    if (!PRODUCT_VALID_MATCH.has(r.match)) {
      throw new Error(`rule ${id} has invalid match ${r.match}`);
    }
    if (!r.pattern) throw new Error(`rule ${id} missing pattern`);
    out.push({
      id,
      vendor: r.vendor,
      product: String(r.product),
      confidence: r.confidence,
      match: r.match,
      regex: new RegExp(r.pattern, "i"),
      sourceDataType: normalizeRuleSdt(r.source_data_type, id),
    });
  }
  return out;
}

function getProductRules() {
  if (!productRules) productRules = loadProductRules();
  return productRules;
}

export function reloadProductRules() {
  productRules = loadProductRules();
  return productRules.length;
}

// Run product rules against every document. Inserts into products
// table on demand, then upserts document_products links. A single
// document can match multiple products (e.g. compatibility lists).
//
// mode controls which rules fire:
//   "all"     — every rule (default; back-compat)
//   "file"    — only rules whose match is "name" or "path"
//   "content" — only rules whose match is "first_page" or "extract"
//
// The split mirrors the menu's Run File / Run Content actions; "Run All"
// uses mode="all". Existing-link wipe is partial when a non-"all" mode
// runs: only links whose source matches the mode are deleted, so a
// content run doesn't clobber file-rule hits and vice versa.
export function classifyAllByProduct(db, opts = {}) {
  reloadProductRules();
  const allRules = getProductRules();
  const mode = opts.mode || "all";
  const FILE_MATCHES    = new Set(["name", "path"]);
  const CONTENT_MATCHES = new Set(["first_page", "extract"]);
  const rules =
    mode === "file"    ? allRules.filter((r) => FILE_MATCHES.has(r.match)) :
    mode === "content" ? allRules.filter((r) => CONTENT_MATCHES.has(r.match)) :
    allRules;

  // Group rules by vendor name for cheap lookup.
  const rulesByVendor = new Map();
  for (const r of rules) {
    if (!rulesByVendor.has(r.vendor)) rulesByVendor.set(r.vendor, []);
    rulesByVendor.get(r.vendor).push(r);
  }

  // Resolve vendor names → ids. Skip rules whose vendor doesn't exist.
  const vendorIdByName = new Map(
    db.prepare("SELECT id, name FROM vendors").all().map((r) => [r.name, r.id]),
  );
  const unknownVendors = new Set();
  for (const v of rulesByVendor.keys()) {
    if (!vendorIdByName.has(v)) unknownVendors.add(v);
  }

  // Pull every document with its vendor + path/name + extract text (when
  // present). Doing this in one query avoids per-row roundtrips. Vendor
  // is now soft-joined via documents.vendor_id (NULL for non-Vendor SDT).
  // The classifier loop below skips documents without a known vendor.
  const rows = db.prepare(`
    SELECT d.id                AS doc_id,
           f.name              AS name,
           f.path              AS path,
           v.id                AS vendor_id,
           v.name              AS vendor_name,
           f.source_data_type  AS source_data_type,
           e.text              AS text
    FROM documents d
    JOIN files f         ON f.id = d.file_id
    LEFT JOIN vendors v  ON v.id = d.vendor_id
    LEFT JOIN document_extracts e ON e.document_id = d.id AND e.error IS NULL
  `).all();

  // Resolve products on demand — INSERT OR IGNORE keeps it idempotent.
  const insertProduct = db.prepare(
    "INSERT OR IGNORE INTO products (vendor_id, name) VALUES (?, ?)",
  );
  const lookupProduct = db.prepare(
    "SELECT id FROM products WHERE vendor_id = ? AND name = ?",
  );
  const insertLink = db.prepare(
    "INSERT OR REPLACE INTO document_products (document_id, product_id, confidence, source) " +
    "VALUES (?, ?, ?, ?)",
  );

  // Cache product ids so we don't lookup repeatedly within a single run.
  const productIdCache = new Map(); // "vendorId:name" -> productId
  function ensureProduct(vendorId, productName) {
    const key = vendorId + ":" + productName;
    if (productIdCache.has(key)) return productIdCache.get(key);
    insertProduct.run(vendorId, productName);
    const r = lookupProduct.get(vendorId, productName);
    const pid = r ? r.id : null;
    if (pid) productIdCache.set(key, pid);
    return pid;
  }

  const byVendor = {};       // vendor_name -> hits count
  const byProduct = {};      // "vendor:product" -> hits count
  let docsWithProducts = 0;
  let totalLinks = 0;
  let docsScanned = 0;

  db.exec("BEGIN");
  try {
    // Wipe existing links for a clean re-classify. Scope the wipe to the
    // rule subset we're about to re-run so the other mode's links survive.
    // (source on document_products is the rule's match value.)
    if (mode === "file") {
      db.exec("DELETE FROM document_products WHERE source IN ('name', 'path')");
    } else if (mode === "content") {
      db.exec("DELETE FROM document_products WHERE source IN ('first_page', 'extract')");
    } else {
      db.exec("DELETE FROM document_products");
    }

    for (const r of rows) {
      docsScanned++;
      const vendorRules = rulesByVendor.get(r.vendor_name);
      if (!vendorRules || vendorRules.length === 0) continue;

      const firstPage = firstPageOf(r.text);
      const seenInDoc = new Set();    // dedupe same product hit twice in one doc

      const fileSdt = r.source_data_type || "Vendor";
      for (const rule of vendorRules) {
        if (!ruleAppliesToSdt(rule.sourceDataType, fileSdt)) continue;
        const target =
          rule.match === "name"       ? r.name :
          rule.match === "path"       ? r.path :
          rule.match === "first_page" ? firstPage :
          /* extract */                 r.text;
        if (!target) continue;
        if (!rule.regex.test(target)) continue;

        const productKey = rule.vendor + ":" + rule.product;
        if (seenInDoc.has(productKey)) continue;
        seenInDoc.add(productKey);

        const pid = ensureProduct(r.vendor_id, rule.product);
        if (!pid) continue;

        insertLink.run(r.doc_id, pid, rule.confidence, rule.match);
        byVendor[r.vendor_name]   = (byVendor[r.vendor_name]   || 0) + 1;
        byProduct[productKey]     = (byProduct[productKey]     || 0) + 1;
        totalLinks++;
      }
      if (seenInDoc.size > 0) docsWithProducts++;
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }

  return {
    mode,
    rulesLoaded: rules.length,
    rulesAvailable: allRules.length,
    docsScanned,
    docsWithProducts,
    totalLinks,
    distinctProducts: db.prepare("SELECT COUNT(*) AS n FROM products").get().n,
    byVendor,
    byProduct,
    unknownVendors: [...unknownVendors],
  };
}

// --- Vendor extractor ----------------------------------------------------
// Same shape as the product extractor but writes vendor names into the
// vendors table and stamps documents.vendor_id. Difference from products:
// the rule's `vendor` field can be a literal name OR a regex backreference
// like "$1" / "$2" — when used, the captured group from the matched text
// becomes the vendor name. The first canonical rule is exactly this:
//   path matches /vendors/(name)/  →  vendor = "$1" → uses captured name.
// Lets one rule cover all vendors found via path layout instead of
// requiring a row per vendor.

const VENDOR_RULES_PATH  = path.join(EXTRACTORS_DIR, "vendor_extractor.yaml");
const VENDOR_VALID_MATCH = new Set(["name", "path", "first_page", "extract"]);

let vendorRules = null;

function loadVendorRules() {
  const raw = fs.readFileSync(VENDOR_RULES_PATH, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("vendor_extractor.yaml must have a top-level rules: list");
  }
  const out = [];
  for (const r of doc.rules) {
    const id = r.id || `vendor_rule_${out.length}`;
    if (!r.vendor)  throw new Error(`rule ${id} missing vendor`);
    if (!VALID_CONFIDENCE.has(r.confidence)) {
      throw new Error(`rule ${id} has invalid confidence ${r.confidence}`);
    }
    if (!VENDOR_VALID_MATCH.has(r.match)) {
      throw new Error(`rule ${id} has invalid match ${r.match}`);
    }
    if (!r.pattern) throw new Error(`rule ${id} missing pattern`);
    out.push({
      id,
      vendor: String(r.vendor),
      confidence: r.confidence,
      match: r.match,
      regex: new RegExp(r.pattern, "i"),
      sourceDataType: normalizeRuleSdt(r.source_data_type, id),
    });
  }
  return out;
}

function getVendorRules() {
  if (!vendorRules) vendorRules = loadVendorRules();
  return vendorRules;
}

export function reloadVendorRules() {
  vendorRules = loadVendorRules();
  return vendorRules.length;
}

// --- Building extractor rules --------------------------------------------
// These run alongside the algorithmic building matcher (see
// buildings_matcher.js). The matcher does broad coverage automatically;
// the rules let users hand-craft specific building references the
// matcher misses or gets wrong. Same rule shape as products + vendors:
// regex against name/path/first_page/extract → building_uid.

const BUILDING_RULES_PATH  = path.join(EXTRACTORS_DIR, "building_extractor.yaml");
const BUILDING_VALID_MATCH = new Set(["name", "path", "first_page", "extract"]);

let buildingExtractorRules = null;

function loadBuildingExtractorRules() {
  const raw = fs.readFileSync(BUILDING_RULES_PATH, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("building_extractor.yaml must have a top-level rules: list");
  }
  const out = [];
  for (const r of doc.rules) {
    const id = r.id || `building_rule_${out.length}`;
    if (!r.building_uid) throw new Error(`rule ${id} missing building_uid`);
    if (!VALID_CONFIDENCE.has(r.confidence)) {
      throw new Error(`rule ${id} has invalid confidence ${r.confidence}`);
    }
    if (!BUILDING_VALID_MATCH.has(r.match)) {
      throw new Error(`rule ${id} has invalid match ${r.match}`);
    }
    if (!r.pattern) throw new Error(`rule ${id} missing pattern`);
    out.push({
      id,
      building_uid: String(r.building_uid),
      confidence: r.confidence,
      match: r.match,
      regex: new RegExp(r.pattern, "i"),
      sourceDataType: normalizeRuleSdt(r.source_data_type, id),
    });
  }
  return out;
}

export function reloadBuildingExtractorRules() {
  buildingExtractorRules = loadBuildingExtractorRules();
  return buildingExtractorRules.length;
}
export function getBuildingExtractorRules() {
  if (!buildingExtractorRules) buildingExtractorRules = loadBuildingExtractorRules();
  return buildingExtractorRules;
}

// Resolve a rule's `vendor` field against a regex match. Literal names
// pass through unchanged; "$N" references the Nth capture group from
// the match (1-indexed; $0 is the whole match for symmetry). Empty or
// whitespace-only result returns null so the caller can skip the row.
function resolveVendorName(rule, matchResult) {
  const raw = rule.vendor;
  if (!raw) return null;
  // Replace $0..$9 with the corresponding capture group.
  const resolved = raw.replace(/\$(\d)/g, (_, d) => {
    const i = Number(d);
    return matchResult[i] != null ? matchResult[i] : "";
  }).trim();
  return resolved || null;
}

// Run vendor rules and populate the vendors table + stamp
// documents.vendor_id. mode: "all" | "file" | "content" — same split
// semantics as classifyAllByProduct.
//
// Vendor extraction only runs against Vendor-SDT documents (JobFiles
// don't reference manufacturers). For each Vendor doc, the first rule
// to match wins — vendor_id is set on documents and the vendors row is
// created on demand. Re-running re-stamps every doc; vendors that no
// longer match any rule fall back to NULL vendor_id (which downstream
// classification handles gracefully).
export function classifyAllByVendor(db, opts = {}) {
  reloadVendorRules();
  const allRules = getVendorRules();
  const mode = opts.mode || "all";
  const FILE_MATCHES    = new Set(["name", "path"]);
  const CONTENT_MATCHES = new Set(["first_page", "extract"]);
  const rules =
    mode === "file"    ? allRules.filter((r) => FILE_MATCHES.has(r.match)) :
    mode === "content" ? allRules.filter((r) => CONTENT_MATCHES.has(r.match)) :
    allRules;

  // Pull the candidate documents — only Vendor SDT, with their existing
  // file path/name + extract (when present).
  const rows = db.prepare(`
    SELECT d.id                AS doc_id,
           f.name              AS name,
           f.path              AS path,
           f.source_data_type  AS source_data_type,
           e.text              AS text
    FROM documents d
    JOIN files f         ON f.id = d.file_id
    LEFT JOIN document_extracts e ON e.document_id = d.id AND e.error IS NULL
    WHERE f.source_data_type = 'Vendor'
  `).all();

  const insertVendor = db.prepare("INSERT OR IGNORE INTO vendors (name) VALUES (?)");
  const lookupVendor = db.prepare("SELECT id FROM vendors WHERE name = ?");
  const updateDocVendor = db.prepare("UPDATE documents SET vendor_id = ? WHERE id = ?");

  // Cache vendor ids in-process; the same vendor name shows up across
  // hundreds of docs and we'd rather hit the cache than re-query.
  const vendorIdByName = new Map();
  function ensureVendor(name) {
    if (vendorIdByName.has(name)) return vendorIdByName.get(name);
    insertVendor.run(name);
    const v = lookupVendor.get(name);
    const id = v ? v.id : null;
    if (id) vendorIdByName.set(name, id);
    return id;
  }

  const byVendor = {};
  let docsMatched = 0;
  let docsScanned = 0;
  let docsCleared = 0;

  db.exec("BEGIN");
  try {
    // Clear vendor_id on Vendor docs scoped to the rule subset that's
    // about to run — same partial-wipe pattern as products. We use the
    // documents.vendor_source column if present; if not (legacy DB),
    // we wipe everything on mode=all and leave it on partial modes.
    // (For now we don't track vendor_source per-doc, so partial-wipe
    // is best-effort: only the "all" mode clears.)
    if (mode === "all") {
      db.exec(
        `UPDATE documents SET vendor_id = NULL
          WHERE id IN (
            SELECT d.id FROM documents d
            JOIN files f ON f.id = d.file_id
            WHERE f.source_data_type = 'Vendor'
          )`
      );
      docsCleared = db.prepare(
        `SELECT COUNT(*) AS n FROM documents d
         JOIN files f ON f.id = d.file_id
         WHERE f.source_data_type = 'Vendor'`
      ).get().n;
    }

    for (const r of rows) {
      docsScanned++;
      const firstPage = firstPageOf(r.text);
      const fileSdt = r.source_data_type || "Vendor";
      // First rule to match wins — order rules from most specific to
      // most general in vendor_extractor.yaml.
      for (const rule of rules) {
        if (!ruleAppliesToSdt(rule.sourceDataType, fileSdt)) continue;
        const target =
          rule.match === "name"       ? r.name :
          rule.match === "path"       ? r.path :
          rule.match === "first_page" ? firstPage :
          /* extract */                 r.text;
        if (!target) continue;
        const m = rule.regex.exec(target);
        if (!m) continue;
        const vendorName = resolveVendorName(rule, m);
        if (!vendorName) continue;
        const vid = ensureVendor(vendorName);
        if (!vid) continue;
        updateDocVendor.run(vid, r.doc_id);
        byVendor[vendorName] = (byVendor[vendorName] || 0) + 1;
        docsMatched++;
        break; // first match wins
      }
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }

  return {
    mode,
    rulesLoaded: rules.length,
    rulesAvailable: allRules.length,
    docsScanned,
    docsCleared,
    docsMatched,
    distinctVendors: db.prepare("SELECT COUNT(*) AS n FROM vendors").get().n,
    byVendor,
  };
}

export function classifyAllByContent(db) {
  reloadContentRules();

  const docTypeIdByName = new Map(
    db.prepare("SELECT id, name FROM document_types").all().map((r) => [r.name, r.id]),
  );

  // Candidate rows: documents that have a successful extract AND are
  // either unclassified or low-confidence. Pull both the existing
  // classification (so we can compare) and the extracted text.
  const rows = db.prepare(`
    SELECT d.id                AS doc_id,
           d.confidence        AS prev_confidence,
           e.text              AS text,
           f.source_data_type  AS source_data_type
    FROM documents d
    JOIN files f             ON f.id = d.file_id
    JOIN document_extracts e ON e.document_id = d.id
    WHERE e.error IS NULL
      AND e.text IS NOT NULL
      AND (d.document_type_id IS NULL OR d.confidence = 'low')
  `).all();

  const update = db.prepare(
    "UPDATE documents SET document_type_id = ?, confidence = ? WHERE id = ?",
  );

  const byType = {};
  const byConfidence = { high: 0, medium: 0, low: 0 };
  let updated = 0;
  let unmatched = 0;
  let kept = 0;            // rule fired but didn't beat existing
  let unknownDocType = 0;

  db.exec("BEGIN");
  try {
    for (const r of rows) {
      const fp = firstPageOf(r.text);
      const result = classifyContent({ text: r.text, firstPage: fp, source_data_type: r.source_data_type });
      if (!result) { unmatched++; continue; }

      const dtId = docTypeIdByName.get(result.documentType);
      if (dtId === undefined) { unknownDocType++; continue; }

      // Policy c2: only fill empty slots or strictly upgrade.
      const prevRank = r.prev_confidence ? (CONFIDENCE_RANK[r.prev_confidence] || 0) : 0;
      const newRank  = CONFIDENCE_RANK[result.confidence] || 0;
      if (prevRank > 0 && newRank <= prevRank) {
        // Existing low rank tied or beat — leave it. (Caller ensures we
        // only see null-or-low rows; we still guard.)
        kept++;
        continue;
      }
      update.run(dtId, result.confidence, r.doc_id);
      byType[result.documentType] = (byType[result.documentType] || 0) + 1;
      byConfidence[result.confidence]++;
      updated++;
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }

  return {
    candidates: rows.length,
    updated,
    unmatched,
    kept,
    unknownDocType,
    byType,
    byConfidence,
    rulesLoaded: getContentRules().length,
  };
}

// --- Rule editor surface ------------------------------------------------
// Read + write the three YAML rule files for the in-app editor.
// readRules*() returns { rules: [...] } as parsed YAML. Each rule object
// is whatever the file holds (id, document_type/vendor+product, confidence,
// match, pattern). Order is preserved.
//
// writeRules*() validates by running the same loader the runtime uses, then
// writes the new YAML. The loader throws on bad shape, which surfaces as
// the API's error message — the disk file is left untouched on failure.

const ALLOWED_FIELDS = {
  filename: new Set(["id", "document_type", "confidence", "match", "pattern", "source_data_type"]),
  content:  new Set(["id", "document_type", "confidence", "match", "pattern", "source_data_type"]),
  product:  new Set(["id", "vendor", "product", "confidence", "match", "pattern", "source_data_type"]),
};

function readRulesFile(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const doc = YAML.parse(raw) || {};
  return Array.isArray(doc.rules) ? doc.rules : [];
}

// Coerce a rule object received from the UI into a plain object containing
// only the fields the loader recognizes. Drops nulls / undefineds so they
// don't serialize as empty `key: null` entries.
function sanitizeRule(rule, kind) {
  const allowed = ALLOWED_FIELDS[kind];
  const out = {};
  for (const k of allowed) {
    const v = rule[k];
    if (v === undefined || v === null || v === "") continue;
    out[k] = String(v);
  }
  return out;
}

function writeRulesFile(filePath, rules, kind, loader) {
  const sanitized = (Array.isArray(rules) ? rules : []).map((r) => sanitizeRule(r, kind));
  const yaml = YAML.stringify({ rules: sanitized });

  // Validate by writing to a temp file, attempting load, then atomic-rename.
  const tmp = filePath + ".tmp";
  fs.writeFileSync(tmp, yaml, "utf8");
  try {
    loader(tmp);
  } catch (e) {
    fs.unlinkSync(tmp);
    throw e;
  }
  fs.renameSync(tmp, filePath);
}

// Loader probes that the writers use for validation. They mirror the
// real loaders byte-for-byte except they read from a caller-supplied path
// (so the temp file gets validated, not the live file).
function validateFilenameRules(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("classifier.yaml must have a top-level `rules:` list");
  }
  for (const r of doc.rules) {
    const id = r.id || "(unnamed)";
    if (!r.document_type) throw new Error(`rule ${id}: missing document_type`);
    if (!VALID_CONFIDENCE.has(r.confidence)) throw new Error(`rule ${id}: invalid confidence ${r.confidence}`);
    if (!VALID_MATCH_FIELDS.has(r.match))    throw new Error(`rule ${id}: invalid match ${r.match}`);
    if (!r.pattern) throw new Error(`rule ${id}: missing pattern`);
    try { new RegExp(r.pattern, "i"); }
    catch (e) { throw new Error(`rule ${id}: bad regex — ${e.message}`); }
    if (r.source_data_type != null && r.source_data_type !== "" && !VALID_SOURCE_DATA_TYPES.has(r.source_data_type)) {
      throw new Error(`rule ${id}: invalid source_data_type ${r.source_data_type} (must be Vendor, JobFiles, or Any)`);
    }
  }
}

function validateContentRules(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("content_classifier.yaml must have a top-level rules: list");
  }
  for (const r of doc.rules) {
    const id = r.id || "(unnamed)";
    if (!r.document_type) throw new Error(`rule ${id}: missing document_type`);
    if (!VALID_CONFIDENCE.has(r.confidence)) throw new Error(`rule ${id}: invalid confidence ${r.confidence}`);
    if (!CONTENT_VALID_MATCH.has(r.match))   throw new Error(`rule ${id}: invalid match ${r.match} (use first_page or extract)`);
    if (!r.pattern) throw new Error(`rule ${id}: missing pattern`);
    try { new RegExp(r.pattern, "i"); }
    catch (e) { throw new Error(`rule ${id}: bad regex — ${e.message}`); }
    if (r.source_data_type != null && r.source_data_type !== "" && !VALID_SOURCE_DATA_TYPES.has(r.source_data_type)) {
      throw new Error(`rule ${id}: invalid source_data_type ${r.source_data_type} (must be Vendor, JobFiles, or Any)`);
    }
  }
}

function validateProductRules(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("product_extractor.yaml must have a top-level rules: list");
  }
  for (const r of doc.rules) {
    const id = r.id || "(unnamed)";
    if (!r.vendor)  throw new Error(`rule ${id}: missing vendor`);
    if (!r.product) throw new Error(`rule ${id}: missing product`);
    if (!VALID_CONFIDENCE.has(r.confidence)) throw new Error(`rule ${id}: invalid confidence ${r.confidence}`);
    if (!PRODUCT_VALID_MATCH.has(r.match))   throw new Error(`rule ${id}: invalid match ${r.match}`);
    if (!r.pattern) throw new Error(`rule ${id}: missing pattern`);
    try { new RegExp(r.pattern, "i"); }
    catch (e) { throw new Error(`rule ${id}: bad regex — ${e.message}`); }
    if (r.source_data_type != null && r.source_data_type !== "" && !VALID_SOURCE_DATA_TYPES.has(r.source_data_type)) {
      throw new Error(`rule ${id}: invalid source_data_type ${r.source_data_type} (must be Vendor, JobFiles, or Any)`);
    }
  }
}

function validateVendorRules(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("vendor_extractor.yaml must have a top-level rules: list");
  }
  for (const r of doc.rules) {
    const id = r.id || "(unnamed)";
    if (!r.vendor) throw new Error(`rule ${id}: missing vendor (literal name or "$1" capture ref)`);
    if (!VALID_CONFIDENCE.has(r.confidence)) throw new Error(`rule ${id}: invalid confidence ${r.confidence}`);
    if (!VENDOR_VALID_MATCH.has(r.match))    throw new Error(`rule ${id}: invalid match ${r.match}`);
    if (!r.pattern) throw new Error(`rule ${id}: missing pattern`);
    try { new RegExp(r.pattern, "i"); }
    catch (e) { throw new Error(`rule ${id}: bad regex — ${e.message}`); }
    if (r.source_data_type != null && r.source_data_type !== "" && !VALID_SOURCE_DATA_TYPES.has(r.source_data_type)) {
      throw new Error(`rule ${id}: invalid source_data_type ${r.source_data_type} (must be Vendor, JobFiles, or Any)`);
    }
  }
}

function validateBuildingExtractorRules(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("building_extractor.yaml must have a top-level rules: list");
  }
  for (const r of doc.rules) {
    const id = r.id || "(unnamed)";
    if (!r.building_uid) throw new Error(`rule ${id}: missing building_uid`);
    if (!VALID_CONFIDENCE.has(r.confidence)) throw new Error(`rule ${id}: invalid confidence ${r.confidence}`);
    if (!BUILDING_VALID_MATCH.has(r.match))  throw new Error(`rule ${id}: invalid match ${r.match}`);
    if (!r.pattern) throw new Error(`rule ${id}: missing pattern`);
    try { new RegExp(r.pattern, "i"); }
    catch (e) { throw new Error(`rule ${id}: bad regex — ${e.message}`); }
    if (r.source_data_type != null && r.source_data_type !== "" && !VALID_SOURCE_DATA_TYPES.has(r.source_data_type)) {
      throw new Error(`rule ${id}: invalid source_data_type ${r.source_data_type} (must be Vendor, JobFiles, or Any)`);
    }
  }
}

export function readFilenameRules() { return readRulesFile(RULES_PATH); }
export function readContentRulesRaw() { return readRulesFile(CONTENT_RULES_PATH); }
export function readProductRulesRaw() { return readRulesFile(PRODUCT_RULES_PATH); }
export function readVendorRulesRaw()  { return readRulesFile(VENDOR_RULES_PATH); }
export function readBuildingExtractorRulesRaw() { return readRulesFile(BUILDING_RULES_PATH); }

export function writeFilenameRules(rules) {
  writeRulesFile(RULES_PATH, rules, "filename", validateFilenameRules);
  reloadRules();
}
export function writeContentRules(rules) {
  writeRulesFile(CONTENT_RULES_PATH, rules, "content", validateContentRules);
  reloadContentRules();
}
export function writeProductRules(rules) {
  writeRulesFile(PRODUCT_RULES_PATH, rules, "product", validateProductRules);
  reloadProductRules();
}
export function writeVendorRules(rules) {
  writeRulesFile(VENDOR_RULES_PATH, rules, "vendor", validateVendorRules);
  reloadVendorRules();
}
export function writeBuildingExtractorRules(rules) {
  writeRulesFile(BUILDING_RULES_PATH, rules, "building", validateBuildingExtractorRules);
  reloadBuildingExtractorRules();
}

// Static metadata for the UI — labels and allowed `match` values per kind.
// The UI fetches this once, then uses it to populate dropdowns.
export const CLASSIFIER_KINDS = {
  filename: {
    label: "Filename rules",
    file:  "classifier.yaml",
    confidences:      [...VALID_CONFIDENCE],
    matches:          [...VALID_MATCH_FIELDS],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "document_type", "confidence", "match", "pattern", "source_data_type"],
  },
  content: {
    label: "Content rules",
    file:  "content_classifier.yaml",
    confidences:      [...VALID_CONFIDENCE],
    matches:          [...CONTENT_VALID_MATCH],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "document_type", "confidence", "match", "pattern", "source_data_type"],
  },
  product: {
    label: "Product rules",
    file:  "product_extractor.yaml",
    confidences:      [...VALID_CONFIDENCE],
    matches:          [...PRODUCT_VALID_MATCH],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "vendor", "product", "confidence", "match", "pattern", "source_data_type"],
  },
  vendor: {
    label: "Vendor extractor rules",
    file:  "vendor_extractor.yaml",
    confidences:      [...VALID_CONFIDENCE],
    matches:          [...VENDOR_VALID_MATCH],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    // No `product` column — vendor accepts a literal name or "$1" capture ref.
    columns:          ["id", "vendor", "confidence", "match", "pattern", "source_data_type"],
  },
  building: {
    label: "Building extractor rules",
    file:  "building_extractor.yaml",
    confidences:      [...VALID_CONFIDENCE],
    matches:          [...BUILDING_VALID_MATCH],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "building_uid", "confidence", "match", "pattern", "source_data_type"],
  },
  // --- File / Content split kinds ----------------------------------------
  // The split kinds share storage with their parent (same YAML file, same
  // columns) but the editor filters the visible rule list by match-type
  // and defaults add-rule's match: to the file/content side accordingly.
  // matchSubset:
  //   "file"    → show rules where match ∈ {name, path}
  //   "content" → show rules where match ∈ {first_page, extract}
  // The editor reads parent / matchSubset / addDefaultMatch from this
  // metadata.
  vendor_file: {
    label: "Vendor file extractor",
    file:  "vendor_extractor.yaml",
    parent: "vendor",
    matchSubset: "file",
    addDefaultMatch: "name",
    confidences:      [...VALID_CONFIDENCE],
    matches:          ["name", "path"],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "vendor", "confidence", "match", "pattern", "source_data_type"],
  },
  vendor_content: {
    label: "Vendor content extractor",
    file:  "vendor_extractor.yaml",
    parent: "vendor",
    matchSubset: "content",
    addDefaultMatch: "first_page",
    confidences:      [...VALID_CONFIDENCE],
    matches:          ["first_page", "extract"],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "vendor", "confidence", "match", "pattern", "source_data_type"],
  },
  product_file: {
    label: "Product file extractor",
    file:  "product_extractor.yaml",
    parent: "product",
    matchSubset: "file",
    addDefaultMatch: "name",
    confidences:      [...VALID_CONFIDENCE],
    matches:          ["name", "path"],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "vendor", "product", "confidence", "match", "pattern", "source_data_type"],
  },
  product_content: {
    label: "Product content extractor",
    file:  "product_extractor.yaml",
    parent: "product",
    matchSubset: "content",
    addDefaultMatch: "first_page",
    confidences:      [...VALID_CONFIDENCE],
    matches:          ["first_page", "extract"],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "vendor", "product", "confidence", "match", "pattern", "source_data_type"],
  },
  building_file: {
    label: "Building file extractor",
    file:  "building_extractor.yaml",
    parent: "building",
    matchSubset: "file",
    addDefaultMatch: "name",
    confidences:      [...VALID_CONFIDENCE],
    matches:          ["name", "path"],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "building_uid", "confidence", "match", "pattern", "source_data_type"],
  },
  building_content: {
    label: "Building content extractor",
    file:  "building_extractor.yaml",
    parent: "building",
    matchSubset: "content",
    addDefaultMatch: "first_page",
    confidences:      [...VALID_CONFIDENCE],
    matches:          ["first_page", "extract"],
    sourceDataTypes:  [...VALID_SOURCE_DATA_TYPES],
    columns:          ["id", "building_uid", "confidence", "match", "pattern", "source_data_type"],
  },
};

// Exposed for UI: the canonical list of source-data-type values.
export const SOURCE_DATA_TYPES = [...VALID_SOURCE_DATA_TYPES];
