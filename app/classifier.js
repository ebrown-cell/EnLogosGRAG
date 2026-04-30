// Filename/path-based document_type classifier.
// Reads classifier.yaml at module load. First matching rule wins.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import YAML from "yaml";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Rule files live at <repo>/classifiers/, sibling to <repo>/app/.
const CLASSIFIERS_DIR = path.join(__dirname, "..", "classifiers");
const RULES_PATH = path.join(CLASSIFIERS_DIR, "classifier.yaml");

const VALID_MATCH_FIELDS = new Set(["name", "parent", "path", "file_type"]);
const VALID_CONFIDENCE = new Set(["high", "medium", "low"]);

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
export function classify({ name, parent, path: fullPath, file_type }) {
  const fields = { name, parent, path: fullPath, file_type };
  for (const rule of getRules()) {
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
      SELECT d.id          AS doc_id,
             d.confidence  AS prev_confidence,
             f.path        AS path,
             f.name        AS name,
             p.name        AS parent,
             f.file_type   AS file_type
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

function classifyContent({ text, firstPage }) {
  for (const rule of getContentRules()) {
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

const PRODUCT_RULES_PATH = path.join(CLASSIFIERS_DIR, "product_classifier.yaml");
const PRODUCT_VALID_MATCH = new Set(["name", "path", "first_page", "extract"]);

let productRules = null;

function loadProductRules() {
  const raw = fs.readFileSync(PRODUCT_RULES_PATH, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("product_classifier.yaml must have a top-level rules: list");
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

// Run all product rules against every document. Inserts into products
// table on demand, then upserts document_products links. A single
// document can match multiple products (e.g. compatibility lists).
export function classifyAllByProduct(db) {
  reloadProductRules();
  const rules = getProductRules();

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
  // present). Doing this in one query avoids per-row roundtrips.
  const rows = db.prepare(`
    SELECT d.id          AS doc_id,
           f.name        AS name,
           f.path        AS path,
           v.id          AS vendor_id,
           v.name        AS vendor_name,
           e.text        AS text
    FROM documents d
    JOIN files f         ON f.id = d.file_id
    JOIN vendors v       ON v.id = f.vendor_id
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
    // Wipe existing links for a clean re-classify. Re-running shouldn't
    // accumulate stale rows when rules change.
    db.exec("DELETE FROM document_products");

    for (const r of rows) {
      docsScanned++;
      const vendorRules = rulesByVendor.get(r.vendor_name);
      if (!vendorRules || vendorRules.length === 0) continue;

      const firstPage = firstPageOf(r.text);
      const seenInDoc = new Set();    // dedupe same product hit twice in one doc

      for (const rule of vendorRules) {
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
    rulesLoaded: rules.length,
    docsScanned,
    docsWithProducts,
    totalLinks,
    distinctProducts: db.prepare("SELECT COUNT(*) AS n FROM products").get().n,
    byVendor,
    byProduct,
    unknownVendors: [...unknownVendors],
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
    SELECT d.id           AS doc_id,
           d.confidence   AS prev_confidence,
           e.text         AS text
    FROM documents d
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
      const result = classifyContent({ text: r.text, firstPage: fp });
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
  filename: new Set(["id", "document_type", "confidence", "match", "pattern"]),
  content:  new Set(["id", "document_type", "confidence", "match", "pattern"]),
  product:  new Set(["id", "vendor", "product", "confidence", "match", "pattern"]),
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
  }
}

function validateProductRules(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const doc = YAML.parse(raw);
  if (!doc || !Array.isArray(doc.rules)) {
    throw new Error("product_classifier.yaml must have a top-level rules: list");
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
  }
}

export function readFilenameRules() { return readRulesFile(RULES_PATH); }
export function readContentRulesRaw() { return readRulesFile(CONTENT_RULES_PATH); }
export function readProductRulesRaw() { return readRulesFile(PRODUCT_RULES_PATH); }

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

// Static metadata for the UI — labels and allowed `match` values per kind.
// The UI fetches this once, then uses it to populate dropdowns.
export const CLASSIFIER_KINDS = {
  filename: {
    label: "Filename rules",
    file:  "classifier.yaml",
    confidences: [...VALID_CONFIDENCE],
    matches:     [...VALID_MATCH_FIELDS],
    columns:     ["id", "document_type", "confidence", "match", "pattern"],
  },
  content: {
    label: "Content rules",
    file:  "content_classifier.yaml",
    confidences: [...VALID_CONFIDENCE],
    matches:     [...CONTENT_VALID_MATCH],
    columns:     ["id", "document_type", "confidence", "match", "pattern"],
  },
  product: {
    label: "Product rules",
    file:  "product_classifier.yaml",
    confidences: [...VALID_CONFIDENCE],
    matches:     [...PRODUCT_VALID_MATCH],
    columns:     ["id", "vendor", "product", "confidence", "match", "pattern"],
  },
};
