// Filename/path-based document_type classifier.
// Reads classifier.yaml at module load. First matching rule wins.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import YAML from "yaml";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RULES_PATH = path.join(__dirname, "classifier.yaml");

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
  // name (for `match: parent` rules). documents.id is the row we update.
  const rows = db
    .prepare(`
      SELECT d.id   AS doc_id,
             f.path AS path,
             f.name AS name,
             p.name AS parent,
             f.file_type AS file_type
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

  db.exec("BEGIN");
  try {
    for (const r of rows) {
      const result = classify(r);
      if (!result) {
        update.run(null, null, r.doc_id);
        byConfidence.none++;
        continue;
      }
      const dtId = docTypeIdByName.get(result.documentType);
      if (dtId === undefined) {
        // Rule pointed at a doc_type that doesn't exist in the DB.
        unknownDocType++;
        update.run(null, null, r.doc_id);
        byConfidence.none++;
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

const CONTENT_RULES_PATH = path.join(__dirname, "content_classifier.yaml");
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
