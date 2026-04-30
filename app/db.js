// SQLite connection + schema for EnLogosGRAG.
// Uses Node's built-in node:sqlite (Node 22+) — no native compile required.
//
// Tables:
//   vendors         predates this project. id + name + notes.
//   document_types  one-time taxonomy snapshot from EFI's Phase 2 prompt.
//   files           inventory of paths from the listing. Integer PK,
//                   parent_id self-FK, vendor_id FK.
//   documents       1:1 with non-dir files. Holds classifier output
//                   (document_type_id + confidence). Created at ingest time
//                   by listing.js. ON DELETE CASCADE so re-ingest churn is
//                   transparent.

import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import path from "node:path";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Repo layout: app/db.js lives next to the other server modules; the SQLite
// file lives at <repo>/db/test.db.
export const DEFAULT_DB_PATH = path.join(__dirname, "..", "db", "test.db");

const SCHEMA = `
CREATE TABLE IF NOT EXISTS vendors (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  TEXT    NOT NULL UNIQUE,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS document_types (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    path       TEXT    NOT NULL UNIQUE,
    name       TEXT    NOT NULL,
    parent_id  INTEGER,
    vendor_id  INTEGER NOT NULL,
    file_type  TEXT,
    is_dir     INTEGER NOT NULL DEFAULT 0,
    depth      INTEGER NOT NULL,
    FOREIGN KEY (parent_id) REFERENCES files(id) ON DELETE SET NULL,
    FOREIGN KEY (vendor_id) REFERENCES vendors(id)
);

CREATE INDEX IF NOT EXISTS idx_files_vendor    ON files(vendor_id);
CREATE INDEX IF NOT EXISTS idx_files_parent    ON files(parent_id);
CREATE INDEX IF NOT EXISTS idx_files_name      ON files(name);
CREATE INDEX IF NOT EXISTS idx_files_filetype  ON files(file_type);

CREATE TABLE IF NOT EXISTS documents (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id           INTEGER NOT NULL UNIQUE,
    document_type_id  INTEGER,
    confidence        TEXT,
    FOREIGN KEY (file_id)          REFERENCES files(id) ON DELETE CASCADE,
    FOREIGN KEY (document_type_id) REFERENCES document_types(id)
);

CREATE INDEX IF NOT EXISTS idx_documents_file    ON documents(file_id);
CREATE INDEX IF NOT EXISTS idx_documents_doctype ON documents(document_type_id);

CREATE TABLE IF NOT EXISTS document_extracts (
    document_id  INTEGER PRIMARY KEY,
    extracted_at TEXT    NOT NULL,
    page_count   INTEGER,
    text         TEXT,
    pages_json   TEXT,
    metadata     TEXT,
    error        TEXT,
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS products (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_id  INTEGER NOT NULL,
    name       TEXT    NOT NULL,
    aliases    TEXT,
    notes      TEXT,
    UNIQUE (vendor_id, name),
    FOREIGN KEY (vendor_id) REFERENCES vendors(id)
);

CREATE INDEX IF NOT EXISTS idx_products_vendor ON products(vendor_id);

CREATE TABLE IF NOT EXISTS document_products (
    document_id  INTEGER NOT NULL,
    product_id   INTEGER NOT NULL,
    confidence   TEXT,
    source       TEXT,
    PRIMARY KEY (document_id, product_id),
    FOREIGN KEY (document_id) REFERENCES documents(id)  ON DELETE CASCADE,
    FOREIGN KEY (product_id)  REFERENCES products(id)   ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_doc_products_doc     ON document_products(document_id);
CREATE INDEX IF NOT EXISTS idx_doc_products_product ON document_products(product_id);

-- Extensions in this table get ingested into files but are NOT given a
-- documents row. That keeps them out of classification, content extraction,
-- products, charts and the dashboard while still letting users see the raw
-- inventory in the files table. Going-forward only: existing documents rows
-- for an extension added later are left alone (re-ingest to apply).
CREATE TABLE IF NOT EXISTS ignored_file_types (
    ext        TEXT PRIMARY KEY,
    added_at   TEXT NOT NULL,
    notes      TEXT
);

-- Folder names ignored at ingest time. Exact name match (case-insensitive)
-- against any segment of the path. The folder itself AND every descendant
-- is dropped from the files table entirely, so they never reach documents,
-- classification, charts, or the dashboard. Going-forward only: existing
-- rows for a folder added later are left alone (re-ingest to apply).
CREATE TABLE IF NOT EXISTS ignored_folders (
    name       TEXT PRIMARY KEY,
    added_at   TEXT NOT NULL,
    notes      TEXT
);
`;

// Hardcoded copy of EFI's Phase 2 prompt taxonomy
// (original_app/prompts/phase_2_synopsis_prompt.md, the document_type enum).
// One-time snapshot — the two sources are intentionally allowed to drift.
export const DOCUMENT_TYPES = [
  ["installation_manual",   "Step-by-step instructions for installing equipment in the field."],
  ["programming_manual",    "Configuration and programming reference for a panel or device."],
  ["operations_manual",     "How to run the system day-to-day after install + commissioning."],
  ["operating_instructions","User-facing how-to for an end user, often a single device."],
  ["reference_guide",       "Lookup material — facts, specs, codes — not a procedure."],
  ["reporting_codes",       "Tables of fault/event/status codes the panel can emit."],
  ["compatibility_document","What devices, panels, or accessories work together."],
  ["datasheet",             "Manufacturer spec sheet for a single product or product family."],
  ["wiring_guide",          "Wiring diagrams and termination instructions."],
  ["network_guide",         "Network topology, IP/serial setup, integration with other systems."],
  ["training",              "Course material, certification guides, training workbooks."],
  ["inspection_report",     "Filled-out inspection forms or summary reports of an inspection."],
  ["service_record",        "Service-call write-ups, maintenance logs, repair documentation."],
  ["technician_note",       "Field notes from a technician — short, often informal."],
  ["field_note",            "Generic on-site observations not tied to a specific service call."],
  ["drawing",               "CAD drawings, floor plans, panel layouts, schematics."],
  ["contract",              "Signed agreements, work-for-hire contracts, scope of work."],
  ["estimate",              "Cost estimates, proposals, quotes, bid documents."],
  ["design_document",       "Engineering design package: calcs, narratives, riser diagrams."],
  ["addendum",              "Supplements, addenda, replacement-page packets that modify or extend an existing manual — not a standalone document."],
  ["listing_certificate",   "Listing approvals, certification letters, regulatory approvals from authorities (CSFM, UL, FM, ETL, other AHJs)."],
  ["program_sheet",         "Vendor-supplied fill-in worksheet capturing per-install programming options (zone assignments, user codes, panel settings). Distinct from a programming_manual, which explains how to program in general."],
  ["other",                 "Doesn't fit any of the categories above."],
];

// Migrates an old-shape `files` (text PRIMARY KEY `path`, text `parent`,
// classification columns inline) to the new shape (integer PK, parent_id,
// classifications hived off into `documents`). Idempotent — bails if the
// new shape is already in place.
//
// Approach: build files_new with the new schema, copy data through, populate
// parent_id with a self-join on path, copy classifications into documents,
// then atomically swap (drop + rename). All inside one transaction.
function migrateFiles(db) {
  const tableInfo = db.prepare("PRAGMA table_info(files)").all();
  if (tableInfo.length === 0) return; // brand-new DB, schema already created the new shape
  const colNames = new Set(tableInfo.map((r) => r.name));

  // Detect new shape: id PK + parent_id (vs. old shape: path PK + parent text).
  const isNewShape = colNames.has("id") && colNames.has("parent_id");
  if (isNewShape) {
    // Just make sure file_type backfill is done (carryover from earlier migration).
    if (colNames.has("file_type")) backfillFileType(db);
    // Repair: an earlier buggy migration may have left documents.file_id
    // pointing at "files_old" instead of "files". Detect and rebuild the
    // table cleanly if so.
    repairDocumentsFK(db);
    return;
  }

  // Old shape detected. Plan: rename files → files_old, let SCHEMA's
  // CREATE TABLE create files in new shape, copy data through, build documents.
  // SCHEMA already ran before us with IF NOT EXISTS, so files_new doesn't
  // exist yet — we have to DROP the in-flight copy of new-shape `files`
  // wait, no: SCHEMA's CREATE TABLE IF NOT EXISTS for `files` saw the OLD
  // table and skipped. So `files` is currently the old-shape table.
  //
  // Steps:
  //   1. ALTER TABLE files RENAME TO files_old;
  //   2. Re-run the new-shape CREATE TABLE files (now no conflict).
  //   3. Same for documents (might have been created earlier without a real
  //      files table, but the FK is deferred until row-write time).
  //   4. INSERT INTO files (...) SELECT ... FROM files_old (without parent_id).
  //   5. UPDATE files SET parent_id = (SELECT id FROM files p WHERE p.path = files_old.parent) JOINed.
  //   6. INSERT INTO documents (file_id, document_type_id, confidence)
  //        SELECT f.id, fo.document_type_id, fo.confidence FROM files f
  //        JOIN files_old fo ON fo.path = f.path
  //        WHERE fo.is_dir = 0;
  //   7. DROP TABLE files_old.
  console.log("[migrate] upgrading files: text PK → integer PK with parent_id and documents table");

  db.exec("PRAGMA foreign_keys = OFF"); // FKs would fight us during the swap
  db.exec("BEGIN");
  try {
    db.exec("ALTER TABLE files RENAME TO files_old");

    // Drop indexes that moved with the renamed table — they kept their
    // original names and would collide with the new ones we create below.
    for (const idx of [
      "idx_files_vendor", "idx_files_parent", "idx_files_name",
      "idx_files_doctype", "idx_files_filetype",
    ]) {
      db.exec(`DROP INDEX IF EXISTS ${idx}`);
    }

    // Now create the new shape (and documents). SCHEMA was already executed
    // by openDb() but `files` got skipped because of the old table. Re-run
    // the file-only DDL now that the conflict is gone.
    db.exec(`
      CREATE TABLE files (
          id         INTEGER PRIMARY KEY AUTOINCREMENT,
          path       TEXT    NOT NULL UNIQUE,
          name       TEXT    NOT NULL,
          parent_id  INTEGER,
          vendor_id  INTEGER NOT NULL,
          file_type  TEXT,
          is_dir     INTEGER NOT NULL DEFAULT 0,
          depth      INTEGER NOT NULL,
          FOREIGN KEY (parent_id) REFERENCES files(id) ON DELETE SET NULL,
          FOREIGN KEY (vendor_id) REFERENCES vendors(id)
      );
      CREATE INDEX idx_files_vendor   ON files(vendor_id);
      CREATE INDEX idx_files_parent   ON files(parent_id);
      CREATE INDEX idx_files_name     ON files(name);
      CREATE INDEX idx_files_filetype ON files(file_type);
    `);

    // SCHEMA may have created `documents` earlier with a FK pointing at
    // whatever `files` was at that moment. To guarantee the FK points at
    // the new `files`, drop+recreate.
    db.exec("DROP TABLE IF EXISTS documents");
    db.exec(`
      CREATE TABLE documents (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          file_id           INTEGER NOT NULL UNIQUE,
          document_type_id  INTEGER,
          confidence        TEXT,
          FOREIGN KEY (file_id)          REFERENCES files(id) ON DELETE CASCADE,
          FOREIGN KEY (document_type_id) REFERENCES document_types(id)
      );
      CREATE INDEX idx_documents_file    ON documents(file_id);
      CREATE INDEX idx_documents_doctype ON documents(document_type_id);
    `);

    // Backfill file_type on the old table first, since it's the easier place
    // to compute (we still have the rows under their old keys).
    if (colNames.has("file_type")) {
      const updateOld = db.prepare("UPDATE files_old SET file_type = ? WHERE path = ?");
      const oldRows = db
        .prepare("SELECT path, name, is_dir FROM files_old WHERE file_type IS NULL")
        .all();
      for (const r of oldRows) {
        const ext = r.is_dir ? null : path.win32.extname(r.name).toLowerCase() || null;
        updateOld.run(ext, r.path);
      }
    }

    // Copy data over, leaving parent_id NULL for now.
    db.exec(`
      INSERT INTO files (path, name, parent_id, vendor_id, file_type, is_dir, depth)
      SELECT path, name, NULL, vendor_id,
             ${colNames.has("file_type") ? "file_type" : "NULL"},
             is_dir, depth
      FROM files_old
    `);

    // Resolve parent_id by joining old.parent (text path) → new.id (integer).
    db.exec(`
      UPDATE files
      SET parent_id = (
        SELECT f.id FROM files f
        WHERE f.path = (SELECT fo.parent FROM files_old fo WHERE fo.path = files.path)
      )
    `);

    // Carry classifications into documents (only for non-dirs that had one).
    if (colNames.has("document_type_id")) {
      db.exec(`
        INSERT INTO documents (file_id, document_type_id, confidence)
        SELECT f.id, fo.document_type_id,
               ${colNames.has("confidence") ? "fo.confidence" : "NULL"}
        FROM files f
        JOIN files_old fo ON fo.path = f.path
        WHERE fo.is_dir = 0 AND fo.document_type_id IS NOT NULL
      `);
    }

    // Eager: every non-dir file gets a documents row, even unclassified.
    // (Classified ones are already there from the previous step — INSERT OR IGNORE.)
    db.exec(`
      INSERT OR IGNORE INTO documents (file_id, document_type_id, confidence)
      SELECT id, NULL, NULL FROM files WHERE is_dir = 0
    `);

    db.exec("DROP TABLE files_old");
    db.exec("COMMIT");
    console.log("[migrate] done");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  } finally {
    db.exec("PRAGMA foreign_keys = ON");
  }
}

// If documents.file_id FK points at "files_old" (an earlier migration bug),
// rebuild the documents table with the FK pointing at the real `files`.
// Idempotent: bails when the FK target is already "files".
function repairDocumentsFK(db) {
  const row = db
    .prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='documents'")
    .get();
  if (!row || !row.sql) return;
  if (!/files_old/i.test(row.sql)) return; // healthy

  console.log("[migrate] repairing documents.file_id FK (was pointing at files_old)");
  db.exec("PRAGMA foreign_keys = OFF");
  db.exec("BEGIN");
  try {
    db.exec("ALTER TABLE documents RENAME TO documents_broken");
    db.exec("DROP INDEX IF EXISTS idx_documents_file");
    db.exec("DROP INDEX IF EXISTS idx_documents_doctype");
    db.exec(`
      CREATE TABLE documents (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          file_id           INTEGER NOT NULL UNIQUE,
          document_type_id  INTEGER,
          confidence        TEXT,
          FOREIGN KEY (file_id)          REFERENCES files(id) ON DELETE CASCADE,
          FOREIGN KEY (document_type_id) REFERENCES document_types(id)
      );
      CREATE INDEX idx_documents_file    ON documents(file_id);
      CREATE INDEX idx_documents_doctype ON documents(document_type_id);
    `);
    db.exec(`
      INSERT INTO documents (id, file_id, document_type_id, confidence)
      SELECT id, file_id, document_type_id, confidence FROM documents_broken
    `);
    db.exec("DROP TABLE documents_broken");
    db.exec("COMMIT");
    console.log("[migrate] documents FK repaired");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  } finally {
    db.exec("PRAGMA foreign_keys = ON");
  }
}

function backfillFileType(db) {
  const update = db.prepare("UPDATE files SET file_type = ? WHERE id = ?");
  const rows = db
    .prepare("SELECT id, name, is_dir FROM files WHERE file_type IS NULL")
    .all();
  if (rows.length === 0) return;
  db.exec("BEGIN");
  try {
    for (const r of rows) {
      const ext = r.is_dir ? null : path.win32.extname(r.name).toLowerCase() || null;
      update.run(ext, r.id);
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
}

// One-time taxonomy snapshot from EFI's Phase 2 prompt.
// Only inserts rows if the table is empty — won't fight you if you edit it.
function seedDocumentTypesIfEmpty(db) {
  const have = db.prepare("SELECT COUNT(*) AS n FROM document_types").get().n;
  if (have > 0) return;
  const insert = db.prepare(
    "INSERT INTO document_types (name, description) VALUES (?, ?)"
  );
  for (const [name, desc] of DOCUMENT_TYPES) {
    insert.run(name, desc);
  }
}

export function openDb(dbPath = DEFAULT_DB_PATH) {
  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA foreign_keys = ON");
  db.exec(SCHEMA);
  migrateFiles(db);
  seedDocumentTypesIfEmpty(db);
  return db;
}
