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
import * as sqliteVec from "sqlite-vec";

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
    description TEXT,
    -- Source-data-type scope: 'Vendor', 'JobFiles', or 'Any'.
    -- 'Any' = applies to both corpora (e.g. 'drawing'). Default is 'Vendor'
    -- because the original taxonomy was authored against vendor manuals.
    source_data_type TEXT NOT NULL DEFAULT 'Vendor'
);

-- files: inventory of paths from the listing. NO vendor association — that
-- moved to documents.vendor_id (nullable). Files can exist without a known
-- vendor (e.g. JobFiles paths, where the owner segment is a job/site name,
-- not a manufacturer).
CREATE TABLE IF NOT EXISTS files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    path       TEXT    NOT NULL UNIQUE,
    name       TEXT    NOT NULL,
    parent_id  INTEGER,
    file_type  TEXT,
    is_dir     INTEGER NOT NULL DEFAULT 0,
    depth      INTEGER NOT NULL,
    -- Derived at ingest from path segments. 'Vendor' if path contains a
    -- 'vendors' segment; 'JobFiles' if it contains a 'JobFiles' segment;
    -- 'Other' otherwise. Used to route classifier rules per source corpus.
    source_data_type TEXT NOT NULL DEFAULT 'Vendor',
    -- Absolute path of the chosen folder at ingest time (the "source root").
    -- Used to derive the cache sibling: <corpus_root>_text/<rel>.json (extracts)
    -- and <corpus_root>_text/<rel>.hash (sha256 sidecars). Lets multi-corpus
    -- DBs route each file's caches to the right sibling without segment magic.
    corpus_root TEXT,
    FOREIGN KEY (parent_id) REFERENCES files(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_files_parent       ON files(parent_id);
CREATE INDEX IF NOT EXISTS idx_files_name         ON files(name);
CREATE INDEX IF NOT EXISTS idx_files_filetype     ON files(file_type);
-- idx_files_source_data is created by migrateAddFilesSourceDataType()
-- so it can wait for the ALTER TABLE on legacy DBs.

CREATE TABLE IF NOT EXISTS documents (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id           INTEGER NOT NULL UNIQUE,
    document_type_id  INTEGER,
    confidence        TEXT,
    sha256            TEXT,
    -- Vendor association moved here from files. NULL when the file's path
    -- has no manufacturer segment (e.g. JobFiles paths). Soft pointer —
    -- not a hard FK requirement; ingest fills it in for Vendor SDT paths.
    vendor_id         INTEGER,
    FOREIGN KEY (file_id)          REFERENCES files(id) ON DELETE CASCADE,
    FOREIGN KEY (document_type_id) REFERENCES document_types(id),
    FOREIGN KEY (vendor_id)        REFERENCES vendors(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_documents_file    ON documents(file_id);
CREATE INDEX IF NOT EXISTS idx_documents_doctype ON documents(document_type_id);
-- idx_documents_sha256 + idx_documents_vendor are created by migrations
-- so they can wait for ALTER TABLE on legacy DBs.

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

-- Specific file paths ignored at ingest time. Adding a row here also
-- deletes the corresponding documents row immediately (cascade to extracts,
-- products, etc.) so the corpus reflects the user's intent right away. The
-- files row stays — re-ingest can drop it on the next walk if needed.
-- notes is freeform; the UI uses "de-duplicated" for cluster cleanups and
-- "manual" for individual ignores.
CREATE TABLE IF NOT EXISTS ignored_files (
    path       TEXT PRIMARY KEY,
    added_at   TEXT NOT NULL,
    notes      TEXT
);
CREATE INDEX IF NOT EXISTS idx_ignored_files_notes ON ignored_files(notes);

-- canonical_buildings: snapshot of Kent's
--   CE_HISTORICAL.BUILDING_IDENTITY.BUILDING_CANONICAL.
-- Refreshed on demand via the snapshot loader. building_uid is Kent's
-- canonical 16-char id. This is reference data — survives Purge ALL,
-- never written to by the extractor.
CREATE TABLE IF NOT EXISTS canonical_buildings (
    building_uid       TEXT PRIMARY KEY,
    canonical_address  TEXT,
    canonical_city     TEXT,
    canonical_state    TEXT,
    canonical_zip      TEXT,
    delivery_line_1    TEXT,
    last_line          TEXT,
    lat                REAL,
    lng                REAL,
    county_name        TEXT,
    campus_group_id    TEXT,
    sources            TEXT,
    source_count       INTEGER,
    total_activity     INTEGER,
    name_count         INTEGER,
    names_sample       TEXT,
    suite_count        INTEGER,
    raw_address_count  INTEGER,
    data_quality       TEXT,
    -- match_tokens: precomputed lowercase, deduplicated, space-joined
    -- string of distinguishing tokens from canonical_address +
    -- delivery_line_1 + names_sample. Built at snapshot time.
    match_tokens       TEXT,
    snapshot_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_canonical_buildings_state ON canonical_buildings(canonical_state);
CREATE INDEX IF NOT EXISTS idx_canonical_buildings_zip   ON canonical_buildings(canonical_zip);

-- buildings: extracted-from-documents building references. One row per
-- distinct building observed in the corpus, deduped via dedup_key:
--   - matched to canonical_buildings → dedup_key = "uid:" + canonical_uid
--   - unmatched (orphan)             → dedup_key = "raw:" + normalized raw form
-- Mirrors document_products' shape (id PK + confidence + source) so a
-- document can be linked to a building with provenance for why.
--
-- A building exists in this table because the extractor surfaced it from
-- some document. The Snowflake snapshot does NOT populate this table.
-- Purging files cascades through documents → document_buildings, but
-- buildings rows themselves are kept (they're cheap, and re-matching can
-- benefit from prior dedup keys).
CREATE TABLE IF NOT EXISTS buildings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_uid   TEXT,             -- nullable: orphan = no canonical match yet
    dedup_key       TEXT NOT NULL UNIQUE,
    raw_name        TEXT,             -- e.g. "JPL B183", "Bordeaux Apts"
    raw_address     TEXT,             -- e.g. "4800 Oak Grove Dr, Pasadena CA"
    raw_street      TEXT,             -- e.g. "4800 oak grove"
    raw_city        TEXT,
    raw_state       TEXT,
    raw_zip         TEXT,
    match_confidence TEXT,            -- 'high' | 'medium' | 'low' | NULL (orphan)
    match_source    TEXT,             -- 'names_token' | 'address_xref' | 'manual' | NULL
    first_seen_at   TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL,
    FOREIGN KEY (canonical_uid) REFERENCES canonical_buildings(building_uid) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_buildings_canonical ON buildings(canonical_uid);
CREATE INDEX IF NOT EXISTS idx_buildings_dedup     ON buildings(dedup_key);

-- Address variants: snapshot of BUILDING_ADDRESS_XREF. Always points at
-- a canonical_buildings row (xref_id is Snowflake's PK; survives re-sync).
CREATE TABLE IF NOT EXISTS building_addresses (
    xref_id            INTEGER PRIMARY KEY,
    building_uid       TEXT NOT NULL,    -- → canonical_buildings.building_uid
    canonical_address  TEXT,
    raw_address        TEXT,
    raw_street         TEXT,
    raw_city           TEXT,
    raw_state          TEXT,
    suite_designator   TEXT,
    source_system      TEXT,
    snapshot_at        TEXT NOT NULL,
    FOREIGN KEY (building_uid) REFERENCES canonical_buildings(building_uid) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_building_addresses_uid ON building_addresses(building_uid);

-- Document → building links. Points at the *extracted* buildings row
-- (NOT canonical_buildings) — that's where dedup happens, and it lets us
-- link to orphans (buildings extracted but not yet canonicalized).
-- M:N: one document can mention multiple buildings; one building has
-- many docs.
CREATE TABLE IF NOT EXISTS document_buildings (
    document_id    INTEGER NOT NULL,
    building_id    INTEGER NOT NULL,
    confidence     TEXT,
    source         TEXT,
    matched_token  TEXT,
    PRIMARY KEY (document_id, building_id),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY (building_id) REFERENCES buildings(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_doc_buildings_doc      ON document_buildings(document_id);
CREATE INDEX IF NOT EXISTS idx_doc_buildings_building ON document_buildings(building_id);

-- Buildings that should never be matched against documents — typically
-- the company's own offices, whose letterhead address appears on every
-- contract/invoice. Keyed by canonical_uid since the blocklist applies
-- at the canonical level (every spelling variant is suppressed).
CREATE TABLE IF NOT EXISTS ignored_buildings (
    building_uid  TEXT PRIMARY KEY,    -- → canonical_buildings.building_uid
    added_at      TEXT NOT NULL,
    notes         TEXT,
    FOREIGN KEY (building_uid) REFERENCES canonical_buildings(building_uid) ON DELETE CASCADE
);

-- =====================================================================
-- Semantic search layer (sqlite-vec)
--
-- Two halves: a regular table with chunk text + metadata, and a vec0
-- virtual table with the embedding only. They share rowid so a vec
-- KNN match joins back to the metadata row in O(1). Splitting like
-- this means re-embedding (different model, different dimension) only
-- rebuilds vec_document_chunks — document_chunks stays put.
--
-- chunking unit: one row = one page of a PDF (file_id, page_no).
-- chunk_index reserved for future sub-page splits without schema churn.
-- char_start / char_end are offsets into document_extracts.text so the
-- UI can highlight the source span.
-- =====================================================================

CREATE TABLE IF NOT EXISTS document_chunks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id   INTEGER NOT NULL,
    file_id       INTEGER NOT NULL,
    page_no       INTEGER,
    chunk_index   INTEGER NOT NULL DEFAULT 0,
    char_start    INTEGER,
    char_end      INTEGER,
    text          TEXT    NOT NULL,
    token_count   INTEGER,
    created_at    TEXT    NOT NULL,
    UNIQUE (document_id, page_no, chunk_index),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY (file_id)     REFERENCES files(id)     ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_document_chunks_doc  ON document_chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_document_chunks_file ON document_chunks(file_id);

-- Per-document embedding job state, mirrors the resumable shape of
-- the extractor worker. One row per document; status drives the
-- background embedder ('pending' | 'done' | 'error' | 'skipped').
-- model + dim recorded so we can detect when the corpus was embedded
-- with an older model and needs a re-run.
CREATE TABLE IF NOT EXISTS embed_jobs (
    document_id   INTEGER PRIMARY KEY,
    status        TEXT    NOT NULL DEFAULT 'pending',
    model         TEXT,
    dim           INTEGER,
    chunk_count   INTEGER,
    started_at    TEXT,
    finished_at   TEXT,
    error         TEXT,
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_embed_jobs_status ON embed_jobs(status);

-- vec0 virtual tables. Keyed by rowid that matches document_chunks.id
-- (resp. vendors.id) so KNN -> metadata join is trivial. Dim 1536
-- chosen to fit OpenAI text-embedding-3-small / Voyage voyage-3-lite;
-- swapping to another model with a different dim means dropping and
-- recreating just these two tables.
CREATE VIRTUAL TABLE IF NOT EXISTS vec_document_chunks USING vec0(
    embedding float[1536]
);

CREATE VIRTUAL TABLE IF NOT EXISTS vec_vendors USING vec0(
    embedding float[1536]
);
`;

// Document type taxonomy. Originally a snapshot of EFI's Phase 2 prompt
// (vendor-manual focus); extended for the JobFiles corpus (per-job records).
// Each row: [name, description, source_data_type] where source_data_type is
// 'Vendor', 'JobFiles', or 'Any'. 'Any' types apply to both corpora.
export const DOCUMENT_TYPES = [
  // --- Vendor corpus types (manufacturer documentation) ---
  ["installation_manual",   "Step-by-step instructions for installing equipment in the field.", "Vendor"],
  ["programming_manual",    "Configuration and programming reference for a panel or device.", "Vendor"],
  ["operations_manual",     "How to run the system day-to-day after install + commissioning.", "Vendor"],
  ["operating_instructions","User-facing how-to for an end user, often a single device.", "Vendor"],
  ["reference_guide",       "Lookup material — facts, specs, codes — not a procedure.", "Vendor"],
  ["reporting_codes",       "Tables of fault/event/status codes the panel can emit.", "Vendor"],
  ["compatibility_document","What devices, panels, or accessories work together.", "Vendor"],
  ["datasheet",             "Manufacturer spec sheet for a single product or product family.", "Vendor"],
  ["wiring_guide",          "Wiring diagrams and termination instructions.", "Vendor"],
  ["network_guide",         "Network topology, IP/serial setup, integration with other systems.", "Vendor"],
  ["training",              "Course material, certification guides, training workbooks.", "Vendor"],
  ["addendum",              "Supplements, addenda, replacement-page packets that modify or extend an existing manual — not a standalone document.", "Vendor"],
  ["listing_certificate",   "Listing approvals, certification letters, regulatory approvals from authorities (CSFM, UL, FM, ETL, other AHJs).", "Vendor"],
  ["program_sheet",         "Vendor-supplied fill-in worksheet capturing per-install programming options (zone assignments, user codes, panel settings). Distinct from a programming_manual, which explains how to program in general.", "Vendor"],

  // --- Universal types (apply across both corpora) ---
  ["drawing",               "CAD drawings, floor plans, panel layouts, schematics, as-builts.", "Any"],
  ["contract",              "Signed agreements, work-for-hire contracts, scope of work.", "Any"],
  ["design_document",       "Engineering design package: calcs, narratives, riser diagrams.", "Any"],
  ["inspection_report",     "Filled-out inspection forms or summary reports of an inspection.", "Any"],
  ["service_record",        "Service-call write-ups, maintenance logs, repair documentation.", "Any"],
  ["technician_note",       "Field notes from a technician — short, often informal.", "Any"],
  ["field_note",            "Generic on-site observations not tied to a specific service call.", "Any"],
  ["other",                 "Doesn't fit any of the categories above.", "Any"],

  // --- JobFiles corpus types (per-job operational records) ---
  ["proposal",              "Customer-facing pitch with scope and pricing — pre-acceptance.", "JobFiles"],
  ["quote",                 "Formal price; signed quote = customer commitment.", "JobFiles"],
  ["bid_workup",            "Internal pricing draft / cost analysis before the customer-facing proposal.", "JobFiles"],
  ["sales_order",           "Internal sales order form, post-acceptance commitment to deliver.", "JobFiles"],
  ["sales_summary",         "Internal sales-pipeline working doc — workups, summaries, sales entries, SOFs. Pre-contract internal pricing/scope drafts. Distinct from bid_workup (cost analysis) and proposal (customer-facing).", "JobFiles"],
  ["purchase_order",        "Customer-issued PO authorizing the work.", "JobFiles"],
  ["letter_of_intent",      "LOI committing to a contract before formal execution.", "JobFiles"],
  ["notice_to_proceed",     "NTP authorizing the start of work, often pre-contract.", "JobFiles"],
  ["estimate",              "Cost estimate, takeoff, or budget — distinct from a customer-facing proposal/quote.", "JobFiles"],
  ["installation_record",   "Per-job sign-off, NFPA 72, inspection card — the record of an install. Distinct from installation_manual (the vendor's how-to).", "JobFiles"],
  ["permit",                "Building / fire permit, plan check receipt, AHJ correspondence.", "JobFiles"],
  ["panel_program",         "Per-install panel program file (.mdb, .accdb, .200, .400 etc.). Concrete programming, not the manual.", "JobFiles"],
  ["site_photo",            "Site / install photographs.", "JobFiles"],
  ["site_layout",           "Per-job layout diagrams — floor plans annotated with device placement.", "JobFiles"],
  ["warranty_nfpa_cert",    "NFPA Cert of Compliance, Inspection & Testing reports; warranties.", "JobFiles"],
  ["rfc",                   "Request for Change / Change Order between bid and as-built.", "JobFiles"],
  ["lien_release",          "Preliminary notice, conditional/unconditional progress payment release.", "JobFiles"],
  ["invoice",               "Customer invoice, billing application.", "JobFiles"],
  ["subcontract",           "Subcontract agreements with trades.", "JobFiles"],
  ["materials_list",        "Bill of materials, materials schedule.", "JobFiles"],
  ["insurance_doc",         "Insurance requirements, certificates, attachments.", "JobFiles"],
  ["affidavit",             "Affidavits, apprentice certifications, worker attestations.", "JobFiles"],
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

// If documents.file_id FK points at a stale renamed-away table name
// (an artifact of earlier rename-rebuild migrations), rebuild documents
// with the FK pointing at the real `files`. Idempotent: bails when the
// FK target is already "files".
//
// Stale candidates seen historically: 'files_old', 'files_premigrate'.
function repairDocumentsFK(db) {
  const row = db
    .prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='documents'")
    .get();
  if (!row || !row.sql) return;
  if (!/files_(old|premigrate)/i.test(row.sql)) return; // healthy

  console.log("[migrate] repairing documents.file_id FK (was pointing at a renamed table)");
  // Discover current columns so we don't accidentally drop sha256, vendor_id, etc.
  const cols = db.prepare("PRAGMA table_info(documents)").all();
  const colNames = cols.map((c) => c.name);
  const has = (n) => colNames.includes(n);

  // Build CREATE statement from observed columns. Keep the original PK,
  // FK targets, and any columns added by later migrations.
  const createCols = [
    "id                INTEGER PRIMARY KEY AUTOINCREMENT",
    "file_id           INTEGER NOT NULL UNIQUE",
    "document_type_id  INTEGER",
    "confidence        TEXT",
  ];
  if (has("sha256"))    createCols.push("sha256            TEXT");
  if (has("vendor_id")) createCols.push("vendor_id         INTEGER");
  const createFKs = [
    "FOREIGN KEY (file_id)          REFERENCES files(id) ON DELETE CASCADE",
    "FOREIGN KEY (document_type_id) REFERENCES document_types(id)",
  ];
  if (has("vendor_id")) createFKs.push("FOREIGN KEY (vendor_id) REFERENCES vendors(id) ON DELETE SET NULL");

  const insertCols = ["id", "file_id", "document_type_id", "confidence"];
  if (has("sha256"))    insertCols.push("sha256");
  if (has("vendor_id")) insertCols.push("vendor_id");

  db.exec("PRAGMA foreign_keys = OFF");
  db.exec("BEGIN");
  try {
    db.exec("ALTER TABLE documents RENAME TO documents_broken");
    for (const idx of ["idx_documents_file", "idx_documents_doctype", "idx_documents_sha256", "idx_documents_vendor"]) {
      db.exec(`DROP INDEX IF EXISTS ${idx}`);
    }
    db.exec(`
      CREATE TABLE documents (
        ${[...createCols, ...createFKs].join(",\n        ")}
      )
    `);
    db.exec(`
      INSERT INTO documents (${insertCols.join(", ")})
      SELECT ${insertCols.join(", ")} FROM documents_broken
    `);
    db.exec("DROP TABLE documents_broken");
    db.exec("CREATE INDEX idx_documents_file    ON documents(file_id)");
    db.exec("CREATE INDEX idx_documents_doctype ON documents(document_type_id)");
    if (has("sha256"))    db.exec("CREATE INDEX idx_documents_sha256 ON documents(sha256)");
    if (has("vendor_id")) db.exec("CREATE INDEX idx_documents_vendor ON documents(vendor_id)");
    db.exec("COMMIT");
    console.log("[migrate] documents FK repaired");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  } finally {
    db.exec("PRAGMA foreign_keys = ON");
  }
}

// When `repairDocumentsFK` renames the documents table to repair its own
// FK target, SQLite cascades that rename into ANY OTHER table whose FK
// points at documents — rewriting their CREATE statements to reference
// the renamed-away table. The rename here is documents → documents_broken,
// so document_extracts and document_products end up with FKs pointing at
// "documents_broken", which gets dropped at the end of repair, leaving
// orphan FKs that crash any insert/cascade.
//
// This function rebuilds those two child tables when their CREATE
// statements still reference documents_broken (or the older documents_old
// from pre-existing legacy code paths). Idempotent.
function repairDocumentsChildFKs(db) {
  const targets = [
    {
      name: "document_extracts",
      create: `CREATE TABLE document_extracts (
          document_id  INTEGER PRIMARY KEY,
          extracted_at TEXT    NOT NULL,
          page_count   INTEGER,
          text         TEXT,
          pages_json   TEXT,
          metadata     TEXT,
          error        TEXT,
          FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
      )`,
      indexes: [],
    },
    {
      name: "document_products",
      create: `CREATE TABLE document_products (
          document_id  INTEGER NOT NULL,
          product_id   INTEGER NOT NULL,
          confidence   TEXT,
          source       TEXT,
          PRIMARY KEY (document_id, product_id),
          FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
          FOREIGN KEY (product_id)  REFERENCES products(id)  ON DELETE CASCADE
      )`,
      indexes: [
        "CREATE INDEX IF NOT EXISTS idx_doc_products_doc     ON document_products(document_id)",
        "CREATE INDEX IF NOT EXISTS idx_doc_products_product ON document_products(product_id)",
      ],
    },
  ];

  for (const t of targets) {
    const row = db
      .prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name=?")
      .get(t.name);
    if (!row || !row.sql) continue;
    if (!/documents_(broken|old)/i.test(row.sql)) continue;

    console.log("[migrate] repairing " + t.name + " FK (was pointing at a renamed-away documents)");
    db.exec("PRAGMA foreign_keys = OFF");
    db.exec("BEGIN");
    try {
      const fixName = t.name + "_fix";
      db.exec(`ALTER TABLE ${t.name} RENAME TO ${fixName}`);
      db.exec(t.create);
      db.exec(`INSERT INTO ${t.name} SELECT * FROM ${fixName}`);
      db.exec(`DROP TABLE ${fixName}`);
      for (const idxSql of t.indexes) db.exec(idxSql);
      db.exec("COMMIT");
      console.log("[migrate] " + t.name + " FK repaired");
    } catch (e) {
      db.exec("ROLLBACK");
      throw e;
    } finally {
      db.exec("PRAGMA foreign_keys = ON");
    }
  }
}

// Older DBs predate the documents.sha256 column. CREATE TABLE IF NOT EXISTS
// won't add columns to an existing table, so we add it via ALTER if missing.
// The index is created here unconditionally (after the ALTER for legacy
// DBs, or just on top of the existing column for fresh DBs).
function migrateAddDocumentSha256(db) {
  const cols = db.prepare("PRAGMA table_info(documents)").all();
  if (cols.length === 0) return; // documents table doesn't exist yet
  if (!cols.some((c) => c.name === "sha256")) {
    db.exec("ALTER TABLE documents ADD COLUMN sha256 TEXT");
  }
  db.exec("CREATE INDEX IF NOT EXISTS idx_documents_sha256 ON documents(sha256)");
}

// Add files.source_data_type column to legacy DBs. Backfills existing rows
// to 'Vendor' (the only corpus that existed before this column was added).
function migrateAddFilesSourceDataType(db) {
  const cols = db.prepare("PRAGMA table_info(files)").all();
  if (cols.length === 0) return;
  if (!cols.some((c) => c.name === "source_data_type")) {
    db.exec("ALTER TABLE files ADD COLUMN source_data_type TEXT NOT NULL DEFAULT 'Vendor'");
    // Backfill: rows that don't already have a value will pick up the default.
    // Anything currently NULL (shouldn't happen with the DEFAULT, but belt-and-braces):
    db.exec("UPDATE files SET source_data_type = 'Vendor' WHERE source_data_type IS NULL OR source_data_type = ''");
  }
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_source_data ON files(source_data_type)");
}

// Add document_types.source_data_type column to legacy DBs. Existing rows
// default to 'Vendor' (the original taxonomy was vendor-only).
function migrateAddDocumentTypesSourceDataType(db) {
  const cols = db.prepare("PRAGMA table_info(document_types)").all();
  if (cols.length === 0) return;
  if (!cols.some((c) => c.name === "source_data_type")) {
    db.exec("ALTER TABLE document_types ADD COLUMN source_data_type TEXT NOT NULL DEFAULT 'Vendor'");
    db.exec("UPDATE document_types SET source_data_type = 'Vendor' WHERE source_data_type IS NULL OR source_data_type = ''");
  }
}

// Add files.corpus_root to legacy DBs. Backfills existing rows by
// inferring the root from the path: walk up to the segment matching
// 'vendors' or 'jobfiles' (case-insensitive) and treat that segment's
// full prefix as the root. Rows with neither segment leave corpus_root
// NULL — caches won't write for them, which is the correct behavior
// (we don't know where to put them).
// Pre-SCHEMA migration: rename the legacy buildings table (canonical
// Snowflake snapshot) to canonical_buildings, and drop the legacy
// document_buildings (which referenced building_uid instead of the new
// buildings.id). Must run BEFORE db.exec(SCHEMA) so SCHEMA's
// `CREATE TABLE IF NOT EXISTS canonical_buildings` doesn't preempt the
// rename by creating an empty new table.
//
// The legacy `buildings` and the new `canonical_buildings` have
// identical column shape — the rename is sufficient. SQLite 3.26+
// auto-rewrites FK references in dependent tables when RENAME is used,
// so building_addresses and ignored_buildings keep their FKs intact.
//
// The legacy document_buildings is dropped because its rows referenced
// canonical UIDs directly; under the new model those links are
// recomputed via the extractor against the new buildings table.
function migrateBuildingsRename(db) {
  const oldCols = db.prepare("PRAGMA table_info(buildings)").all();
  if (oldCols.length === 0) return;          // table doesn't exist yet
  // New shape: id column + no building_uid PK. Old shape: building_uid PK.
  const isOldShape = oldCols.some((c) => c.name === "building_uid")
                  && !oldCols.some((c) => c.name === "id");
  if (!isOldShape) return;

  // Drop document_buildings first — its FK references the old
  // buildings(building_uid) and would block the rename.
  db.exec("DROP TABLE IF EXISTS document_buildings");
  db.exec("ALTER TABLE buildings RENAME TO canonical_buildings");
  // Old indexes were named idx_buildings_*; drop so SCHEMA's
  // idx_canonical_buildings_* can be created cleanly.
  db.exec("DROP INDEX IF EXISTS idx_buildings_state");
  db.exec("DROP INDEX IF EXISTS idx_buildings_zip");
}

function migrateAddFilesCorpusRoot(db) {
  const cols = db.prepare("PRAGMA table_info(files)").all();
  if (cols.length === 0) return;
  if (!cols.some((c) => c.name === "corpus_root")) {
    db.exec("ALTER TABLE files ADD COLUMN corpus_root TEXT");
    // Backfill: for each existing row, infer root = path-up-to-and-including
    // the 'vendors' or 'JobFiles' segment.
    const rows = db.prepare("SELECT id, path FROM files WHERE corpus_root IS NULL").all();
    if (rows.length > 0) {
      const upd = db.prepare("UPDATE files SET corpus_root = ? WHERE id = ?");
      db.exec("BEGIN");
      try {
        for (const r of rows) {
          const root = inferCorpusRoot(r.path);
          if (root) upd.run(root, r.id);
        }
        db.exec("COMMIT");
      } catch (e) {
        db.exec("ROLLBACK");
        throw e;
      }
    }
  }
}

// Walk path segments and return the prefix up to and including the first
// 'vendors' or 'jobfiles' match (case-insensitive). NULL if neither found.
// Preserves the original separator style.
function inferCorpusRoot(p) {
  if (!p) return null;
  const sep = p.includes("\\") ? "\\" : "/";
  const parts = p.split(/[\\/]/);
  for (let i = 0; i < parts.length; i++) {
    const lower = parts[i].toLowerCase();
    if (lower === "vendors" || lower === "jobfiles") {
      return parts.slice(0, i + 1).join(sep);
    }
  }
  return null;
}

// Move vendor association from files.vendor_id (legacy) to
// documents.vendor_id (current). Files no longer have a hard FK to vendors;
// documents optionally point at a vendor (NULL for non-Vendor SDT files).
//
// Step 1: add documents.vendor_id (nullable, soft FK).
// Step 2: copy files.vendor_id → documents.vendor_id for every doc.
// Step 3: rebuild files without the vendor_id column (SQLite-portable
//         table-rename swap rather than DROP COLUMN, for Node-version
//         portability).
// Idempotent: bails when files.vendor_id is already gone.
function migrateMoveVendorIdToDocuments(db) {
  const docCols = db.prepare("PRAGMA table_info(documents)").all();
  if (docCols.length === 0) return;
  const filesCols = db.prepare("PRAGMA table_info(files)").all();
  const filesHasVendor = filesCols.some((c) => c.name === "vendor_id");

  // Step 1: ensure documents.vendor_id exists.
  if (!docCols.some((c) => c.name === "vendor_id")) {
    db.exec("ALTER TABLE documents ADD COLUMN vendor_id INTEGER REFERENCES vendors(id) ON DELETE SET NULL");
  }
  db.exec("CREATE INDEX IF NOT EXISTS idx_documents_vendor ON documents(vendor_id)");

  // Step 2: copy files.vendor_id into documents.vendor_id where docs don't
  // yet have one. Only meaningful while files still has vendor_id.
  if (filesHasVendor) {
    db.exec(`
      UPDATE documents
      SET vendor_id = (SELECT f.vendor_id FROM files f WHERE f.id = documents.file_id)
      WHERE vendor_id IS NULL
    `);
  } else {
    return; // already migrated
  }

  // Step 3: rebuild files without vendor_id. SQLite portable swap.
  console.log("[migrate] dropping files.vendor_id (moved to documents)");
  db.exec("PRAGMA foreign_keys = OFF");
  db.exec("BEGIN");
  try {
    db.exec("ALTER TABLE files RENAME TO files_premigrate");
    // Drop the indexes that travelled with the renamed table — they
    // kept their original names and would collide.
    for (const idx of [
      "idx_files_vendor", "idx_files_parent", "idx_files_name",
      "idx_files_filetype", "idx_files_source_data",
    ]) {
      db.exec(`DROP INDEX IF EXISTS ${idx}`);
    }
    db.exec(`
      CREATE TABLE files (
          id         INTEGER PRIMARY KEY AUTOINCREMENT,
          path       TEXT    NOT NULL UNIQUE,
          name       TEXT    NOT NULL,
          parent_id  INTEGER,
          file_type  TEXT,
          is_dir     INTEGER NOT NULL DEFAULT 0,
          depth      INTEGER NOT NULL,
          source_data_type TEXT NOT NULL DEFAULT 'Vendor',
          FOREIGN KEY (parent_id) REFERENCES files(id) ON DELETE SET NULL
      );
      CREATE INDEX idx_files_parent      ON files(parent_id);
      CREATE INDEX idx_files_name        ON files(name);
      CREATE INDEX idx_files_filetype    ON files(file_type);
      CREATE INDEX idx_files_source_data ON files(source_data_type);
    `);
    db.exec(`
      INSERT INTO files (id, path, name, parent_id, file_type, is_dir, depth, source_data_type)
      SELECT id, path, name, parent_id, file_type, is_dir, depth, source_data_type
      FROM files_premigrate
    `);
    db.exec("DROP TABLE files_premigrate");
    db.exec("COMMIT");
    console.log("[migrate] files.vendor_id removed");
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

// One-time taxonomy snapshot. Only inserts rows if the table is empty —
// won't fight you if you've edited the taxonomy. New types added later
// via mergeNewDocumentTypes().
function seedDocumentTypesIfEmpty(db) {
  const have = db.prepare("SELECT COUNT(*) AS n FROM document_types").get().n;
  if (have > 0) return;
  const insert = db.prepare(
    "INSERT INTO document_types (name, description, source_data_type) VALUES (?, ?, ?)"
  );
  for (const [name, desc, sdt] of DOCUMENT_TYPES) {
    insert.run(name, desc, sdt || "Vendor");
  }
}

// Insert any DOCUMENT_TYPES rows that don't already exist by name, and
// reconcile source_data_type for rows that exist but were tagged 'Vendor'
// by the column-add migration default. Specifically: the original 23
// vendor-only types had several universals (drawing, contract, etc.) that
// belong tagged 'Any' once the JobFiles corpus is in scope. We trust the
// canonical DOCUMENT_TYPES taxonomy as source-of-truth.
//
// Reconciliation rule: if an existing row's source_data_type is 'Vendor'
// (the legacy default) and the canonical taxonomy says 'Any' or 'JobFiles',
// update it. We never overwrite a non-default value (e.g. if the user
// hand-edited a row to 'Any', we leave it).
function mergeNewDocumentTypes(db) {
  const existing = new Map(
    db.prepare("SELECT name, source_data_type FROM document_types").all()
      .map((r) => [r.name, r.source_data_type]),
  );
  const insert = db.prepare(
    "INSERT INTO document_types (name, description, source_data_type) VALUES (?, ?, ?)"
  );
  const updateBlank = db.prepare(
    "UPDATE document_types SET source_data_type = ? WHERE name = ? AND (source_data_type IS NULL OR source_data_type = '')"
  );
  const reconcileFromVendor = db.prepare(
    "UPDATE document_types SET source_data_type = ? WHERE name = ? AND source_data_type = 'Vendor'"
  );
  for (const [name, desc, sdt] of DOCUMENT_TYPES) {
    const want = sdt || "Vendor";
    if (!existing.has(name)) {
      insert.run(name, desc, want);
    } else if (!existing.get(name)) {
      updateBlank.run(want, name);
    } else if (existing.get(name) === "Vendor" && want !== "Vendor") {
      // Legacy default → upgrade to canonical (Any or JobFiles).
      reconcileFromVendor.run(want, name);
    }
  }
}

export function openDb(dbPath = DEFAULT_DB_PATH) {
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.exec("PRAGMA foreign_keys = ON");
  // Load sqlite-vec so SCHEMA's `CREATE VIRTUAL TABLE ... USING vec0`
  // statements are recognized. Must precede db.exec(SCHEMA).
  sqliteVec.load(db);
  // Pre-SCHEMA: rename legacy buildings → canonical_buildings before
  // SCHEMA's IF NOT EXISTS would create an empty new canonical_buildings.
  migrateBuildingsRename(db);
  db.exec(SCHEMA);
  migrateFiles(db);
  migrateAddDocumentSha256(db);
  migrateAddFilesSourceDataType(db);
  migrateAddDocumentTypesSourceDataType(db);
  migrateAddFilesCorpusRoot(db);
  // Must run AFTER files + documents tables exist with their other columns.
  migrateMoveVendorIdToDocuments(db);
  // Renaming files in the migration above triggers SQLite to rewrite the
  // documents.file_id FK target name. Repair it now.
  repairDocumentsFK(db);
  // …and `repairDocumentsFK` renames documents, which similarly cascades
  // into document_extracts + document_products FKs. Repair those too.
  repairDocumentsChildFKs(db);
  seedDocumentTypesIfEmpty(db);
  mergeNewDocumentTypes(db);
  return db;
}
