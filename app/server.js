// Web app for EnLogosGRAG: browse the test.db tables and run ingest actions.
// Single-page UI bundled inline — open http://localhost:8780.

import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { openDb } from "./db.js";
import { readListing, ingestVendors, ingestFiles } from "./listing.js";
import {
  classifyAll, classifyAllByContent, classifyAllByProduct, classifyAllByVendor,
  readFilenameRules, readContentRulesRaw, readProductRulesRaw,
  readVendorRulesRaw, readBuildingExtractorRulesRaw,
  writeFilenameRules, writeContentRules, writeProductRules,
  writeVendorRules, writeBuildingExtractorRules,
  CLASSIFIER_KINDS,
} from "./classifier.js";
import * as extractor from "./extractor.js";
import * as hasher from "./hasher.js";
import * as buildingsMatcher from "./buildings_matcher.js";
import { snapshotBuildings, snapshotBuildingAddresses, seedPyrocommIgnored } from "./buildings_loader.js";

// The extractor and hasher both translate canonical S:\ paths to local
// paths using the same remap table the open-file feature uses. Wire that
// up once at module load.
extractor.setPathRemapper(remapPath);
hasher.setPathRemapper(remapPath);
buildingsMatcher.setPathRemapper(remapPath);

const SERVER_DIR = path.dirname(fileURLToPath(import.meta.url));
// Repo root sits one level above app/. Used to resolve siblings like
// public/, uploads/, db/, classifiers/.
const REPO_DIR = path.resolve(SERVER_DIR, "..");
const PUBLIC_DIR  = path.join(REPO_DIR, "public");
const UPLOADS_DIR = path.join(REPO_DIR, "uploads");

const PORT = Number(process.env.PORT) || 8780;
const DEFAULT_SOURCE = "C:\\data\\PyroCommData";
const DEFAULT_LISTING_NAME = "basic_listing.txt";

const TABLES = ["vendors", "document_types", "files", "documents", "document_extracts", "products", "document_products", "canonical_buildings", "buildings", "building_addresses", "document_buildings", "ignored_buildings"];

// Prefix remap for opening files. The listing in the DB uses canonical
// share paths (S:\vendors\...) generated on a different machine. Locally,
// the same files live under C:\data\PyroCommData\PyroCommSubset\Vendors\.
// We translate at open-time only — the DB stays canonical so the listing
// remains portable.
const PATH_REMAPS = [
  { from: "S:\\vendors\\", to: "C:\\data\\PyroCommData\\PyroCommSubset\\Vendors\\" },
];

function remapPath(p) {
  if (!p) return p;
  // Case-insensitive prefix match — the DB has lowercase "vendors", the
  // local copy has capital "Vendors". Windows opens either way, but be
  // tolerant of both at the JS level.
  const lower = p.toLowerCase();
  for (const { from, to } of PATH_REMAPS) {
    if (lower.startsWith(from.toLowerCase())) {
      return to + p.slice(from.length);
    }
  }
  return p;
}

// --- Helpers ---------------------------------------------------------------

function findListingInFolder(folder) {
  const direct = path.join(folder, DEFAULT_LISTING_NAME);
  if (fs.existsSync(direct)) return direct;
  for (const entry of fs.readdirSync(folder)) {
    if (/listing.*\.txt$/i.test(entry)) return path.join(folder, entry);
  }
  return null;
}

function resolveListingPath(source) {
  if (!source) return null;
  if (!fs.existsSync(source)) return null;
  const stat = fs.statSync(source);
  if (stat.isFile()) return source;
  if (stat.isDirectory()) return findListingInFolder(source);
  return null;
}

function tableCounts(db) {
  return {
    vendors:             db.prepare("SELECT COUNT(*) AS n FROM vendors").get().n,
    document_types:      db.prepare("SELECT COUNT(*) AS n FROM document_types").get().n,
    files:               db.prepare("SELECT COUNT(*) AS n FROM files").get().n,
    documents:           db.prepare("SELECT COUNT(*) AS n FROM documents").get().n,
    document_extracts:   db.prepare("SELECT COUNT(*) AS n FROM document_extracts").get().n,
    products:            db.prepare("SELECT COUNT(*) AS n FROM products").get().n,
    document_products:   db.prepare("SELECT COUNT(*) AS n FROM document_products").get().n,
    canonical_buildings: db.prepare("SELECT COUNT(*) AS n FROM canonical_buildings").get().n,
    buildings:           db.prepare("SELECT COUNT(*) AS n FROM buildings").get().n,
    building_addresses:  db.prepare("SELECT COUNT(*) AS n FROM building_addresses").get().n,
    document_buildings:  db.prepare("SELECT COUNT(*) AS n FROM document_buildings").get().n,
    ignored_buildings:   db.prepare("SELECT COUNT(*) AS n FROM ignored_buildings").get().n,
  };
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      try {
        resolve(raw ? JSON.parse(raw) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

function sendJson(res, status, body) {
  const buf = Buffer.from(JSON.stringify(body), "utf8");
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": buf.length,
  });
  res.end(buf);
}

function sendHtml(res, html) {
  const buf = Buffer.from(html, "utf8");
  res.writeHead(200, {
    "content-type": "text/html; charset=utf-8",
    "content-length": buf.length,
  });
  res.end(buf);
}

// --- API -------------------------------------------------------------------

function handleStatus() {
  const db = openDb();
  try {
    const counts = tableCounts(db);
    // Per-confidence classification breakdown for the sidebar status line.
    const totalDocs = counts.documents;
    const classified = db
      .prepare("SELECT COUNT(*) AS n FROM documents WHERE document_type_id IS NOT NULL")
      .get().n;
    const byConfRows = db
      .prepare(
        `SELECT confidence, COUNT(*) AS n FROM documents
         WHERE document_type_id IS NOT NULL
         GROUP BY confidence`,
      )
      .all();
    const byConfidence = { high: 0, medium: 0, low: 0 };
    for (const r of byConfRows) {
      if (r.confidence in byConfidence) byConfidence[r.confidence] = r.n;
    }
    const ignored = {
      types:   db.prepare("SELECT COUNT(*) AS n FROM ignored_file_types").get().n,
      folders: db.prepare("SELECT COUNT(*) AS n FROM ignored_folders").get().n,
      files:   db.prepare("SELECT COUNT(*) AS n FROM ignored_files").get().n,
    };
    return {
      counts,
      defaultSource: DEFAULT_SOURCE,
      classification: {
        total: totalDocs,
        classified,
        unclassified: totalDocs - classified,
        byConfidence,
      },
      ignored,
    };
  } finally {
    db.close();
  }
}

function handleResolveSource(body) {
  const source = (body.source || "").trim();
  if (!source) return { ok: false, error: "Source path is empty" };
  if (!fs.existsSync(source)) {
    return { ok: false, error: `Path does not exist: ${source}` };
  }
  const listing = resolveListingPath(source);
  if (!listing) {
    return {
      ok: false,
      error: `No listing file found at ${source} (looked for *_listing.txt)`,
    };
  }
  return { ok: true, listingPath: listing };
}

// Per-table allowlist: client column name -> SQL ORDER BY expression.
// Keys are the column names the client surfaces (matching what each
// browseXxx() returns); values are the SQL fragments safe to splice
// into ORDER BY. Anything not in this map is rejected — never let a
// raw client value into a SQL string.
const SORT_KEYS = {
  vendors: {
    id:    "id",
    name:  "name",
    notes: "notes",
  },
  document_types: {
    id:          "id",
    name:        "name",
    description: "description",
  },
  files: {
    id:        "id",
    name:      "name",
    path:      "path",
    parent_id: "parent_id",
    vendor_id: "vendor_id",
    file_type: "file_type",
    is_dir:    "is_dir",
    depth:     "depth",
  },
  documents: {
    id:            "d.id",
    document_name: "document_name",
    vendor_name:   "v.name",
    document_type: "dt.name",
    confidence:    "d.confidence",
    file_type:     "f.file_type",
    extract:       "extract",
    sha256:        "d.sha256",
    file_path:     "f.path",
  },
  document_extracts: {
    document_id: "e.document_id",
    file_name:   "f.name",
    page_count:  "e.page_count",
    extracted_at:"e.extracted_at",
    error:       "e.error",
  },
  products: {
    id:          "p.id",
    name:        "p.name",
    vendor_name: "v.name",
    notes:       "p.notes",
  },
  document_products: {
    product_name:  "p.name",
    vendor_name:   "v.name",
    document_name: "document_name",
    confidence:    "dp.confidence",
    source:        "dp.source",
  },
  // canonical_buildings: read-only Snowflake mirror, sortable on the
  // useful canonical columns. (Was previously named "buildings".)
  canonical_buildings: {
    building_uid:      "building_uid",
    canonical_address: "canonical_address",
    canonical_city:    "canonical_city",
    canonical_state:   "canonical_state",
    canonical_zip:     "canonical_zip",
    total_activity:    "total_activity",
    name_count:        "name_count",
    suite_count:       "suite_count",
    data_quality:      "data_quality",
  },
  // buildings: extracted-from-documents references. The interesting
  // sort axes are dedup/identification columns + the doc-link count.
  buildings: {
    id:               "id",
    canonical_uid:    "canonical_uid",
    raw_name:         "raw_name",
    raw_address:      "raw_address",
    raw_city:         "raw_city",
    raw_state:        "raw_state",
    match_confidence: "match_confidence",
    match_source:     "match_source",
    first_seen_at:    "first_seen_at",
    last_seen_at:     "last_seen_at",
  },
  building_addresses: {
    xref_id:           "xref_id",
    building_uid:      "building_uid",
    canonical_address: "canonical_address",
    raw_address:       "raw_address",
    raw_city:          "raw_city",
    raw_state:         "raw_state",
    source_system:     "source_system",
  },
  document_buildings: {
    document_id:    "db.document_id",
    document_name:  "document_name",
    building_id:    "db.building_id",
    raw_name:       "b.raw_name",
    raw_address:    "b.raw_address",
    confidence:     "db.confidence",
    source:         "db.source",
  },
  ignored_buildings: {
    building_uid:      "ib.building_uid",
    canonical_address: "cb.canonical_address",
    canonical_city:    "cb.canonical_city",
    canonical_state:   "cb.canonical_state",
    canonical_zip:     "cb.canonical_zip",
    total_activity:    "cb.total_activity",
    notes:             "ib.notes",
    added_at:          "ib.added_at",
  },
};

// Resolves a (table, sortColumn, sortDir) request into a safe ORDER BY
// clause. Falls back to a sensible default per table when the input is
// missing or invalid.
function resolveOrderBy(table, sortColumn, sortDir, fallback) {
  const map = SORT_KEYS[table] || {};
  const dir = sortDir === "desc" ? "DESC" : "ASC";
  if (sortColumn && Object.prototype.hasOwnProperty.call(map, sortColumn)) {
    return `ORDER BY ${map[sortColumn]} ${dir}`;
  }
  return fallback;
}

// `table` is checked against the TABLES allowlist before being substituted
// into the SQL string — never use req input directly in a query.
function handleBrowse(body) {
  const table = body.table;
  if (!TABLES.includes(table)) {
    return { ok: false, error: `Unknown table: ${table}` };
  }
  const limit = Math.max(1, Math.min(500, Number(body.limit) || 100));
  const offset = Math.max(0, Number(body.offset) || 0);
  const filter = (body.filter || "").trim();
  const sortColumn = body.sortColumn || "";
  const sortDir = body.sortDir === "desc" ? "desc" : "asc";

  const db = openDb();
  try {
    // documents gets a custom joined SELECT — the raw row (file_id /
    // document_type_id integers) is mostly opaque, so we resolve the
    // foreign keys to readable values for browsing.
    if (table === "documents") {
      return browseDocuments(db, {
        limit,
        offset,
        filter,
        classifiedFilter: body.classifiedFilter || "all",
        minConfidence: body.minConfidence || "all",
        exactConfidence: body.exactConfidence || "",
        fileTypeFilters: Array.isArray(body.fileTypeFilters) ? body.fileTypeFilters : [],
        documentTypeFilters: Array.isArray(body.documentTypeFilters) ? body.documentTypeFilters : [],
        productFilter: body.productFilter || "",
        hasProductFilter: body.hasProductFilter || "",
        sdtFilter: body.sdtFilter || "",
        buildingFilter: body.buildingFilter || "",
        hasBuildingFilter: body.hasBuildingFilter || "",
        sortColumn, sortDir,
      });
    }
    if (table === "document_extracts") {
      return browseExtracts(db, { limit, offset, filter, sortColumn, sortDir });
    }
    if (table === "products") {
      return browseProducts(db, { limit, offset, filter, sortColumn, sortDir });
    }
    if (table === "document_products") {
      return browseDocumentProducts(db, { limit, offset, filter, sortColumn, sortDir });
    }
    if (table === "canonical_buildings") {
      return browseCanonicalBuildings(db, { limit, offset, filter, sortColumn, sortDir });
    }
    if (table === "buildings") {
      return browseBuildings(db, { limit, offset, filter, sortColumn, sortDir });
    }
    if (table === "document_buildings") {
      return browseDocumentBuildings(db, { limit, offset, filter, sortColumn, sortDir });
    }
    if (table === "ignored_buildings") {
      return browseIgnoredBuildings(db, { limit, offset, filter, sortColumn, sortDir });
    }

    const clauses = [];
    const args = [];
    if (filter) {
      // Filter shape per table — the columns we expose for free-text search.
      if (table === "vendors") {
        clauses.push("(name LIKE ? OR (notes IS NOT NULL AND notes LIKE ?))");
        args.push(`%${filter}%`, `%${filter}%`);
      } else if (table === "document_types") {
        clauses.push("(name LIKE ? OR (description IS NOT NULL AND description LIKE ?))");
        args.push(`%${filter}%`, `%${filter}%`);
      } else if (table === "files") {
        clauses.push("(path LIKE ? OR name LIKE ?)");
        args.push(`%${filter}%`, `%${filter}%`);
      }
    }
    // Source-data-type filter on document_types. Mirrors the editor
    // convention: 'Vendor' shows Vendor + Any types; 'JobFiles' shows
    // JobFiles + Any; 'Any' shows everything (no filter applied).
    if (table === "document_types" && body.sourceDataTypeFilter && body.sourceDataTypeFilter !== "Any") {
      clauses.push("(source_data_type = ? OR source_data_type = 'Any')");
      args.push(body.sourceDataTypeFilter);
    }
    if (table === "files" && Array.isArray(body.fileTypeFilters) && body.fileTypeFilters.length) {
      const placeholders = body.fileTypeFilters.map(() => "?").join(",");
      clauses.push(`file_type IN (${placeholders})`);
      args.push(...body.fileTypeFilters);
    }
    // Files table: filter by whether the file has a paired documents row.
    // active = in-scope (documents row exists); ignored = no documents row,
    // i.e. extension was on the ignore list at last ingest. Directories are
    // excluded from "active"/"ignored" since they never get documents rows.
    if (table === "files" && body.ignoredFilter && body.ignoredFilter !== "all") {
      if (body.ignoredFilter === "active") {
        clauses.push(
          "is_dir = 0 AND EXISTS (SELECT 1 FROM documents d WHERE d.file_id = files.id)",
        );
      } else if (body.ignoredFilter === "ignored") {
        clauses.push(
          "is_dir = 0 AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.file_id = files.id)",
        );
      }
    }
    const where = clauses.length ? "WHERE " + clauses.join(" AND ") : "";
    const totalRow = db
      .prepare(`SELECT COUNT(*) AS n FROM ${table} ${where}`)
      .get(...args);
    const total = totalRow.n;

    // Default order: PK ASC. Most ephemeral tables have an integer `id`
    // PK; a few canonical/ignored tables don't (their PK is a string
    // like building_uid or path). Pick a sensible default per table so
    // the generic browse query doesn't error out with "no such column: id".
    const DEFAULT_ORDER = {
      ignored_buildings:  "ORDER BY added_at DESC",
      ignored_files:      "ORDER BY added_at DESC",
      ignored_folders:    "ORDER BY added_at DESC",
      ignored_file_types: "ORDER BY added_at DESC",
      canonical_buildings:"ORDER BY building_uid ASC",
      building_addresses: "ORDER BY xref_id ASC",
      document_types:     "ORDER BY id ASC",
    };
    const fallbackOrder = DEFAULT_ORDER[table] || "ORDER BY id ASC";
    const orderBy = resolveOrderBy(table, sortColumn, sortDir, fallbackOrder);
    const rows = db
      .prepare(`SELECT * FROM ${table} ${where} ${orderBy} LIMIT ? OFFSET ?`)
      .all(...args, limit, offset);

    const columns = rows[0] ? Object.keys(rows[0]) : tableColumns(db, table);
    return { ok: true, total, limit, offset, filter, columns, rows };
  } finally {
    db.close();
  }
}

// Joined view: documents resolved through files + vendors + document_types.
// Columns chosen for browsability — the raw integer FKs aren't shown.
//
// classifiedFilter: "all" | "classified" | "unclassified"
// minConfidence:    "all" | "low" | "medium" | "high"
//   "low"    keeps low/medium/high (and excludes unclassified)
//   "medium" keeps medium/high
//   "high"   keeps high only
function browseDocuments(db, { limit, offset, filter, classifiedFilter, minConfidence, exactConfidence, fileTypeFilters, documentTypeFilters, productFilter, hasProductFilter, sdtFilter, buildingFilter, hasBuildingFilter, sortColumn, sortDir }) {
  const { from, where, args } = buildDocumentsWhere({
    filter, classifiedFilter, minConfidence, exactConfidence, fileTypeFilters, documentTypeFilters, productFilter, hasProductFilter, sdtFilter, buildingFilter, hasBuildingFilter,
  });
  const total = db
    .prepare(`SELECT COUNT(*) AS n ${from} ${where}`)
    .get(...args).n;

  // Strip the file extension from f.name to produce document_name.
  // file_type is the lowercased extension with leading dot (e.g. ".pdf").
  // Length-based slice: substr(name, 1, length(name) - length(file_type)).
  // Falls back to the full name when file_type is NULL (dirs shouldn't
  // appear here, but be defensive).
  // The from clause from buildDocumentsWhere already joins documents/files/
  // vendors/document_types. We add a left join to document_extracts here so
  // we can surface "has extract" / error per row in the joined view.
  const fromWithExtract = from + " LEFT JOIN document_extracts e ON e.document_id = d.id ";
  const rows = db
    .prepare(
      `SELECT d.id            AS id,
              CASE
                WHEN f.file_type IS NOT NULL
                  AND length(f.name) > length(f.file_type)
                  AND lower(substr(f.name, length(f.name) - length(f.file_type) + 1)) = f.file_type
                THEN substr(f.name, 1, length(f.name) - length(f.file_type))
                ELSE f.name
              END             AS document_name,
              v.name          AS vendor_name,
              dt.name         AS document_type,
              dt.description  AS document_type_description,
              d.confidence    AS confidence,
              f.file_type     AS file_type,
              CASE
                WHEN e.document_id IS NULL THEN ''
                WHEN e.error IS NOT NULL   THEN 'err'
                ELSE 'ok'
              END             AS extract,
              d.sha256        AS sha256,
              f.path          AS file_path
       ${fromWithExtract} ${where}
       ${resolveOrderBy("documents", sortColumn, sortDir, "ORDER BY d.id")}
       LIMIT ? OFFSET ?`,
    )
    .all(...args, limit, offset);

  // 'extract' is intentionally NOT in this list — it's used by the per-row
  // action menu (View Extract enabled-state), not as a visible column.
  const columns = [
    "id",
    "document_name",
    "vendor_name",
    "document_type",
    "confidence",
    "file_type",
    "sha256",
    "file_path",
  ];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// Joined view of document_extracts with the file name + char count, since
// the raw row would just show document_id (an int) and a giant text blob.
function browseExtracts(db, { limit, offset, filter, sortColumn, sortDir }) {
  const FROM = `
    FROM document_extracts e
    JOIN documents d ON d.id = e.document_id
    JOIN files f     ON f.id = d.file_id
  `;
  let where = "";
  const args = [];
  if (filter) {
    where = "WHERE f.name LIKE ? OR f.path LIKE ? OR (e.error IS NOT NULL AND e.error LIKE ?)";
    const like = `%${filter}%`;
    args.push(like, like, like);
  }

  const total = db.prepare(`SELECT COUNT(*) AS n ${FROM} ${where}`).get(...args).n;
  const rows = db.prepare(
    `SELECT e.document_id   AS document_id,
            f.name          AS file_name,
            e.page_count    AS page_count,
            length(e.text)  AS chars,
            CASE WHEN e.error IS NOT NULL THEN 'err' ELSE 'ok' END
                            AS extract,
            e.error         AS error,
            e.extracted_at  AS extracted_at,
            f.path          AS file_path
     ${FROM} ${where}
     ${resolveOrderBy("document_extracts", sortColumn, sortDir, "ORDER BY e.document_id")}
     LIMIT ? OFFSET ?`,
  ).all(...args, limit, offset);

  const columns = [
    "document_id", "file_name", "page_count", "chars",
    "extract", "error", "extracted_at", "file_path",
  ];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// Joined view of products with vendor_name resolved + a doc count from
// document_products. Replaces the raw products view because vendor_id
// alone is opaque.
function browseProducts(db, { limit, offset, filter, sortColumn, sortDir }) {
  const FROM = `
    FROM products p
    JOIN vendors v ON v.id = p.vendor_id
  `;
  let where = "";
  const args = [];
  if (filter) {
    where = "WHERE v.name LIKE ? OR p.name LIKE ? OR (p.notes IS NOT NULL AND p.notes LIKE ?)";
    const like = `%${filter}%`;
    args.push(like, like, like);
  }

  const total = db.prepare(`SELECT COUNT(*) AS n ${FROM} ${where}`).get(...args).n;
  const rows = db.prepare(
    `SELECT p.id      AS id,
            v.name    AS vendor_name,
            p.name    AS name,
            p.aliases AS aliases,
            p.notes   AS notes,
            (SELECT COUNT(*) FROM document_products dp WHERE dp.product_id = p.id) AS docs
     ${FROM} ${where}
     ${resolveOrderBy("products", sortColumn, sortDir, "ORDER BY v.name, p.name")}
     LIMIT ? OFFSET ?`,
  ).all(...args, limit, offset);

  const columns = ["id", "vendor_name", "name", "aliases", "notes", "docs"];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// Joined view of document_products. Replaces the raw two-int row with
// readable names (same document_name treatment as the documents view —
// extension stripped).
function browseDocumentProducts(db, { limit, offset, filter, sortColumn, sortDir }) {
  const FROM = `
    FROM document_products dp
    JOIN documents d ON d.id = dp.document_id
    JOIN files f     ON f.id = d.file_id
    JOIN products p  ON p.id = dp.product_id
    JOIN vendors v   ON v.id = p.vendor_id
  `;
  let where = "";
  const args = [];
  if (filter) {
    where = "WHERE f.name LIKE ? OR f.path LIKE ? OR p.name LIKE ? OR v.name LIKE ? OR (dp.source IS NOT NULL AND dp.source LIKE ?)";
    const like = `%${filter}%`;
    args.push(like, like, like, like, like);
  }

  const total = db.prepare(`SELECT COUNT(*) AS n ${FROM} ${where}`).get(...args).n;
  const rows = db.prepare(
    `SELECT p.name          AS product_name,
            v.name          AS vendor_name,
            dp.document_id  AS document_id,
            CASE
              WHEN f.file_type IS NOT NULL
                AND length(f.name) > length(f.file_type)
                AND lower(substr(f.name, length(f.name) - length(f.file_type) + 1)) = f.file_type
              THEN substr(f.name, 1, length(f.name) - length(f.file_type))
              ELSE f.name
            END             AS document_name,
            dp.confidence   AS confidence,
            dp.source       AS source,
            f.path          AS file_path
     ${FROM} ${where}
     ${resolveOrderBy("document_products", sortColumn, sortDir, "ORDER BY p.name, v.name, dp.document_id")}
     LIMIT ? OFFSET ?`,
  ).all(...args, limit, offset);

  const columns = [
    "product_name", "vendor_name", "document_id", "document_name",
    "confidence", "source", "file_path",
  ];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// Canonical buildings browse — read-only mirror of Kent's BUILDING_CANONICAL.
// Joins in a doc-link count via the *new* buildings → document_buildings
// chain so the row is still useful at a glance.
function browseCanonicalBuildings(db, { limit, offset, filter, sortColumn, sortDir }) {
  let where = "";
  const args = [];
  if (filter) {
    where = `WHERE (canonical_address LIKE ? OR canonical_city LIKE ?
                   OR (names_sample IS NOT NULL AND names_sample LIKE ?)
                   OR building_uid LIKE ?)`;
    const like = `%${filter}%`;
    args.push(like, like, like, like);
  }
  const total = db.prepare(
    "SELECT COUNT(*) AS n FROM canonical_buildings " + where,
  ).get(...args).n;

  const rows = db.prepare(
    `SELECT cb.building_uid       AS building_uid,
            cb.canonical_address  AS canonical_address,
            cb.canonical_city     AS canonical_city,
            cb.canonical_state    AS canonical_state,
            cb.canonical_zip      AS canonical_zip,
            cb.total_activity     AS total_activity,
            cb.name_count         AS name_count,
            cb.suite_count        AS suite_count,
            cb.data_quality       AS data_quality,
            (SELECT COUNT(*) FROM document_buildings db
                JOIN buildings b ON b.id = db.building_id
              WHERE b.canonical_uid = cb.building_uid) AS doc_count,
            cb.names_sample       AS names_sample
     FROM canonical_buildings cb
     ${where}
     ${resolveOrderBy("canonical_buildings", sortColumn, sortDir, "ORDER BY total_activity DESC, canonical_address")}
     LIMIT ? OFFSET ?`,
  ).all(...args, limit, offset);

  const columns = [
    "building_uid", "canonical_address", "canonical_city", "canonical_state",
    "canonical_zip", "doc_count", "total_activity", "name_count",
    "suite_count", "data_quality", "names_sample",
  ];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// Buildings browse — extracted-from-documents references. Each row is
// one observed building (deduped by canonical_uid or by raw form). Joins
// in the doc-link count and the canonical_address (for matched rows).
function browseBuildings(db, { limit, offset, filter, sortColumn, sortDir }) {
  let where = "";
  const args = [];
  if (filter) {
    where = `WHERE (b.raw_name LIKE ? OR b.raw_address LIKE ? OR b.raw_city LIKE ?
                   OR b.canonical_uid LIKE ?
                   OR EXISTS (
                     SELECT 1 FROM canonical_buildings cb
                      WHERE cb.building_uid = b.canonical_uid
                        AND (cb.canonical_address LIKE ? OR cb.names_sample LIKE ?)
                   ))`;
    const like = `%${filter}%`;
    args.push(like, like, like, like, like, like);
  }
  const total = db.prepare(
    "SELECT COUNT(*) AS n FROM buildings b " + where,
  ).get(...args).n;

  const rows = db.prepare(
    `SELECT b.id                 AS id,
            b.canonical_uid      AS canonical_uid,
            b.raw_name           AS raw_name,
            b.raw_address        AS raw_address,
            b.raw_city           AS raw_city,
            b.raw_state          AS raw_state,
            b.match_confidence   AS match_confidence,
            b.match_source       AS match_source,
            b.first_seen_at      AS first_seen_at,
            b.last_seen_at       AS last_seen_at,
            (SELECT COUNT(*) FROM document_buildings db
              WHERE db.building_id = b.id) AS doc_count,
            cb.canonical_address AS canonical_address
     FROM buildings b
     LEFT JOIN canonical_buildings cb ON cb.building_uid = b.canonical_uid
     ${where}
     ${resolveOrderBy("buildings", sortColumn, sortDir, "ORDER BY b.last_seen_at DESC, b.id")}
     LIMIT ? OFFSET ?`,
  ).all(...args, limit, offset);

  const columns = [
    "id", "canonical_uid", "raw_name", "canonical_address",
    "raw_address", "raw_city", "raw_state",
    "match_confidence", "match_source", "doc_count",
    "first_seen_at", "last_seen_at",
  ];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// document_buildings browse — joins through documents/files + buildings
// + canonical_buildings to expose the full provenance for each link.
function browseDocumentBuildings(db, { limit, offset, filter, sortColumn, sortDir }) {
  const FROM = `
    FROM document_buildings db
    JOIN documents d                 ON d.id = db.document_id
    JOIN files f                     ON f.id = d.file_id
    JOIN buildings b                 ON b.id = db.building_id
    LEFT JOIN canonical_buildings cb ON cb.building_uid = b.canonical_uid
  `;
  let where = "";
  const args = [];
  if (filter) {
    where = `WHERE f.name LIKE ? OR f.path LIKE ?
                  OR (b.raw_name IS NOT NULL AND b.raw_name LIKE ?)
                  OR (b.raw_address IS NOT NULL AND b.raw_address LIKE ?)
                  OR (cb.canonical_address IS NOT NULL AND cb.canonical_address LIKE ?)
                  OR (db.source IS NOT NULL AND db.source LIKE ?)`;
    const like = `%${filter}%`;
    args.push(like, like, like, like, like, like);
  }
  const total = db.prepare(`SELECT COUNT(*) AS n ${FROM} ${where}`).get(...args).n;

  const rows = db.prepare(
    `SELECT db.document_id      AS document_id,
            CASE
              WHEN f.file_type IS NOT NULL
                AND length(f.name) > length(f.file_type)
                AND lower(substr(f.name, length(f.name) - length(f.file_type) + 1)) = f.file_type
              THEN substr(f.name, 1, length(f.name) - length(f.file_type))
              ELSE f.name
            END                  AS document_name,
            db.building_id       AS building_id,
            b.raw_name           AS raw_name,
            COALESCE(cb.canonical_address, b.raw_address) AS raw_address,
            db.confidence        AS confidence,
            db.source            AS source,
            db.matched_token     AS matched_token,
            f.path               AS file_path
     ${FROM} ${where}
     ${resolveOrderBy("document_buildings", sortColumn, sortDir, "ORDER BY db.document_id, b.id")}
     LIMIT ? OFFSET ?`,
  ).all(...args, limit, offset);

  const columns = [
    "document_id", "document_name", "building_id", "raw_name", "raw_address",
    "confidence", "source", "matched_token", "file_path",
  ];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// Ignored buildings browse — joins with canonical_buildings so each row
// shows the address + sample names + activity level alongside the user's
// note. Without the join the view is just opaque UIDs.
function browseIgnoredBuildings(db, { limit, offset, filter, sortColumn, sortDir }) {
  const FROM = `
    FROM ignored_buildings ib
    LEFT JOIN canonical_buildings cb ON cb.building_uid = ib.building_uid
  `;
  let where = "";
  const args = [];
  if (filter) {
    where = `WHERE ib.building_uid LIKE ?
                  OR (ib.notes IS NOT NULL AND ib.notes LIKE ?)
                  OR (cb.canonical_address IS NOT NULL AND cb.canonical_address LIKE ?)
                  OR (cb.canonical_city IS NOT NULL AND cb.canonical_city LIKE ?)
                  OR (cb.names_sample IS NOT NULL AND cb.names_sample LIKE ?)`;
    const like = `%${filter}%`;
    args.push(like, like, like, like, like);
  }
  const total = db.prepare(`SELECT COUNT(*) AS n ${FROM} ${where}`).get(...args).n;

  // Default order: most recently added first. Caller's sort wins via
  // SORT_KEYS — see below.
  const rows = db.prepare(
    `SELECT ib.building_uid       AS building_uid,
            ib.added_at           AS added_at,
            ib.notes              AS notes,
            cb.canonical_address  AS canonical_address,
            cb.canonical_city     AS canonical_city,
            cb.canonical_state    AS canonical_state,
            cb.canonical_zip      AS canonical_zip,
            cb.total_activity     AS total_activity,
            cb.names_sample       AS names_sample
     ${FROM} ${where}
     ${resolveOrderBy("ignored_buildings", sortColumn, sortDir, "ORDER BY ib.added_at DESC")}
     LIMIT ? OFFSET ?`,
  ).all(...args, limit, offset);

  const columns = [
    "building_uid", "canonical_address", "canonical_city", "canonical_state",
    "canonical_zip", "total_activity", "notes", "added_at", "names_sample",
  ];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// Single-extract fetch for the modal viewer. Returns text + metadata +
// page count + the joined file info, but not pages_json (huge — leave
// for future structured viewer).
function handleGetExtract(body) {
  const docId = Number(body.documentId);
  if (!Number.isFinite(docId) || docId <= 0) {
    return { ok: false, error: "Missing or invalid documentId." };
  }
  const db = openDb();
  try {
    const row = db.prepare(`
      SELECT e.document_id, e.extracted_at, e.page_count, e.text,
             e.metadata, e.error,
             f.name AS file_name, f.path AS file_path
      FROM document_extracts e
      JOIN documents d ON d.id = e.document_id
      JOIN files f     ON f.id = d.file_id
      WHERE e.document_id = ?
    `).get(docId);
    if (!row) return { ok: false, error: "No extract for this document." };
    let metadata = null;
    if (row.metadata) {
      try { metadata = JSON.parse(row.metadata); } catch { metadata = row.metadata; }
    }
    return { ok: true, ...row, metadata };
  } finally {
    db.close();
  }
}

// Returns the confidence tiers at or above the given threshold, or null
// for "all" (caller should skip the IN clause entirely).
function confidenceTiersAtOrAbove(threshold) {
  switch (threshold) {
    case "high":   return ["high"];
    case "medium": return ["high", "medium"];
    case "low":    return ["high", "medium", "low"];
    case "all":
    default:       return null;
  }
}

// Builds the FROM/WHERE/args triple shared by browseDocuments and handleStats.
// Both endpoints filter the same way; only the SELECT and aggregation differ.
function buildDocumentsWhere({ filter, classifiedFilter, minConfidence, exactConfidence, fileTypeFilters, documentTypeFilters, productFilter, hasProductFilter, sdtFilter, buildingFilter, hasBuildingFilter }) {
  const from = `
    FROM documents d
    JOIN files f          ON f.id = d.file_id
    LEFT JOIN vendors v   ON v.id = d.vendor_id
    LEFT JOIN document_types dt ON dt.id = d.document_type_id
  `;
  const clauses = [];
  const args = [];
  if (filter) {
    clauses.push(`(f.name LIKE ?
                   OR f.path LIKE ?
                   OR v.name LIKE ?
                   OR (dt.name IS NOT NULL AND dt.name LIKE ?))`);
    const like = `%${filter}%`;
    args.push(like, like, like, like);
  }
  if (classifiedFilter === "classified") {
    clauses.push("d.document_type_id IS NOT NULL");
  } else if (classifiedFilter === "unclassified") {
    clauses.push("d.document_type_id IS NULL");
  }
  if (Array.isArray(fileTypeFilters) && fileTypeFilters.length) {
    const placeholders = fileTypeFilters.map(() => "?").join(",");
    clauses.push(`f.file_type IN (${placeholders})`);
    args.push(...fileTypeFilters);
  }
  // SDT filter on the underlying file's source_data_type. '(none)' matches
  // legacy rows where the column wasn't stamped.
  if (sdtFilter) {
    if (sdtFilter === "(none)") {
      clauses.push("(f.source_data_type IS NULL OR f.source_data_type = '')");
    } else {
      clauses.push("f.source_data_type = ?");
      args.push(sdtFilter);
    }
  }
  if (Array.isArray(documentTypeFilters) && documentTypeFilters.length) {
    // dt.name is unique on document_types (the schema enforces it).
    const placeholders = documentTypeFilters.map(() => "?").join(",");
    clauses.push(`dt.name IN (${placeholders})`);
    args.push(...documentTypeFilters);
  }
  // exactConfidence (drilldown click) overrides minConfidence (toolbar dropdown).
  // Both exclude unclassified rows.
  if (exactConfidence) {
    clauses.push("d.confidence = ?");
    args.push(exactConfidence);
  } else {
    const allowedConfidences = confidenceTiersAtOrAbove(minConfidence);
    if (allowedConfidences) {
      const placeholders = allowedConfidences.map(() => "?").join(",");
      clauses.push(`d.confidence IN (${placeholders})`);
      args.push(...allowedConfidences);
    }
  }
  // Product link membership. productFilter narrows to documents that link
  // to a product with the given name; hasProductFilter narrows to docs
  // with at least one (or zero) product links.
  if (productFilter) {
    clauses.push(
      "EXISTS (SELECT 1 FROM document_products dp_f " +
      "JOIN products p_f ON p_f.id = dp_f.product_id " +
      "WHERE dp_f.document_id = d.id AND p_f.name = ?)",
    );
    args.push(productFilter);
  }
  if (hasProductFilter === "yes") {
    clauses.push("EXISTS (SELECT 1 FROM document_products dp_f WHERE dp_f.document_id = d.id)");
  } else if (hasProductFilter === "no") {
    clauses.push("NOT EXISTS (SELECT 1 FROM document_products dp_f WHERE dp_f.document_id = d.id)");
  }
  // Building link membership. buildingFilter narrows to documents linked
  // to that specific buildings.id; hasBuildingFilter narrows to docs
  // with at least one (or zero) building links.
  if (buildingFilter) {
    clauses.push(
      "EXISTS (SELECT 1 FROM document_buildings db_f " +
      "WHERE db_f.document_id = d.id AND db_f.building_id = ?)",
    );
    args.push(buildingFilter);
  }
  if (hasBuildingFilter === "yes") {
    clauses.push("EXISTS (SELECT 1 FROM document_buildings db_f WHERE db_f.document_id = d.id)");
  } else if (hasBuildingFilter === "no") {
    clauses.push("NOT EXISTS (SELECT 1 FROM document_buildings db_f WHERE db_f.document_id = d.id)");
  }
  const where = clauses.length ? "WHERE " + clauses.join(" AND ") : "";
  return { from, where, args };
}

function tableColumns(db, table) {
  return db
    .prepare(`PRAGMA table_info(${table})`)
    .all()
    .map((r) => r.name);
}

function handleIngest(body, which) {
  const source = (body.source || "").trim();
  // useIgnores defaults to true when absent — matches old behavior for any
  // caller that hasn't been updated yet.
  const applyIgnores = body.useIgnores !== false;
  // sourceDataType: scope-of-wipe for files-ingest. The two corpora share
  // a single DB but each ingest only refreshes ITS OWN SDT's rows. Caller
  // (the UI) passes state.activeSdt; falls back to 'Vendor' for legacy.
  const sourceDataType = body.sourceDataType || "Vendor";
  const listing = resolveListingPath(source);
  if (!listing) {
    return {
      ok: false,
      error: `No listing file found for source: ${source || "(empty)"}`,
    };
  }
  // Corpus root for cache routing: the chosen folder. If `source` is a
  // listing.txt, use its dirname.
  let corpusRoot = source;
  try {
    const st = fs.statSync(source);
    if (st.isFile()) corpusRoot = path.dirname(source);
  } catch { /* fall through with raw source */ }

  const db = openDb();
  try {
    const paths = readListing(listing);
    if (which === "vendors") {
      const r = ingestVendors(db, paths);
      return { ok: true, action: "vendors", listing, ...r, counts: tableCounts(db) };
    }
    if (which === "files") {
      const r = ingestFiles(db, paths, { applyIgnores, sourceDataType, corpusRoot });
      return { ok: true, action: "files", listing, ...r, counts: tableCounts(db) };
    }
    if (which === "full") {
      const v = ingestVendors(db, paths);
      const f = ingestFiles(db, paths, { applyIgnores, sourceDataType, corpusRoot });
      return {
        ok: true,
        action: "full",
        listing,
        vendors: v,
        files: f,
        counts: tableCounts(db),
      };
    }
    return { ok: false, error: `Unknown ingest action: ${which}` };
  } catch (e) {
    return { ok: false, error: e.message };
  } finally {
    db.close();
  }
}

// Aggregate counts for the Charts page, scoped by the same filters
// /api/browse accepts. With no filters it returns whole-corpus stats.
//
// When fileTypeFilter is active, the filetype pies become trivial (a single
// slice). The client decides whether to render them — we always return the
// data so the API stays simple.
// Distinct file_type values across non-dir files. Used to populate the
// toolbar's file-type dropdown — independent of any active filter so the
// list is stable as the user changes other filters.
function handleFileTypes() {
  const db = openDb();
  try {
    const rows = db
      .prepare(
        `SELECT file_type, COUNT(*) AS n
         FROM files
         WHERE is_dir = 0 AND file_type IS NOT NULL
         GROUP BY file_type
         ORDER BY n DESC`,
      )
      .all();
    return { ok: true, fileTypes: rows.map((r) => ({ ext: r.file_type, n: r.n })) };
  } finally {
    db.close();
  }
}

// --- Ignored file types --------------------------------------------------
// Extensions on this list get ingested into files but skip the documents
// row, removing them from classification / charts / dashboard. Adding here
// only affects future ingests — existing documents are left in place.
// Re-ingest files to apply the list retroactively.

function normalizeExt(raw) {
  let s = String(raw || "").trim().toLowerCase();
  if (!s) return null;
  if (!s.startsWith(".")) s = "." + s;
  // Reject anything that looks more like a path / has whitespace / has slashes.
  if (!/^\.[a-z0-9]{1,12}$/i.test(s)) return null;
  return s;
}

function handleListIgnoredTypes() {
  const db = openDb();
  try {
    const rows = db
      .prepare(
        `SELECT it.ext AS ext,
                it.added_at AS added_at,
                it.notes AS notes,
                (SELECT COUNT(*) FROM files f
                  WHERE f.is_dir = 0 AND f.file_type = it.ext) AS file_count
         FROM ignored_file_types it
         ORDER BY it.ext ASC`,
      )
      .all();
    return { ok: true, types: rows };
  } finally {
    db.close();
  }
}

function handleAddIgnoredType(body) {
  const ext = normalizeExt(body.ext);
  if (!ext) {
    return { ok: false, error: "Invalid extension. Use a short alphanumeric like .tmp" };
  }
  const notes = (body.notes || "").trim() || null;
  const db = openDb();
  try {
    db.prepare(
      "INSERT OR IGNORE INTO ignored_file_types (ext, added_at, notes) VALUES (?, ?, ?)",
    ).run(ext, new Date().toISOString(), notes);
    return { ok: true, ext };
  } finally {
    db.close();
  }
}

function handleRemoveIgnoredType(body) {
  const ext = normalizeExt(body.ext);
  if (!ext) return { ok: false, error: "Invalid extension." };
  const db = openDb();
  try {
    const r = db.prepare("DELETE FROM ignored_file_types WHERE ext = ?").run(ext);
    return { ok: true, ext, removed: Number(r.changes) };
  } finally {
    db.close();
  }
}

// --- Ignored folders -----------------------------------------------------
// Folder-name matches at ingest time. Folder + all descendants are dropped
// from `files` (and therefore from documents/charts). Going-forward only.

function normalizeFolderName(raw) {
  let s = String(raw || "").trim();
  if (!s) return null;
  // Reject anything with path separators or trailing slashes — this is an
  // exact folder-name match, not a path glob.
  if (/[\\/]/.test(s)) return null;
  // Cap length to avoid garbage; allow letters, digits, spaces, common punctuation.
  if (s.length > 128) return null;
  return s;
}

// --- Classifier rule editor ----------------------------------------------
// Three YAML rule files (filename / content / product) are editable from
// the UI. GET returns the parsed rule list; POST validates + writes back.

// Each classifier "kind" maps to one YAML file. Split kinds (e.g.
// vendor_file / vendor_content) share storage with their parent
// (vendor) — they just filter the visible rules by match-type and
// default the add-rule's match: accordingly.
const KIND_READERS = {
  filename: readFilenameRules,
  content:  readContentRulesRaw,
  product:  readProductRulesRaw,
  vendor:   readVendorRulesRaw,
  building: readBuildingExtractorRulesRaw,
};
const KIND_WRITERS = {
  filename: writeFilenameRules,
  content:  writeContentRules,
  product:  writeProductRules,
  vendor:   writeVendorRules,
  building: writeBuildingExtractorRules,
};

const FILE_MATCHES    = new Set(["name", "path"]);
const CONTENT_MATCHES = new Set(["first_page", "extract"]);

function handleListClassifierRules(kind) {
  const meta = CLASSIFIER_KINDS[kind];
  if (!meta) return { ok: false, error: `Unknown classifier kind: ${kind}` };
  // Split kinds delegate to their parent file. They show only the matching
  // half of the rules; the editor then writes back the merged whole.
  const parentKind = meta.parent || kind;
  const reader = KIND_READERS[parentKind];
  if (!reader) return { ok: false, error: `No reader for kind: ${parentKind}` };
  try {
    let rules = reader();
    if (meta.matchSubset === "file") {
      rules = rules.filter((r) => FILE_MATCHES.has(r.match));
    } else if (meta.matchSubset === "content") {
      rules = rules.filter((r) => CONTENT_MATCHES.has(r.match));
    }
    return { ok: true, kind, meta, rules };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

function handleSaveClassifierRules(kind, body) {
  const meta = CLASSIFIER_KINDS[kind];
  if (!meta) return { ok: false, error: `Unknown classifier kind: ${kind}` };
  if (!Array.isArray(body && body.rules)) {
    return { ok: false, error: "Body must include `rules` array." };
  }
  const parentKind = meta.parent || kind;
  const writer = KIND_WRITERS[parentKind];
  const reader = KIND_READERS[parentKind];
  if (!writer) return { ok: false, error: `No writer for kind: ${parentKind}` };
  try {
    let toWrite = body.rules;
    // Split kinds: merge with the OTHER half of the parent file so we
    // don't clobber it. The incoming `rules` are the user's edited view
    // of the file/content half; we read the other half and concat.
    if (meta.matchSubset && reader) {
      const all = reader();
      const isMatchInSubset = meta.matchSubset === "file" ? FILE_MATCHES : CONTENT_MATCHES;
      const otherHalf = all.filter((r) => !isMatchInSubset.has(r.match));
      // Validate: every rule the user supplied must be in the right half
      // (otherwise the save would silently corrupt the file).
      for (const r of body.rules) {
        if (!isMatchInSubset.has(r.match)) {
          return {
            ok: false,
            error: `Rule "${r.id}" has match=${r.match} which doesn't belong on the ${meta.matchSubset} side`,
          };
        }
      }
      toWrite = otherHalf.concat(body.rules);
    }
    writer(toWrite);
    return { ok: true, kind, count: toWrite.length };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// Per-rule coverage stats. Replays the in-file rules against the live
// non-dir files and tallies, for each rule, how many docs it would match
// (regex test against the rule's match field) AND how many it actually
// wins (first-match-wins replay). Divergence = the rule is shadowed by
// an earlier rule.
//
// Filename rules only for v1 — content/product rules need extracts data
// which is more expensive and the editor surface for those is different.
function handleClassifierRuleCoverage(kind) {
  if (kind !== "filename") {
    return { ok: false, error: "Coverage stats only available for filename rules in this version." };
  }
  let rules;
  try { rules = readFilenameRules(); }
  catch (e) { return { ok: false, error: e.message }; }

  const compiled = [];
  for (const r of rules) {
    let regex = null;
    try { regex = new RegExp(r.pattern, "i"); }
    catch { /* skip invalid regex — show 0/0 */ }
    compiled.push({ id: r.id, match: r.match, regex });
  }

  const db = openDb();
  let files;
  try {
    files = db.prepare(`
      SELECT f.path, f.name, p.name AS parent, f.file_type
      FROM files f
      LEFT JOIN files p ON p.id = f.parent_id
      WHERE f.is_dir = 0
    `).all();
  } finally {
    db.close();
  }

  const wouldMatch = Object.fromEntries(rules.map((r) => [r.id, 0]));
  const winning    = Object.fromEntries(rules.map((r) => [r.id, 0]));

  for (const f of files) {
    const fields = { name: f.name, parent: f.parent || "", path: f.path, file_type: f.file_type || "" };
    let won = false;
    for (const c of compiled) {
      if (!c.regex) continue;
      const target = fields[c.match] ?? "";
      if (c.regex.test(target)) {
        wouldMatch[c.id]++;
        if (!won) {
          winning[c.id]++;
          won = true;
        }
      }
    }
  }

  return {
    ok: true,
    kind,
    totalFiles: files.length,
    coverage: rules.map((r) => ({
      id: r.id,
      wouldMatch: wouldMatch[r.id] || 0,
      winning:    winning[r.id]    || 0,
    })),
  };
}

function handleListIgnoredFolders() {
  const db = openDb();
  try {
    // file_count: how many CURRENT rows in `files` sit at or under a folder
    // segment whose name matches (case-insensitive). This counts files that
    // *would* be excluded if we re-ingested today.
    const folders = db.prepare("SELECT name, added_at, notes FROM ignored_folders ORDER BY name ASC").all();
    const result = folders.map((f) => {
      const like = "%\\" + f.name + "\\%";
      const eqEnd = "%\\" + f.name;
      const n = db.prepare(
        `SELECT COUNT(*) AS n FROM files
         WHERE LOWER(path) LIKE LOWER(?)
            OR LOWER(path) LIKE LOWER(?)`,
      ).get(like, eqEnd).n;
      return { ...f, file_count: n };
    });
    return { ok: true, folders: result };
  } finally {
    db.close();
  }
}

function handleAddIgnoredFolder(body) {
  const name = normalizeFolderName(body.name);
  if (!name) {
    return { ok: false, error: "Invalid folder name. Use a plain folder name (no slashes)." };
  }
  const notes = (body.notes || "").trim() || null;
  const db = openDb();
  try {
    db.prepare(
      "INSERT OR IGNORE INTO ignored_folders (name, added_at, notes) VALUES (?, ?, ?)",
    ).run(name, new Date().toISOString(), notes);
    return { ok: true, name };
  } finally {
    db.close();
  }
}

function handleRemoveIgnoredFolder(body) {
  const name = normalizeFolderName(body.name);
  if (!name) return { ok: false, error: "Invalid folder name." };
  const db = openDb();
  try {
    const r = db.prepare("DELETE FROM ignored_folders WHERE name = ?").run(name);
    return { ok: true, name, removed: Number(r.changes) };
  } finally {
    db.close();
  }
}

// Lists the ignored_files table joined with files so the UI can show
// vendor + status (still-present-on-disk vs. already-gone-from-files-row).
function handleIgnoredFiles() {
  const db = openDb();
  try {
    const rows = db.prepare(
      `SELECT i.path        AS path,
              i.added_at    AS added_at,
              i.notes       AS notes,
              v.name        AS vendor,
              CASE WHEN f.id IS NULL THEN 0 ELSE 1 END AS in_files
         FROM ignored_files i
    LEFT JOIN files f      ON f.path = i.path
    LEFT JOIN documents d  ON d.file_id = f.id
    LEFT JOIN vendors v    ON v.id   = d.vendor_id
        ORDER BY i.added_at DESC, i.path`,
    ).all();
    return { ok: true, files: rows };
  } finally {
    db.close();
  }
}

// Adds one or many paths to ignored_files. For each path we also delete
// any matching documents row (cascades to extracts/products/document_products
// because all those FKs are ON DELETE CASCADE). The files row itself is
// left in place — the next ingest can drop it if needed.
function handleAddIgnoredFiles(body) {
  const paths = Array.isArray(body.paths)
    ? body.paths
    : (body.path ? [body.path] : []);
  const cleaned = paths.map((p) => String(p || "").trim()).filter(Boolean);
  if (cleaned.length === 0) return { ok: false, error: "No paths provided." };
  const notes = (body.notes || "").trim() || null;
  const db = openDb();
  try {
    db.exec("BEGIN");
    const insert = db.prepare(
      "INSERT OR IGNORE INTO ignored_files (path, added_at, notes) VALUES (?, ?, ?)",
    );
    const dropDoc = db.prepare(
      "DELETE FROM documents WHERE file_id = (SELECT id FROM files WHERE path = ?)",
    );
    let added = 0, dropped = 0;
    const now = new Date().toISOString();
    for (const p of cleaned) {
      const r = insert.run(p, now, notes);
      if (r.changes > 0) added += 1;
      const d = dropDoc.run(p);
      dropped += Number(d.changes);
    }
    db.exec("COMMIT");
    return { ok: true, added, dropped, total: cleaned.length };
  } catch (e) {
    db.exec("ROLLBACK");
    return { ok: false, error: String(e) };
  } finally {
    db.close();
  }
}

function handleRemoveIgnoredFile(body) {
  const path = String(body.path || "").trim();
  if (!path) return { ok: false, error: "Missing path." };
  const db = openDb();
  try {
    const r = db.prepare("DELETE FROM ignored_files WHERE path = ?").run(path);
    return { ok: true, path, removed: Number(r.changes) };
  } finally {
    db.close();
  }
}

// All document_types names. Counts come from the documents join — types
// with zero documents still show in the list since they're real options.
function handleDocumentTypeOptions() {
  const db = openDb();
  try {
    const rows = db
      .prepare(
        `SELECT dt.name AS name,
                (SELECT COUNT(*) FROM documents d WHERE d.document_type_id = dt.id) AS n
         FROM document_types dt
         ORDER BY n DESC, dt.name ASC`,
      )
      .all();
    return { ok: true, documentTypes: rows.map((r) => ({ name: r.name, n: r.n })) };
  } finally {
    db.close();
  }
}

// Whole-corpus snapshot for the Home dashboard. Returns per-table row counts
// plus a per-document_type confidence breakdown. No filters — this is the
// "overview" page, distinct from /api/stats which respects the table filters.
// sdt: optional filter scoping documents/files-derived stats to a single
// source-data-type ("Vendor", "JobFiles", "Sales", "Any", or "" for all).
// Vendors and products tables are inherently global so they aren't scoped —
// the dashboard's vendor count means "vendors known to the system", not
// "vendors with docs in this SDT".
function handleDashboard(sdt) {
  const db = openDb();
  try {
    const counts = tableCounts(db);
    const useSdt = sdt && sdt !== "All";
    // Reusable scope clause + arg list for documents-derived queries.
    // The SDT lives on files, so every scoped query joins files in.
    const docScope = useSdt
      ? "JOIN files f ON f.id = d.file_id WHERE f.source_data_type = ?"
      : "";
    const docScopeArgs = useSdt ? [sdt] : [];

    // Total documents in scope (replaces counts.documents when SDT-scoped).
    const totalDocs = useSdt
      ? db.prepare("SELECT COUNT(*) AS n FROM documents d " + docScope)
          .get(...docScopeArgs).n
      : counts.documents;

    // When SDT-scoped, override the per-table counts that *can* be
    // SDT-filtered (everything that joins through files). Vendors and
    // products stay global since they're inherently cross-corpus.
    // document_products is scoped via its document → file chain.
    // document_types is overridden to count distinct types that have AT
    // LEAST ONE doc in the SDT (so JobFiles shows ~21 vs the global 43)
    // — that matches what the user sees in the per-type breakdown table
    // on the same page.
    if (useSdt) {
      counts.documents = totalDocs;
      counts.files = db.prepare(
        "SELECT COUNT(*) AS n FROM files WHERE source_data_type = ?",
      ).get(sdt).n;
      counts.document_extracts = db.prepare(
        `SELECT COUNT(*) AS n
         FROM document_extracts e
         JOIN documents d ON d.id = e.document_id
         JOIN files f     ON f.id = d.file_id
         WHERE f.source_data_type = ?`,
      ).get(sdt).n;
      counts.document_products = db.prepare(
        `SELECT COUNT(*) AS n
         FROM document_products dp
         JOIN documents d ON d.id = dp.document_id
         JOIN files f     ON f.id = d.file_id
         WHERE f.source_data_type = ?`,
      ).get(sdt).n;
      counts.document_types = db.prepare(
        `SELECT COUNT(DISTINCT d.document_type_id) AS n
         FROM documents d
         JOIN files f ON f.id = d.file_id
         WHERE f.source_data_type = ?
           AND d.document_type_id IS NOT NULL`,
      ).get(sdt).n;
    }

    const classifiedTotal = db
      .prepare(
        useSdt
          ? "SELECT COUNT(*) AS n FROM documents d " + docScope +
            " AND d.document_type_id IS NOT NULL"
          : "SELECT COUNT(*) AS n FROM documents WHERE document_type_id IS NOT NULL",
      )
      .get(...docScopeArgs).n;

    // Per-doctype confidence breakdown. LEFT JOIN so types with zero documents
    // still appear (count = 0); the client decides whether to dim/hide them.
    // When SDT-scoped, we join through files so only docs in the chosen SDT
    // contribute to the per-type counts; types still appear (LEFT JOIN) but
    // with total=0 if no in-scope docs use them.
    const byTypeSql = useSdt
      ? `SELECT dt.name AS name,
                COUNT(d.id)                                              AS total,
                SUM(CASE WHEN d.confidence = 'high'   THEN 1 ELSE 0 END) AS high,
                SUM(CASE WHEN d.confidence = 'medium' THEN 1 ELSE 0 END) AS medium,
                SUM(CASE WHEN d.confidence = 'low'    THEN 1 ELSE 0 END) AS low
         FROM document_types dt
         LEFT JOIN documents d
                ON d.document_type_id = dt.id
         LEFT JOIN files f
                ON f.id = d.file_id AND f.source_data_type = ?
         WHERE d.id IS NULL OR f.id IS NOT NULL
         GROUP BY dt.id, dt.name
         ORDER BY total DESC, dt.name ASC`
      : `SELECT dt.name AS name,
                COUNT(d.id)                                              AS total,
                SUM(CASE WHEN d.confidence = 'high'   THEN 1 ELSE 0 END) AS high,
                SUM(CASE WHEN d.confidence = 'medium' THEN 1 ELSE 0 END) AS medium,
                SUM(CASE WHEN d.confidence = 'low'    THEN 1 ELSE 0 END) AS low
         FROM document_types dt
         LEFT JOIN documents d ON d.document_type_id = dt.id
         GROUP BY dt.id, dt.name
         ORDER BY total DESC, dt.name ASC`;
    const rows = db.prepare(byTypeSql).all(...docScopeArgs);

    const byType = rows.map((r) => {
      const total = Number(r.total) || 0;
      const high = Number(r.high) || 0;
      const medium = Number(r.medium) || 0;
      const low = Number(r.low) || 0;
      // (A) share of all classified documents that landed in this type.
      const pctOfClassified = classifiedTotal > 0 ? (total / classifiedTotal) * 100 : 0;
      // (B) confidence mix within this type. Sums to 100 when total > 0.
      const pctHigh   = total > 0 ? (high   / total) * 100 : 0;
      const pctMedium = total > 0 ? (medium / total) * 100 : 0;
      const pctLow    = total > 0 ? (low    / total) * 100 : 0;
      return {
        name: r.name,
        total, high, medium, low,
        pctOfClassified, pctHigh, pctMedium, pctLow,
      };
    });

    const ignored = {
      types:   db.prepare("SELECT COUNT(*) AS n FROM ignored_file_types").get().n,
      folders: db.prepare("SELECT COUNT(*) AS n FROM ignored_folders").get().n,
      files:   db.prepare("SELECT COUNT(*) AS n FROM ignored_files").get().n,
    };

    // Files-in-scope split. "Active" = file has a documents row (will be
    // classified/charted). "Ignored" = file row exists but no documents row,
    // i.e. the ext is on ignored_file_types. Folder-ignored files don't
    // reach the files table at all when ignores were applied at ingest, so
    // they're invisible here — that's correct. Scoped to SDT when set.
    const sdtAndClause = useSdt ? "AND f.source_data_type = ?" : "";
    const totalFiles = db
      .prepare("SELECT COUNT(*) AS n FROM files f WHERE f.is_dir = 0 " + sdtAndClause)
      .get(...docScopeArgs).n;
    const activeFiles = db
      .prepare(
        "SELECT COUNT(*) AS n FROM files f " +
        "WHERE f.is_dir = 0 " + sdtAndClause +
        "  AND EXISTS (SELECT 1 FROM documents d WHERE d.file_id = f.id)",
      )
      .get(...docScopeArgs).n;
    const ignoredFiles = totalFiles - activeFiles;

    // KPIs surfaced as headline cards on the dashboard. Each "% with product"
    // metric counts a doc as covered if it has at least one row in
    // document_products (any confidence, any source). When SDT-scoped, both
    // numerator and denominator are restricted to docs in that SDT.
    function pctWithProduct(typeNames) {
      const placeholders = typeNames.map(() => "?").join(",");
      const sdtJoin = useSdt ? " JOIN files f ON f.id = d.file_id " : "";
      const sdtAnd  = useSdt ? " AND f.source_data_type = ? " : "";
      const argsScoped = [...typeNames, ...docScopeArgs];
      const total = db.prepare(
        "SELECT COUNT(*) AS n FROM documents d " +
        " JOIN document_types dt ON dt.id = d.document_type_id " +
        sdtJoin +
        ` WHERE dt.name IN (${placeholders}) ` + sdtAnd,
      ).get(...argsScoped).n;
      const withProd = db.prepare(
        "SELECT COUNT(*) AS n FROM documents d " +
        " JOIN document_types dt ON dt.id = d.document_type_id " +
        sdtJoin +
        ` WHERE dt.name IN (${placeholders}) ` + sdtAnd +
        "   AND EXISTS (SELECT 1 FROM document_products dp WHERE dp.document_id = d.id)",
      ).get(...argsScoped).n;
      return {
        total,
        withProduct: withProd,
        pct: total > 0 ? (withProd / total) * 100 : 0,
      };
    }
    const manualsWithProduct   = pctWithProduct(["installation_manual", "programming_manual", "operations_manual"]);
    const datasheetsWithProduct = pctWithProduct(["datasheet"]);

    // Reverse view of "manuals with a product": how many products have at
    // least one manual document tied to them. Pct is over total products.
    // Not SDT-scoped because products are inherently global (a Notifier
    // panel's manual could be referenced from a Vendor doc OR a JobFiles
    // submittal; collapsing it would mislead).
    const MANUAL_TYPES = ["installation_manual", "programming_manual", "operations_manual"];
    const productsWithManualsRow = db.prepare(
      `SELECT COUNT(DISTINCT dp.product_id) AS n
       FROM document_products dp
       JOIN documents d   ON d.id = dp.document_id
       JOIN document_types dt ON dt.id = d.document_type_id
       WHERE dt.name IN (${MANUAL_TYPES.map(() => "?").join(",")})`,
    ).get(...MANUAL_TYPES);
    const productsWithManuals = {
      total:        counts.products,
      withManuals:  productsWithManualsRow.n,
      pct:          counts.products > 0 ? (productsWithManualsRow.n / counts.products) * 100 : 0,
    };

    // Building coverage KPIs — the JobFiles analog of the product KPIs
    // above. Same shape: pct of doc types in scope that got at least one
    // building link, plus a reverse view counting how many buildings have
    // at least one linked doc. Only emitted when SDT-scoped to JobFiles
    // (or the all-docs view) since Vendor docs don't link to buildings.
    function pctWithBuilding(typeNames) {
      const placeholders = typeNames.map(() => "?").join(",");
      const sdtJoin = useSdt ? " JOIN files f ON f.id = d.file_id " : "";
      const sdtAnd  = useSdt ? " AND f.source_data_type = ? " : "";
      const argsScoped = [...typeNames, ...docScopeArgs];
      const total = db.prepare(
        "SELECT COUNT(*) AS n FROM documents d " +
        " JOIN document_types dt ON dt.id = d.document_type_id " +
        sdtJoin +
        ` WHERE dt.name IN (${placeholders}) ` + sdtAnd,
      ).get(...argsScoped).n;
      const withBldg = db.prepare(
        "SELECT COUNT(*) AS n FROM documents d " +
        " JOIN document_types dt ON dt.id = d.document_type_id " +
        sdtJoin +
        ` WHERE dt.name IN (${placeholders}) ` + sdtAnd +
        "   AND EXISTS (SELECT 1 FROM document_buildings db WHERE db.document_id = d.id)",
      ).get(...argsScoped).n;
      return {
        total,
        withBuilding: withBldg,
        pct: total > 0 ? (withBldg / total) * 100 : 0,
      };
    }
    const contractsWithBuilding = pctWithBuilding(["contract", "subcontract", "purchase_order", "sales_order"]);
    const invoicesWithBuilding  = pctWithBuilding(["invoice"]);
    const proposalsWithBuilding = pctWithBuilding(["proposal", "quote", "estimate", "bid_workup"]);

    // Reverse view: how many buildings (excluding ignored) have at least
    // one linked document. Counts distinct extracted buildings (one row
    // per canonical site or orphan), excluding any whose canonical_uid
    // is in ignored_buildings. SDT-scoped via the JOIN through files.
    const buildingsWithDocsRow = db.prepare(
      `SELECT COUNT(DISTINCT db.building_id) AS n
         FROM document_buildings db
         JOIN documents d ON d.id = db.document_id
         JOIN files f     ON f.id = d.file_id
         JOIN buildings b ON b.id = db.building_id
         WHERE NOT EXISTS (
           SELECT 1 FROM ignored_buildings ib
            WHERE ib.building_uid = b.canonical_uid
         )` + (useSdt ? " AND f.source_data_type = ?" : ""),
    ).get(...docScopeArgs);
    // Denominator: total extracted buildings excluding ignored ones.
    const buildingsTotalRow = db.prepare(
      `SELECT COUNT(*) AS n FROM buildings b
         WHERE NOT EXISTS (
           SELECT 1 FROM ignored_buildings ib
            WHERE ib.building_uid = b.canonical_uid
         )`,
    ).get();
    const buildingsWithDocs = {
      total:       buildingsTotalRow.n,
      withDocs:    buildingsWithDocsRow.n,
      pct:         buildingsTotalRow.n > 0 ? (buildingsWithDocsRow.n / buildingsTotalRow.n) * 100 : 0,
    };

    const kpis = {
      pctClassified: totalDocs > 0 ? (classifiedTotal / totalDocs) * 100 : 0,
      vendors:       counts.vendors,
      products:      counts.products,
      buildings:     counts.buildings,
      manualsWithProduct,
      datasheetsWithProduct,
      productsWithManuals,
      contractsWithBuilding,
      invoicesWithBuilding,
      proposalsWithBuilding,
      buildingsWithDocs,
    };

    return {
      ok: true,
      sdt: useSdt ? sdt : "",
      counts,
      classification: {
        totalDocuments: totalDocs,
        classified: classifiedTotal,
        unclassified: totalDocs - classifiedTotal,
        pctClassified: totalDocs > 0 ? (classifiedTotal / totalDocs) * 100 : 0,
      },
      ignored,
      filesScope: {
        total:    totalFiles,
        active:   activeFiles,
        ignored:  ignoredFiles,
      },
      kpis,
      byType,
    };
  } finally {
    db.close();
  }
}

function handleStats(body = {}) {
  const filterArgs = {
    filter:              body.filter || "",
    classifiedFilter:    body.classifiedFilter || "all",
    minConfidence:       body.minConfidence || "all",
    exactConfidence:     body.exactConfidence || "",
    fileTypeFilters:     Array.isArray(body.fileTypeFilters) ? body.fileTypeFilters : [],
    documentTypeFilters: Array.isArray(body.documentTypeFilters) ? body.documentTypeFilters : [],
    productFilter:       body.productFilter || "",
    hasProductFilter:    body.hasProductFilter || "",
    sdtFilter:           body.sdtFilter || "",
    buildingFilter:      body.buildingFilter || "",
    hasBuildingFilter:   body.hasBuildingFilter || "",
  };
  const { from, where, args } = buildDocumentsWhere(filterArgs);

  const db = openDb();
  try {
    const total = db.prepare(`SELECT COUNT(*) AS n ${from} ${where}`).get(...args).n;
    const classified = db
      .prepare(
        `SELECT COUNT(*) AS n ${from} ${where}` +
          (where ? " AND" : " WHERE") +
          " d.document_type_id IS NOT NULL",
      )
      .get(...args).n;
    const unclassified = total - classified;

    const confRows = db
      .prepare(
        `SELECT COALESCE(d.confidence, 'unclassified') AS bucket, COUNT(*) AS n
         ${from} ${where}
         GROUP BY bucket`,
      )
      .all(...args);
    const byConfidence = { high: 0, medium: 0, low: 0, unclassified: 0 };
    for (const r of confRows) {
      if (r.bucket in byConfidence) byConfidence[r.bucket] = r.n;
    }

    // Source Data Type breakdown (Vendor / JobFiles / Sales / Any / unstamped).
    // Pulled from the joined files row. Same filter chain as everything else.
    const sdtRows = db
      .prepare(
        `SELECT COALESCE(NULLIF(f.source_data_type, ''), '(none)') AS sdt, COUNT(*) AS n
         ${from} ${where}
         GROUP BY sdt ORDER BY n DESC`,
      )
      .all(...args);
    const bySdt = sdtRows.map((r) => ({ sdt: r.sdt, n: r.n }));

    // File extensions: same subset as the rest of the stats.
    const extRows = db
      .prepare(
        `SELECT COALESCE(f.file_type, '(none)') AS ext, COUNT(*) AS n
         ${from} ${where}
         GROUP BY ext ORDER BY n DESC`,
      )
      .all(...args);
    const TOP_N = 10;
    const byFileTypeTop = extRows.slice(0, TOP_N).map((r) => ({ ext: r.ext, n: r.n }));
    const byFileTypeOther = extRows.slice(TOP_N).map((r) => ({ ext: r.ext, n: r.n }));

    // Document types over classified rows only. Top 10 by count plus an
    // "other" rollup. Excludes unclassified (NULL document_type_id) so we
    // don't double-count what the classified pie already shows.
    const dtRows = db
      .prepare(
        `SELECT dt.name AS dtype, COUNT(*) AS n
         ${from} ${where}` +
          (where ? " AND" : " WHERE") +
          ` d.document_type_id IS NOT NULL
         GROUP BY dt.id ORDER BY n DESC`,
      )
      .all(...args);
    const byDocumentTypeTop = dtRows.slice(0, TOP_N).map((r) => ({ dtype: r.dtype, n: r.n }));
    const byDocumentTypeOther = dtRows.slice(TOP_N).map((r) => ({ dtype: r.dtype, n: r.n }));

    // --- Product-related aggregates -------------------------------------
    // (a) Top products by document count, scoped to the filter chain.
    //     Reuses buildDocumentsWhere's FROM/WHERE/args (aliases d/f/v/dt
    //     already defined) and tacks document_products + products + a
    //     second vendor alias on top.
    const PRODUCT_TOP_N = 20;
    const productRows = db
      .prepare(
        `SELECT pp.id        AS product_id,
                pp.name      AS product_name,
                pv.name      AS vendor_name,
                COUNT(*)     AS n
         ${from}
         JOIN document_products dp ON dp.document_id = d.id
         JOIN products pp           ON pp.id = dp.product_id
         JOIN vendors  pv           ON pv.id = pp.vendor_id
         ${where}
         GROUP BY pp.id ORDER BY n DESC`,
      )
      .all(...args);
    const byProductTop   = productRows.slice(0, PRODUCT_TOP_N).map(
      (r) => ({ product: r.product_name, vendor: r.vendor_name, n: r.n }));
    const byProductOther = productRows.slice(PRODUCT_TOP_N).map(
      (r) => ({ product: r.product_name, vendor: r.vendor_name, n: r.n }));

    // (b) Distinct products discovered per vendor — scoped to the filter
    //     chain. Counts products that are linked to at least one document
    //     in the filtered set, grouped by vendor. Goes empty when the
    //     filter narrows to a corpus with no product links (e.g. JobFiles),
    //     and the loadCharts code below hides the card in that case.
    const productsPerVendor = db
      .prepare(
        `SELECT pv.name AS vendor, COUNT(DISTINCT pp.id) AS n
         ${from}
         JOIN document_products dp ON dp.document_id = d.id
         JOIN products pp           ON pp.id = dp.product_id
         JOIN vendors  pv           ON pv.id = pp.vendor_id
         ${where}
         GROUP BY pv.id ORDER BY n DESC, pv.name ASC LIMIT 30`,
      )
      .all(...args);

    // (c) Coverage: how many of the (filtered) documents have at least
    //     one product link vs. zero. Subject to the same filter chain.
    const docsWithProducts = db
      .prepare(
        `SELECT COUNT(DISTINCT d.id) AS n ${from} ${where}` +
          (where ? " AND" : " WHERE") +
          " EXISTS (SELECT 1 FROM document_products dp WHERE dp.document_id = d.id)",
      )
      .get(...args).n;
    const docsWithoutProducts = total - docsWithProducts;

    // --- Building-related aggregates -----------------------------------
    // Top buildings by linked-doc count, scoped to the chart filter chain.
    // Each chart slice represents one buildings.id (extracted reference);
    // when a canonical row is matched, the label uses canonical_address,
    // otherwise it falls back to the raw_address/raw_name on buildings.
    const BUILDING_TOP_N = 20;
    const buildingRows = db
      .prepare(
        `SELECT b.id                 AS building_id,
                b.canonical_uid      AS canonical_uid,
                b.raw_name           AS raw_name,
                b.raw_address        AS raw_address,
                b.raw_city           AS raw_city,
                b.raw_state          AS raw_state,
                cb.canonical_address AS canonical_address,
                cb.canonical_city    AS canonical_city,
                cb.canonical_state   AS canonical_state,
                COUNT(DISTINCT d.id) AS n
         ${from}
         JOIN document_buildings db ON db.document_id = d.id
         JOIN buildings b           ON b.id = db.building_id
         LEFT JOIN canonical_buildings cb ON cb.building_uid = b.canonical_uid
         ${where}
         GROUP BY b.id ORDER BY n DESC, b.id`,
      )
      .all(...args);
    const byBuildingTop = buildingRows.slice(0, BUILDING_TOP_N).map((r) => {
      const addr = r.canonical_address || r.raw_address || r.raw_name || "(no address)";
      const city = r.canonical_city || r.raw_city;
      const st   = r.canonical_state || r.raw_state;
      return {
        building_id: r.building_id,
        canonical_uid: r.canonical_uid,
        label: addr + (city ? " · " + city : "") + (st ? ", " + st : ""),
        n: r.n,
      };
    });

    // Coverage: of the filtered docs, how many got at least one building link.
    const docsWithBuildings = db
      .prepare(
        `SELECT COUNT(DISTINCT d.id) AS n ${from} ${where}` +
          (where ? " AND" : " WHERE") +
          " EXISTS (SELECT 1 FROM document_buildings db WHERE db.document_id = d.id)",
      )
      .get(...args).n;
    const docsWithoutBuildings = total - docsWithBuildings;

    return {
      ok: true,
      total,
      classified,
      unclassified,
      byConfidence,
      bySdt,
      byFileTypeTop,
      byFileTypeOther,
      byDocumentTypeTop,
      byDocumentTypeOther,
      byProductTop,
      byProductOther,
      productsPerVendor,
      docsWithProducts,
      docsWithoutProducts,
      byBuildingTop,
      docsWithBuildings,
      docsWithoutBuildings,
      totalFiles: extRows.reduce((a, r) => a + r.n, 0),
      activeFilter: filterArgs,
    };
  } finally {
    db.close();
  }
}

// Open a file with the OS default app. Whitelisted by membership in
// files.path so only paths the DB knows about can be opened — caller can't
// trigger arbitrary shell commands by sending a forged path.
async function handleOpenFile(body) {
  const reqPath = (body.path || "").trim();
  if (!reqPath) return { ok: false, error: "Missing path." };

  const db = openDb();
  let known;
  try {
    known = db
      .prepare("SELECT path, is_dir FROM files WHERE path = ?")
      .get(reqPath);
  } finally {
    db.close();
  }
  if (!known) return { ok: false, error: "Path not in files table." };
  if (known.is_dir) return { ok: false, error: "Path is a directory, not a file." };

  const localPath = remapPath(reqPath);
  if (!fs.existsSync(localPath)) {
    return {
      ok: false,
      error: `File not found locally at ${localPath}` +
        (localPath !== reqPath ? ` (remapped from ${reqPath})` : ""),
    };
  }

  // Spawn the OS opener and return immediately. We don't wait for the
  // opened app to exit — that's the user's session's problem.
  const { spawn } = await import("node:child_process");
  try {
    if (process.platform === "win32") {
      // `start` is a cmd builtin, not a separate exe. cmd parses the line
      // before start sees it, so any `& | < > ( ) ^ %` in the path becomes
      // a metacharacter and the open silently breaks. Escape with `^` and
      // wrap in quotes; the empty "" is start's window-title slot (start
      // treats the first quoted arg as the title).
      const cmdEscaped = localPath.replace(/[&|<>()^%]/g, "^$&");
      const line = `start "" "${cmdEscaped}"`;
      const child = spawn("cmd.exe", ["/c", line], {
        detached: true,
        stdio: "ignore",
        windowsVerbatimArguments: true,
      });
      child.unref();
    } else if (process.platform === "darwin") {
      const child = spawn("open", [localPath], { detached: true, stdio: "ignore" });
      child.unref();
    } else {
      const child = spawn("xdg-open", [localPath], { detached: true, stdio: "ignore" });
      child.unref();
    }
    return { ok: true, path: reqPath, localPath };
  } catch (e) {
    return { ok: false, error: "Spawn failed: " + e.message };
  }
}

// Open the parent folder of a known file (or the folder itself if the
// path IS a directory). On Windows we use `explorer /select,<file>` so
// the file gets highlighted; on macOS `open -R`; on Linux we just open
// the dirname with xdg-open. Same whitelist as handleOpenFile — caller
// can only act on paths the DB knows about.
async function handleOpenFolder(body) {
  const reqPath = (body.path || "").trim();
  if (!reqPath) return { ok: false, error: "Missing path." };

  const db = openDb();
  let known;
  try {
    known = db
      .prepare("SELECT path, is_dir FROM files WHERE path = ?")
      .get(reqPath);
  } finally {
    db.close();
  }
  if (!known) return { ok: false, error: "Path not in files table." };

  const localPath = remapPath(reqPath);
  // For a file, the target the OS opener acts on differs by platform:
  //   Windows: pass the FILE to `explorer /select,` to highlight it.
  //   macOS:   pass the FILE to `open -R` to reveal in Finder.
  //   Linux:   pass the DIRNAME to xdg-open (no widely supported reveal).
  // Either way we need the file to exist locally.
  if (!fs.existsSync(localPath)) {
    return {
      ok: false,
      error: `Path not found locally at ${localPath}` +
        (localPath !== reqPath ? ` (remapped from ${reqPath})` : ""),
    };
  }

  const { spawn } = await import("node:child_process");
  try {
    if (process.platform === "win32") {
      // explorer.exe /select,<path> highlights the file in its parent
      // folder. The path can't be quoted (explorer parses it differently
      // than cmd does); we pass it as a separate argv element to avoid
      // the cmd-escaping needed for the `start` builtin.
      const child = spawn("explorer.exe", ["/select,", localPath], {
        detached: true,
        stdio: "ignore",
      });
      child.unref();
    } else if (process.platform === "darwin") {
      const child = spawn("open", ["-R", localPath], { detached: true, stdio: "ignore" });
      child.unref();
    } else {
      const dir = path.dirname(localPath);
      const child = spawn("xdg-open", [dir], { detached: true, stdio: "ignore" });
      child.unref();
    }
    return { ok: true, path: reqPath, localPath };
  } catch (e) {
    return { ok: false, error: "Spawn failed: " + e.message };
  }
}

// Walk a folder recursively and write one absolute path per line to
// <folder>/basic_listing.txt. Equivalent to Windows `dir /b /s`. No
// canonicalization — the path that lands in the file is the real path
// on this machine. vendorOf()/sourceDataTypeOf() find the 'vendors' or
// 'JobFiles' segment by name regardless of the prefix.
//
// Caller verified `folder` is a real directory.
function generateBasicListing(folder) {
  const lines = [];
  const stack = [folder];
  while (stack.length) {
    const dir = stack.pop();
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch (e) {
      // Surface the first failure but keep walking — bad ACL on one
      // subdir shouldn't kill the whole scan.
      console.warn("[generateBasicListing]", dir, e.code || e.message);
      continue;
    }
    entries.sort((a, b) => a.name.localeCompare(b.name));
    for (const entry of entries) {
      if (entry.name === "basic_listing.txt" || entry.name === "listing.txt") continue;
      const abs = path.join(dir, entry.name);
      lines.push(abs);
      if (entry.isDirectory()) stack.push(abs);
    }
  }
  // Sort so the output is byte-stable regardless of fs walk order.
  lines.sort();
  const dest = path.join(folder, "basic_listing.txt");
  fs.writeFileSync(dest, lines.join("\n") + "\n", "utf8");
  return { path: dest, lineCount: lines.length };
}

// Force-regenerate a folder's basic_listing.txt by deleting the existing
// file and walking the folder again. Used by the "Rescan listing" button —
// keeps the path the user already chose, just refreshes its contents.
function handleRescanListing(body) {
  const folder = (body.folder || "").trim();
  if (!folder) return { ok: false, error: "Missing folder." };
  let stat;
  try { stat = fs.statSync(folder); }
  catch (e) { return { ok: false, error: "Cannot access: " + folder + " (" + e.code + ")" }; }
  if (!stat.isDirectory()) return { ok: false, error: "Not a directory: " + folder };

  const existing = path.join(folder, "basic_listing.txt");
  let deleted = false;
  if (fs.existsSync(existing)) {
    try {
      fs.unlinkSync(existing);
      deleted = true;
    } catch (e) {
      return { ok: false, error: "Could not delete existing listing: " + e.message };
    }
  }

  let result;
  try {
    result = generateBasicListing(folder);
  } catch (e) {
    return { ok: false, error: "Generation failed: " + e.message };
  }

  // Keep the open-file remap pointing at this folder (matches choose-folder).
  PATH_REMAPS.length = 0;
  PATH_REMAPS.push({
    from: "S:\\vendors\\",
    to: folder.endsWith("\\") || folder.endsWith("/") ? folder : folder + "\\",
  });

  return {
    ok: true,
    folder,
    listingPath: result.path,
    lineCount: result.lineCount,
    deletedExisting: deleted,
  };
}

// Resolve a chosen folder to a listing file: use the existing
// basic_listing.txt if there is one, otherwise generate it now.
// Side effect: PATH_REMAPS gets a "S:\vendors\ -> <folder>\" entry
// added at runtime so the open-file feature finds the actual files.
function handleChooseFolder(body) {
  const folder = (body.folder || "").trim();
  if (!folder) return { ok: false, error: "Missing folder." };
  let stat;
  try { stat = fs.statSync(folder); }
  catch (e) { return { ok: false, error: "Cannot access: " + folder + " (" + e.code + ")" }; }
  if (!stat.isDirectory()) return { ok: false, error: "Not a directory: " + folder };

  const listing = path.join(folder, "basic_listing.txt");
  let used, lineCount, generated = false;
  if (fs.existsSync(listing)) {
    used = listing;
    try {
      lineCount = fs.readFileSync(listing, "utf8").split(/\r?\n/).filter((l) => l.trim()).length;
    } catch { lineCount = 0; }
  } else {
    try {
      const r = generateBasicListing(folder);
      used = r.path;
      lineCount = r.lineCount;
      generated = true;
    } catch (e) {
      return { ok: false, error: "Generation failed: " + e.message };
    }
  }

  // Register a remap so opening files works for this corpus.
  // Keep only one remap entry — replacing if the user picks another folder.
  PATH_REMAPS.length = 0;
  PATH_REMAPS.push({
    from: "S:\\vendors\\",
    to: folder.endsWith("\\") || folder.endsWith("/") ? folder : folder + "\\",
  });

  return {
    ok: true,
    folder,
    listingPath: used,
    lineCount,
    generated,
  };
}

// List a directory's contents for the file-picker modal. Returns
// subdirectories and .txt files only (the picker is listing-specific).
// `path` may be empty/missing — in that case we return the list of drive
// letters on Windows so the user has a starting point.
function handleListDir(body) {
  const requested = (body.path || "").trim();

  if (!requested) {
    // Top of the tree on Windows: enumerate available drive letters.
    if (process.platform === "win32") {
      const drives = [];
      for (let c = 65; c <= 90; c++) {
        const letter = String.fromCharCode(c);
        const root = letter + ":\\";
        try {
          fs.accessSync(root);
          drives.push(root);
        } catch {
          /* drive not present */
        }
      }
      return {
        ok: true,
        path: "",
        parent: null,
        dirs: drives.map((d) => ({ name: d, path: d })),
        files: [],
      };
    }
    // Non-Windows: start at /.
    return handleListDir({ path: "/" });
  }

  let stat;
  try {
    stat = fs.statSync(requested);
  } catch (e) {
    return { ok: false, error: "Cannot access: " + requested + " (" + e.code + ")" };
  }
  if (!stat.isDirectory()) {
    return { ok: false, error: "Not a directory: " + requested };
  }

  let entries;
  try {
    entries = fs.readdirSync(requested, { withFileTypes: true });
  } catch (e) {
    return { ok: false, error: "Read failed: " + e.message };
  }

  const dirs = [];
  const files = [];
  for (const entry of entries) {
    if (entry.name.startsWith(".")) continue;
    const full = path.join(requested, entry.name);
    if (entry.isDirectory()) {
      dirs.push({ name: entry.name, path: full });
    } else if (entry.isFile() && /\.txt$/i.test(entry.name)) {
      let size = 0;
      try { size = fs.statSync(full).size; } catch { /* ignore */ }
      files.push({ name: entry.name, path: full, size });
    }
  }
  dirs.sort((a, b) => a.name.localeCompare(b.name));
  files.sort((a, b) => a.name.localeCompare(b.name));

  // Compute parent. On Windows, the parent of "C:\" is the drives view (empty path).
  let parent = path.dirname(requested);
  if (parent === requested) parent = ""; // we're at a root

  return { ok: true, path: requested, parent, dirs, files };
}

// Save an uploaded listing file under EnLogosGRAG/uploads/. The body is
// a small JSON wrapper { name, content } since the listing is plain text
// and well under any reasonable size limit. Returns the local absolute
// path so the client can display it and the user can pick "Run files"
// against it.
function handleUploadListing(body) {
  const name = (body.name || "").trim();
  const content = body.content;
  if (!name) return { ok: false, error: "Missing filename." };
  if (typeof content !== "string") return { ok: false, error: "Missing content." };

  // Sanitize filename — strip directory separators / parent traversal.
  const safe = path.basename(name);
  if (!safe || safe.startsWith(".")) {
    return { ok: false, error: "Invalid filename." };
  }

  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
  const dest = path.join(UPLOADS_DIR, safe);

  try {
    fs.writeFileSync(dest, content, "utf8");
  } catch (e) {
    return { ok: false, error: "Write failed: " + e.message };
  }

  // Quick sanity check — count non-blank lines so the user sees something
  // useful in the log immediately.
  const lineCount = content.split(/\r?\n/).filter((l) => l.trim()).length;
  return { ok: true, name: safe, path: dest, lineCount };
}

function handleClassify() {
  const db = openDb();
  try {
    const result = classifyAll(db);
    return { ok: true, ...result, counts: tableCounts(db) };
  } catch (e) {
    return { ok: false, error: e.message };
  } finally {
    db.close();
  }
}

// Purge filesystem text backups for the currently-loaded corpus.
// Same vendors-segment heuristic the extractor uses: find each unique
// "vendors"-named segment across files.path, walk up to its parent dir,
// then delete the sibling whose name is "<segment>_text". Best-effort —
// missing folders are skipped, errors are reported but don't abort.
function handlePurgeTextBackups() {
  const db = openDb();
  const targets = new Set();
  try {
    const rows = db.prepare(
      "SELECT DISTINCT path FROM files WHERE is_dir = 0 LIMIT 5000",
    ).all();
    for (const r of rows) {
      // Map canonical paths through PATH_REMAPS to local form first, since
      // the backup folder lives next to the actual on-disk vendors dir.
      const local = remapPath(r.path);
      const target = backupRootFor(local);
      if (target) targets.add(target);
    }
  } finally {
    db.close();
  }

  const results = [];
  for (const target of targets) {
    if (!fs.existsSync(target)) {
      results.push({ path: target, status: "missing" });
      continue;
    }
    try {
      // Sanity: refuse to delete anything that doesn't end with _text.
      // Belt-and-braces against a future bug in backupRootFor.
      if (!/_text$/i.test(target)) {
        results.push({ path: target, status: "skipped: name does not end with _text" });
        continue;
      }
      fs.rmSync(target, { recursive: true, force: true });
      results.push({ path: target, status: "deleted" });
    } catch (e) {
      results.push({ path: target, status: "error: " + e.message });
    }
  }

  return { ok: true, targets: results, count: results.length };
}

// Compute the backup-tree root from a local file path. Mirrors the
// extractor's per-file mapping but truncates at the renamed segment.
//   in:  C:\data\PyroCommData\PyroCommSubset\Vendors\Notifier\foo.pdf
//   out: C:\data\PyroCommData\PyroCommSubset\Vendors_text
function backupRootFor(localPath) {
  if (!localPath) return null;
  const parts = localPath.split(/[\\/]/);
  const idx = parts.findIndex((p) => p.toLowerCase() === "vendors");
  if (idx < 0) return null;
  const newParts = parts.slice(0, idx + 1);
  newParts[idx] = parts[idx] + "_text";
  const sep = localPath.includes("\\") ? "\\" : "/";
  return newParts.join(sep);
}

function handleClassifyByContent() {
  const db = openDb();
  try {
    const result = classifyAllByContent(db);
    return { ok: true, ...result, counts: tableCounts(db) };
  } catch (e) {
    return { ok: false, error: e.message };
  } finally {
    db.close();
  }
}

function handleClassifyProducts(body = {}) {
  const allowed = new Set(["all", "file", "content"]);
  const mode = allowed.has(body.mode) ? body.mode : "all";
  const db = openDb();
  try {
    const result = classifyAllByProduct(db, { mode });
    return { ok: true, ...result, counts: tableCounts(db) };
  } catch (e) {
    return { ok: false, error: e.message };
  } finally {
    db.close();
  }
}

function handleClassifyVendors(body = {}) {
  const allowed = new Set(["all", "file", "content"]);
  const mode = allowed.has(body.mode) ? body.mode : "all";
  const db = openDb();
  try {
    const result = classifyAllByVendor(db, { mode });
    return { ok: true, ...result, counts: tableCounts(db) };
  } catch (e) {
    return { ok: false, error: e.message };
  } finally {
    db.close();
  }
}

// PDF extraction kickoff. Returns immediately — caller polls
// /api/extract-status to track progress.
function handleExtractStart(body = {}) {
  const onlyMissing = body.onlyMissing !== false;
  const started = extractor.start({ openDb, onlyMissing });
  if (!started) {
    return { ok: false, error: "Extractor is already running.", status: extractor.getStatus() };
  }
  return { ok: true, status: extractor.getStatus() };
}

function handleExtractStop() {
  const stopped = extractor.stop();
  return { ok: true, stopped, status: extractor.getStatus() };
}

function handleExtractStatus() {
  // Plus a quick count of how many extracts are in the DB so the UI can
  // show "X / Y extractable docs extracted" even between runs. Counts
  // .pdf and .docx — the two formats the extractor currently handles.
  const db = openDb();
  let extractedCount = 0, totalExtractable = 0;
  try {
    extractedCount = db.prepare(
      "SELECT COUNT(*) AS n FROM document_extracts WHERE error IS NULL"
    ).get().n;
    totalExtractable = db.prepare(
      `SELECT COUNT(*) AS n FROM documents d
       JOIN files f ON f.id = d.file_id
       WHERE f.file_type IN ('.pdf', '.docx')`
    ).get().n;
  } finally {
    db.close();
  }
  // Keep the legacy `totalPdfs` field name for back-compat with the
  // status-poll client; it now means "total extractable", not "total PDFs".
  return {
    ok: true,
    status: extractor.getStatus(),
    extractedCount,
    totalPdfs: totalExtractable,
    totalExtractable,
  };
}

// File hashing kickoff. Same shape as the extractor: caller polls
// /api/hash-status. onlyMissing defaults true so re-clicking is cheap.
function handleHashStart(body = {}) {
  const onlyMissing = body.onlyMissing !== false;
  const started = hasher.start({ openDb, onlyMissing });
  if (!started) {
    return { ok: false, error: "Hasher is already running.", status: hasher.getStatus() };
  }
  return { ok: true, status: hasher.getStatus() };
}

function handleHashStop() {
  const stopped = hasher.stop();
  return { ok: true, stopped, status: hasher.getStatus() };
}

function handleHashStatus() {
  const db = openDb();
  let hashedCount = 0, totalDocs = 0;
  try {
    hashedCount = db.prepare("SELECT COUNT(*) AS n FROM documents WHERE sha256 IS NOT NULL").get().n;
    totalDocs   = db.prepare("SELECT COUNT(*) AS n FROM documents").get().n;
  } finally {
    db.close();
  }
  return { ok: true, status: hasher.getStatus(), hashedCount, totalDocs };
}

// Snowflake snapshot of Kent's BUILDING_CANONICAL + BUILDING_ADDRESS_XREF
// into local buildings + building_addresses tables. Buildings are loaded
// first so the addresses upsert can reference them via FK; address rows
// whose building isn't in the new snapshot are reported as "orphaned" but
// not an error. Idempotent — re-running upserts on PK collision.
async function handleBuildingsSnapshot() {
  try {
    const b = await snapshotBuildings(openDb);
    const a = await snapshotBuildingAddresses(openDb);
    // Seed the ignored_buildings table with Pyrocomm's own offices once
    // the buildings rows exist. Idempotent — re-running upserts on PK so
    // the user's manual additions are never overwritten.
    const i = seedPyrocommIgnored(openDb);
    return { ok: true, buildings: b, addresses: a, ignored: i };
  } catch (e) {
    return { ok: false, error: String(e.message || e) };
  }
}

function handleBuildingsMatchStart(body = {}) {
  const onlyMissing = body.onlyMissing !== false;
  const sdt = body.sdt || "JobFiles";
  const allowed = new Set(["all", "file", "content"]);
  const mode = allowed.has(body.mode) ? body.mode : "all";
  const started = buildingsMatcher.start({ openDb, onlyMissing, sdt, mode });
  if (!started) {
    return { ok: false, error: "Buildings matcher is already running.", status: buildingsMatcher.getStatus() };
  }
  return { ok: true, status: buildingsMatcher.getStatus() };
}

function handleBuildingsMatchStop() {
  const stopped = buildingsMatcher.stop();
  return { ok: true, stopped, status: buildingsMatcher.getStatus() };
}

function handleBuildingsMatchStatus() {
  const db = openDb();
  let docBuildings = 0, totalCanonical = 0, extractedBuildings = 0;
  try {
    docBuildings       = db.prepare("SELECT COUNT(DISTINCT document_id) AS n FROM document_buildings").get().n;
    totalCanonical     = db.prepare("SELECT COUNT(*) AS n FROM canonical_buildings").get().n;
    extractedBuildings = db.prepare("SELECT COUNT(*) AS n FROM buildings").get().n;
  } finally {
    db.close();
  }
  return {
    ok: true,
    status: buildingsMatcher.getStatus(),
    docBuildings,
    extractedBuildings,
    // Keep the legacy field name `totalBuildings` to avoid breaking the
    // status-pill text on the client; semantically it now means
    // "canonical buildings in snapshot" and the client label still reads
    // correctly.
    totalBuildings: totalCanonical,
  };
}

// Read-only duplicate report — returns clusters of documents sharing
// a sha256. UI renders one section per cluster.
function handleDuplicates() {
  try {
    const result = hasher.findDuplicates(openDb);
    return { ok: true, ...result };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

function handlePurge(body) {
  const target = body.target;
  const db = openDb();
  try {
    if (target === "all") {
      // Order matters: child tables first to satisfy FK constraints.
      //   files     → cascades to documents, document_extracts,
      //               document_products, document_buildings
      //   buildings → independent of files; cleared explicitly here
      //               since they're ephemeral extraction artifacts
      //   products  → owned by vendors via FK; clear before deleting vendors
      //   vendors   → safe to delete once products is empty
      // Intentionally preserved (canonical / curated reference data):
      //   document_types       taxonomy, no reason to reseed
      //   ignored_file_types   user-curated ingest config
      //   ignored_folders      user-curated ingest config
      //   ignored_files        user-curated per-file blocklist
      //   canonical_buildings  snapshotted from Kent's Snowflake; expensive
      //                        to refetch and not regenerable locally
      //   building_addresses   same — snapshotted from Snowflake
      //   ignored_buildings    user-curated blocklist (Pyrocomm offices etc)
      db.exec("DELETE FROM files");
      db.exec("DELETE FROM buildings");
      db.exec("DELETE FROM products");
      db.exec("DELETE FROM vendors");
      db.exec(
        "DELETE FROM sqlite_sequence WHERE name IN " +
        "('vendors', 'files', 'documents', 'products', 'buildings')",
      );
      return { ok: true, target, counts: tableCounts(db) };
    }
    if (target === "files") {
      // Cascade wipes documents → document_extracts → document_products.
      // products and vendors are independent of files, so they stay.
      db.exec("DELETE FROM files");
      db.exec("DELETE FROM sqlite_sequence WHERE name IN ('files', 'documents')");
      return { ok: true, target, counts: tableCounts(db) };
    }
    if (target === "vendors") {
      const filesCount    = db.prepare("SELECT COUNT(*) AS n FROM files").get().n;
      const productsCount = db.prepare("SELECT COUNT(*) AS n FROM products").get().n;
      if (filesCount > 0) {
        return {
          ok: false,
          error:
            `Cannot purge vendors while files has ${filesCount} rows ` +
            `(FK constraint). Purge files first, or use "Purge ALL".`,
        };
      }
      if (productsCount > 0) {
        return {
          ok: false,
          error:
            `Cannot purge vendors while products has ${productsCount} rows ` +
            `(FK constraint). Purge products first, or use "Purge ALL".`,
        };
      }
      db.exec("DELETE FROM vendors");
      db.exec("DELETE FROM sqlite_sequence WHERE name = 'vendors'");
      return { ok: true, target, counts: tableCounts(db) };
    }
    if (target === "hashes") {
      // Clear documents.sha256 only — leaves the .hash sidecar files on
      // disk so the next hash run can restore from cache. Use "Purge text
      // backups" to also delete the on-disk sidecars.
      const r = db.prepare("UPDATE documents SET sha256 = NULL WHERE sha256 IS NOT NULL").run();
      return { ok: true, target, cleared: Number(r.changes), counts: tableCounts(db) };
    }
    return { ok: false, error: `Unknown purge target: ${target}` };
  } catch (e) {
    return { ok: false, error: e.message };
  } finally {
    db.close();
  }
}

// --- HTML page -------------------------------------------------------------

const PAGE = String.raw`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>EnLogosGRAG — DB browser</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #ffffff;
    --panel: #f4f4f4;
    --inset: #fafafa;        /* inputs, code blocks, log strip */
    --hover: #ebebeb;        /* row hover, button hover */
    --hover-strong: #dcdcdc; /* deeper hover for nested controls */
    --border: #d0d0d0;
    --border-row: #e6e6e6;   /* horizontal table row separators */
    --border-col: #d8d8d8;   /* vertical column separators */
    --text: #111111;
    --muted: #666666;
    --accent: #c8102e;
    --accent-bg: #fbe6ea;
    --danger: #c8102e;
    --danger-bg: #fbe6ea;
    --danger-text: #8a0a1f;
    --ok: #111111;
    --error-text: #c8102e;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 0;
    font-family: "Inter", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Segoe UI Variable", "Segoe UI", system-ui, sans-serif;
    font-feature-settings: "cv11", "ss01", "ss03";
    -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;
    background: var(--bg); color: var(--text);
    height: 100vh; display: flex; flex-direction: column; overflow: hidden;
  }
  header {
    padding: 6px 12px; background: var(--bg);
    border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  }
  header h1 {
    font-size: 13px; font-weight: 600; margin: 0; color: var(--text);
    letter-spacing: -0.01em;
  }
  header h1::before {
    content: ""; display: inline-block; width: 8px; height: 8px;
    background: var(--accent); margin-right: 8px; vertical-align: 1px;
  }
  header .meta { font-size: 11.5px; color: var(--muted); }
  header #sdt-control {
    display: inline-flex; align-items: center; margin-left: 14px;
    padding: 2px 8px;
    background: var(--inset); border: 1px solid var(--border); border-radius: 2px;
  }
  header #sdt-control select {
    font-size: 11.5px; padding: 1px 4px;
    background: var(--bg); color: var(--text);
    border: 1px solid var(--border); border-radius: 2px;
    cursor: pointer; font-family: inherit;
  }
  header #sdt-control select:hover { border-color: var(--text); }

  /* SDT pill — appears in sidebar Source data + Status sections.
     Reads "Using Vendor Rules" / "Using JobFiles Rules" / "Using Any Rules". */
  .sdt-pill {
    display: block;
    background: var(--inset); color: var(--text);
    border: 1px solid var(--border); border-left: 3px solid var(--accent);
    border-radius: 2px;
    padding: 3px 8px; margin: 3px 0 6px 0;
    font-size: 10.5px; font-weight: 500; letter-spacing: 0.02em;
    text-align: left;
  }
  header .spacer { flex: 1; }
  header a.help-link {
    font-size: 11.5px; color: var(--text); text-decoration: none;
    padding: 2px 8px; border: 1px solid var(--border); border-radius: 2px;
    background: var(--inset);
  }
  header a.help-link:hover { background: var(--hover); border-color: var(--text); }
  main { flex: 1; display: flex; min-height: 0; }
  aside {
    width: 232px; border-right: 1px solid var(--border);
    padding: 8px 6px;
    background: var(--panel);
    position: relative;
    transition: width 0.12s ease;
    font-size: 12px;
    /* Flex column so #sidebar-nav can scroll while #sidebar-status stays
       pinned at the bottom. min-height:0 lets the nav child actually
       shrink; without it the column would just grow past the viewport. */
    display: flex; flex-direction: column; min-height: 0;
  }
  /* Scrollable navigation region — everything above the pinned Status. */
  #sidebar-nav {
    flex: 1 1 auto; min-height: 0; overflow-y: auto;
    margin: 0 -6px; padding: 0 6px;
  }
  /* Pinned status footer — always visible regardless of nav scroll. */
  #sidebar-status {
    flex: 0 0 auto;
    margin: 8px -6px 0 -6px; padding: 8px 6px 0 6px;
    border-top: 1px solid var(--border);
  }
  #sidebar-status-header {
    /* Same treatment as the top-level <details> summaries so it reads
       as a peer section header, just non-collapsible. */
    font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em;
    font-weight: 600; color: var(--muted);
    padding: 0 8px; margin: 0 0 4px 0;
  }
  /* Collapsed: narrow strip just wide enough for the toggle button.
     All children except the toggle are hidden. */
  aside.collapsed { width: 24px; padding: 6px 2px; overflow: hidden; display: block; }
  aside.collapsed > :not(#sidebar-toggle) { display: none; }
  /* Drag handle on the sidebar's right edge. 4px wide hit area, only
     visible on hover/drag so it doesn't compete with content. The
     transition on aside.width is suppressed during drag so the panel
     follows the pointer 1:1 (handled in JS via .resizing class). */
  #sidebar-resizer {
    position: absolute; top: 0; right: -2px; bottom: 0; width: 4px;
    cursor: ew-resize;
    background: transparent;
    z-index: 10;
  }
  #sidebar-resizer:hover, #sidebar-resizer.dragging {
    background: var(--accent);
  }
  aside.collapsed #sidebar-resizer { display: none; }
  aside.resizing { transition: none !important; }
  body.resizing-sidebar, body.resizing-sidebar * { cursor: ew-resize !important; user-select: none !important; }
  #sidebar-toggle {
    position: absolute; top: 6px; right: 4px;
    width: 16px; height: 20px; padding: 0;
    background: transparent; color: var(--muted);
    border: 1px solid var(--border); border-radius: 2px;
    cursor: pointer; font-size: 11px; line-height: 1;
  }
  #sidebar-toggle:hover { color: var(--text); border-color: var(--accent); }
  aside.collapsed #sidebar-toggle {
    /* Centered when collapsed, with a different glyph. */
    top: 6px; left: 50%; right: auto; transform: translateX(-50%);
  }
  /* Master sidebar typography — every navigation row in the sidebar uses
     the same font / size / case / weight. Hierarchy is conveyed by left
     padding (see --depth-* tokens below) and by an optional disclosure
     caret. Color stays muted by default so the active row pops. */
  aside h2,
  aside details > summary,
  aside .table-row,
  aside .sub-summary,
  aside .chart-folder-summary,
  aside .chart-preset,
  aside details.action-sub > summary,
  aside details.extractor-sub > summary,
  aside .action-row,
  aside table.sidebar-summary td,
  aside .menu-trigger,
  aside .menu-item,
  aside #use-ignores-wrap,
  aside label {
    font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em;
    font-weight: 600; line-height: 1.3; color: var(--muted);
  }
  aside h2 { margin: 6px 0 2px 0; }
  aside h2:first-child { margin-top: 0; padding-right: 22px; }

  /* .table-row default: depth-1 (16px), used when the row sits directly
     under a top-level <details> body (no sub-summary parent). Deeper
     nesting is handled by the more-specific selectors below. */
  aside .table-row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 1px 8px 1px 16px;
    cursor: pointer; border-radius: 2px;
    color: var(--text);
  }
  aside .table-row:hover { background: var(--hover); }
  aside .table-row.active { background: var(--text); color: var(--bg); }
  aside .table-row.active .count { color: var(--bg); opacity: 0.7; }
  aside .table-row .count {
    color: var(--muted); font-variant-numeric: tabular-nums;
    font-size: 10px; letter-spacing: 0.08em;
  }
  aside .table-row.active .count { color: var(--bg); }

  /* Compact 2-col summary table inside Source data. Same typography +
     depth-1 indent as .table-row so it reads as a sibling clickable row. */
  aside table.sidebar-summary {
    width: 100%; border-collapse: collapse; margin-top: 2px;
  }
  aside table.sidebar-summary tr { cursor: pointer; }
  aside table.sidebar-summary tr:hover td { background: var(--hover); }
  aside table.sidebar-summary tr.active td { background: var(--text); color: var(--bg); }
  aside table.sidebar-summary tr.active td.count { color: var(--bg); opacity: 0.7; }
  aside table.sidebar-summary td {
    padding: 1px 8px 1px 16px; border-radius: 2px; color: var(--text);
  }
  aside table.sidebar-summary td.count {
    color: var(--muted); font-variant-numeric: tabular-nums;
    text-align: right; width: 40px;
  }

  /* Depth-2: sub-summary headers (DASHBOARDS, CHARTS, DUPLICATES under
     Reports). Same master typography; depth differentiated by indent. */
  aside .sub-summary {
    padding: 1px 8px 1px 16px; margin-top: 1px;
    cursor: pointer; color: var(--text);
    display: flex; align-items: center; gap: 5px;
  }
  aside .sub-summary::-webkit-details-marker { display: none; }
  aside .sub-summary::before {
    content: "▸"; font-size: 8px; transition: transform 0.1s;
    display: inline-block; width: 8px;
  }
  aside details[open] > .sub-summary::before { transform: rotate(90deg); }
  aside .sub-summary:hover { color: var(--accent); }

  /* Depth-3: inner chart-folder headers (CLASSIFICATION, SOURCE DATA
     TYPE, FILETYPE inside Charts). */
  aside .chart-folder { margin-left: 0; }
  aside .chart-folder-summary {
    padding: 1px 8px 1px 24px; margin-top: 1px;
    cursor: pointer; color: var(--text);
    display: flex; align-items: center; gap: 5px;
  }
  aside .chart-folder-summary::-webkit-details-marker { display: none; }
  aside .chart-folder-summary::before {
    content: "▸"; font-size: 8px; transition: transform 0.1s;
    display: inline-block; width: 8px;
  }
  aside details[open] > .chart-folder-summary::before { transform: rotate(90deg); }
  aside .chart-folder-summary:hover { color: var(--accent); }

  /* Depth-2 leaves: rows whose enclosing <details> has a .sub-summary
     header. Covers .table-row siblings of sub-summaries (Find duplicates
     under Duplicates; Content/Filename rules under Type Classifiers;
     Building/Product Extractor under Entity Extraction; All documents/
     JobFiles/Vendor under Dashboards; Ephemeral sidebar table-rows). */
  aside details:has(> .sub-summary) > .table-row,
  aside details:has(> .sub-summary) #table-list .table-row,
  aside details:has(> .sub-summary) .table-list .table-row,
  aside details:has(> .sub-summary) > table.sidebar-summary td {
    padding-left: 24px;
  }

  /* Depth-3 leaves: rows whose enclosing <details> has a
     .chart-folder-summary header. Covers chart-preset rows AND the
     folder's "All …" anchor (.table-row sibling of chart-folder-summary). */
  aside .chart-preset {
    padding: 1px 8px 1px 32px; cursor: pointer; border-radius: 2px;
    color: var(--text);
  }
  aside .chart-preset:hover { background: var(--hover); }
  aside .chart-preset.active { background: var(--text); color: var(--bg); }
  aside .chart-folder > .table-row {
    padding-left: 32px;
  }
  aside label { display: block; font-size: 12px; line-height: 1.4; color: var(--muted); margin: 4px 0 2px 0; }
  aside input[type=text] {
    width: 100%; padding: 4px 6px; font-size: 12px; line-height: 1.4;
    background: var(--inset); color: var(--text);
    border: 1px solid var(--border); border-radius: 2px;
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
  }
  #listing-display {
    padding: 6px 8px;
    border: 1px solid var(--border); border-radius: 2px;
    background: var(--inset); font-size: 12px; line-height: 1.4;
    word-break: break-all;
  }
  .extract-status {
    padding: 4px 8px; margin-bottom: 3px;
    background: var(--inset); border: 1px solid var(--border); border-radius: 2px;
    font-size: 10.5px; color: var(--muted);
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
  }
  .extract-status.running { color: var(--accent); border-color: var(--accent); }
  .extract-status .current {
    margin-top: 3px; word-break: break-all; color: var(--text);
    font-size: 10px; opacity: 0.8;
  }
  #listing-display .listing-empty { color: var(--muted); font-style: italic; }
  #listing-display .listing-name { color: var(--text); font-weight: 600; }
  #listing-display .listing-path {
    color: var(--muted);
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    font-size: 10.5px; margin-top: 2px;
  }
  /* Modal: dimmed backdrop + centered panel. */
  .modal-backdrop {
    position: fixed; inset: 0;
    background: rgba(0,0,0,0.6); z-index: 100;
    display: flex; align-items: center; justify-content: center;
  }
  .modal-panel {
    background: var(--panel); color: var(--text);
    border: 1px solid var(--border); border-radius: 5px;
    width: 640px; max-width: 90vw;
    max-height: 80vh; display: flex; flex-direction: column;
    box-shadow: 0 8px 24px rgba(0,0,0,0.6);
  }
  .modal-panel h3 {
    font-size: 14px; margin: 0; padding: 10px 14px;
    border-bottom: 1px solid var(--border); color: var(--accent);
    display: flex; justify-content: space-between; align-items: center;
  }
  .modal-panel .close {
    background: transparent; border: 1px solid var(--border); color: var(--muted);
    width: 22px; height: 22px; border-radius: 3px; padding: 0; line-height: 1; cursor: pointer;
  }
  .modal-panel .close:hover { color: var(--text); border-color: var(--accent); }
  .modal-cwd {
    padding: 8px 14px; font-family: monospace; font-size: 12px;
    color: var(--muted); border-bottom: 1px solid var(--border);
    word-break: break-all; background: var(--inset);
  }
  .modal-actions {
    display: flex; align-items: center; gap: 12px;
    padding: 8px 14px; border-bottom: 1px solid var(--border);
  }
  .modal-actions .primary {
    padding: 6px 12px; font-size: 12px;
    background: var(--accent-bg); color: var(--text);
    border: 1px solid var(--accent); border-radius: 3px; cursor: pointer;
  }
  .modal-actions .primary:hover { background: var(--accent); color: #fff; border-color: var(--accent); }
  .modal-actions .primary:disabled { opacity: 0.4; cursor: not-allowed; }
  .modal-hint { color: var(--muted); font-size: 11px; }
  .modal-list { flex: 1; overflow-y: auto; padding: 4px 0; }
  .modal-list .row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 5px 14px; cursor: pointer; font-size: 13px;
  }
  .modal-list .row:hover { background: var(--hover); }
  .modal-list .row.dir   { color: var(--text); }
  .modal-list .row.file  { color: var(--text); }
  .modal-list .row .icon { width: 18px; flex-shrink: 0; opacity: 0.7; }
  .modal-list .row .name { flex: 1; }
  .modal-list .row .meta { color: var(--muted); font-variant-numeric: tabular-nums; font-size: 11px; }
  .modal-list .empty { padding: 20px; text-align: center; color: var(--muted); }
  .modal-list .err   { padding: 12px 14px; color: var(--error-text); font-family: monospace; font-size: 12px; }

  /* Extract viewer modal: wider than the folder picker so the wrapped
     text stays readable. */
  .extract-panel { width: 900px; max-width: 95vw; max-height: 88vh; }
  .extract-panel .extract-meta {
    padding: 8px 14px; border-bottom: 1px solid var(--border);
    font-size: 12px; background: var(--inset);
  }
  .extract-panel .extract-meta .meta-row {
    display: flex; gap: 12px; padding: 2px 0;
  }
  .extract-panel .extract-meta .k {
    color: var(--muted); min-width: 90px; font-family: monospace; text-transform: uppercase; font-size: 10px;
    padding-top: 2px;
  }
  .extract-panel .extract-meta .v {
    flex: 1; word-break: break-all; font-family: monospace; font-size: 12px;
  }
  .extract-panel .extract-body {
    flex: 1; overflow: auto; padding: 0;
    background: var(--bg);
  }
  .extract-panel .extract-text {
    margin: 0; padding: 12px 14px;
    font-family: monospace; font-size: 12px; line-height: 1.5;
    white-space: pre-wrap; word-break: break-word;
    color: var(--text);
  }
  .extract-panel .extract-err {
    padding: 14px; color: var(--error-text); font-family: monospace; font-size: 12px;
  }
  .extract-panel .empty { padding: 24px; color: var(--muted); text-align: center; }
  aside button {
    width: 100%; padding: 5px 8px; margin-top: 4px; font-size: 11.5px;
    background: var(--text); color: var(--bg);
    border: 1px solid var(--text); border-radius: 2px;
    cursor: pointer; font-family: inherit; font-weight: 500;
  }
  aside button:hover { background: var(--accent); border-color: var(--accent); color: #fff; }
  aside button.danger { background: var(--danger-bg); border-color: var(--danger); color: var(--danger-text); }
  aside button.danger:hover { background: var(--danger); color: #fff; }
  aside button.secondary { background: var(--inset); border-color: var(--border); color: var(--text); }
  aside button.secondary:hover { background: var(--hover); border-color: var(--text); }
  aside button:disabled { opacity: 0.5; cursor: not-allowed; }
  /* Progress-fill: button background becomes a sharp left-to-right split
     between accent (filled) and inset (unfilled) at --progress percent.
     Used by the extract button to show overall corpus progress while the
     worker runs. Disabled state stays vivid so the bar reads clearly. */
  aside button.progress-fill {
    background: linear-gradient(
      to right,
      var(--accent) 0%,
      var(--accent) var(--progress, 0%),
      var(--inset) var(--progress, 0%),
      var(--inset) 100%
    );
    color: var(--text);
    border-color: var(--accent);
    opacity: 1;
    cursor: progress;
    position: relative;
    overflow: hidden;
  }
  /* Force the label to sit above the gradient and stay readable on either
     side of the boundary. White-on-accent / dark-on-inset contrast comes
     from a text-shadow that smears either way. */
  aside button.progress-fill {
    text-shadow: 0 0 2px var(--bg);
  }
  section.content { flex: 1; padding: 12px; overflow: hidden; display: flex; flex-direction: column; min-width: 0; }
  .toolbar { display: flex; gap: 8px; align-items: center; margin-bottom: 8px; }
  .toolbar input {
    flex: 1; padding: 5px 7px; font-size: 13px;
    background: var(--inset); color: var(--text);
    border: 1px solid var(--border); border-radius: 3px;
  }
  .toolbar button {
    padding: 5px 10px; font-size: 12px;
    background: var(--hover); color: var(--text);
    border: 1px solid var(--border); border-radius: 3px; cursor: pointer;
  }
  .toolbar button:hover { background: var(--hover-strong); }
  .toolbar button:disabled { opacity: 0.4; cursor: not-allowed; }
  .toolbar select {
    padding: 5px 7px; font-size: 12px;
    background: var(--inset); color: var(--text);
    border: 1px solid var(--border); border-radius: 3px;
    cursor: pointer;
  }
  .toolbar select:hover { background: var(--panel); }
  /* docs-only controls are hidden when a non-documents table is active. */
  .toolbar .docs-only.hidden { display: none; }
  /* ext-only is shown for files + documents (anything with a file_type column).
     Hidden on vendors / document_types where it makes no sense. */
  .toolbar .ext-only.hidden { display: none; }
  /* files-only is shown only on the files table — the ignored/active filter
     compares against the documents table and is meaningless elsewhere. */
  .toolbar .files-only.hidden { display: none; }

  /* Custom multi-select dropdown. Native <select multiple> is awful UX on
     Windows, so we roll our own checkbox panel. */
  .multi-dropdown { position: relative; }
  .multi-btn {
    padding: 5px 8px; font-size: 12px;
    background: var(--inset); color: var(--text);
    border: 1px solid var(--border); border-radius: 3px;
    cursor: pointer; white-space: nowrap;
    display: inline-flex; align-items: center; gap: 4px;
  }
  .multi-btn:hover { background: var(--panel); border-color: var(--accent); }
  .multi-btn .caret { color: var(--muted); font-size: 10px; }
  .multi-btn.active { border-color: var(--accent); color: var(--accent); }
  .multi-panel {
    position: absolute; top: 100%; left: 0;
    margin-top: 2px; padding: 6px;
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 3px;
    min-width: 200px; max-height: 360px; overflow-y: auto;
    z-index: 50;
    box-shadow: 0 4px 12px rgba(0,0,0,0.5);
  }
  .multi-panel .row {
    display: flex; align-items: center; gap: 6px;
    padding: 3px 6px; cursor: pointer; border-radius: 3px;
    font-size: 12px;
    /* Pin everything to the left so the column of checkboxes stays in
       a single vertical line. Without this, parent text-align rules can
       cascade in and push later rows rightward. */
    justify-content: flex-start; text-align: left;
    padding-left: 0; margin-left: 0;
  }
  .multi-panel .row:hover { background: var(--hover); }
  .multi-panel .row input {
    cursor: pointer;
    margin: 0; flex: 0 0 auto;
  }
  .multi-panel .row > span:not(.count) { flex: 1 1 auto; min-width: 0; text-align: left; }
  .multi-panel .row .count { color: var(--muted); margin-left: auto; font-variant-numeric: tabular-nums; flex: 0 0 auto; }
  .multi-panel .actions {
    display: flex; gap: 6px; padding: 4px 0; margin-bottom: 4px;
    border-bottom: 1px solid var(--border);
  }
  .multi-panel .actions button {
    padding: 3px 7px; font-size: 11px;
    background: var(--hover); color: var(--text);
    border: 1px solid var(--border); border-radius: 3px; cursor: pointer;
  }
  .multi-panel .actions button:hover { background: var(--hover-strong); }
  .chart-card { background: var(--panel); border: 1px solid var(--border); border-radius: 4px; padding: 12px; }
  .chart-card h3 { font-size: 13px; margin: 0 0 8px 0; color: var(--accent); }
  /* Chart.js needs a sized parent to know how big to render. Without an
     explicit height the canvas grows unboundedly inside a flex container. */
  .chart-box { width: 360px; height: 360px; position: relative; }
  .chart-box canvas { cursor: pointer; }
  /* Filter chips: one per active selection in the chart-page filter chain.
     Each carries an × that drops just that dimension. */
  .chip { display: inline-flex; align-items: center; gap: 6px;
          background: var(--panel); border: 1px solid var(--border);
          border-radius: 3px; padding: 2px 4px 2px 10px; font-size: 12px;
          color: var(--text); margin-left: 6px; }
  .chip.root { background: transparent; border-color: transparent;
               color: var(--muted); padding-left: 0; padding-right: 0;
               margin-left: 0; margin-right: 4px; }
  .chip .chip-x { background: transparent; border: 0; color: var(--muted);
                  cursor: pointer; font-size: 14px; line-height: 1;
                  padding: 0 4px; border-radius: 2px; }
  .chip .chip-x:hover { background: var(--hover); color: var(--text); }
  .chip-sep { color: var(--muted); margin-left: 6px; }
  /* Column-reorder affordances. Cursor hint on draggable headers, then
     visible feedback for the dragged source and the current drop target. */
  th.col-draggable .sort-label { cursor: grab; }
  th.col-dragging { opacity: 0.5; }
  th.col-drop-target { box-shadow: inset 3px 0 0 var(--accent); }
  .toolbar .meta { font-size: 12px; color: var(--muted); margin-left: auto; }
  .table-wrap { flex: 1; overflow: auto; border: 1px solid var(--border); border-radius: 3px; }
  /* table-layout:fixed honors explicit column widths from the first row.
     We force the table to a tiny width (1px) so it has no ambition of its
     own — column widths sum from the explicit <th> widths and that's it.
     Without this, browsers default to width:auto which expands the table
     to fit the widest unbreakable content (long file paths), redistributing
     extra width across columns and overriding our explicit widths. */
  table {
    width: 1px;
    border-collapse: separate;
    border-spacing: 0;
    font-size: 12px;
    table-layout: fixed;
  }
  /* Vertical column borders + horizontal row borders, both at higher
     contrast than before. Using right-only on cells avoids double-borders
     while keeping each column visibly separated. */
  thead th, tbody td {
    border-right: 1px solid var(--border-col);
    border-bottom: 1px solid var(--border-row);
  }
  thead th:last-child, tbody td:last-child { border-right: none; }
  thead th {
    position: sticky; top: 0; background: var(--panel);
    text-align: left; padding: 6px 10px;
    font-weight: 600; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.5px;
    white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis;
  }
  thead th .col-resizer {
    position: absolute; top: 0; right: 0; height: 100%;
    width: 6px; cursor: col-resize; user-select: none;
    background: transparent;
  }
  thead th .col-resizer:hover,
  thead th .col-resizer.dragging { background: var(--accent); }
  tbody td {
    padding: 4px 10px;
    font-family: monospace; vertical-align: top;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }
  tbody td.null { color: var(--muted); font-style: italic; }
  tbody tr:hover { background: var(--hover); }
  /* Clickable file path: dotted underline so the cell still feels tabular,
     not a heavy hyperlink. Color brightens on hover. */
  tbody td a.open-link {
    color: var(--text); text-decoration: underline dotted var(--muted);
    text-underline-offset: 2px; cursor: pointer;
  }
  tbody td a.open-link:hover { color: var(--accent); text-decoration-color: var(--accent); }
  /* Trailing actions column on documents/files. Holds the ⋮ button or
     inline links and must not truncate — overflow:visible defeats the
     table-wide ellipsis, and the explicit width on the th sizes the
     column for the full content. */
  thead th.actions-cell, tbody td.actions-cell {
    overflow: visible;
    text-overflow: clip;
    padding-right: 6px;
  }
  /* ⋮ button on the documents row. Subtle by default, accent on hover. */
  .row-menu-btn {
    background: transparent; border: 1px solid transparent; color: var(--muted);
    cursor: pointer; font-size: 16px; line-height: 1;
    padding: 0 6px; border-radius: 3px;
  }
  .row-menu-btn:hover { background: var(--hover); color: var(--text); border-color: var(--border); }
  /* Popup floats above the table; absolute positioning + body-level mount
     so we don't get clipped by the table-wrap's overflow. */
  .row-menu {
    position: fixed; z-index: 1000;
    background: var(--bg); border: 1px solid var(--border); border-radius: 2px;
    box-shadow: 0 4px 14px rgba(0,0,0,0.12);
    min-width: 180px; padding: 3px 0;
  }
  .row-menu-item {
    display: block; width: 100%; text-align: left; box-sizing: border-box;
    background: transparent; border: 0; color: var(--text);
    font: inherit; font-size: 11.5px;
    padding: 5px 12px; cursor: pointer;
  }
  .row-menu-item:hover:not(:disabled) { background: var(--hover); color: var(--text); }
  .row-menu-item:disabled { color: var(--muted); cursor: default; }
  #log-wrap {
    margin-top: 8px;
    background: var(--inset); border: 1px solid var(--border); border-radius: 3px;
  }
  #log-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 4px 8px; cursor: pointer;
    border-bottom: 1px solid var(--border);
    font-size: 11px; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.04em;
    user-select: none;
  }
  #log-header:hover { color: var(--text); }
  #log-header .caret { font-size: 10px; transition: transform 0.15s; }
  #log-wrap.collapsed #log-header { border-bottom: none; }
  #log-wrap.collapsed #log-header .caret { transform: rotate(-90deg); }
  #log-wrap.collapsed #log { display: none; }
  #log {
    padding: 8px 10px; max-height: 120px; overflow-y: auto;
    font-size: 12px; font-family: monospace;
  }
  #log .entry { margin: 0 0 2px 0; }
  #log .entry.ok { color: var(--ok); }
  #log .entry.err { color: var(--error-text); }
  #log .entry.info { color: var(--muted); }
  /* Replayed entries (loaded from localStorage at boot) get a subtle
     left bar so users can see what's history vs. live. The class is
     applied alongside the ok/err/info kind class. */
  #log .entry.replayed {
    border-left: 2px solid var(--border);
    padding-left: 6px; opacity: 0.75;
  }
  .empty { padding: 20px; text-align: center; color: var(--muted); font-size: 13px; }

  /* Collapsible sidebar sections. <summary> acts as the section header
     (mirroring the existing h2 styling) and clicking toggles the body. */
  aside details {
    margin: 4px 0 0 0;
  }
  aside details > summary {
    list-style: none;
    cursor: pointer;
    /* font-size / text-transform / letter-spacing / weight / color come
       from the master rule that names every nav row. Override only the
       depth-specific bits here. */
    margin: 0 0 1px 0;
    padding: 0 8px;
    user-select: none;
    display: flex; align-items: center; gap: 5px;
  }
  aside details > summary::-webkit-details-marker { display: none; }
  aside details > summary::before {
    content: "▸";
    font-size: 8px;
    transition: transform 0.1s;
    display: inline-block;
    width: 8px;
  }
  aside details[open] > summary::before { transform: rotate(90deg); }
  aside details > summary:hover { color: var(--text); }
  /* "Full Process" gets its own visual distinction — it's the headline
     action, runs the whole pipeline. */
  aside button.headline {
    background: var(--accent); color: var(--bg);
    border-color: var(--accent); font-weight: 600;
  }
  aside button.headline:hover { background: #a30d26; border-color: #a30d26; }

  /* Click-to-open menus (Purge, Ingest). The trigger looks like an aside
     button; the panel pops out below and floats above following content. */
  aside .menu-wrap { position: relative; }
  aside .menu-trigger {
    width: 100%; padding: 5px 7px; margin-top: 4px;
    font-size: 12px; line-height: 1.4;
    background: var(--inset); color: var(--text);
    border: 1px solid var(--border); border-radius: 2px;
    cursor: pointer; text-align: left;
    display: flex; justify-content: space-between; align-items: center;
  }
  aside .menu-trigger:hover { background: var(--hover); border-color: var(--text); }
  aside .menu-trigger.open  { background: var(--hover); border-color: var(--text); }
  aside .menu-trigger .caret { color: var(--muted); font-size: 9px; }
  aside .menu-panel {
    position: absolute; top: 100%; left: 0; right: 0;
    margin-top: 2px; padding: 3px;
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 2px;
    z-index: 60;
    box-shadow: 0 2px 8px rgba(0,0,0,0.12);
  }
  aside .menu-item {
    padding: 4px 8px; cursor: pointer; border-radius: 2px;
    font-size: 12px; line-height: 1.4; color: var(--text);
  }
  aside .menu-item:hover { background: var(--hover); }
  aside .menu-item.danger { color: var(--danger-text); }
  aside .menu-item.danger:hover { background: var(--danger); color: #fff; }
  aside .menu-item.disabled { opacity: 0.4; cursor: not-allowed; pointer-events: none; }

  /* Inline action menu inside the sidebar. Outer details is the "Actions"
     section; nested details.action-sub are the three groups (Purge /
     Ingest / Classify & Extract). Action rows look the same in any group. */
  /* Depth-2: action-sub summary (PURGE, INGEST, EXTRACT, …). */
  aside details.action-sub { margin: 0; }
  aside details.action-sub > summary {
    padding: 1px 8px 1px 16px; margin-top: 1px; border-radius: 2px;
    color: var(--text); cursor: pointer;
    display: flex; align-items: center; gap: 5px;
  }
  aside details.action-sub > summary::-webkit-details-marker { display: none; }
  aside details.action-sub > summary::before {
    content: "▸"; font-size: 8px; transition: transform 0.1s;
    display: inline-block; width: 8px;
  }
  aside details.action-sub[open] > summary::before { transform: rotate(90deg); }
  aside details.action-sub > summary:hover { background: var(--hover); }
  /* Depth-2: action rows (PURGE ALL, RUN ALL, …) sit one level deeper
     than their action-sub summary. */
  aside .action-row {
    padding: 1px 8px 1px 24px; cursor: pointer; border-radius: 2px;
    color: var(--text);
  }
  aside .action-row:hover { background: var(--hover); }
  aside .action-row.danger { color: var(--danger-text); }
  aside .action-row.danger:hover { background: var(--danger); color: #fff; }
  aside .action-row.headline { color: var(--accent); }
  aside .action-row.disabled { opacity: 0.4; cursor: not-allowed; pointer-events: none; }
  /* Depth-3: extractor-sub summary (VENDOR EXTRACTOR, PRODUCT EXTRACTOR,
     BUILDING EXTRACTOR — nested inside the EXTRACTORS action-sub). */
  aside details.extractor-sub > summary {
    padding: 1px 8px 1px 24px; border-radius: 2px;
    color: var(--text); cursor: pointer;
    display: flex; align-items: center; gap: 5px;
  }
  aside details.extractor-sub > summary::-webkit-details-marker { display: none; }
  aside details.extractor-sub > summary::before {
    content: "▸"; font-size: 8px; transition: transform 0.1s;
    display: inline-block; width: 8px;
  }
  aside details.extractor-sub[open] > summary::before { transform: rotate(90deg); }
  aside details.extractor-sub > summary:hover { background: var(--hover); }
  /* Depth-4: action-rows inside an extractor-sub (RUN ALL, RUN FILE
     EXTRACTOR, …). Selector wins over the generic .action-row above
     because of the parent-class qualifier. */
  aside details.extractor-sub > .action-row {
    padding-left: 32px;
  }
  aside .action-row .desc {
    /* Description sub-line: slightly smaller still uppercase, normal weight,
       sits under the action label. */
    display: block; color: var(--muted);
    font-size: 9px; font-weight: 400; letter-spacing: 0.06em;
    line-height: 1.3; margin-top: 0;
  }
  aside .action-row.danger:hover .desc { color: rgba(255,255,255,0.85); }
  /* Inline checkbox label inside the Ingest sub-menu — same depth as
     its sibling action rows. */
  aside #use-ignores-wrap {
    display: flex; align-items: center; gap: 5px;
    padding: 3px 8px 3px 24px;
    color: var(--muted);
    cursor: pointer; user-select: none;
  }
  aside #use-ignores-wrap input { margin: 0; cursor: pointer; }
  aside #use-ignores-wrap:hover { color: var(--text); }
  /* Progress fill on the Extracted status line: linear gradient with a
     hard stop at --progress so the row reads as a 0-100% bar. */
  .extract-status.with-progress {
    background: linear-gradient(
      to right,
      var(--accent) 0%,
      var(--accent) var(--progress, 0%),
      var(--inset) var(--progress, 0%),
      var(--inset) 100%
    );
    color: var(--bg);
    border-color: var(--accent);
    text-shadow: 0 0 2px rgba(0,0,0,0.2);
  }
  /* Home dashboard — counts grid + per-type classification breakdown.
     Lives at the top of #help-view, above the help prose. */
  #dashboard { margin-bottom: 32px; }
  #dashboard h2.dash-h {
    color: var(--accent); margin: 0 0 14px 0; font-size: 22px;
  }
  #dashboard h3.dash-h {
    color: var(--accent); margin: 22px 0 10px 0; font-size: 17px;
  }
  #dashboard .dash-sub {
    color: var(--muted); font-size: 12px; margin: -6px 0 12px 0;
  }
  /* KPI cards — headline metrics at the top of the dashboard. Bigger than
     the record-counts row, with a subtitle line under each value. */
  .dash-kpis-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
    gap: 10px; margin-bottom: 18px;
  }
  .dash-kpis-grid .kpi-card {
    background: var(--inset); border: 1px solid var(--border);
    border-radius: 4px; padding: 12px 14px;
  }
  .dash-kpis-grid .kpi-card .label {
    color: var(--muted); font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .dash-kpis-grid .kpi-card .value {
    color: var(--accent); font-size: 28px; font-weight: 600; margin-top: 4px;
    font-variant-numeric: tabular-nums;
  }
  .dash-kpis-grid .kpi-card .sub {
    color: var(--muted); font-size: 11px; margin-top: 2px;
  }

  /* Two-column split: Ephemeral (data tables) | Canonical (curated/seed).
     Right column auto-sizes to the natural width of its cards so we don't
     get an awkward empty cell when Canonical has fewer cards than fit
     in a fixed 1fr slot; left column fills whatever's left. */
  #dash-counts-cols {
    display: grid; grid-template-columns: 1fr auto; gap: 18px; margin-bottom: 8px;
    align-items: start;
  }
  @media (max-width: 720px) {
    #dash-counts-cols { grid-template-columns: 1fr; }
  }
  .dash-counts-col-h {
    color: var(--muted); font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.04em; margin-bottom: 6px;
  }
  .dash-counts-grid {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 8px;
  }
  .dash-counts-grid .dash-card {
    background: var(--inset); border: 1px solid var(--border);
    border-radius: 4px; padding: 10px 12px;
  }
  .dash-counts-grid .dash-card .label {
    color: var(--muted); font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .dash-counts-grid .dash-card .value {
    color: var(--text); font-size: 22px; font-weight: 600; margin-top: 4px;
    font-variant-numeric: tabular-nums;
  }
  #dash-class-summary {
    background: var(--inset); border: 1px solid var(--border);
    border-radius: 4px; padding: 10px 12px; margin-bottom: 18px;
    font-size: 13px; color: var(--text);
  }
  #dash-class-summary strong { color: var(--accent); }
  table.dash-table {
    width: 100%; border-collapse: collapse; font-size: 13px;
    font-variant-numeric: tabular-nums;
  }
  table.dash-table th, table.dash-table td {
    border-bottom: 1px solid var(--border);
    padding: 6px 8px; text-align: right;
  }
  table.dash-table th:first-child, table.dash-table td:first-child {
    text-align: left;
  }
  table.dash-table th {
    color: var(--muted); font-weight: 500; font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.04em;
    background: var(--inset);
  }
  table.dash-table td.zero { color: var(--muted); }
  table.dash-table td.pct { color: var(--muted); font-size: 11px; }
  table.dash-table tr.empty-type td { color: var(--muted); opacity: 0.55; }

  /* Duplicate clusters report. One card per cluster: small header with the
     short hash + count, then a table of member docs. */
  .dup-cluster {
    border: 1px solid var(--border); border-radius: 6px;
    margin-bottom: 14px; background: var(--bg-card, #fff);
    overflow: hidden;
  }
  .dup-cluster-h {
    padding: 8px 12px; background: var(--inset);
    border-bottom: 1px solid var(--border);
    font-size: 12px; display: flex; align-items: center; gap: 10px;
    cursor: pointer; user-select: none;
    list-style: none;
  }
  /* Suppress the default <summary> triangle in WebKit/Firefox so our own
     caret is the only disclosure indicator. */
  .dup-cluster-h::-webkit-details-marker { display: none; }
  .dup-cluster-h::marker { content: ""; }
  .dup-cluster-h:hover { background: var(--bg-card-hover, #f4ecf5); }
  .dup-caret {
    display: inline-block; width: 10px; font-size: 10px;
    color: var(--muted); transition: transform 0.15s;
  }
  details.dup-cluster[open] > .dup-cluster-h .dup-caret { transform: rotate(90deg); }
  details.dup-cluster[open] > .dup-cluster-h { border-bottom: 1px solid var(--border); }
  /* Hide it if the section is open — the open state is the hint. */
  .dup-toggle-hint {
    margin-left: auto; font-size: 10px; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.04em;
  }
  details.dup-cluster[open] > .dup-cluster-h .dup-toggle-hint::before { content: "click to collapse"; }
  details.dup-cluster[open] > .dup-cluster-h .dup-toggle-hint { font-size: 0; }
  details.dup-cluster[open] > .dup-cluster-h .dup-toggle-hint::before { font-size: 10px; }
  .dup-cluster-h code {
    font-family: ui-monospace, Menlo, monospace; font-size: 12px;
    color: var(--accent); cursor: help;
  }
  .dup-count {
    color: var(--muted); font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  table.dup-table {
    width: 100%; border-collapse: collapse; font-size: 12px;
    font-variant-numeric: tabular-nums;
  }
  table.dup-table th, table.dup-table td {
    border-bottom: 1px solid var(--border);
    padding: 5px 12px; text-align: left; vertical-align: top;
  }
  table.dup-table th {
    color: var(--muted); font-weight: 500; font-size: 10px;
    text-transform: uppercase; letter-spacing: 0.04em;
  }
  table.dup-table tr:last-child td { border-bottom: none; }
  td.dup-path {
    font-family: ui-monospace, Menlo, monospace; font-size: 11px;
    word-break: break-all;
  }
  /* SHA-256 cell in the documents table: short prefix in mono. Full hash
     is on the cell's title attribute. cursor:help nudges the user to hover. */
  td.sha-cell {
    font-family: ui-monospace, Menlo, monospace; font-size: 11px;
    color: var(--muted); cursor: help;
  }
  /* Sort affordance on column headers — pointer + tiny arrow when active.
     Applies to every table that opts in. */
  .sort-label {
    cursor: pointer; user-select: none; display: inline-block;
    max-width: 100%;
    overflow: hidden; text-overflow: ellipsis;
  }
  .sort-label:hover { color: var(--accent); }
  th .sort-label { font: inherit; }

  /* Classifier rule editor table — wider, editable inputs in cells. */
  table.cedit-table {
    width: 100%; border-collapse: collapse; font-size: 12px;
  }
  table.cedit-table th {
    color: var(--muted); font-weight: 500; font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.04em;
    background: var(--inset);
    text-align: left; padding: 6px 6px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  table.cedit-table td {
    padding: 4px 4px;
    border-bottom: 1px solid var(--border-row);
    vertical-align: middle;
  }
  table.cedit-table input,
  table.cedit-table select {
    width: 100%; box-sizing: border-box;
    padding: 4px 6px; font-size: 12px;
    border: 1px solid var(--border); border-radius: 3px;
    background: var(--inset); color: var(--text);
    font-family: inherit;
  }
  table.cedit-table input:focus,
  table.cedit-table select:focus {
    outline: none; border-color: var(--accent);
  }
  /* Vendor-grouped layout for product rules. The group header is a
     full-width clickable row with caret + vendor name + rule count. */
  table.cedit-table tr.cedit-group-header td {
    cursor: pointer; user-select: none;
    background: var(--inset);
    border-top: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
    padding: 6px 8px;
  }
  table.cedit-table tr.cedit-group-header td:hover { background: var(--hover); }
  table.cedit-table tr.cedit-group-header .caret {
    color: var(--muted); font-size: 11px; margin-right: 8px;
    display: inline-block; width: 12px;
  }
  table.cedit-table tr.cedit-group-header .vendor-name {
    color: var(--accent); font-weight: 600; font-size: 13px;
  }
  table.cedit-table tr.cedit-group-header .rule-count {
    color: var(--muted); font-size: 11px; margin-left: 10px;
    text-transform: uppercase; letter-spacing: 0.04em;
  }
  table.cedit-table tr.cedit-group-body td:first-child { padding-left: 20px; }

  /* Coverage column. Tabular numbers, color-coded:
     - clean (winning == would-match)  → muted
     - shadowed (winning < would-match) → orange-ish accent, signals problem
     - zero matches (would-match == 0)  → faded, signals dead rule */
  table.cedit-table th.cedit-coverage-h {
    text-align: right; width: 90px; white-space: nowrap;
  }
  table.cedit-table td.cedit-coverage {
    text-align: right; font-variant-numeric: tabular-nums;
    font-size: 11px; padding-right: 8px; white-space: nowrap;
  }
  table.cedit-table td.cedit-coverage:hover { background: var(--hover); }
  table.cedit-table td.cedit-coverage.loading { color: var(--muted); opacity: 0.5; }
  table.cedit-table td.cedit-coverage span.clean    { color: var(--muted); }
  table.cedit-table td.cedit-coverage span.zero     { color: var(--muted); opacity: 0.4; }
  table.cedit-table td.cedit-coverage span.shadowed {
    color: #b07020; font-weight: 600;
  }

  table.cedit-table td.cedit-pattern { position: relative; }
  table.cedit-table td.cedit-pattern input {
    font-family: monospace; padding-right: 26px;
  }
  table.cedit-table td.cedit-pattern .rx-open {
    position: absolute; right: 6px; top: 50%; transform: translateY(-50%);
    width: 20px; height: 20px; padding: 0; line-height: 18px; font-size: 13px;
    border: 1px solid var(--border); border-radius: 3px;
    background: var(--inset); color: var(--muted); cursor: pointer;
  }
  table.cedit-table td.cedit-pattern .rx-open:hover {
    background: var(--hover); color: var(--text); border-color: var(--accent);
  }

  /* Regex editor modal — opens from the pattern cell. */
  .rx-modal { position: fixed; inset: 0; z-index: 1000; }
  .rx-modal-backdrop {
    position: absolute; inset: 0;
    background: rgba(0,0,0,0.35);
  }
  .rx-modal-panel {
    position: relative; max-width: 880px; width: calc(100% - 40px);
    margin: 5vh auto; max-height: 90vh; display: flex; flex-direction: column;
    background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
    box-shadow: 0 10px 40px rgba(0,0,0,0.25);
  }
  .rx-modal-header {
    display: flex; justify-content: space-between; align-items: center;
    padding: 10px 16px; border-bottom: 1px solid var(--border);
  }
  .rx-modal-header h3 { margin: 0; font-size: 15px; color: var(--accent); }
  .rx-modal-x {
    background: transparent; border: none; font-size: 22px; line-height: 1;
    color: var(--muted); cursor: pointer; padding: 0 6px;
  }
  .rx-modal-x:hover { color: var(--text); }
  .rx-modal-body {
    display: grid; grid-template-columns: 1fr 320px; gap: 18px;
    padding: 16px; overflow: auto;
  }
  @media (max-width: 720px) {
    .rx-modal-body { grid-template-columns: 1fr; }
  }
  .rx-label {
    display: block; font-size: 11px; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 4px;
  }
  #rx-pattern {
    width: 100%; box-sizing: border-box; resize: vertical;
    font-family: monospace; font-size: 13px;
    padding: 8px 10px; border: 1px solid var(--border); border-radius: 3px;
    background: var(--inset); color: var(--text);
  }
  #rx-pattern:focus, #rx-sample:focus { outline: none; border-color: var(--accent); }
  #rx-pattern.invalid { border-color: var(--danger); background: var(--danger-bg); }
  #rx-sample {
    width: 100%; box-sizing: border-box;
    font-family: monospace; font-size: 13px;
    padding: 6px 10px; border: 1px solid var(--border); border-radius: 3px;
    background: var(--inset); color: var(--text);
  }
  .rx-error {
    margin-top: 6px; padding: 6px 10px; font-size: 12px;
    background: var(--danger-bg); color: var(--danger-text);
    border: 1px solid var(--danger); border-radius: 3px; white-space: pre-wrap;
  }
  .rx-result {
    margin-top: 6px; font-size: 12px; padding: 6px 10px;
    border: 1px solid var(--border); border-radius: 3px;
    background: var(--inset); color: var(--muted); min-height: 22px;
  }
  .rx-result.match    { background: #e6f4e6; color: #1a5a1a; border-color: #4a9d4a; }
  .rx-result.nomatch  { background: var(--danger-bg); color: var(--danger-text); border-color: var(--danger); }
  .rx-result mark {
    background: #ffeb78; color: var(--text); padding: 0 1px; border-radius: 2px;
  }
  .rx-modal-hints {
    background: var(--inset); border: 1px solid var(--border);
    border-radius: 4px; padding: 10px 12px;
    font-size: 12px; max-height: 65vh; overflow: auto;
  }
  .rx-hints-h {
    color: var(--muted); font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.04em; margin-bottom: 6px;
  }
  dl.rx-hints { margin: 0; }
  dl.rx-hints dt {
    margin-top: 6px; font-family: monospace;
  }
  dl.rx-hints dt:first-child { margin-top: 0; }
  dl.rx-hints dd {
    margin: 1px 0 0 0; color: var(--muted); line-height: 1.4;
  }
  dl.rx-hints code, ul.rx-hints-list code {
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 2px; padding: 0 3px; font-family: monospace; font-size: 11px;
    color: var(--text);
  }
  ul.rx-hints-list {
    margin: 4px 0 0 18px; padding: 0; line-height: 1.5; color: var(--muted);
  }
  ul.rx-hints-list li { margin: 4px 0; }
  .rx-modal-footer {
    display: flex; justify-content: flex-end; gap: 8px;
    padding: 10px 16px; border-top: 1px solid var(--border);
  }
  .rx-modal-footer button { padding: 5px 14px; }
  table.cedit-table tr.invalid td { background: var(--danger-bg); }
  table.cedit-table td.cedit-actions {
    text-align: right; white-space: nowrap; width: 96px;
  }
  table.cedit-table td.cedit-actions button {
    padding: 2px 6px; font-size: 11px; margin-left: 2px;
    background: var(--inset); border: 1px solid var(--border);
    border-radius: 3px; cursor: pointer; color: var(--text);
  }
  table.cedit-table td.cedit-actions button:hover { background: var(--hover); }
  table.cedit-table td.cedit-actions button.danger { color: var(--danger-text); }
  table.cedit-table td.cedit-actions button.danger:hover { background: var(--danger); color: #fff; }
  table.cedit-table td.cedit-actions button:disabled { opacity: 0.3; cursor: not-allowed; }

  /* Home/Dashboard view styling. The original max-width was for prose;
     we moved help to its own page (/help.html), so #help-view is now
     dashboard-only and should fill the full available width — otherwise
     the scrollbar sits mid-page with empty gutter to its right. */
  /* #help-view max-width removed intentionally. */
  #help-view .help-h {
    color: var(--accent); margin: 24px 0 8px 0; font-size: 17px;
  }
  #help-view .help-h:first-child { margin-top: 0; font-size: 22px; }
  #help-view .help-p {
    line-height: 1.55; margin: 0 0 12px 0; font-size: 14px;
  }
  #help-view .help-ol, #help-view .help-ul {
    line-height: 1.55; margin: 0 0 14px 20px; font-size: 14px;
  }
  #help-view .help-ol li, #help-view .help-ul li { margin: 4px 0; }
  #help-view code {
    background: var(--inset); border: 1px solid var(--border);
    border-radius: 3px; padding: 1px 5px; font-size: 12px;
  }
</style>
</head>
<body>
<header>
  <h1 id="brand" title="Go to the All-documents dashboard" style="cursor: pointer;">EnLogosGRAG</h1>
  <span id="sdt-control" title="UI filter: which source-data-type's rules and editor view are active. Every file is always classified by its own type's rules; this is just what you SEE.">
    <label class="meta" for="sdt-switcher" style="margin-right:6px;">Source Data Type Filter</label>
    <select id="sdt-switcher">
      <option value="Vendor">Vendor</option>
      <option value="JobFiles">JobFiles</option>
      <option value="Sales">Sales</option>
      <option value="Any">Any</option>
      <option value="All">No filter (show all)</option>
    </select>
  </span>
  <span class="spacer"></span>
  <a class="help-link" href="/help" target="_blank" rel="noopener">Help</a>
</header>
<main>
  <aside id="sidebar">
    <div id="sidebar-resizer" title="Drag to resize sidebar"></div>
    <button id="sidebar-toggle" type="button" title="Collapse sidebar">‹</button>
    <div id="sidebar-nav">
    <details>
      <summary>Inputs</summary>

      <details>
        <summary class="sub-summary">Source</summary>
        <div id="sdt-sidebar-banner" class="sdt-pill" title="Active source-data-type — change in the header"></div>
        <div id="listing-display">
          <div id="listing-display-empty" class="listing-empty">No folder chosen.</div>
          <div id="listing-display-set" class="listing-set" style="display:none;">
            <div class="listing-name" id="listing-name"></div>
            <div class="listing-path" id="listing-path"></div>
          </div>
        </div>
        <button id="pick-listing" class="secondary">Choose folder…</button>
        <button id="rescan-listing" class="secondary">Rescan listing (regenerate basic_listing.txt)</button>
      </details>

      <details>
        <summary class="sub-summary">Rules</summary>

        <details class="chart-folder">
          <summary class="chart-folder-summary">Type Classifiers</summary>
          <div id="classifier-row-content" class="table-row" data-classifier="content">
            <span>Content rules</span>
          </div>
          <div id="classifier-row-filename" class="table-row" data-classifier="filename">
            <span>Filename rules</span>
          </div>
        </details>

        <details class="chart-folder">
          <summary class="chart-folder-summary">Vendor</summary>
          <div id="classifier-row-vendor_file" class="table-row" data-classifier="vendor_file">
            <span>Vendor file extractor</span>
          </div>
          <div id="classifier-row-vendor_content" class="table-row" data-classifier="vendor_content">
            <span>Vendor content extractor</span>
          </div>
        </details>
        <details class="chart-folder">
          <summary class="chart-folder-summary">Product</summary>
          <div id="classifier-row-product_file" class="table-row" data-classifier="product_file">
            <span>Product file extractor</span>
          </div>
          <div id="classifier-row-product_content" class="table-row" data-classifier="product_content">
            <span>Product content extractor</span>
          </div>
        </details>
        <details class="chart-folder">
          <summary class="chart-folder-summary">Building</summary>
          <div id="classifier-row-building_file" class="table-row" data-classifier="building_file">
            <span>Building file extractor</span>
          </div>
          <div id="classifier-row-building_content" class="table-row" data-classifier="building_content">
            <span>Building content extractor</span>
          </div>
        </details>
      </details>

      <details>
        <summary class="sub-summary">Canonical</summary>
        <div class="table-list">
          <div id="building-addresses-row" class="table-row" data-table="building_addresses">
            <span>Building addresses</span><span class="count" id="building-addresses-count">0</span>
          </div>
          <div id="canonical-buildings-row" class="table-row" data-table="canonical_buildings">
            <span>Canonical buildings</span><span class="count" id="canonical-buildings-count">0</span>
          </div>
          <div id="doctypes-row" class="table-row" data-table="document_types">
            <span>Document types</span><span class="count" id="doctypes-count">0</span>
          </div>
        </div>
      </details>
    </details>

    <details>
      <summary>Actions</summary>
      <details class="action-sub">
        <summary>Purge</summary>
        <div class="action-row danger" data-action="purge-all">
          Purge ephemeral tables
          <span class="desc">Clears 8 tables: vendors, files, documents, document_extracts, products, document_products, buildings, document_buildings. Canonical tables (canonical_buildings, building_addresses, ignored_*) preserved.</span>
        </div>
        <div class="action-row danger" data-action="purge-hashes">
          Purge hashes
          <span class="desc">Clear documents.sha256 (sidecars on disk preserved)</span>
        </div>
        <div class="action-row danger" data-action="purge-text-backups">
          Purge text backups
          <span class="desc">Delete the &lt;Vendors&gt;_text filesystem cache</span>
        </div>
      </details>
      <details class="action-sub">
        <summary>Full Process</summary>
        <div class="action-row headline" data-action="run-full-process">
          Run Full Process
          <!-- Description text is rewritten by applyActiveSdtToUi() so the
               step list reflects which entity extractors will actually
               fire for the currently-selected SDT. -->
          <span class="desc" id="full-process-desc"></span>
        </div>
      </details>
      <details class="action-sub">
        <summary>Ingest</summary>
        <label id="use-ignores-wrap" title="Apply the ignored types and ignored folders lists during file ingest">
          <input type="checkbox" id="use-ignores" checked>
          <span>Apply ignore lists</span>
        </label>
        <div class="action-row" data-action="run-files">Ingest Files</div>
        <div class="action-row" data-action="hash-start">
          Hash files (sha256)
          <span class="desc">Background; non-ignored files only; only-missing</span>
        </div>
        <div class="action-row" data-action="find-duplicates">
          Find duplicates
          <span class="desc">Reports docs sharing a sha256</span>
        </div>
        <div class="action-row" data-action="extract-start">
          Convert to text
          <span class="desc">PDF + Word (.docx) → plain text; background; uses filesystem cache when present</span>
        </div>
      </details>
      <details class="action-sub">
        <summary>Classify</summary>
        <div class="action-row" data-action="classify-all">
          Classify all files
          <span class="desc">Filename / path rules</span>
        </div>
        <div class="action-row" data-action="classify-by-content">
          Classify by extract content
          <span class="desc">PDF-text rules; only fills empty / upgrades low</span>
        </div>
      </details>
      <details class="action-sub">
        <summary>Extract</summary>
        <details class="extractor-sub">
          <summary>Vendor extractor</summary>
          <div class="action-row" data-action="extract-vendors-all">
            Run All
            <span class="desc">Vendor SDT only; both filename/path and content rules</span>
          </div>
          <div class="action-row" data-action="extract-vendors-file">
            Run File Extractor
            <span class="desc">Only filename / path rules (match: name | path)</span>
          </div>
          <div class="action-row" data-action="extract-vendors-content">
            Run Content Extractor
            <span class="desc">Only content rules (match: first_page | extract); needs extracted text</span>
          </div>
        </details>
        <details class="extractor-sub">
          <summary>Product extractor</summary>
          <div class="action-row" data-action="extract-products-all">
            Run All
            <span class="desc">Vendor-scoped rules; both filename/path and content signals</span>
          </div>
          <div class="action-row" data-action="extract-products-file">
            Run File Extractor
            <span class="desc">Only filename / path rules (match: name | path)</span>
          </div>
          <div class="action-row" data-action="extract-products-content">
            Run Content Extractor
            <span class="desc">Only content rules (match: first_page | extract); needs extracted text</span>
          </div>
        </details>
        <details class="extractor-sub">
          <summary>Building extractor</summary>
          <div class="action-row" data-action="buildings-snapshot">
            Snapshot from Snowflake
            <span class="desc">Refresh canonical_buildings + building_addresses</span>
          </div>
          <div class="action-row" data-action="extract-buildings-all">
            Run All
            <span class="desc">JobFiles only; reads filename + extract text</span>
          </div>
          <div class="action-row" data-action="extract-buildings-file">
            Run File Extractor
            <span class="desc">Filename / path tokens only</span>
          </div>
          <div class="action-row" data-action="extract-buildings-content">
            Run Content Extractor
            <span class="desc">Extract-text tokens only; needs extracted text</span>
          </div>
          <div id="action-row-buildings-match-stop" class="action-row disabled" data-action="buildings-match-stop" title="No building match running">
            Stop matching
            <span class="desc">Signals the worker to stop after current file</span>
          </div>
        </details>
      </details>
    </details>

    <details>
      <summary>Results</summary>

      <details>
        <summary class="sub-summary">Dashboards</summary>
        <div id="help-row" class="table-row" data-view="help" data-sdt="">
          <span>All documents</span>
        </div>
        <div id="dashboard-row-jobfiles" class="table-row" data-view="help" data-sdt="JobFiles">
          <span>JobFiles</span>
        </div>
        <div id="dashboard-row-vendor" class="table-row" data-view="help" data-sdt="Vendor">
          <span>Vendor</span>
        </div>
        <div id="dashboard-row-sales" class="table-row" data-view="help" data-sdt="Sales">
          <span>Sales</span>
        </div>
      </details>

      <details>
        <summary class="sub-summary">Charts</summary>

        <details class="chart-folder">
          <summary class="chart-folder-summary">Classification</summary>
          <div id="charts-row" class="table-row" data-view="charts">
            <span>All documents</span>
          </div>
          <div class="chart-preset" data-preset="classified">
            <span>Classified only</span>
          </div>
          <div class="chart-preset" data-preset="with-products">
            <span>Documents with products</span>
          </div>
          <div class="chart-preset" data-preset="without-products">
            <span>Documents without products</span>
          </div>
          <div class="chart-preset" data-preset="unclassified">
            <span>Unclassified only</span>
          </div>
        </details>

        <details class="chart-folder">
          <summary class="chart-folder-summary">Filetype</summary>
          <div class="chart-preset" data-preset="ft-all">
            <span>All filetypes</span>
          </div>
          <div class="chart-preset" data-preset="drawings">
            <span>Drawings (.dwg / .dxf)</span>
          </div>
          <div class="chart-preset" data-preset="images">
            <span>Images (.jpg / .jpeg / .png / .heic)</span>
          </div>
          <div class="chart-preset" data-preset="office">
            <span>Office docs (.doc / .docx / .xls / .xlsx)</span>
          </div>
          <div class="chart-preset" data-preset="pdfs">
            <span>PDFs</span>
          </div>
        </details>

        <details class="chart-folder">
          <summary class="chart-folder-summary">Source Data Type</summary>
          <div class="chart-preset" data-preset="sdt-all">
            <span>All SDTs</span>
          </div>
          <div class="chart-preset" data-preset="sdt-jobfiles">
            <span>JobFiles</span>
          </div>
          <div class="chart-preset" data-preset="sdt-vendor">
            <span>Vendor</span>
          </div>
          <div class="chart-preset" data-preset="sdt-sales">
            <span>Sales</span>
          </div>
        </details>
      </details>

      <details>
        <summary class="sub-summary">Reports</summary>
        <div id="report-row-classification-all" class="table-row" data-view="report-classification" data-sdt="">
          <span>Classification — all documents</span>
        </div>
        <div id="report-row-classification-vendor" class="table-row" data-view="report-classification" data-sdt="Vendor">
          <span>Classification — Vendor</span>
        </div>
        <div id="report-row-classification-jobfiles" class="table-row" data-view="report-classification" data-sdt="JobFiles">
          <span>Classification — JobFiles</span>
        </div>
        <div id="report-row-classification-sales" class="table-row" data-view="report-classification" data-sdt="Sales">
          <span>Classification — Sales</span>
        </div>
        <div id="duplicates-row" class="table-row" data-view="duplicates">
          <span>Find duplicates</span>
        </div>
      </details>

      <details>
        <summary class="sub-summary">Ignored</summary>
        <div class="table-list">
          <div id="ignored-buildings-row" class="table-row" data-table="ignored_buildings">
            <span>Ignored buildings</span><span class="count" id="ignored-buildings-count">0</span>
          </div>
          <div id="ignored-files-row" class="table-row" data-view="ignored-files">
            <span>Ignored files</span><span class="count" id="ignored-files-count">0</span>
          </div>
          <div id="ignored-folders-row" class="table-row" data-view="ignored-folders">
            <span>Ignored folders</span><span class="count" id="ignored-folders-count">0</span>
          </div>
          <div id="ignored-row" class="table-row" data-view="ignored">
            <span>Ignored types</span><span class="count" id="ignored-types-count">0</span>
          </div>
        </div>
      </details>

      <details>
        <summary class="sub-summary">Ephemeral</summary>
        <div id="table-list"></div>
      </details>
    </details>

    </div>
    <div id="sidebar-status">
      <div id="sidebar-status-header">Status</div>
      <div id="sdt-status-banner" class="sdt-pill" title="Active source-data-type — change in the header"></div>
      <button id="refresh" class="secondary">Refresh status</button>
      <div id="classify-status" class="extract-status">Classified: …</div>
      <!-- Single dynamic worker bar. The label rotates by which worker
           is currently running (extract / hash / buildings); when idle,
           shows the most-recent corpus snapshot. The Stop button binds
           to the active worker via worker-stop.dataset.worker. -->
      <div id="worker-progress" class="extract-status">Idle</div>
      <button id="worker-stop" class="secondary" style="display:none; margin-top: 4px;" data-worker="">Stop</button>
    </div>
  </aside>
  <section class="content">
    <div class="toolbar">
      <input type="text" id="filter" placeholder="Filter rows… (matches name/path)">
      <div id="filetype-filter-wrap" class="multi-dropdown ext-only">
        <button type="button" id="filetype-filter-btn" class="multi-btn" title="Limit to selected file extensions (multi-select)">
          <span id="filetype-filter-label">Any file type</span> <span class="caret">▾</span>
        </button>
        <div id="filetype-filter-panel" class="multi-panel" style="display:none;"></div>
      </div>
      <div id="doctype-filter-wrap" class="multi-dropdown docs-only">
        <button type="button" id="doctype-filter-btn" class="multi-btn" title="Limit to selected document types (multi-select)">
          <span id="doctype-filter-label">Any document type</span> <span class="caret">▾</span>
        </button>
        <div id="doctype-filter-panel" class="multi-panel" style="display:none;"></div>
      </div>
      <select id="classified-filter" class="docs-only" title="Show only classified or unclassified documents">
        <option value="all">All documents</option>
        <option value="classified">Classified only</option>
        <option value="unclassified">Unclassified only</option>
      </select>
      <select id="sdt-filter" class="ext-only" title="Filter by Source Data Type (Vendor / JobFiles / Sales / unstamped). 'All' shows every SDT.">
        <option value="">All SDTs</option>
        <option value="Vendor">Vendor</option>
        <option value="JobFiles">JobFiles</option>
        <option value="Sales">Sales</option>
        <option value="Any">Any</option>
        <option value="(none)">(unstamped)</option>
      </select>
      <div id="columns-wrap" class="multi-dropdown docs-only" style="position: relative;">
        <button type="button" id="columns-btn" class="multi-btn" title="Show or hide columns in this table">
          <span id="columns-label">Columns</span> <span class="caret">▾</span>
        </button>
        <div id="columns-panel" class="multi-panel" style="display:none;"></div>
      </div>
      <select id="ignored-filter" class="files-only" title="Files in scope (active = has a documents row) vs ignored (extension on the ignore list at last ingest)">
        <option value="all">All files</option>
        <option value="active">Active only</option>
        <option value="ignored">Ignored only</option>
      </select>
      <button id="prev" disabled>&larr; Prev</button>
      <button id="next" disabled>Next &rarr;</button>
      <span class="meta" id="page-meta"></span>
    </div>
    <div id="table-view" class="table-wrap">
      <table id="data-table"><thead></thead><tbody></tbody></table>
    </div>
    <div id="help-view" style="display:none; flex:1; overflow:auto; padding:24px 32px;">
      <div id="dashboard">
        <h2 class="dash-h" id="dash-title">Dashboard</h2>
        <p class="dash-sub" id="dash-empty" style="display:none;">No data yet. Run an ingest to populate the database.</p>

        <h3 class="dash-h">KPIs</h3>
        <div id="dash-kpis" class="dash-kpis-grid"></div>

        <h3 class="dash-h">Record counts</h3>
        <div id="dash-counts-cols">
          <div class="dash-counts-col">
            <div class="dash-counts-col-h">Ephemeral tables</div>
            <div id="dash-counts-ephemeral" class="dash-counts-grid"></div>
          </div>
          <div class="dash-counts-col">
            <div class="dash-counts-col-h">Canonical tables</div>
            <div id="dash-counts-canonical" class="dash-counts-grid"></div>
          </div>
        </div>

        <h3 class="dash-h">Classification by document type</h3>
        <div id="dash-class-summary">…</div>
        <p class="dash-sub">% of classified shows each type's share of the classified corpus. High/Medium/Low show the confidence mix within that type (each row sums to 100%). Unclassified documents have no type and don't appear here — see the summary above for the corpus-wide unclassified count.</p>
        <table class="dash-table" id="dash-types-table">
          <thead>
            <tr>
              <th>Document type</th>
              <th>Count</th>
              <th>% of classified</th>
              <th>High</th>
              <th>Medium</th>
              <th>Low</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>

        <h3 class="dash-h">Files in scope</h3>
        <p class="dash-sub" id="dash-files-summary">…</p>
        <div id="dash-files-chart-wrap" style="max-width: 320px;">
          <canvas id="dash-files-chart"></canvas>
        </div>
      </div>
    </div>
    <div id="duplicates-view" style="display:none; flex:1; overflow:auto; padding:24px 32px;">
      <h2 class="dash-h" style="color: var(--accent); margin: 0 0 8px 0; font-size: 22px;">Duplicate documents</h2>
      <p class="dash-sub" id="duplicates-summary" style="color: var(--muted); font-size: 12px; margin: 0 0 16px 0;">
        Loading…
      </p>
      <div id="duplicates-body"></div>
    </div>
    <!-- Classification report — the bottom half of the home dashboard
         pulled out into a focused view. Three entries in the sidebar
         (all / Vendor / JobFiles) feed the same template; SDT scope is
         held on state.reportSdt and used for the /api/dashboard fetch. -->
    <div id="report-classification-view" style="display:none; flex:1; overflow:auto; padding:24px 32px;">
      <h2 class="dash-h" id="report-classification-title" style="color: var(--accent); margin: 0 0 8px 0; font-size: 22px;">Classification by document type</h2>
      <div id="report-class-summary" style="color: var(--text); margin: 0 0 8px 0;">…</div>
      <p class="dash-sub">% of classified shows each type's share of the classified corpus. High/Medium/Low show the confidence mix within that type (each row sums to 100%). Unclassified documents have no type and don't appear here.</p>
      <table class="dash-table" id="report-class-types-table">
        <thead>
          <tr>
            <th>Document type</th>
            <th>Count</th>
            <th>% of classified</th>
            <th>High</th>
            <th>Medium</th>
            <th>Low</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
    <div id="ignored-view" style="display:none; flex:1; overflow:auto; padding:24px 32px; max-width: 760px;">
      <h2 class="dash-h" style="color: var(--accent); margin: 0 0 8px 0; font-size: 22px;">Ignored file types</h2>
      <p class="dash-sub" style="color: var(--muted); font-size: 12px; margin: 0 0 16px 0;">
        Extensions on this list are ingested into the <code>files</code> table but skipped for classification, charts and the dashboard.
        Adding here only affects future ingests — existing rows for an extension keep their classifications until you re-run files ingest.
      </p>

      <div style="display:flex; gap:8px; align-items:center; margin-bottom: 18px;">
        <input id="ignored-input" type="text" placeholder=".ext (e.g. .tmp)"
               style="flex:0 0 180px; padding:5px 8px; font-size:13px; border:1px solid var(--border); border-radius:3px; background: var(--inset);">
        <input id="ignored-notes" type="text" placeholder="optional notes"
               style="flex:1; padding:5px 8px; font-size:13px; border:1px solid var(--border); border-radius:3px; background: var(--inset);">
        <button id="ignored-add" class="secondary" style="padding: 5px 12px;">Add</button>
      </div>

      <table class="dash-table" id="ignored-table">
        <thead>
          <tr>
            <th>Extension</th>
            <th>Files matching (current)</th>
            <th>Notes</th>
            <th>Added</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr><td colspan="5" style="color: var(--muted); text-align:center; padding: 18px;">No ignored types yet.</td></tr>
        </tbody>
      </table>
    </div>
    <div id="ignored-folders-view" style="display:none; flex:1; overflow:auto; padding:24px 32px; max-width: 760px;">
      <h2 class="dash-h" style="color: var(--accent); margin: 0 0 8px 0; font-size: 22px;">Ignored folders</h2>
      <p class="dash-sub" style="color: var(--muted); font-size: 12px; margin: 0 0 16px 0;">
        Folder names listed here are dropped at ingest time — the folder itself <strong>and every descendant</strong> are excluded from the <code>files</code> table entirely. Match is exact and case-insensitive against any segment of the path.
        Going-forward only — adding here doesn't delete existing rows. Re-run files ingest to apply.
      </p>

      <div style="display:flex; gap:8px; align-items:center; margin-bottom: 18px;">
        <input id="ignored-folders-input" type="text" placeholder="folder name (e.g. _archive)"
               style="flex:0 0 220px; padding:5px 8px; font-size:13px; border:1px solid var(--border); border-radius:3px; background: var(--inset);">
        <input id="ignored-folders-notes" type="text" placeholder="optional notes"
               style="flex:1; padding:5px 8px; font-size:13px; border:1px solid var(--border); border-radius:3px; background: var(--inset);">
        <button id="ignored-folders-add" class="secondary" style="padding: 5px 12px;">Add</button>
      </div>

      <table class="dash-table" id="ignored-folders-table">
        <thead>
          <tr>
            <th>Folder name</th>
            <th>Rows under (current)</th>
            <th>Notes</th>
            <th>Added</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr><td colspan="5" style="color: var(--muted); text-align:center; padding: 18px;">No ignored folders yet.</td></tr>
        </tbody>
      </table>
    </div>
    <div id="ignored-files-view" style="display:none; flex:1; overflow:auto; padding:24px 32px;">
      <h2 class="dash-h" style="color: var(--accent); margin: 0 0 8px 0; font-size: 22px;">Ignored files</h2>
      <p class="dash-sub" style="color: var(--muted); font-size: 12px; margin: 0 0 16px 0;">
        Specific file paths excluded from the document set. Adding a file here removes its <code>documents</code> row immediately (cascading to extracts, products, etc.) so dashboards and reports update right away. The corresponding <code>files</code> row is left in place — re-ingest will skip the path next time.
        Notes show <em>why</em>: <code>de-duplicated</code> for cluster cleanups, <code>manual</code> for one-offs.
      </p>

      <div style="display:flex; gap:8px; align-items:center; margin-bottom: 18px;">
        <input id="ignored-files-input" type="text" placeholder="full path (e.g. S:\vendors\…\file.pdf)"
               style="flex:1 1 460px; padding:5px 8px; font-size:13px; border:1px solid var(--border); border-radius:3px; background: var(--inset); font-family: ui-monospace, Menlo, monospace;">
        <input id="ignored-files-notes" type="text" placeholder="notes (e.g. manual)" value="manual"
               style="flex:0 0 160px; padding:5px 8px; font-size:13px; border:1px solid var(--border); border-radius:3px; background: var(--inset);">
        <button id="ignored-files-add" class="secondary" style="padding: 5px 12px;">Add</button>
      </div>

      <table class="dash-table" id="ignored-files-table">
        <thead>
          <tr>
            <th>Path</th>
            <th>Vendor</th>
            <th>Notes</th>
            <th>Added</th>
            <th>Status</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr><td colspan="6" style="color: var(--muted); text-align:center; padding: 18px;">No ignored files yet.</td></tr>
        </tbody>
      </table>
    </div>
    <div id="classifier-editor-view" style="display:none; flex:1; overflow:auto; padding:24px 32px;">
      <div style="display:flex; align-items:baseline; justify-content:space-between; gap:16px; margin-bottom: 8px;">
        <h2 id="cedit-title" style="color: var(--accent); margin: 0; font-size: 22px;">Filename rules</h2>
        <div style="font-size:12px; color:var(--muted);" id="cedit-meta">…</div>
      </div>
      <p class="dash-sub" id="cedit-desc" style="color: var(--muted); font-size: 12px; margin: 0 0 14px 0;">…</p>

      <div id="cedit-sdt-reminder" class="sdt-pill" style="display:none; margin: 0 0 12px 0; text-align: left; padding: 6px 10px;"></div>

      <div id="cedit-error" style="display:none; background: var(--danger-bg); color: var(--danger-text); border: 1px solid var(--danger); border-radius: 4px; padding: 8px 12px; margin-bottom: 12px; font-size: 13px; white-space: pre-wrap;"></div>

      <div style="display:flex; gap:8px; align-items:center; margin-bottom: 14px;">
        <button id="cedit-save"    class="secondary" style="padding: 5px 14px;" disabled>Save changes</button>
        <button id="cedit-discard" class="secondary" style="padding: 5px 14px;" disabled>Discard</button>
        <span id="cedit-dirty" style="color: var(--muted); font-size: 12px;"></span>
        <span id="cedit-group-controls" style="margin-left:auto; display:none;">
          <button id="cedit-expand-all"   class="secondary" style="padding: 5px 10px; font-size: 12px;" type="button">Expand all</button>
          <button id="cedit-collapse-all" class="secondary" style="padding: 5px 10px; font-size: 12px;" type="button">Collapse all</button>
        </span>
      </div>

      <table class="cedit-table" id="cedit-table">
        <thead><tr id="cedit-thead-row"></tr></thead>
        <tbody></tbody>
      </table>

      <button id="cedit-add" class="secondary" style="margin-top: 12px; padding: 5px 14px;">+ Add rule</button>
    </div>
    <div id="building-extractor-view" style="display:none; flex:1; overflow:auto; padding:24px 32px;">
      <div style="display:flex; align-items:baseline; justify-content:space-between; gap:16px; margin-bottom: 8px;">
        <h2 style="color: var(--accent); margin: 0; font-size: 22px;">Building Extractor</h2>
        <div style="font-size:12px; color:var(--muted);">stub · not yet wired</div>
      </div>
      <p class="dash-sub" style="color: var(--muted); font-size: 12px; margin: 0 0 14px 0;">
        Entity extraction rules that match buildings in document filenames and content. Buildings are matched against the canonical Buildings table, which is sourced from Snowflake. The extractor is currently driven by the snapshot + match jobs in the Actions panel; rule-driven extraction will live here.
      </p>
      <div style="background: var(--inset); border: 1px solid var(--border); border-radius: 2px; padding: 14px;">
        <div style="font-size: 12px; color: var(--text); margin-bottom: 6px; font-weight: 500;">Today this is run from Actions → Buildings:</div>
        <ul style="margin: 0 0 0 18px; padding: 0; font-size: 12px; color: var(--muted); line-height: 1.7;">
          <li>Snapshot from Snowflake — refresh local copy of BUILDING_CANONICAL + xref</li>
          <li>Match documents → buildings — JobFiles only; uses NAMES_SAMPLE + raw addresses</li>
        </ul>
      </div>
    </div>
    <div id="chart-view" style="display:none; flex:1; overflow:auto; padding:14px;">
      <div id="chart-breadcrumb" style="display:flex; align-items:center; gap:10px; flex-wrap:wrap; margin-bottom:10px;">
        <span id="chart-crumb" style="font-size:13px; color:var(--text);">All documents</span>
        <button id="chart-clear" class="secondary" style="display:none; padding:3px 9px; font-size:11px;">× Clear</button>
        <button id="chart-view-docs" class="secondary" style="display:none; padding:3px 9px; font-size:11px;">View these documents →</button>
      </div>
      <div id="chart-meta" style="color:var(--muted); font-size:12px; margin-bottom:14px;"></div>
      <div id="chart-hint" style="color:var(--muted); font-size:11px; margin-bottom:14px;">
        Click a slice to drill in. Filters compose. Use "View these documents" to escape to the table.
      </div>
      <div style="display:flex; gap:30px; flex-wrap:wrap;">
        <div class="chart-card" id="card-classified">
          <h3>Classified vs. unclassified</h3>
          <div class="chart-box"><canvas id="chart-classified"></canvas></div>
        </div>
        <div class="chart-card" id="card-sdt">
          <h3>By Source Data Type</h3>
          <div class="chart-box"><canvas id="chart-sdt"></canvas></div>
        </div>
        <div class="chart-card" id="card-confidence">
          <h3>By confidence</h3>
          <div class="chart-box"><canvas id="chart-confidence"></canvas></div>
        </div>
        <div class="chart-card" id="card-filetype">
          <h3>Top ten filetypes</h3>
          <div class="chart-box"><canvas id="chart-filetype"></canvas></div>
        </div>
        <div class="chart-card" id="card-filetype-other">
          <h3>Other filetypes</h3>
          <div class="chart-box"><canvas id="chart-filetype-other"></canvas></div>
        </div>
        <div class="chart-card" id="card-doctype">
          <h3>By document type (top 10)</h3>
          <div class="chart-box"><canvas id="chart-doctype"></canvas></div>
        </div>
        <div class="chart-card" id="card-doctype-other">
          <h3>Other document types</h3>
          <div class="chart-box"><canvas id="chart-doctype-other"></canvas></div>
        </div>
        <div class="chart-card" id="card-product-coverage">
          <h3>Documents with products</h3>
          <div class="chart-box"><canvas id="chart-product-coverage"></canvas></div>
        </div>
        <div class="chart-card" id="card-products-top">
          <h3>By product (top 20)</h3>
          <div class="chart-box"><canvas id="chart-products-top"></canvas></div>
        </div>
        <div class="chart-card" id="card-products-vendor">
          <h3>Products discovered per vendor</h3>
          <div class="chart-box"><canvas id="chart-products-vendor"></canvas></div>
        </div>
        <div class="chart-card" id="card-building-coverage">
          <h3>Documents with buildings</h3>
          <div class="chart-box"><canvas id="chart-building-coverage"></canvas></div>
        </div>
        <div class="chart-card" id="card-buildings-top">
          <h3>By building (top 20)</h3>
          <div class="chart-box"><canvas id="chart-buildings-top"></canvas></div>
        </div>
      </div>
    </div>
    <div id="log-wrap">
      <div id="log-header" title="Click to collapse/expand">
        <span>Log</span>
        <span class="caret">▾</span>
      </div>
      <div id="log"></div>
    </div>
  </section>
</main>

<div id="regex-modal" class="rx-modal" style="display:none;">
  <div class="rx-modal-backdrop"></div>
  <div class="rx-modal-panel" role="dialog" aria-modal="true" aria-labelledby="rx-modal-title">
    <div class="rx-modal-header">
      <h3 id="rx-modal-title">Edit pattern</h3>
      <button id="rx-modal-close" class="rx-modal-x" type="button" aria-label="Close">×</button>
    </div>
    <div class="rx-modal-body">
      <div class="rx-modal-col">
        <label class="rx-label" for="rx-pattern">Pattern (JS regex, case-insensitive)</label>
        <textarea id="rx-pattern" rows="3" spellcheck="false"></textarea>
        <div id="rx-error" class="rx-error" style="display:none;"></div>

        <label class="rx-label" for="rx-sample" style="margin-top:14px;">Test against sample</label>
        <input id="rx-sample" type="text" placeholder="e.g. 5499_Datasheet.pdf" spellcheck="false">
        <div id="rx-result" class="rx-result"></div>
      </div>
      <div class="rx-modal-hints">
        <div class="rx-hints-h">Cheat sheet</div>
        <dl class="rx-hints">
          <dt><code>\b</code></dt>
          <dd>Word boundary. Won't fire next to <code>_</code> (underscore is a word char).</dd>
          <dt><code>(?:^|[\W_])</code></dt>
          <dd>Start-of-string OR non-word OR <code>_</code>. Use as a left boundary that respects underscores.</dd>
          <dt><code>(?:[\W_]|$)</code></dt>
          <dd>Mirror right boundary.</dd>
          <dt><code>[\s_-]*</code></dt>
          <dd>Zero or more space / underscore / hyphen — separator-tolerant.</dd>
          <dt><code>(?:foo|bar)</code></dt>
          <dd>Non-capturing alternation.</dd>
          <dt><code>[A-Z0-9]+</code></dt>
          <dd>Char class (case-insensitive flag is always on).</dd>
          <dt><code>\d{2,5}</code></dt>
          <dd>2 to 5 digits.</dd>
          <dt><code>?</code></dt>
          <dd>Optional preceding token. <code>foo[\s_-]?bar</code> matches <code>foo bar</code>, <code>foo_bar</code>, or <code>foobar</code>.</dd>
          <dt><code>^</code> / <code>$</code></dt>
          <dd>Anchors. <code>^4850\b</code> requires the string to start with 4850.</dd>
          <dt><code>\.(pdf|dwg)</code></dt>
          <dd>Literal dot, then alternation.</dd>
        </dl>
        <div class="rx-hints-h" style="margin-top:14px;">Common patterns</div>
        <ul class="rx-hints-list">
          <li>Model code allowing <code>-</code> / <code>_</code> / space:<br><code>\bABC[\s_-]?123\b</code></li>
          <li>Match in filename even after underscore prefix:<br><code>(?:^|[\W_])datasheet(?:[\W_]|$)</code></li>
          <li>Vendor folder anywhere in path:<br><code>\\Vendors\\</code></li>
        </ul>
      </div>
    </div>
    <div class="rx-modal-footer">
      <button id="rx-modal-cancel" class="secondary" type="button">Cancel</button>
      <button id="rx-modal-save" class="secondary" type="button">Save</button>
    </div>
  </div>
</div>

<script>
const state = {
  view: "table",         // "table" | "charts"
  table: "vendors",
  offset: 0,
  listingPath: "",        // absolute path to a listing .txt file on disk
  // Active source-data-type. UI-level filter only — every file is ALWAYS
  // classified using rules matching its own source_data_type. This just
  // controls what the user sees in the editor / status text. Persisted in
  // localStorage under SDT_KEY. Initialized below in the boot section.
  activeSdt: "Vendor",
  // Active dashboard scope: "" (all docs), "Vendor", "JobFiles", or "Any".
  // Set by clicking a dashboard row in the Reports → Dashboards group.
  dashboardSdt: "",
  // Active Classification report scope (Results → Reports → Classification).
  // Same shape as dashboardSdt: "" / "Vendor" / "JobFiles".
  reportSdt: "",

  limit: 100,
  filter: "",
  classifiedFilter: "all",
  minConfidence: "all",
  exactConfidence: "",      // set by drilldown click; wins over minConfidence
  fileTypeFilters: [],      // multi-select: ['.pdf', '.dwg', …]
  documentTypeFilters: [],  // multi-select: ['installation_manual', …]
  productFilter: "",        // single-select: 'NFS2-3030' (chart drilldown)
  hasProductFilter: "",     // 'yes' | 'no' | '' — chart drilldown only
  sdtFilter: "",            // 'Vendor' | 'JobFiles' | 'Any' | '(none)' | ''
  buildingFilter: "",       // a buildings.id (chart drilldown)
  hasBuildingFilter: "",    // 'yes' | 'no' | '' (chart drilldown)
  ignoredFilter: "all",     // 'all' | 'active' | 'ignored' — files table only
  // Per-table sort: column name + direction. Reset when switching tables.
  sortColumn: "",
  sortDir: "asc",
  total: 0,
  // Chart-page drilldown filter chain. Independent from the table filters
  // above — switching to charts clears it. Drilldowns compose into this.
  chartFilter: {
    classifiedFilter: "all",
    exactConfidence: "",
    fileTypeFilters: [],
    documentTypeFilters: [],
    productFilter: "",
    hasProductFilter: "",
    sdtFilter: "",
    buildingFilter: "",
    hasBuildingFilter: "",
  },
};

async function api(path, body) {
  const opts = body
    ? { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }
    : { method: "GET" };
  const res = await fetch(path, opts);
  return res.json();
}

// Log entries are persisted to localStorage as a 50-entry ring buffer so
// they survive page reloads. The most recent entry is at index 0 (matches
// the on-screen order, which inserts at firstChild). Each entry is
//   { t: "HH:MM:SS", msg, kind }
// Replayed entries get a "replayed" CSS class at restore time so we
// visually de-emphasize them with a thin left border.
const LOG_KEY  = "enlogosgrag.log";
const LOG_KEEP = 50;
function persistLogEntry(entry) {
  let buf;
  try {
    const raw = localStorage.getItem(LOG_KEY);
    buf = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(buf)) buf = [];
  } catch { buf = []; }
  buf.unshift(entry);
  if (buf.length > LOG_KEEP) buf.length = LOG_KEEP;
  try { localStorage.setItem(LOG_KEY, JSON.stringify(buf)); } catch { /* full quota — drop */ }
}

function log(msg, kind = "info") {
  const el = document.getElementById("log");
  const t = new Date().toTimeString().slice(0, 8);
  const e = document.createElement("div");
  e.className = "entry " + kind;
  e.textContent = "[" + t + "] " + msg;
  el.insertBefore(e, el.firstChild);
  persistLogEntry({ t, msg, kind });
}

// Replay the persisted ring buffer at boot. Entries get a "replayed"
// class so the user can tell at a glance which lines are from the
// previous session.
function restoreLog() {
  const el = document.getElementById("log");
  if (!el) return;
  let buf;
  try {
    const raw = localStorage.getItem(LOG_KEY);
    buf = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(buf)) buf = [];
  } catch { return; }
  // The buffer is newest-first; rendering newest-first too means we
  // append in order (instead of prepending each, which would reverse).
  for (const entry of buf) {
    if (!entry || typeof entry !== "object") continue;
    const e = document.createElement("div");
    e.className = "entry replayed " + (entry.kind || "info");
    e.textContent = "[" + (entry.t || "??:??:??") + "] " + (entry.msg || "");
    el.appendChild(e);
  }
}

function setActiveTable(name) {
  state.view = "table";
  state.table = name;
  state.offset = 0;
  state.filter = "";
  state.classifiedFilter = "all";
  state.minConfidence = "all";
  state.exactConfidence = "";
  state.fileTypeFilters = [];
  state.documentTypeFilters = [];
  state.productFilter = "";
  state.hasProductFilter = "";
  state.sdtFilter = "";
  state.buildingFilter = "";
  state.hasBuildingFilter = "";
  state.sortColumn = "";
  state.sortDir = "asc";
  state.ignoredFilter = "all";
  document.getElementById("filter").value = "";
  document.getElementById("classified-filter").value = "all";
  document.getElementById("sdt-filter").value = "";
  document.getElementById("ignored-filter").value = "all";
  refreshMultiButton("filetype");
  refreshMultiButton("doctype");
  // Highlight only the matching row across both Views and Tables groups.
  // The Source data summary table has its own <tr data-table="document_types">
  // row that should also light up when document_types is the active table.
  document.querySelectorAll(".sidebar-summary tr").forEach((el) => {
    el.classList.toggle("active", el.dataset.table === name);
  });
  document.querySelectorAll(".table-row").forEach((el) => {
    el.classList.toggle("active",
      el.dataset.table === name && el.dataset.view !== "charts");
  });
  document.getElementById("charts-row").classList.remove("active");
  document.querySelectorAll(".chart-preset").forEach((el) => el.classList.remove("active"));
  // Visibility of per-table controls.
  // docs-only:  classified + confidence dropdowns (documents only).
  // ext-only:   file-type dropdown (files + documents — anything with extensions).
  // files-only: ignored/active filter (files only — needs to compare against documents table).
  const showDocsControls  = name === "documents";
  const showExtControls   = name === "documents" || name === "files";
  const showFilesControls = name === "files";
  for (const el of document.querySelectorAll(".toolbar .docs-only")) {
    el.classList.toggle("hidden", !showDocsControls);
  }
  for (const el of document.querySelectorAll(".toolbar .ext-only")) {
    el.classList.toggle("hidden", !showExtControls);
  }
  for (const el of document.querySelectorAll(".toolbar .files-only")) {
    el.classList.toggle("hidden", !showFilesControls);
  }
  showTableView();
  loadRows();
}

function setActiveCharts(preset) {
  state.view = "charts";
  // Reset chart-page drilldown chain — entering charts is a fresh start,
  // unless the caller passed a preset to apply.
  state.chartFilter = {
    classifiedFilter: "all",
    exactConfidence: "",
    fileTypeFilters: [],
    documentTypeFilters: [],
    productFilter: "",
    hasProductFilter: "",
    sdtFilter: "",
    buildingFilter: "",
    hasBuildingFilter: "",
  };
  if (preset && typeof preset === "object") {
    Object.assign(state.chartFilter, preset);
  }
  document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) => {
    el.classList.remove("active");
  });
  document.getElementById("charts-row").classList.add("active");
  // Hide all per-table filters on the chart view — charts have their own breadcrumb.
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  showChartView();
  loadCharts();
}

// Preset shortcuts: each clicks-through to setActiveCharts() with a
// starting filter chain. Defined here so the wiring at the bottom of
// the file stays declarative.
const CHART_PRESETS = {
  // Classification folder
  classified:           { classifiedFilter: "classified" },
  unclassified:         { classifiedFilter: "unclassified" },
  "with-products":      { hasProductFilter: "yes" },
  "without-products":   { hasProductFilter: "no" },
  // Source Data Type folder. "sdt-all" is intentionally an empty filter —
  // clicking it shows the corpus-wide chart (same as the Classification
  // folder's All documents); we still expose it as a labelled entry so
  // the folder has a clear "show everything" anchor.
  "sdt-all":            {},
  "sdt-vendor":         { sdtFilter: "Vendor" },
  "sdt-jobfiles":       { sdtFilter: "JobFiles" },
  "sdt-sales":          { sdtFilter: "Sales" },
  // Filetype folder
  "ft-all":             {},
  pdfs:                 { fileTypeFilters: [".pdf"] },
  drawings:             { fileTypeFilters: [".dwg", ".dxf"] },
  office:               { fileTypeFilters: [".doc", ".docx", ".xls", ".xlsx"] },
  images:               { fileTypeFilters: [".jpg", ".jpeg", ".png", ".heic"] },
};

function hideAllViews() {
  document.getElementById("table-view").style.display              = "none";
  document.getElementById("chart-view").style.display              = "none";
  document.getElementById("help-view").style.display               = "none";
  document.getElementById("ignored-view").style.display            = "none";
  document.getElementById("ignored-folders-view").style.display    = "none";
  document.getElementById("classifier-editor-view").style.display  = "none";
  const dv = document.getElementById("duplicates-view");
  if (dv) dv.style.display = "none";
  const rcv = document.getElementById("report-classification-view");
  if (rcv) rcv.style.display = "none";
  const ifv = document.getElementById("ignored-files-view");
  if (ifv) ifv.style.display = "none";
  const bev = document.getElementById("building-extractor-view");
  if (bev) bev.style.display = "none";
  // Always stop chart polling when leaving — startChartPolling() is called
  // from showChartView when the user enters charts.
  stopChartPolling();
}

// Refresh the chart view's data on a timer so it reflects ongoing work
// (a classify run, a content classify, etc.). Stops when user leaves
// the charts view. 2s cadence is cheap — /api/stats is a single SQL pass.
let chartPollTimer = null;
function startChartPolling() {
  if (chartPollTimer) return;
  chartPollTimer = setInterval(() => {
    if (state.view !== "charts") { stopChartPolling(); return; }
    loadCharts();
  }, 2000);
}
function stopChartPolling() {
  if (chartPollTimer) { clearInterval(chartPollTimer); chartPollTimer = null; }
}
// Reload the charts iff the user is currently on the charts view. Used
// at the end of classify/extract/etc actions so the user doesn't have
// to wait for the 2s poll tick to see the post-action numbers.
function refreshChartsIfActive() {
  if (state.view === "charts") loadCharts();
}

function showTableView() {
  hideAllViews();
  document.getElementById("table-view").style.display = "";
  // Re-show toolbar pagination controls (hidden in chart mode would be wrong;
  // they're already there — but disabling them keeps them inert).
  document.querySelectorAll(".toolbar input, .toolbar button").forEach((el) => {
    el.style.display = "";
  });
  document.getElementById("page-meta").style.display = "";
}

function showChartView() {
  hideAllViews();
  document.getElementById("chart-view").style.display = "block";
  // Hide toolbar inputs that don't apply to the chart view.
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
  startChartPolling();
}

function showHelpView() {
  hideAllViews();
  document.getElementById("help-view").style.display  = "block";
  // Help is purely text — hide the table toolbar entirely.
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
}

function showIgnoredView() {
  hideAllViews();
  document.getElementById("ignored-view").style.display = "block";
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
}

function showIgnoredFoldersView() {
  hideAllViews();
  document.getElementById("ignored-folders-view").style.display = "block";
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
}

function setActiveHelp(sdt) {
  state.view = "help";
  // Persist the chosen scope so loadDashboard() and the polling loop keep
  // hitting the right /api/dashboard?sdt=… endpoint until the user picks a
  // different one. Empty string = "all docs" (legacy default).
  state.dashboardSdt = sdt || "";
  document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) => {
    el.classList.remove("active");
  });
  // Highlight the dashboard row that matches the chosen scope.
  const targetRow = document.querySelector(
    '.table-row[data-view="help"][data-sdt="' + state.dashboardSdt + '"]',
  );
  if (targetRow) targetRow.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  showHelpView();
  loadDashboard();
  // If any background worker is currently running, start auto-refreshing
  // the dashboard so the user can watch KPIs/charts evolve. The pollers
  // below also start/stop this when a worker transitions running.
  if (anyWorkerRunning()) startDashboardPolling();
}

// Live-refresh the dashboard while ingest / classify / extract / hash /
// dedup work is happening. Every 3s on the home view; a no-op when the
// user is on any other view (no point computing KPIs the user can't see).
let dashboardPollTimer = 0;
const DASHBOARD_POLL_MS = 3000;
function startDashboardPolling() {
  if (dashboardPollTimer) return;
  dashboardPollTimer = setInterval(() => {
    if (state.view !== "help") return; // only refresh when home is visible
    loadDashboard();
  }, DASHBOARD_POLL_MS);
}
function stopDashboardPolling() {
  if (!dashboardPollTimer) return;
  clearInterval(dashboardPollTimer);
  dashboardPollTimer = 0;
}
// True if any background worker the user can launch is currently running.
// Extract + hash are the long-runners; ingest/classify are synchronous so
// they're already finished by the time control returns. The flag
// variables are declared further down — guard so the function works
// before they're initialised.
function anyWorkerRunning() {
  const ex = (typeof extractWasRunning !== "undefined") && extractWasRunning;
  const hs = (typeof hashWasRunning   !== "undefined") && hashWasRunning;
  return Boolean(ex || hs);
}

function setActiveIgnored() {
  state.view = "ignored";
  document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) => {
    el.classList.remove("active");
  });
  const row = document.getElementById("ignored-row");
  if (row) row.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  showIgnoredView();
  loadIgnoredTypes();
}

function setActiveIgnoredFolders() {
  state.view = "ignored-folders";
  document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) => {
    el.classList.remove("active");
  });
  const row = document.getElementById("ignored-folders-row");
  if (row) row.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  showIgnoredFoldersView();
  loadIgnoredFolders();
}

function showIgnoredFilesView() {
  hideAllViews();
  document.getElementById("ignored-files-view").style.display = "block";
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
}

function setActiveIgnoredFiles() {
  state.view = "ignored-files";
  document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) => {
    el.classList.remove("active");
  });
  const row = document.getElementById("ignored-files-row");
  if (row) row.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  showIgnoredFilesView();
  loadIgnoredFiles();
}

async function loadIgnoredFiles() {
  const tbody = document.querySelector("#ignored-files-table tbody");
  if (!tbody) return;
  let r;
  try {
    r = await fetch("/api/ignored-files").then((res) => res.json());
  } catch (e) {
    tbody.innerHTML = '<tr><td colspan="6" style="color: var(--danger-text);">Failed to load: ' + (e.message || e) + '</td></tr>';
    return;
  }
  if (!r.ok) {
    tbody.innerHTML = '<tr><td colspan="6" style="color: var(--danger-text);">' + r.error + '</td></tr>';
    return;
  }
  if (!r.files || r.files.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" style="color: var(--muted); text-align:center; padding: 18px;">No ignored files yet. Add one above, or use the Duplicates report to ignore-all-but-shortest per cluster.</td></tr>';
    return;
  }
  tbody.innerHTML = "";
  for (const f of r.files) {
    const tr = document.createElement("tr");
    const added = f.added_at ? new Date(f.added_at).toLocaleDateString() : "";
    const status = f.in_files
      ? '<span style="color: var(--muted); font-size: 11px;">in files</span>'
      : '<span style="color: var(--muted); font-size: 11px; opacity: 0.6;">orphaned</span>';
    const tdPath = document.createElement("td");
    const a = document.createElement("a");
    a.href = "#"; a.className = "open-link"; a.textContent = f.path;
    a.title = "Open " + f.path;
    a.addEventListener("click", (ev) => { ev.preventDefault(); openFile(f.path); });
    tdPath.appendChild(a);
    tdPath.style.fontFamily = "ui-monospace, Menlo, monospace";
    tdPath.style.fontSize = "11px";
    tdPath.style.wordBreak = "break-all";
    tr.appendChild(tdPath);
    tr.insertAdjacentHTML("beforeend",
      '<td>' + (f.vendor ? escapeHtml(f.vendor) : "—") + '</td>' +
      '<td style="color: var(--muted);">' + escapeHtml(f.notes || "") + '</td>' +
      '<td style="color: var(--muted);">' + added + '</td>' +
      '<td>' + status + '</td>' +
      '<td><button class="secondary ignored-files-remove" data-path="' + escapeHtml(f.path) + '" style="padding: 2px 8px; font-size: 11px;">Restore</button></td>',
    );
    tbody.appendChild(tr);
  }
  for (const btn of tbody.querySelectorAll(".ignored-files-remove")) {
    btn.addEventListener("click", async () => {
      const p = btn.dataset.path;
      const rr = await api("/api/ignored-files/remove", { path: p });
      if (!rr.ok) { log("restore failed: " + rr.error, "err"); return; }
      log("restored " + p + " (note: re-ingest to recreate documents row)", "ok");
      loadIgnoredFiles();
      refreshTableCounts();
    });
  }
  enableClientSortAndResize(document.getElementById("ignored-files-table"), "ignored-files");
}

function showDuplicatesView() {
  hideAllViews();
  document.getElementById("duplicates-view").style.display = "block";
  // Hide table-only toolbar bits.
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
}

function setActiveDuplicates() {
  state.view = "duplicates";
  document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) => {
    el.classList.remove("active");
  });
  const row = document.getElementById("duplicates-row");
  if (row) row.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  showDuplicatesView();
  loadDuplicates();
}

// Classification report view — the per-doc-type breakdown lifted out of
// the home dashboard. sdt is "" / "Vendor" / "JobFiles" so the same
// /api/dashboard?sdt=… endpoint scopes the table; rendering reuses the
// dashboard's #report-class-* DOM ids (separate from the home's
// #dash-class-* ids so both views can coexist).
function showClassificationReportView() {
  hideAllViews();
  document.getElementById("report-classification-view").style.display = "block";
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
}

function setActiveClassificationReport(sdt) {
  state.view = "report-classification";
  state.reportSdt = sdt || "";
  document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) => {
    el.classList.remove("active");
  });
  const targetRow = document.querySelector(
    '.table-row[data-view="report-classification"][data-sdt="' + state.reportSdt + '"]',
  );
  if (targetRow) targetRow.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  showClassificationReportView();
  loadClassificationReport();
}

async function loadClassificationReport() {
  const titleEl = document.getElementById("report-classification-title");
  const summary = document.getElementById("report-class-summary");
  const tbody   = document.querySelector("#report-class-types-table tbody");
  if (!titleEl || !summary || !tbody) return;
  const sdt = state.reportSdt || "";
  titleEl.textContent = sdt
    ? "Classification by document type — " + sdt
    : "Classification by document type";

  let r;
  try {
    const url = sdt
      ? "/api/dashboard?sdt=" + encodeURIComponent(sdt)
      : "/api/dashboard";
    r = await fetch(url).then((res) => res.json());
  } catch (e) {
    summary.textContent = "Failed to load report: " + (e.message || e);
    return;
  }
  if (!r.ok) {
    summary.textContent = "Failed to load report: " + (r.error || "unknown");
    return;
  }

  const c = r.classification;
  if (!c || c.totalDocuments === 0) {
    summary.textContent = "No documents in scope.";
    tbody.innerHTML = "";
    return;
  }
  summary.innerHTML =
    '<strong>' + c.classified.toLocaleString() + '</strong> of <strong>' +
    c.totalDocuments.toLocaleString() + '</strong> documents classified ' +
    '(' + c.pctClassified.toFixed(1) + '%) — ' +
    c.unclassified.toLocaleString() + ' remaining.';

  // Per-type table — same shape as the home dashboard's render block but
  // scoped to this view's tbody.
  tbody.innerHTML = "";
  const fmtPct = (p) => (p > 0 ? p.toFixed(1) + '%' : '—');
  const fmtN   = (n) => (n > 0 ? Number(n).toLocaleString() : '0');
  for (const t of (r.byType || [])) {
    const tr = document.createElement("tr");
    if (t.total === 0) tr.className = "empty-type";
    tr.innerHTML =
      '<td>' + t.name + '</td>' +
      '<td>' + fmtN(t.total) + '</td>' +
      '<td>' + fmtPct(t.pctOfClassified) + '</td>' +
      '<td>' + fmtPct(t.pctHigh) + '</td>' +
      '<td>' + fmtPct(t.pctMedium) + '</td>' +
      '<td>' + fmtPct(t.pctLow) + '</td>';
    tbody.appendChild(tr);
  }
}

function setActiveBuildingExtractor() {
  if (cedit.kind && ceditIsDirty()) {
    if (!window.confirm("You have unsaved changes to the " + cedit.kind + " rules. Discard?")) return;
  }
  state.view = "building-extractor";
  cedit.kind = null;
  document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) => {
    el.classList.remove("active");
  });
  const row = document.getElementById("extractor-row-building");
  if (row) row.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  hideAllViews();
  document.getElementById("building-extractor-view").style.display = "block";
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
}

async function loadDuplicates() {
  const summary = document.getElementById("duplicates-summary");
  const body    = document.getElementById("duplicates-body");
  summary.textContent = "Loading…";
  body.innerHTML = "";
  let r;
  try {
    r = await fetch("/api/duplicates").then((res) => res.json());
  } catch (e) {
    summary.textContent = "Failed to load: " + e;
    return;
  }
  if (!r.ok) {
    summary.textContent = "Failed to load: " + r.error;
    return;
  }
  // Also fetch hash-status so we can tell the user how many docs have
  // actually been hashed yet — a "no duplicates" result is meaningless
  // until the hashes exist.
  let hs = null;
  try {
    hs = await fetch("/api/hash-status").then((res) => res.json());
  } catch {}
  const hashedNote = hs && hs.ok
    ? hs.hashedCount + " / " + hs.totalDocs + " documents hashed"
    : "";

  if (r.clusterCount === 0) {
    summary.innerHTML =
      "<strong>No duplicates found</strong> across hashed documents."
      + (hashedNote ? "  ·  " + hashedNote : "");
    return;
  }
  summary.innerHTML =
    "<strong>" + r.clusterCount + "</strong> cluster" + (r.clusterCount === 1 ? "" : "s") +
    " of duplicates · <strong>" + r.totalDocs + "</strong> documents involved · " +
    "<strong>" + r.wastedCopies + "</strong> redundant cop" + (r.wastedCopies === 1 ? "y" : "ies")
    + (hashedNote ? "  ·  " + hashedNote : "");

  // Render one card per cluster, wrapped in a <details> so the user can
  // collapse / expand each cluster. The first cluster opens by default
  // so the page isn't a wall of closed sections on first visit.
  r.clusters.forEach((c, idx) => {
    const card = document.createElement("details");
    card.className = "dup-cluster";
    if (idx === 0) card.open = true;
    const summary = document.createElement("summary");
    summary.className = "dup-cluster-h";
    summary.innerHTML =
      '<span class="dup-caret">▶</span>' +
      '<code title="' + c.sha256 + '">' + c.sha256.slice(0, 12) + '…</code>' +
      ' <span class="dup-count">' + c.count + ' copies</span>' +
      ' <button type="button" class="secondary dup-cluster-ignore" style="margin-left: 12px; padding: 2px 10px; font-size: 11px;">Ignore all but shortest</button>' +
      ' <span class="dup-toggle-hint">click to expand</span>';
    card.appendChild(summary);
    // Pick the shortest path as the keeper. Tie-break: lexicographic.
    const shortest = c.docs
      .map((d) => d.file_path || "")
      .filter(Boolean)
      .sort((a, b) => a.length - b.length || a.localeCompare(b))[0] || "";
    const ignoreBtn = summary.querySelector(".dup-cluster-ignore");
    ignoreBtn.title = "Keep " + (shortest || "(first)") + ", ignore the rest";
    ignoreBtn.addEventListener("click", async (ev) => {
      // Stop the click from also toggling the <details> summary.
      ev.preventDefault();
      ev.stopPropagation();
      const losers = c.docs
        .map((d) => d.file_path)
        .filter((p) => p && p !== shortest);
      if (losers.length === 0) return;
      const r = await api("/api/ignored-files/add", { paths: losers, notes: "de-duplicated" });
      if (!r.ok) { log("ignore failed: " + r.error, "err"); return; }
      log("kept " + shortest + "; ignored " + losers.length + " duplicate" + (losers.length === 1 ? "" : "s") +
          " (dropped " + r.dropped + " documents row" + (r.dropped === 1 ? "" : "s") + ")", "ok");
      loadDuplicates();
      refreshTableCounts();
    });
    const tbl = document.createElement("table");
    tbl.className = "dup-table sortable-table";
    const thead = document.createElement("thead");
    thead.innerHTML =
      '<tr>' +
        '<th data-key="vendor">Vendor</th>' +
        '<th data-key="document_type">Document type</th>' +
        '<th data-key="file_path">Path</th>' +
        '<th></th>' +
      '</tr>';
    tbl.appendChild(thead);
    const tbody = document.createElement("tbody");
    for (const d of c.docs) {
      const tr = document.createElement("tr");
      const tdV = document.createElement("td"); tdV.textContent = d.vendor || "—";
      const tdT = document.createElement("td"); tdT.textContent = d.document_type || "—";
      const tdP = document.createElement("td");
      tdP.className = "dup-path";
      const path = d.file_path || "";
      if (path) {
        // Clickable path → opens the file with the same /api/open-file
        // route the documents table uses. Path-remapping (canonical S:\ →
        // local) is handled server-side, so we just send the canonical
        // string we already have.
        const a = document.createElement("a");
        a.href = "#";
        a.className = "open-link";
        a.textContent = path;
        a.title = "Open " + path;
        a.addEventListener("click", (ev) => {
          ev.preventDefault();
          openFile(path);
        });
        tdP.appendChild(a);
      } else {
        tdP.textContent = "";
      }
      const tdAction = document.createElement("td");
      tdAction.style.textAlign = "right";
      if (path) {
        const ig = document.createElement("a");
        ig.href = "#"; ig.className = "open-link";
        ig.textContent = "ignore";
        ig.title = "Ignore this single copy (note: manual)";
        ig.style.fontSize = "11px";
        ig.addEventListener("click", async (ev) => {
          ev.preventDefault();
          const r = await api("/api/ignored-files/add", { paths: [path], notes: "manual" });
          if (!r.ok) { log("ignore failed: " + r.error, "err"); return; }
          log("ignored " + path, "ok");
          loadDuplicates();
          refreshTableCounts();
        });
        tdAction.appendChild(ig);
      }
      tr.appendChild(tdV); tr.appendChild(tdT); tr.appendChild(tdP); tr.appendChild(tdAction);
      tbody.appendChild(tr);
    }
    tbl.appendChild(tbody);
    card.appendChild(tbl);
    body.appendChild(card);
    enableClientSortAndResize(tbl);
  });

  // Bulk expand / collapse controls at the top — useful with 100+ clusters.
  const controls = document.createElement("div");
  controls.style.cssText = "margin: 0 0 12px 0; font-size: 11px; color: var(--muted);";
  controls.innerHTML =
    '<a href="#" id="dup-expand-all" style="margin-right: 12px;">Expand all</a>' +
    '<a href="#" id="dup-collapse-all">Collapse all</a>';
  body.insertBefore(controls, body.firstChild);
  controls.querySelector("#dup-expand-all").addEventListener("click", (e) => {
    e.preventDefault();
    body.querySelectorAll("details.dup-cluster").forEach((d) => { d.open = true; });
  });
  controls.querySelector("#dup-collapse-all").addEventListener("click", (e) => {
    e.preventDefault();
    body.querySelectorAll("details.dup-cluster").forEach((d) => { d.open = false; });
  });
}

async function loadIgnoredFolders() {
  const tbody = document.querySelector("#ignored-folders-table tbody");
  if (!tbody) return;
  let r;
  try {
    r = await api("/api/ignored-folders");
  } catch (e) {
    tbody.innerHTML = '<tr><td colspan="5" style="color: var(--danger-text);">Failed to load: ' +
      (e.message || e) + '</td></tr>';
    return;
  }
  if (!r.folders || r.folders.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" style="color: var(--muted); text-align:center; padding: 18px;">No ignored folders yet. Add one above.</td></tr>';
    return;
  }
  tbody.innerHTML = "";
  for (const f of r.folders) {
    const tr = document.createElement("tr");
    const added = f.added_at ? new Date(f.added_at).toLocaleDateString() : "";
    const notes = f.notes || "";
    tr.innerHTML =
      '<td><code>' + escapeHtml(f.name) + '</code></td>' +
      '<td>' + Number(f.file_count || 0).toLocaleString() + '</td>' +
      '<td style="color: var(--muted);">' + escapeHtml(notes) + '</td>' +
      '<td style="color: var(--muted);">' + added + '</td>' +
      '<td><button class="secondary ignored-folders-remove" data-name="' + escapeHtml(f.name) +
        '" style="padding: 2px 8px; font-size: 11px;">Remove</button></td>';
    tbody.appendChild(tr);
  }
  for (const btn of tbody.querySelectorAll(".ignored-folders-remove")) {
    btn.addEventListener("click", async () => {
      const name = btn.dataset.name;
      const r = await api("/api/ignored-folders/remove", { name });
      if (!r.ok) { log("remove failed: " + r.error, "err"); return; }
      log("removed folder " + name + " from ignore list", "ok");
      loadIgnoredFolders();
      refreshTableCounts();
    });
  }
  enableClientSortAndResize(document.getElementById("ignored-folders-table"), "ignored-folders");
}

async function loadIgnoredTypes() {
  const tbody = document.querySelector("#ignored-table tbody");
  if (!tbody) return;
  let r;
  try {
    r = await api("/api/ignored-types");
  } catch (e) {
    tbody.innerHTML = '<tr><td colspan="5" style="color: var(--danger-text);">Failed to load: ' +
      (e.message || e) + '</td></tr>';
    return;
  }
  if (!r.types || r.types.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" style="color: var(--muted); text-align:center; padding: 18px;">No ignored types yet. Add one above.</td></tr>';
    return;
  }
  tbody.innerHTML = "";
  for (const t of r.types) {
    const tr = document.createElement("tr");
    const added = t.added_at ? new Date(t.added_at).toLocaleDateString() : "";
    const notes = t.notes || "";
    tr.innerHTML =
      '<td><code>' + t.ext + '</code></td>' +
      '<td>' + Number(t.file_count || 0).toLocaleString() + '</td>' +
      '<td style="color: var(--muted);">' + escapeHtml(notes) + '</td>' +
      '<td style="color: var(--muted);">' + added + '</td>' +
      '<td><button class="secondary ignored-remove" data-ext="' + t.ext +
        '" style="padding: 2px 8px; font-size: 11px;">Remove</button></td>';
    tbody.appendChild(tr);
  }
  for (const btn of tbody.querySelectorAll(".ignored-remove")) {
    btn.addEventListener("click", async () => {
      const ext = btn.dataset.ext;
      const r = await api("/api/ignored-types/remove", { ext });
      if (!r.ok) { log("remove failed: " + r.error, "err"); return; }
      log("removed " + ext + " from ignored types", "ok");
      loadIgnoredTypes();
      refreshTableCounts();
    });
  }
  enableClientSortAndResize(document.getElementById("ignored-table"), "ignored-types");
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// --- Classifier rule editor ----------------------------------------------
// One generic table editor that switches column layout based on the kind
// (filename / content / product). State is held in cedit.* — a snapshot of
// the rule list returned by the API plus an edit-in-progress copy. Save
// validates server-side; failures stay in the UI without touching disk.

const cedit = {
  kind: null,                // "filename" | "content" | "product"
  meta: null,                // server-supplied meta (label, columns, enums)
  pristine: [],              // last-saved rule snapshot, deep-cloned
  rules: [],                 // current edit buffer (array of plain objects)
  coverage: null,            // { totalFiles, byId: Map<id, {winning, wouldMatch}> } | null
};

const CEDIT_KIND_DESC = {
  filename: "Rules run in order against each file. First match wins. The match field selects which string to test (name / parent / path / file_type). Pattern is a JS regex (case-insensitive).",
  content:  "Rules run against extracted PDF text. first_page matches the first page only; extract matches the full body. Only fires against documents that are unclassified or low-confidence.",
  product:  "Vendor-scoped rules linking documents to (vendor, product). A single document can match multiple products. The match field selects name / path / first_page / extract.",
};

function ceditIsDirty() {
  return JSON.stringify(cedit.rules) !== JSON.stringify(cedit.pristine);
}

function ceditUpdateButtons() {
  const dirty = ceditIsDirty();
  document.getElementById("cedit-save").disabled    = !dirty;
  document.getElementById("cedit-discard").disabled = !dirty;
  document.getElementById("cedit-dirty").textContent = dirty ? "● unsaved changes" : "";
}

function setActiveClassifierEditor(kind) {
  if (cedit.kind && ceditIsDirty()) {
    if (!window.confirm("You have unsaved changes to the " + cedit.kind + " rules. Discard?")) {
      // Re-highlight the current row — user cancelled the navigation.
      document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) =>
        el.classList.remove("active"));
      const cur = document.getElementById("classifier-row-" + cedit.kind);
      if (cur) cur.classList.add("active");
      return;
    }
  }
  state.view = "classifier-editor";
  document.querySelectorAll(".table-row, .chart-preset, .sidebar-summary tr").forEach((el) => {
    el.classList.remove("active");
  });
  const row = document.getElementById("classifier-row-" + kind);
  if (row) row.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  hideAllViews();
  document.getElementById("classifier-editor-view").style.display = "block";
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
  loadClassifierRules(kind);
}

async function loadClassifierRules(kind) {
  const errEl = document.getElementById("cedit-error");
  errEl.style.display = "none";
  errEl.textContent = "";

  let r;
  try { r = await api("/api/classifier-rules/" + kind); }
  catch (e) {
    showCeditError("Failed to load rules: " + (e.message || e));
    return;
  }
  if (!r.ok) { showCeditError(r.error || "Load failed."); return; }

  cedit.kind     = kind;
  cedit.meta     = r.meta;
  cedit.pristine = JSON.parse(JSON.stringify(r.rules));
  cedit.rules    = JSON.parse(JSON.stringify(r.rules));
  cedit.coverage = null;
  // Switching classifiers resets which vendor groups are expanded.
  ceditOpenVendors.clear();

  document.getElementById("cedit-title").textContent = r.meta.label;
  document.getElementById("cedit-meta").textContent  = r.meta.file + " · " + r.rules.length + " rule" + (r.rules.length === 1 ? "" : "s");
  document.getElementById("cedit-desc").textContent  = CEDIT_KIND_DESC[kind] || "";

  // Expand/Collapse-all only makes sense for the grouped product view.
  const groupCtrls = document.getElementById("cedit-group-controls");
  if (groupCtrls) groupCtrls.style.display = kind === "product" ? "" : "none";

  // SDT-scope reminder banner — visible on filename + content editors so
  // the user remembers the rule list is filtered. Product editor has its
  // own grouping and gets the same banner for consistency.
  updateCeditSdtReminder();

  renderCeditTable();
  ceditUpdateButtons();

  // Coverage stats — filename rules only. Fire-and-forget; the table
  // re-renders when the data arrives.
  if (kind === "filename") {
    loadCoverage();
  }
}

async function loadCoverage() {
  // Coverage stats per rule. Best-effort — swallow errors so a coverage
  // hiccup doesn't take down the editor.
  try {
    const r = await api("/api/classifier-rules/" + cedit.kind + "/coverage");
    if (r.ok) {
      const byId = {};
      for (const c of r.coverage) byId[c.id] = c;
      cedit.coverage = { totalFiles: r.totalFiles, byId };
      renderCeditTable();
    }
  } catch (e) { /* coverage is best-effort, swallow */ }
}

function showCeditError(msg) {
  const el = document.getElementById("cedit-error");
  el.style.display = "block";
  el.textContent = msg;
}

// True when a rule should be visible given the header source-data-type
// switcher. Vendor active → Vendor + Any rules; JobFiles active → JobFiles
// + Any; Any active → everything; All → everything ("no filter").
// Empty/missing source_data_type on a rule counts as Vendor (back-compat
// for the original rule files).
function ruleVisibleForActiveSdt(rule) {
  const ruleSdt = rule.source_data_type || "Vendor";
  const active = state.activeSdt || "Vendor";
  if (active === "Any" || active === "All") return true;
  return ruleSdt === active || ruleSdt === "Any";
}

function renderCeditTable() {
  const thead = document.getElementById("cedit-thead-row");
  const tbody = document.querySelector("#cedit-table tbody");
  if (!thead || !tbody) return;

  // Header row: column labels + (filename only) coverage column + actions.
  thead.innerHTML = "";
  for (const col of cedit.meta.columns) {
    const th = document.createElement("th");
    th.textContent = col.replace(/_/g, " ");
    thead.appendChild(th);
  }
  if (cedit.kind === "filename") {
    const thCov = document.createElement("th");
    thCov.textContent = "matches";
    thCov.title = "winning / would-match. If they differ, the rule is shadowed by an earlier rule.";
    thCov.className = "cedit-coverage-h";
    thead.appendChild(thCov);
  }
  const thAct = document.createElement("th");
  thAct.textContent = "";
  thAct.style.textAlign = "right";
  thead.appendChild(thAct);

  tbody.innerHTML = "";
  if (cedit.rules.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = cedit.meta.columns.length + 1;
    td.style.color = "var(--muted)";
    td.style.textAlign = "center";
    td.style.padding = "24px";
    td.textContent = "No rules yet. Click + Add rule to start.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  // Product rules are vendor-scoped — rules only fire against documents
  // whose vendor matches. Render them grouped under collapsible vendor
  // sections so the list is scannable. Filename/content rules don't have
  // vendor column — keep the flat layout.
  if ((cedit.meta.parent === "product" || cedit.kind === "product")
      && cedit.meta.columns.includes("vendor")) {
    renderCeditGroupedByVendor(tbody);
  } else {
    let visibleCount = 0;
    for (let i = 0; i < cedit.rules.length; i++) {
      if (!ruleVisibleForActiveSdt(cedit.rules[i])) continue;
      tbody.appendChild(renderCeditRow(i));
      visibleCount++;
    }
    // Tell the user the active filter when nothing matches.
    if (visibleCount === 0) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = cedit.meta.columns.length + 1 + (cedit.kind === "filename" ? 1 : 0);
      td.style.color = "var(--muted)";
      td.style.textAlign = "center";
      td.style.padding = "24px";
      td.textContent = 'No rules match the active "' + (state.activeSdt || "Vendor") +
        '" Source Data Type. Switch in the header to see other rules.';
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
  }
}

// Walk the rules array in order, breaking into runs by vendor. Each run
// becomes a collapsible group. Persists open/closed state per vendor in
// the in-memory ceditOpenVendors set so re-render after edits doesn't
// collapse the section the user is working in.
const ceditOpenVendors = new Set();

function renderCeditGroupedByVendor(tbody) {
  // Group consecutive rules by vendor while preserving original indices.
  // (Non-consecutive rules with the same vendor become separate groups —
  // visual cue that something is out of order.) Filter by active SDT.
  const groups = [];
  let cur = null;
  for (let i = 0; i < cedit.rules.length; i++) {
    if (!ruleVisibleForActiveSdt(cedit.rules[i])) continue;
    const v = cedit.rules[i].vendor || "(no vendor)";
    if (!cur || cur.vendor !== v) {
      cur = { vendor: v, indices: [] };
      groups.push(cur);
    }
    cur.indices.push(i);
  }
  if (groups.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = cedit.meta.columns.length + 1;
    td.style.color = "var(--muted)";
    td.style.textAlign = "center";
    td.style.padding = "24px";
    td.textContent = 'No rules match the active "' + (state.activeSdt || "Vendor") +
      '" Source Data Type. Switch in the header to see other rules.';
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  // Merge contiguous groups for display: aggregate count by vendor name,
  // but keep indices ordered. (We DON'T reorder rules — the YAML order
  // wins, the UI just renders.)
  const colSpan = cedit.meta.columns.length + 1;

  for (const g of groups) {
    // Group header row — clickable, toggles open/closed.
    const headerRow = document.createElement("tr");
    headerRow.className = "cedit-group-header";
    const headerTd = document.createElement("td");
    headerTd.colSpan = colSpan;
    const isOpen = ceditOpenVendors.has(g.vendor);
    headerTd.innerHTML =
      '<span class="caret">' + (isOpen ? "▾" : "▸") + '</span>' +
      '<span class="vendor-name">' + escapeHtml(g.vendor) + '</span>' +
      '<span class="rule-count">' + g.indices.length + ' rule' + (g.indices.length === 1 ? '' : 's') + '</span>';
    headerTd.addEventListener("click", () => {
      if (ceditOpenVendors.has(g.vendor)) ceditOpenVendors.delete(g.vendor);
      else ceditOpenVendors.add(g.vendor);
      renderCeditTable();
    });
    headerRow.appendChild(headerTd);
    tbody.appendChild(headerRow);

    if (!isOpen) continue;

    for (const i of g.indices) {
      const row = renderCeditRow(i);
      row.classList.add("cedit-group-body");
      tbody.appendChild(row);
    }
  }
}

function renderCeditRow(i) {
  const rule = cedit.rules[i];
  const tr = document.createElement("tr");
  for (const col of cedit.meta.columns) {
    const td = document.createElement("td");
    if (col === "pattern") td.classList.add("cedit-pattern");
    let input;
    if (col === "confidence") {
      input = document.createElement("select");
      for (const v of cedit.meta.confidences) {
        const o = document.createElement("option");
        o.value = v; o.textContent = v;
        input.appendChild(o);
      }
    } else if (col === "match") {
      input = document.createElement("select");
      for (const v of cedit.meta.matches) {
        const o = document.createElement("option");
        o.value = v; o.textContent = v;
        input.appendChild(o);
      }
    } else if (col === "source_data_type") {
      input = document.createElement("select");
      // Empty option = back-compat default (Vendor at runtime). Render
      // explicitly so users can author "leave blank" rules from the YAML.
      const sdts = cedit.meta.sourceDataTypes || ["Vendor", "JobFiles", "Sales", "Any"];
      for (const v of sdts) {
        const o = document.createElement("option");
        o.value = v; o.textContent = v;
        input.appendChild(o);
      }
    } else {
      input = document.createElement("input");
      input.type = "text";
    }
    // Display value: source_data_type defaults to 'Vendor' when empty, to
    // mirror runtime behavior. Other columns render their stored value as-is.
    let displayValue = rule[col] != null ? String(rule[col]) : "";
    if (col === "source_data_type" && displayValue === "") displayValue = "Vendor";
    input.value = displayValue;
    input.addEventListener("input", () => {
      rule[col] = input.value;
      ceditUpdateButtons();
    });
    input.addEventListener("change", () => {
      rule[col] = input.value;
      ceditUpdateButtons();
      // Editing the vendor field changes which group this rule belongs to.
      // Auto-open the new group and re-render so the user can keep editing.
      if (col === "vendor" && (cedit.meta.parent === "product" || cedit.kind === "product")) {
        ceditOpenVendors.add(input.value || "(no vendor)");
        renderCeditTable();
      }
    });
    td.appendChild(input);
    // Pattern cells get a small ⋯ trigger that opens the regex modal.
    // Inline edit still works for short tweaks; the modal is for room +
    // sample-testing + cheat sheet.
    if (col === "pattern") {
      const opener = document.createElement("button");
      opener.type = "button";
      opener.className = "rx-open";
      opener.textContent = "⋯";
      opener.title = "Open pattern editor";
      opener.addEventListener("click", (e) => {
        e.preventDefault();
        openRegexModal(input.value || "", (newVal) => {
          input.value = newVal;
          rule[col] = newVal;
          ceditUpdateButtons();
        });
      });
      td.appendChild(opener);
    }
    tr.appendChild(td);
  }
  // Coverage column (filename only): "winning / would-match" — diverging
  // numbers mean the rule is shadowed by an earlier matching rule.
  if (cedit.kind === "filename") {
    const tdCov = document.createElement("td");
    tdCov.className = "cedit-coverage";
    const cov = cedit.coverage && cedit.coverage.byId[rule.id];
    if (!cov) {
      tdCov.textContent = "…";
      tdCov.classList.add("loading");
    } else {
      const w = cov.winning, m = cov.wouldMatch;
      const span = document.createElement("span");
      span.textContent = w + " / " + m;
      if (m === 0) span.classList.add("zero");
      else if (w < m) span.classList.add("shadowed");
      else span.classList.add("clean");
      tdCov.appendChild(span);
      tdCov.title = w === m
        ? w + " docs would match and " + w + " currently win — clean."
        : w + " win, but the regex would match " + m + ". The other " + (m - w) + " are claimed by an earlier rule (shadowed).";
    }
    tr.appendChild(tdCov);
  }
  // Actions column: ↑ ↓ ×
  const tdAct = document.createElement("td");
  tdAct.className = "cedit-actions";
  const btnUp   = document.createElement("button");
  const btnDown = document.createElement("button");
  const btnDel  = document.createElement("button");
  btnUp.textContent = "↑";   btnUp.title   = "Move up";
  btnDown.textContent = "↓"; btnDown.title = "Move down";
  btnDel.textContent = "×";  btnDel.title  = "Delete rule";
  btnDel.classList.add("danger");
  btnUp.disabled   = i === 0;
  btnDown.disabled = i === cedit.rules.length - 1;
  btnUp.addEventListener("click", () => {
    if (i === 0) return;
    [cedit.rules[i - 1], cedit.rules[i]] = [cedit.rules[i], cedit.rules[i - 1]];
    renderCeditTable();
    ceditUpdateButtons();
  });
  btnDown.addEventListener("click", () => {
    if (i === cedit.rules.length - 1) return;
    [cedit.rules[i + 1], cedit.rules[i]] = [cedit.rules[i], cedit.rules[i + 1]];
    renderCeditTable();
    ceditUpdateButtons();
  });
  btnDel.addEventListener("click", () => {
    if (!window.confirm("Delete rule " + (rule.id || "(unnamed)") + "?")) return;
    cedit.rules.splice(i, 1);
    renderCeditTable();
    ceditUpdateButtons();
  });
  tdAct.appendChild(btnUp);
  tdAct.appendChild(btnDown);
  tdAct.appendChild(btnDel);
  tr.appendChild(tdAct);
  return tr;
}

function ceditAddRule() {
  // Build a blank rule with sensible defaults. The new rule's source_data_type
  // matches the user's active SDT — they're authoring "for the corpus they're
  // looking at" — except Any-while-active stays Vendor (the conservative default).
  const blank = {};
  const activeSdt = (state.activeSdt && state.activeSdt !== "Any" && state.activeSdt !== "All")
    ? state.activeSdt
    : "Vendor";
  for (const col of cedit.meta.columns) {
    if (col === "confidence")          blank[col] = "medium";
    else if (col === "match")          blank[col] = cedit.meta.matches[0];
    else if (col === "source_data_type") blank[col] = activeSdt;
    else                               blank[col] = "";
  }
  cedit.rules.push(blank);
  // For grouped (product) view: auto-open the group the new row lands in
  // so it's immediately visible — empty vendor → "(no vendor)" bucket.
  if (cedit.meta.parent === "product" || cedit.kind === "product") {
    ceditOpenVendors.add(blank.vendor || "(no vendor)");
  }
  renderCeditTable();
  ceditUpdateButtons();
}

async function ceditSave() {
  const errEl = document.getElementById("cedit-error");
  errEl.style.display = "none";
  errEl.textContent = "";
  let r;
  try { r = await api("/api/classifier-rules/" + cedit.kind, { rules: cedit.rules }); }
  catch (e) { showCeditError("Save failed: " + (e.message || e)); return; }
  if (!r.ok) { showCeditError(r.error || "Save failed."); return; }
  cedit.pristine = JSON.parse(JSON.stringify(cedit.rules));
  ceditUpdateButtons();
  log("saved " + cedit.kind + " rules · " + r.count + " rule" + (r.count === 1 ? "" : "s"), "ok");
  // Update count in title meta.
  const meta = document.getElementById("cedit-meta");
  if (meta && cedit.meta) {
    meta.textContent = cedit.meta.file + " · " + r.count + " rule" + (r.count === 1 ? "" : "s");
  }
  // Coverage stats reflect the saved YAML; refresh after save (filename only).
  if (cedit.kind === "filename") loadCoverage();
}

function ceditDiscard() {
  if (!window.confirm("Discard your changes and reload from disk?")) return;
  cedit.rules = JSON.parse(JSON.stringify(cedit.pristine));
  document.getElementById("cedit-error").style.display = "none";
  renderCeditTable();
  ceditUpdateButtons();
}

// Inverse-edit entry point: invoked from the documents table's per-row
// "fix" link. Switches to the filename classifier editor, appends a stub
// rule pre-filled with sensible defaults, and opens the regex modal with
// this file's name as the test sample so the user can author a regex
// starting from the misclassification they're looking at.
async function startFixFromFile({ name, path, currentType }) {
  if (cedit.kind && ceditIsDirty()) {
    if (!window.confirm("You have unsaved classifier edits. Discard and start a new rule from this file?")) return;
  }
  // Switch to filename rules editor; loadClassifierRules awaits the GET.
  await new Promise((resolve) => {
    setActiveClassifierEditor("filename");
    // setActiveClassifierEditor calls loadClassifierRules but doesn't await.
    // Poll briefly until cedit is populated for the right kind.
    const start = Date.now();
    (function wait() {
      if (cedit.kind === "filename" && cedit.meta) return resolve();
      if (Date.now() - start > 3000) return resolve();
      setTimeout(wait, 30);
    })();
  });
  if (cedit.kind !== "filename") return;

  // Append a stub rule with defaults. The user will edit name + pattern;
  // the regex modal will help with the pattern. New rule's source_data_type
  // = active SDT (or Vendor if "Any" is active — conservative default).
  const blank = {};
  const activeSdt = (state.activeSdt && state.activeSdt !== "Any" && state.activeSdt !== "All")
    ? state.activeSdt
    : "Vendor";
  for (const col of cedit.meta.columns) {
    if (col === "confidence")    blank[col] = "high";
    else if (col === "match")    blank[col] = "name";
    else if (col === "id")       blank[col] = "fix_" + (name.replace(/[^a-z0-9]+/gi, "_").slice(0, 24).toLowerCase() || "new");
    else if (col === "document_type") blank[col] = "";
    else if (col === "source_data_type") blank[col] = activeSdt;
    else blank[col] = "";
  }
  cedit.rules.push(blank);
  renderCeditTable();
  ceditUpdateButtons();

  // Scroll the new row into view, open the regex modal with the file's
  // name pre-loaded as the sample. The save callback writes the resulting
  // pattern back into the new rule's pattern field.
  setTimeout(() => {
    const tbody = document.querySelector("#cedit-table tbody");
    if (tbody && tbody.lastChild && tbody.lastChild.scrollIntoView) {
      tbody.lastChild.scrollIntoView({ block: "center", behavior: "smooth" });
    }
    openRegexModal("", (newPat) => {
      blank.pattern = newPat;
      renderCeditTable();
      ceditUpdateButtons();
    });
    // Pre-load the sample input with this filename so the user sees match
    // feedback immediately when they type a pattern.
    const sample = document.getElementById("rx-sample");
    if (sample) {
      sample.value = name;
      // Trigger validation so the result panel updates.
      const ev = new Event("input");
      sample.dispatchEvent(ev);
    }
  }, 60);
  log("seeded new filename rule from " + name + (currentType ? " (currently typed " + currentType + ")" : ""), "info");
}

// --- Regex pattern modal ------------------------------------------------
// Opened from the cedit pattern cells (small ⋯ button). Shows the pattern
// in a roomy textarea, validates on every keystroke, lets you test against
// a sample string, and offers a cheat-sheet panel of common idioms.
//
// onSave fires only when the user clicks Save AND the regex parses; the
// caller writes the new value back into the row + state.

let rxModalOnSave = null;          // (newPattern: string) => void
let rxModalOriginal = "";          // initial value, for Cancel comparison

function openRegexModal(initialPattern, onSave) {
  rxModalOnSave = onSave || null;
  rxModalOriginal = initialPattern || "";
  const modal  = document.getElementById("regex-modal");
  const ta     = document.getElementById("rx-pattern");
  const sample = document.getElementById("rx-sample");
  if (!modal || !ta || !sample) return;
  ta.value = rxModalOriginal;
  sample.value = "";
  rxValidateAndShow();
  modal.style.display = "block";
  // Focus + select so common case (replace whole pattern) is one keystroke.
  setTimeout(() => { ta.focus(); ta.select(); }, 0);
}

function closeRegexModal() {
  const modal = document.getElementById("regex-modal");
  if (modal) modal.style.display = "none";
  rxModalOnSave = null;
}

// Run on every keystroke. Updates: parse error message, sample-test result.
function rxValidateAndShow() {
  const ta     = document.getElementById("rx-pattern");
  const errEl  = document.getElementById("rx-error");
  const result = document.getElementById("rx-result");
  const sample = document.getElementById("rx-sample");
  if (!ta || !errEl || !result || !sample) return;

  const pat = ta.value;
  let re = null;
  if (pat.length === 0) {
    ta.classList.remove("invalid");
    errEl.style.display = "none";
  } else {
    try {
      re = new RegExp(pat, "i");
      ta.classList.remove("invalid");
      errEl.style.display = "none";
    } catch (e) {
      ta.classList.add("invalid");
      errEl.textContent = "Regex error: " + e.message;
      errEl.style.display = "block";
    }
  }

  // Sample test (only meaningful when both regex parses and sample exists).
  result.classList.remove("match", "nomatch");
  result.innerHTML = "";
  if (!re) {
    result.textContent = pat.length === 0 ? "Enter a pattern…" : "Fix the regex above to test.";
    return;
  }
  const s = sample.value;
  if (s.length === 0) {
    result.textContent = "Type a sample to test against.";
    return;
  }
  const m = s.match(re);
  if (!m) {
    result.classList.add("nomatch");
    result.textContent = "no match";
    return;
  }
  result.classList.add("match");
  // Render the sample with the matched span highlighted via <mark>.
  const start = m.index;
  const end   = start + m[0].length;
  const before = s.slice(0, start);
  const hit    = s.slice(start, end);
  const after  = s.slice(end);
  result.innerHTML = "match: " +
    escapeHtml(before) +
    '<mark>' + escapeHtml(hit) + '</mark>' +
    escapeHtml(after);
}

// Wire up modal — once at page load.
(function initRegexModal() {
  const modal  = document.getElementById("regex-modal");
  const ta     = document.getElementById("rx-pattern");
  const sample = document.getElementById("rx-sample");
  const xBtn   = document.getElementById("rx-modal-close");
  const cancel = document.getElementById("rx-modal-cancel");
  const save   = document.getElementById("rx-modal-save");
  const backdrop = modal && modal.querySelector(".rx-modal-backdrop");
  if (!modal || !ta || !sample || !xBtn || !cancel || !save) return;

  ta.addEventListener("input",     rxValidateAndShow);
  sample.addEventListener("input", rxValidateAndShow);
  xBtn.addEventListener("click",   closeRegexModal);
  cancel.addEventListener("click", closeRegexModal);
  if (backdrop) backdrop.addEventListener("click", closeRegexModal);

  save.addEventListener("click", () => {
    const pat = ta.value;
    // Block save when regex doesn't parse — empty is allowed (the caller
    // can decide how to handle that; the rule loader will reject it).
    if (pat.length > 0) {
      try { new RegExp(pat, "i"); }
      catch (e) {
        ta.classList.add("invalid");
        const errEl = document.getElementById("rx-error");
        if (errEl) {
          errEl.textContent = "Regex error: " + e.message;
          errEl.style.display = "block";
        }
        return;
      }
    }
    if (rxModalOnSave) rxModalOnSave(pat);
    closeRegexModal();
  });

  document.addEventListener("keydown", (e) => {
    if (modal.style.display === "none") return;
    if (e.key === "Escape") closeRegexModal();
    if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) save.click();
  });
})();

// Renders the Home dashboard: per-table counts + per-doctype confidence mix.
// Idempotent — called every time Home is opened so numbers reflect current DB.
async function loadDashboard() {
  const ephemeralEl = document.getElementById("dash-counts-ephemeral");
  const canonicalEl = document.getElementById("dash-counts-canonical");
  const tbody       = document.querySelector("#dash-types-table tbody");
  const summary     = document.getElementById("dash-class-summary");
  const empty       = document.getElementById("dash-empty");
  const titleEl     = document.getElementById("dash-title");
  if (!ephemeralEl || !canonicalEl || !tbody || !summary) return;

  // Echo the active dashboard scope in the heading. Updated unconditionally
  // here so toggling between scoped views keeps the title in sync even
  // before the API responds.
  if (titleEl) {
    const sdt = state.dashboardSdt || "";
    titleEl.textContent = sdt
      ? "Dashboard — " + sdt
      : "Dashboard";
  }

  let r;
  try {
    const url = state.dashboardSdt
      ? "/api/dashboard?sdt=" + encodeURIComponent(state.dashboardSdt)
      : "/api/dashboard";
    const res = await fetch(url);
    r = await res.json();
  } catch (e) {
    summary.textContent = "Failed to load dashboard: " + (e.message || e);
    return;
  }

  // Two columns:
  //   Ephemeral — populated from ingest/classify, wiped on Purge ALL.
  //   Canonical — taxonomy + curated reference data, persistent across
  //               Purge ALL. buildings + building_addresses are sourced
  //               from Kent's Snowflake (refresh via "Snapshot from
  //               Snowflake"), not from any local ingest, so they live
  //               in canonical alongside document_types and the ignore
  //               lists. document_buildings + buildings are ephemeral
  //               — those rows come from "Match documents → buildings"
  //               and are wiped along with files when the corpus is
  //               purged.
  // SDT-specific drops:
  //   JobFiles → hide products / document_products (manufacturer concept)
  //   Vendor   → hide buildings / building_addresses / document_buildings
  //              / canonical_buildings / ignored_buildings (entirely a
  //              JobFiles concept)
  const CANONICAL = new Set([
    "document_types",
    "canonical_buildings",
    "building_addresses",
    "ignored_buildings",
  ]);
  const PRODUCT_TABLES  = new Set(["products", "document_products"]);
  const BUILDING_TABLES = new Set([
    "canonical_buildings", "buildings", "building_addresses",
    "document_buildings", "ignored_buildings",
  ]);
  // JobFiles + Sales share the same shape — both reference buildings/jobs
  // and never carry products. Vendor is the inverse: products, no buildings.
  const isJobFilesDashboard = state.dashboardSdt === "JobFiles";
  const isSalesDashboard    = state.dashboardSdt === "Sales";
  const isVendorDashboard   = state.dashboardSdt === "Vendor";
  const isBuildingDashboard = isJobFilesDashboard || isSalesDashboard;
  const ephemeral = [];
  const canonical = [];
  for (const [tbl, n] of Object.entries(r.counts)) {
    if (isBuildingDashboard && PRODUCT_TABLES.has(tbl))  continue;
    if (isVendorDashboard   && BUILDING_TABLES.has(tbl)) continue;
    (CANONICAL.has(tbl) ? canonical : ephemeral).push([tbl, n]);
  }
  ephemeral.sort(([a], [b]) => a.localeCompare(b));
  canonical.sort(([a], [b]) => a.localeCompare(b));
  if (r.ignored) {
    canonical.push(["ignored_file_types",   r.ignored.types   || 0]);
    canonical.push(["ignored_folders",      r.ignored.folders || 0]);
  }
  function renderCards(host, entries) {
    host.innerHTML = "";
    for (const [tbl, n] of entries) {
      const card = document.createElement("div");
      card.className = "dash-card";
      card.innerHTML =
        '<div class="label">' + tbl + '</div>' +
        '<div class="value">' + Number(n).toLocaleString() + '</div>';
      host.appendChild(card);
    }
  }

  function renderKpis(kpis) {
    const host = document.getElementById("dash-kpis");
    if (!host) return;
    host.innerHTML = "";
    if (!kpis) return;
    const fmtN  = (n) => Number(n || 0).toLocaleString();
    const fmtPct = (p) => (p == null ? "—" : Number(p).toFixed(1) + "%");

    // KPI shape depends on dashboard scope:
    //   Vendor / All       → product-coverage KPIs (datasheets+manuals → product)
    //   JobFiles / Sales   → building-coverage KPIs (contracts/invoices → building)
    // Same shape (count + pct + sub-line); just different entities. The
    // "Documents classified" headline is universal.
    const isJobFiles = state.dashboardSdt === "JobFiles" || state.dashboardSdt === "Sales";

    const cards = [
      {
        label: "Documents classified",
        value: fmtPct(kpis.pctClassified),
      },
    ];
    if (isJobFiles) {
      cards.push(
        {
          label: "Buildings",
          value: fmtN(kpis.buildings),
        },
        {
          label: "Contracts/POs with a building",
          value: fmtPct(kpis.contractsWithBuilding?.pct),
          sub: kpis.contractsWithBuilding
            ? fmtN(kpis.contractsWithBuilding.withBuilding) + " / " + fmtN(kpis.contractsWithBuilding.total)
              + "  · contract/subcontract/PO/sales_order"
            : undefined,
        },
        {
          label: "Invoices with a building",
          value: fmtPct(kpis.invoicesWithBuilding?.pct),
          sub: kpis.invoicesWithBuilding
            ? fmtN(kpis.invoicesWithBuilding.withBuilding) + " / " + fmtN(kpis.invoicesWithBuilding.total)
            : undefined,
        },
        {
          label: "Proposals/Quotes with a building",
          value: fmtPct(kpis.proposalsWithBuilding?.pct),
          sub: kpis.proposalsWithBuilding
            ? fmtN(kpis.proposalsWithBuilding.withBuilding) + " / " + fmtN(kpis.proposalsWithBuilding.total)
              + "  · proposal/quote/estimate/bid_workup"
            : undefined,
        },
        {
          label: "Buildings with docs",
          value: kpis.buildingsWithDocs ? fmtPct(kpis.buildingsWithDocs.pct) : "—",
          sub: kpis.buildingsWithDocs
            ? fmtN(kpis.buildingsWithDocs.withDocs) + " / " + fmtN(kpis.buildingsWithDocs.total)
              + "  · excludes ignored_buildings"
            : undefined,
        },
      );
    } else {
      cards.push({
        label: "Vendors",
        value: fmtN(kpis.vendors),
      });
      cards.push(
        {
          label: "Products extracted",
          value: fmtN(kpis.products),
        },
        {
          label: "Manuals with a product",
          value: fmtPct(kpis.manualsWithProduct.pct),
          sub: fmtN(kpis.manualsWithProduct.withProduct) + " / " + fmtN(kpis.manualsWithProduct.total)
               + "  · install/program/operations",
        },
        {
          label: "Datasheets with a product",
          value: fmtPct(kpis.datasheetsWithProduct.pct),
          sub: fmtN(kpis.datasheetsWithProduct.withProduct) + " / " + fmtN(kpis.datasheetsWithProduct.total),
        },
        {
          label: "Products with manuals",
          value: kpis.productsWithManuals ? fmtPct(kpis.productsWithManuals.pct) : "—",
          sub: kpis.productsWithManuals
            ? fmtN(kpis.productsWithManuals.withManuals) + " / " + fmtN(kpis.productsWithManuals.total)
            : undefined,
        },
      );
    }
    for (const k of cards) {
      const card = document.createElement("div");
      card.className = "kpi-card";
      let html =
        '<div class="label">' + k.label + '</div>' +
        '<div class="value">' + k.value + '</div>';
      if (k.sub) html += '<div class="sub">' + k.sub + '</div>';
      card.innerHTML = html;
      host.appendChild(card);
    }
  }
  renderCards(ephemeralEl, ephemeral);
  renderCards(canonicalEl, canonical);

  // KPI cards — headline metrics. Order intentionally: classification
  // rate first (overall pipeline health), then taxonomy breadth (vendors,
  // products), then product-coverage rates for the two doc-type families
  // most likely to be tied to a product.
  renderKpis(r.kpis);

  // Classification summary line.
  const c = r.classification;
  if (c.totalDocuments === 0) {
    summary.textContent = "No documents yet.";
    if (empty) empty.style.display = "";
  } else {
    if (empty) empty.style.display = "none";
    summary.innerHTML =
      '<strong>' + c.classified.toLocaleString() + '</strong> of <strong>' +
      c.totalDocuments.toLocaleString() + '</strong> documents classified ' +
      '(' + c.pctClassified.toFixed(1) + '%) — ' +
      c.unclassified.toLocaleString() + ' remaining.';
  }

  // Per-type breakdown table.
  tbody.innerHTML = "";
  const fmtPct = (p) => (p > 0 ? p.toFixed(1) + '%' : '—');
  const fmtN   = (n) => (n > 0 ? Number(n).toLocaleString() : '0');
  for (const t of r.byType) {
    const tr = document.createElement("tr");
    if (t.total === 0) tr.className = "empty-type";
    tr.innerHTML =
      '<td>' + t.name + '</td>' +
      '<td' + (t.total === 0 ? ' class="zero"' : '') + '>' + fmtN(t.total) + '</td>' +
      '<td class="pct">' + fmtPct(t.pctOfClassified) + '</td>' +
      '<td>' + fmtN(t.high)   + ' <span class="pct">' + fmtPct(t.pctHigh)   + '</span></td>' +
      '<td>' + fmtN(t.medium) + ' <span class="pct">' + fmtPct(t.pctMedium) + '</span></td>' +
      '<td>' + fmtN(t.low)    + ' <span class="pct">' + fmtPct(t.pctLow)    + '</span></td>';
    tbody.appendChild(tr);
  }
  enableClientSortAndResize(document.getElementById("dash-types-table"), "dash-types");

  // Files-in-scope doughnut: active (has documents row) vs ignored (file
  // exists but no documents row, ie. extension on the ignore list).
  renderFilesScopeChart(r.filesScope);
}

function renderFilesScopeChart(scope) {
  const summary = document.getElementById("dash-files-summary");
  const wrap    = document.getElementById("dash-files-chart-wrap");
  const canvas  = document.getElementById("dash-files-chart");
  if (!summary || !wrap || !canvas) return;
  if (!scope || !scope.total) {
    summary.textContent = "No files yet.";
    wrap.style.display = "none";
    if (liveCharts["dash-files-chart"]) {
      liveCharts["dash-files-chart"].destroy();
      delete liveCharts["dash-files-chart"];
    }
    return;
  }
  wrap.style.display = "";
  const pctIgnored = scope.total > 0 ? (scope.ignored / scope.total) * 100 : 0;
  summary.innerHTML =
    '<strong>' + scope.active.toLocaleString() + '</strong> active · ' +
    '<strong>' + scope.ignored.toLocaleString() + '</strong> ignored ' +
    '(' + pctIgnored.toFixed(1) + '%) of ' +
    scope.total.toLocaleString() + ' files. Ignored = file row exists but no documents row (extension on the ignore list at last ingest).';

  if (liveCharts["dash-files-chart"]) {
    liveCharts["dash-files-chart"].destroy();
    delete liveCharts["dash-files-chart"];
  }
  liveCharts["dash-files-chart"] = new Chart(canvas, {
    type: "doughnut",
    data: {
      labels: ["Active (in scope)", "Ignored (no documents row)"],
      datasets: [{
        data:   [scope.active, scope.ignored],
        backgroundColor: ["#4a9d4a", "#5a5563"],
        borderWidth: 1,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      animation: false,
      animations: { colors: false, x: false, y: false },
      transitions: { active: { animation: { duration: 0 } } },
      plugins: {
        legend: { position: "bottom", labels: { boxWidth: 12, font: { size: 12 } } },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const n = ctx.parsed;
              const pct = scope.total > 0 ? ((n / scope.total) * 100).toFixed(1) : "0";
              return ctx.label + ": " + n.toLocaleString() + " (" + pct + "%)";
            },
          },
        },
      },
    },
  });
}

async function refreshStatus() {
  const r = await api("/api/status");
  // (Header status pill removed; counts now live in the sidebar tables.)

  const list = document.getElementById("table-list");
  list.innerHTML = "";
  // Alphabetize the sidebar so users find tables by name, not by the order
  // tableCounts() happened to enumerate them. Skip canonical/curated tables —
  // those have explicit rows under "Canonical tables" in the Source data section.
  const CANONICAL_IN_SIDEBAR = new Set([
    "document_types",
    "canonical_buildings",
    "building_addresses",
    "ignored_buildings",
  ]);
  const entries = Object.entries(r.counts)
    .filter(([tbl]) => !CANONICAL_IN_SIDEBAR.has(tbl))
    .sort(([a], [b]) => a.localeCompare(b));
  for (const [tbl, n] of entries) {
    const row = document.createElement("div");
    row.className = "table-row" + (state.table === tbl ? " active" : "");
    row.dataset.table = tbl;
    row.innerHTML = '<span>' + tbl + '</span><span class="count">' + n + '</span>';
    row.addEventListener("click", () => setActiveTable(tbl));
    list.appendChild(row);
  }

  updateClassifyStatus(r.classification);
  updateSidebarSummary(r);
  // If the home dashboard is visible, refresh it on the same beat as the
  // sidebar status. Cheap (one /api/dashboard fetch) and keeps KPIs in
  // step with whatever just changed.
  if (state.view === "help") loadDashboard();
}

// Light-weight version: just refresh the row counts beside each table.
// Avoids rebuilding DOM / event listeners while polling during a long
// extraction run.
async function refreshTableCounts() {
  let r;
  try { r = await api("/api/status"); } catch { return; }
  if (!r || !r.counts) return;
  for (const row of document.querySelectorAll("#table-list .table-row")) {
    const tbl = row.dataset.table;
    if (!tbl || !(tbl in r.counts)) continue;
    const span = row.querySelector(".count");
    if (span) span.textContent = String(r.counts[tbl]);
  }
  updateClassifyStatus(r.classification);
  updateSidebarSummary(r);
}

// Update the Source data summary table (document types + buildings tables + ignore lists).
function updateSidebarSummary(r) {
  if (!r) return;
  if (r.counts) {
    const setCount = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.textContent = String(val ?? 0);
    };
    setCount("doctypes-count",            r.counts.document_types);
    setCount("canonical-buildings-count", r.counts.canonical_buildings);
    setCount("building-addresses-count",  r.counts.building_addresses);
    setCount("ignored-buildings-count",   r.counts.ignored_buildings);
  }
  if (r.ignored) {
    const t = document.getElementById("ignored-types-count");
    const f = document.getElementById("ignored-folders-count");
    if (t) t.textContent = String(r.ignored.types || 0);
    if (f) f.textContent = String(r.ignored.folders || 0);
    const fi = document.getElementById("ignored-files-count");
    if (fi) fi.textContent = String(r.ignored.files || 0);
  }
}

function updateClassifyStatus(c) {
  const el = document.getElementById("classify-status");
  if (!el || !c) return;
  if (c.total === 0) {
    el.textContent = "Classified: 0 / 0 (—)";
    return;
  }
  const pct = ((c.classified / c.total) * 100).toFixed(1);
  let msg = "Classified: " + c.classified + " / " + c.total + " (" + pct + "%)";
  const conf = c.byConfidence;
  if (conf && c.classified > 0) {
    msg += "  ·  " + conf.high + " high · " + conf.medium + " med · " + conf.low + " low";
  }
  el.textContent = msg;
}

// --- Column resizing ------------------------------------------------------

const COL_DEFAULT_WIDTH = 160;
const COL_MIN_WIDTH = 40;
const COL_MAX_WIDTH = 800; // cap so a giant file_path doesn't blow out the layout

function colWidthKey(table, col) { return "colw:" + table + ":" + col; }

function getSavedColWidth(table, col) {
  const v = localStorage.getItem(colWidthKey(table, col));
  if (!v) return null;
  const n = Number(v);
  return Number.isFinite(n) && n >= COL_MIN_WIDTH ? n : null;
}

function saveColWidth(table, col, w) {
  localStorage.setItem(colWidthKey(table, col), String(Math.round(w)));
}

// Column-order persistence. The "order" is the user's preferred sequence
// of column ids; the actual rendered list intersects it with whatever
// columns the current data has, then appends any unknown columns at the
// end so a schema change doesn't hide a new column from view.
function colOrderKey(table) { return "colorder:" + table; }

function getSavedColOrder(table) {
  const raw = localStorage.getItem(colOrderKey(table));
  if (!raw) return null;
  try {
    const v = JSON.parse(raw);
    if (Array.isArray(v) && v.every((x) => typeof x === "string")) return v;
  } catch { /* fall through */ }
  return null;
}

function saveColOrder(table, order) {
  localStorage.setItem(colOrderKey(table), JSON.stringify(order));
}

// Hidden-columns persistence. Stored as an array of colIds the user has
// hidden for this table. We store hidden (not visible) so a schema change
// that adds a new column shows it by default — opposite of how an
// allowlist would behave.
function colHiddenKey(table) { return "colhidden:" + table; }

function getSavedColHidden(table) {
  const raw = localStorage.getItem(colHiddenKey(table));
  if (!raw) return [];
  try {
    const v = JSON.parse(raw);
    if (Array.isArray(v) && v.every((x) => typeof x === "string")) return v;
  } catch { /* fall through */ }
  return [];
}

function saveColHidden(table, hidden) {
  localStorage.setItem(colHiddenKey(table), JSON.stringify(hidden));
}

// Sticky-once defaults: track which "scope keys" we've already applied
// default hides for. The first time the user enters a scope (e.g.
// documents+unclassified), we hide the matching set of columns; from
// then on, the scope key is recorded and we never auto-touch the user's
// hidden list for that scope again — even if they re-enter it later.
function colDefaultsAppliedKey() { return "colhidden:defaults-applied"; }
function getDefaultsApplied() {
  const raw = localStorage.getItem(colDefaultsAppliedKey());
  if (!raw) return new Set();
  try {
    const v = JSON.parse(raw);
    if (Array.isArray(v)) return new Set(v.filter((x) => typeof x === "string"));
  } catch { /* fall through */ }
  return new Set();
}
function markDefaultsApplied(scope) {
  const s = getDefaultsApplied();
  s.add(scope);
  localStorage.setItem(colDefaultsAppliedKey(), JSON.stringify(Array.from(s)));
}

// Apply the default-hide rules for the documents view (sticky-once).
// Scopes:
//   documents:base          → hide file_path + sha256 (any documents view)
//   documents:unclassified  → also hide document_type + confidence
//   documents:sdt-non-vendor → also hide vendor_name (any sdtFilter that
//                              is set and isn't Vendor)
// Each scope only fires once per browser; subsequent visits leave the
// user's existing hidden list alone, so manual show/hide always wins.
function applyDocumentsDefaultHides() {
  if (state.table !== "documents") return;
  const applied = getDefaultsApplied();
  const scopes = [];
  scopes.push({
    key: "documents:base",
    cols: ["file_path", "sha256"],
  });
  if (state.classifiedFilter === "unclassified") {
    scopes.push({
      key: "documents:unclassified",
      cols: ["document_type", "confidence"],
    });
  }
  if (state.sdtFilter && state.sdtFilter !== "Vendor") {
    scopes.push({
      key: "documents:sdt-non-vendor",
      cols: ["vendor_name"],
    });
  }
  let mutated = false;
  const hidden = new Set(getSavedColHidden("documents"));
  for (const s of scopes) {
    if (applied.has(s.key)) continue;
    for (const c of s.cols) hidden.add(c);
    markDefaultsApplied(s.key);
    mutated = true;
  }
  if (mutated) saveColHidden("documents", Array.from(hidden));
}

// Apply saved order to a freshly-loaded column list. Preserves column ids
// that no longer exist (drops them) and appends new column ids that the
// saved order didn't know about — keeps the UI from "losing" a column
// when the schema changes.
function applyColOrder(table, columns) {
  const saved = getSavedColOrder(table);
  if (!saved || !saved.length) return columns.slice();
  const set = new Set(columns);
  const head = saved.filter((c) => set.has(c));
  const tail = columns.filter((c) => !head.includes(c));
  return head.concat(tail);
}

// Wire DnD on a <th> to let the user drag columns into a new order.
// The handler:
//   - reads the current column id list from the header row
//   - moves the dragged id to the drop target position
//   - saves and re-renders by invoking applyNewOrder(nextOrder)
// This works for both the server-paginated data-table (re-render via
// loadRows after saving) and the client-side ephemeral tables (re-render
// by rearranging DOM cells in place).
function attachColDrag(th, table, colId, getColumnIds, applyNewOrder) {
  th.draggable = true;
  th.classList.add("col-draggable");
  th.addEventListener("dragstart", (ev) => {
    ev.dataTransfer.effectAllowed = "move";
    ev.dataTransfer.setData("text/plain", colId);
    th.classList.add("col-dragging");
  });
  th.addEventListener("dragend", () => {
    th.classList.remove("col-dragging");
    document.querySelectorAll("th.col-drop-target").forEach((el) =>
      el.classList.remove("col-drop-target"));
  });
  th.addEventListener("dragover", (ev) => {
    // Required to allow drop. Suppress the default which is "no drop".
    ev.preventDefault();
    ev.dataTransfer.dropEffect = "move";
    th.classList.add("col-drop-target");
  });
  th.addEventListener("dragleave", () => {
    th.classList.remove("col-drop-target");
  });
  th.addEventListener("drop", (ev) => {
    ev.preventDefault();
    th.classList.remove("col-drop-target");
    const dragged = ev.dataTransfer.getData("text/plain");
    if (!dragged || dragged === colId) return;
    const ids = getColumnIds();
    const fromIdx = ids.indexOf(dragged);
    const toIdx   = ids.indexOf(colId);
    if (fromIdx < 0 || toIdx < 0) return;
    const next = ids.slice();
    next.splice(fromIdx, 1);
    next.splice(toIdx, 0, dragged);
    saveColOrder(table, next);
    applyNewOrder(next);
  });
}

function attachResizer(th, table, col) {
  const grip = document.createElement("span");
  grip.className = "col-resizer";
  th.appendChild(grip);

  // --- Drag to resize ---
  grip.addEventListener("pointerdown", (e) => {
    e.preventDefault();
    e.stopPropagation();
    grip.setPointerCapture(e.pointerId);
    grip.classList.add("dragging");
    const startX = e.clientX;
    const startW = th.getBoundingClientRect().width;

    function onMove(ev) {
      const w = Math.max(COL_MIN_WIDTH, Math.min(COL_MAX_WIDTH, startW + (ev.clientX - startX)));
      th.style.width = w + "px";
    }
    function onUp() {
      grip.classList.remove("dragging");
      grip.removeEventListener("pointermove", onMove);
      grip.removeEventListener("pointerup", onUp);
      saveColWidth(table, col, th.getBoundingClientRect().width);
    }
    grip.addEventListener("pointermove", onMove);
    grip.addEventListener("pointerup", onUp);
  });

  // --- Double-click to auto-fit content ---
  grip.addEventListener("dblclick", (e) => {
    e.preventDefault();
    e.stopPropagation();
    autoFitColumn(th, table, col);
  });
}

// Adds click-to-sort + drag-to-resize to any static table (one whose rows
// all live in the DOM at once — i.e. not the server-paginated data-table).
// Each <th> becomes clickable and gets a small ▲ / ▼ arrow when active.
// Sorting is in-place and number-aware: cells whose textContent parses as
// a finite number sort numerically; everything else sorts alphabetically.
//
// The optional second arg is a stable storage key for column widths so
// per-column resize survives page reloads. When omitted, resize is still
// available but widths are not persisted.
function enableClientSortAndResize(table, storageKey) {
  if (!table || table.dataset.sortingWired === "1") return;
  table.dataset.sortingWired = "1";
  const ths = Array.from(table.querySelectorAll("thead th"));

  // Compute a stable column id for each th up front. Stamping it on the
  // element lets the reorder code track headers across DOM moves without
  // relying on positional indices (which change during drag).
  ths.forEach((th, idx) => {
    const original = th.textContent;
    th.dataset.origLabel = original;
    const colId = original.toLowerCase().replace(/\s+/g, "_") || ("col" + idx);
    th.dataset.colId = colId;
  });

  // Apply saved column order before wiring sort handlers, so closures
  // that capture column ids stay correct after the initial reorder.
  if (storageKey) {
    const ids = ths.map((th) => th.dataset.colId);
    const saved = getSavedColOrder(storageKey);
    if (saved && saved.length) {
      const set = new Set(ids);
      const head = saved.filter((c) => set.has(c));
      const tail = ids.filter((c) => !head.includes(c));
      const next = head.concat(tail);
      if (next.join(",") !== ids.join(",")) {
        applyClientColOrder(table, next);
      }
    }
  }
  // Re-read ths after possible reorder so subsequent code uses the new
  // visual sequence.
  const orderedThs = Array.from(table.querySelectorAll("thead th"));

  let sortedColId = null;
  let sortDir = "asc";

  orderedThs.forEach((th, idx) => {
    const original = th.dataset.origLabel ?? th.textContent;
    th.textContent = "";
    const label = document.createElement("span");
    label.className = "sort-label";
    label.textContent = original;
    th.appendChild(label);

    label.addEventListener("click", () => {
      // Resolve the column's CURRENT position at click time — DOM order
      // may have shifted since wiring (drag-reorder).
      const currentThs = Array.from(table.querySelectorAll("thead th"));
      const colIdx = currentThs.indexOf(th);
      if (colIdx < 0) return;
      const myId = th.dataset.colId;
      if (sortedColId === myId) {
        sortDir = sortDir === "asc" ? "desc" : "asc";
      } else {
        sortedColId = myId;
        sortDir = "asc";
      }
      sortRows(table, colIdx, sortDir);
      // Refresh arrows on all headers.
      currentThs.forEach((other) => {
        const ll = other.querySelector(".sort-label");
        if (!ll) return;
        const base = ll.dataset.label || ll.textContent.replace(/ [▲▼]$/, "");
        ll.dataset.label = base;
        if (other === th) {
          ll.textContent = base + (sortDir === "desc" ? " ▼" : " ▲");
        } else {
          ll.textContent = base;
        }
      });
    });

    // Persist resize widths only when caller gives us a key.
    if (storageKey) {
      const colId = th.dataset.colId;
      const saved = getSavedColWidth(storageKey, colId);
      if (saved != null) th.style.width = saved + "px";
      attachResizer(th, storageKey, colId);
      // Drag-reorder: drop handler reorders DOM cells in place and persists.
      attachColDrag(
        th,
        storageKey,
        colId,
        () => Array.from(table.querySelectorAll("thead th")).map((x) => x.dataset.colId),
        (nextOrder) => applyClientColOrder(table, nextOrder),
      );
    } else {
      // Resize without persistence — minimal grip.
      const grip = document.createElement("span");
      grip.className = "col-resizer";
      th.appendChild(grip);
      grip.addEventListener("pointerdown", (e) => {
        e.preventDefault();
        e.stopPropagation();
        grip.setPointerCapture(e.pointerId);
        const startX = e.clientX;
        const startW = th.getBoundingClientRect().width;
        function onMove(ev) {
          const w = Math.max(COL_MIN_WIDTH, startW + (ev.clientX - startX));
          th.style.width = w + "px";
        }
        function onUp() {
          grip.removeEventListener("pointermove", onMove);
          grip.removeEventListener("pointerup", onUp);
        }
        grip.addEventListener("pointermove", onMove);
        grip.addEventListener("pointerup", onUp);
      });
    }
  });
}

// Reorder columns of a static client-side table in place. nextOrder is
// an array of colId strings; missing ids are dropped (rare — only happens
// if a column was removed since order was saved). Header cells and every
// body row's cells get rearranged to match.
function applyClientColOrder(table, nextOrder) {
  const headRow = table.querySelector("thead tr");
  if (!headRow) return;
  const ths = Array.from(headRow.children);
  const idToIdx = new Map(ths.map((th, i) => [th.dataset.colId, i]));
  const headFrag = document.createDocumentFragment();
  for (const id of nextOrder) {
    const i = idToIdx.get(id);
    if (i == null) continue;
    headFrag.appendChild(ths[i]);
  }
  headRow.appendChild(headFrag);
  // Reorder body cells to match. Each row's cell at original index i
  // should now appear at the new sequence — using the same idToIdx map.
  const tbody = table.tBodies[0];
  if (!tbody) return;
  for (const tr of tbody.rows) {
    const cells = Array.from(tr.children);
    if (cells.length !== ths.length) continue; // empty/spanning rows
    const frag = document.createDocumentFragment();
    for (const id of nextOrder) {
      const i = idToIdx.get(id);
      if (i == null) continue;
      frag.appendChild(cells[i]);
    }
    tr.appendChild(frag);
  }
}

function sortRows(table, colIdx, dir) {
  const tbody = table.tBodies[0];
  if (!tbody) return;
  const rows = Array.from(tbody.rows);
  const mul = dir === "desc" ? -1 : 1;
  rows.sort((a, b) => {
    const av = (a.cells[colIdx]?.textContent || "").trim();
    const bv = (b.cells[colIdx]?.textContent || "").trim();
    // Number-aware: if both parse as finite numbers, sort numerically.
    // Strip common formatting (commas, %) before parsing.
    const an = parseFloat(av.replace(/[,%]/g, ""));
    const bn = parseFloat(bv.replace(/[,%]/g, ""));
    if (Number.isFinite(an) && Number.isFinite(bn)) return mul * (an - bn);
    return mul * av.localeCompare(bv, undefined, { numeric: true });
  });
  // Reattach in new order.
  rows.forEach((r) => tbody.appendChild(r));
}

function autoFitColumn(th, table, col) {
  // Measure widest text in this column by walking the column's tds.
  // Use a hidden canvas to measure, then add padding (10px each side, 6px for
  // the resizer strip).
  const idx = Array.from(th.parentNode.children).indexOf(th);
  const tds = document.querySelectorAll("#data-table tbody tr");
  const ctx = autoFitColumn._ctx ||
    (autoFitColumn._ctx = document.createElement("canvas").getContext("2d"));
  // Match table cell font: monospace 12px (see tbody td CSS).
  ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, monospace";
  let max = ctx.measureText(th.textContent.replace(/\s+$/, "")).width; // header
  for (const tr of tds) {
    const cell = tr.children[idx];
    if (!cell) continue;
    const w = ctx.measureText(cell.textContent).width;
    if (w > max) max = w;
  }
  const target = Math.max(
    COL_MIN_WIDTH,
    Math.min(COL_MAX_WIDTH, Math.ceil(max) + 26),
  );
  th.style.width = target + "px";
  saveColWidth(table, col, target);
}

// "Columns" dropdown wiring + render. Wires the button-toggle on first
// call (idempotent). Re-renders the panel checkbox list every loadRows
// so it always reflects the current table's columns and the saved
// hidden set. Toggling a checkbox re-runs loadRows so the table redraws
// without an extra API roundtrip.
function refreshColumnsDropdown(table, allCols) {
  const btn   = document.getElementById("columns-btn");
  const panel = document.getElementById("columns-panel");
  const label = document.getElementById("columns-label");
  if (!btn || !panel) return;

  if (!btn.dataset.wired) {
    btn.dataset.wired = "1";
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      panel.style.display = panel.style.display === "none" ? "" : "none";
    });
    document.addEventListener("click", (e) => {
      if (panel.style.display === "none") return;
      if (panel.contains(e.target) || btn.contains(e.target)) return;
      panel.style.display = "none";
    });
  }

  const hidden = new Set(getSavedColHidden(table));
  panel.innerHTML = "";
  for (const col of allCols) {
    const row = document.createElement("label");
    row.className = "row";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = !hidden.has(col);
    cb.addEventListener("change", () => {
      const next = new Set(getSavedColHidden(table));
      if (cb.checked) next.delete(col);
      else next.add(col);
      saveColHidden(table, Array.from(next));
      loadRows();
    });
    const name = document.createElement("span");
    name.textContent = col;
    row.appendChild(cb);
    row.appendChild(name);
    panel.appendChild(row);
  }
  // Footer actions: show all / reset.
  const actions = document.createElement("div");
  actions.className = "actions";
  const showAll = document.createElement("button");
  showAll.type = "button";
  showAll.textContent = "Show all";
  showAll.addEventListener("click", () => {
    saveColHidden(table, []);
    loadRows();
  });
  actions.appendChild(showAll);
  panel.appendChild(actions);

  // Update the button label so users see when columns are hidden.
  const hiddenCount = allCols.filter((c) => hidden.has(c)).length;
  if (label) {
    label.textContent = hiddenCount === 0
      ? "Columns"
      : "Columns (" + hiddenCount + " hidden)";
  }
  btn.classList.toggle("active", hiddenCount > 0);
}

async function loadRows() {
  if (state.view !== "table") return;
  const r = await api("/api/browse", {
    table: state.table,
    limit: state.limit,
    offset: state.offset,
    filter: state.filter,
    classifiedFilter: state.classifiedFilter,
    minConfidence: state.minConfidence,
    exactConfidence: state.exactConfidence,
    fileTypeFilters: state.fileTypeFilters,
    documentTypeFilters: state.documentTypeFilters,
    productFilter: state.productFilter,
    hasProductFilter: state.hasProductFilter,
    sdtFilter: state.sdtFilter || "",
    buildingFilter: state.buildingFilter || "",
    hasBuildingFilter: state.hasBuildingFilter || "",
    ignoredFilter: state.ignoredFilter,
    // Filter document_types by the active source-data-type. Server treats
    // a missing/empty filter as "no filter" (shows everything). The "All"
    // sentinel from the header switcher passes through as "" for the same
    // server-side semantics.
    sourceDataTypeFilter: (state.table === "document_types" && state.activeSdt && state.activeSdt !== "All")
      ? state.activeSdt
      : "",
    sortColumn: state.sortColumn,
    sortDir: state.sortDir,
  });
  const thead = document.querySelector("#data-table thead");
  const tbody = document.querySelector("#data-table tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";
  if (!r.ok) {
    log(r.error, "err");
    return;
  }
  state.total = r.total;
  // Sticky-once default hides for the documents view (file_path / sha256
  // always; document_type + confidence under classifiedFilter=unclassified;
  // vendor_name under any non-Vendor SDT filter). Each scope fires at most
  // once per browser, so the user's manual show/hide always wins after.
  applyDocumentsDefaultHides();
  // Apply user's saved column order before rendering. Any new columns
  // introduced since the order was saved get appended at the end.
  const orderedCols = applyColOrder(state.table, r.columns);
  // Hidden-column filter is layered on top of order. Stored as colIds the
  // user has explicitly hidden, so a NEW column added by a schema change
  // appears by default — visible-by-default semantics.
  const hiddenSet = new Set(getSavedColHidden(state.table));
  const visibleCols = orderedCols.filter((c) => !hiddenSet.has(c));
  // Refresh the docs-only Columns dropdown panel with the current column
  // list so it reflects the columns of THIS table. Only re-renders if the
  // dropdown is wired (idempotent — first call wires it once).
  refreshColumnsDropdown(state.table, orderedCols);
  // header — each th gets explicit width (saved or default), a resizer grip,
  // and a click-to-sort label. Clicking the active column toggles direction.
  const hr = document.createElement("tr");
  for (const c of visibleCols) {
    const th = document.createElement("th");
    th.title = c;
    th.dataset.colId = c;
    const label = document.createElement("span");
    label.className = "sort-label";
    let arrow = "";
    if (state.sortColumn === c) arrow = state.sortDir === "desc" ? " ▼" : " ▲";
    label.textContent = c + arrow;
    label.addEventListener("click", () => {
      if (state.sortColumn === c) {
        state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      } else {
        state.sortColumn = c;
        state.sortDir = "asc";
      }
      state.offset = 0;
      loadRows();
    });
    th.appendChild(label);
    const w = getSavedColWidth(state.table, c) ?? COL_DEFAULT_WIDTH;
    th.style.width = w + "px";
    attachResizer(th, state.table, c);
    // Make this header draggable. Drop handler saves new order and
    // re-runs loadRows so cells get rebuilt in the new order.
    attachColDrag(
      th,
      state.table,
      c,
      () => orderedCols.slice(),
      () => loadRows(),
    );
    hr.appendChild(th);
  }
  // Trailing actions column.
  //   documents: a single ⋮ button → tight column.
  //   files:     inline folder · ignore links → wider column.
  if (state.table === "documents" || state.table === "files") {
    const thAct = document.createElement("th");
    thAct.className = "actions-cell";
    thAct.style.width = state.table === "documents" ? "40px" : "110px";
    thAct.textContent = "";
    hr.appendChild(thAct);
  }
  thead.appendChild(hr);
  // rows
  if (r.rows.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = visibleCols.length || 1;
    td.className = "empty";
    td.textContent = state.filter
      ? "No rows match the filter."
      : "Table is empty. Run an ingest from the sidebar.";
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    for (const row of r.rows) {
      const tr = document.createElement("tr");
      for (const c of visibleCols) {
        const td = document.createElement("td");
        const v = row[c];
        if (v === null || v === undefined) {
          td.textContent = "NULL";
          td.classList.add("null");
        } else if (c === "extract" &&
                   (state.table === "documents" || state.table === "document_extracts")) {
          // 'ok' → clickable view link; 'err' → muted error label;
          // ''   → em-dash, no extract yet (only documents view emits this).
          // documents-view rows are keyed by row.id; document_extracts rows
          // are keyed by row.document_id.
          const docId = state.table === "documents" ? row.id : row.document_id;
          if (v === "ok") {
            const a = document.createElement("a");
            a.href = "#";
            a.className = "open-link";
            a.textContent = "view";
            a.title = "View extracted text";
            a.addEventListener("click", (ev) => {
              ev.preventDefault();
              showExtract(docId);
            });
            td.appendChild(a);
          } else if (v === "err") {
            td.textContent = "error";
            td.classList.add("null");
            td.title = "Extraction failed; click to see why";
            td.style.cursor = "pointer";
            td.addEventListener("click", () => showExtract(docId));
          } else {
            td.textContent = "—";
            td.classList.add("null");
            td.title = "No extract yet";
          }
        } else if (state.table === "document_extracts" && c === "file_name") {
          // Make the filename the trigger for the extract modal — same
          // modal the documents view uses, just keyed off document_id.
          const a = document.createElement("a");
          a.href = "#";
          a.className = "open-link";
          a.textContent = String(v);
          a.title = "View extracted text for " + v;
          a.addEventListener("click", (ev) => {
            ev.preventDefault();
            showExtract(row.document_id);
          });
          td.appendChild(a);
        } else if (state.table === "document_products" && c === "document_name") {
          // Same treatment in the document_products view: the document
          // name opens the extract modal for that document.
          const a = document.createElement("a");
          a.href = "#";
          a.className = "open-link";
          a.textContent = String(v);
          a.title = "View extracted text for " + v;
          a.addEventListener("click", (ev) => {
            ev.preventDefault();
            showExtract(row.document_id);
          });
          td.appendChild(a);
        } else if (c === "sha256") {
          // Show the first 12 chars of the hex digest in a fixed-width font
          // and put the full 64-char hash in the title for hover.
          const s = String(v);
          td.textContent = s.slice(0, 12) + "…";
          td.title = s;
          td.classList.add("sha-cell");
        } else if (c === "document_type" && state.table === "documents") {
          // Hover shows the canonical description from document_types so
          // the user can recall what an opaque short name (rfc, ntp, coi)
          // actually means without leaving the table.
          td.textContent = String(v);
          const desc = row.document_type_description;
          td.title = desc ? (v + " — " + desc) : String(v);
        } else {
          const openPath = openablePathFor(state.table, c, row);
          if (openPath) {
            const a = document.createElement("a");
            a.href = "#";
            a.className = "open-link";
            a.textContent = String(v);
            a.title = "Open " + openPath;
            a.addEventListener("click", (ev) => {
              ev.preventDefault();
              openFile(openPath);
            });
            td.appendChild(a);
          } else {
            td.textContent = String(v);
            td.title = String(v);
          }
        }
        tr.appendChild(td);
      }
      // Per-row trailing actions.
      // - documents: a single ⋮ menu button opens a popup with all five
      //   row actions (View Document, View Extract, Open Parent Folder,
      //   Ignore File, Add to Rule). Keeps the row narrow even with many
      //   columns visible, and gives each action a full readable label.
      // - files: keeps the inline folder · ignore links (no extract / no
      //   classification on this view, so a menu would be overkill).
      if (state.table === "documents") {
        const td = document.createElement("td");
        td.className = "actions-cell";
        td.style.textAlign = "right";
        const path = row.file_path;
        const fname = row.document_name || row.file_name ||
          (path ? path.split(/[\\/]/).pop() : "");
        const currentType = row.document_type || "";
        const extractStatus = row.extract; // 'ok' | 'err' | '' (no extract yet)

        const menuBtn = document.createElement("button");
        menuBtn.type = "button";
        menuBtn.className = "row-menu-btn";
        menuBtn.title = "Row actions";
        menuBtn.textContent = "⋮";
        menuBtn.addEventListener("click", (ev) => {
          ev.preventDefault();
          ev.stopPropagation();
          openRowMenu(menuBtn, [
            {
              label: "View Document",
              disabled: !path,
              title: path ? ("Open " + path) : "No file path on this row",
              onClick: () => { if (path) openFile(path); },
            },
            {
              label: "View Extract",
              disabled: extractStatus !== "ok" && extractStatus !== "err",
              title: extractStatus === "ok"
                ? "View extracted text"
                : (extractStatus === "err"
                   ? "Extraction failed — show error"
                   : "No extract yet"),
              onClick: () => { if (row.id != null) showExtract(row.id); },
            },
            {
              label: "Open Parent Folder",
              disabled: !path,
              title: "Reveal this file in Explorer",
              onClick: () => { if (path) openFolder(path); },
            },
            {
              label: "Ignore File",
              disabled: !path,
              title: "Add this file to ignored_files (note: manual)",
              onClick: async () => {
                const rr = await api("/api/ignored-files/add", { paths: [path], notes: "manual" });
                if (!rr.ok) { log("ignore failed: " + rr.error, "err"); return; }
                log("ignored " + path, "ok");
                loadRows();
                refreshTableCounts();
              },
            },
            {
              label: "Add to Rule",
              disabled: !fname,
              title: "Open filename rule editor seeded with this file as a test sample",
              onClick: () => startFixFromFile({ name: fname, path, currentType }),
            },
          ]);
        });
        td.appendChild(menuBtn);
        tr.appendChild(td);
      } else if (state.table === "files") {
        const td = document.createElement("td");
        td.className = "actions-cell";
        td.style.textAlign = "right";
        const path = row.path;
        if (path) {
          const folder = document.createElement("a");
          folder.href = "#"; folder.className = "open-link";
          folder.style.fontSize = "11px"; folder.style.marginRight = "8px";
          folder.textContent = "folder";
          folder.title = "Open parent folder (reveal this file in Explorer)";
          folder.addEventListener("click", (ev) => {
            ev.preventDefault();
            openFolder(path);
          });
          td.appendChild(folder);

          const a = document.createElement("a");
          a.href = "#"; a.className = "open-link";
          a.style.fontSize = "11px";
          a.textContent = "ignore";
          a.title = "Ignore this file (note: manual)";
          a.addEventListener("click", async (ev) => {
            ev.preventDefault();
            const rr = await api("/api/ignored-files/add", { paths: [path], notes: "manual" });
            if (!rr.ok) { log("ignore failed: " + rr.error, "err"); return; }
            log("ignored " + path, "ok");
            loadRows();
            refreshTableCounts();
          });
          td.appendChild(a);
        }
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
  }
  // toolbar
  const last = Math.min(state.offset + r.rows.length, state.total);
  document.getElementById("page-meta").textContent =
    state.total === 0 ? "0 rows" : (state.offset + 1) + "–" + last + " of " + state.total;
  document.getElementById("prev").disabled = state.offset <= 0;
  document.getElementById("next").disabled = state.offset + state.limit >= state.total;
}

// --- Charts (Chart.js) ----------------------------------------------------
// Categorical palette used for filetype pies (no semantic mapping).
const EXT_PALETTE = [
  "#d35bd6", "#4a9d4a", "#d6a23a", "#5a8fd6",
  "#c43a3a", "#3ad6c4", "#a35bd6", "#d6d63a",
  "#3a8fd6", "#d63a8f", "#5a5563",
];

// Live Chart instances by canvas id — destroyed before rebuild so we don't
// leak DOM/event listeners when navigating away and back.
const liveCharts = {};

// Drilldown handler shape:
//   filterFor(label) -> partial state delta to apply to documents view,
//                       or null to skip drilldown for that slice.
function makeDoughnut(canvasId, slices, filterFor) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  if (liveCharts[canvasId]) {
    liveCharts[canvasId].destroy();
    delete liveCharts[canvasId];
  }
  const visible = slices.filter((s) => s.value > 0);
  const total = visible.reduce((a, s) => a + s.value, 0);

  const data = {
    labels: visible.map((s) => s.label),
    datasets: [{
      data: visible.map((s) => s.value),
      backgroundColor: visible.map((s) => s.color),
      // Slice separator — picks up the page background so segments
      // stay visually distinct in either theme.
      borderColor: getCssVar("--bg") || "#ffffff",
      borderWidth: 2,
      hoverOffset: 8,
    }],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    cutout: "58%",
    animation: false,
    animations: { colors: false, x: false, y: false },
    transitions: { active: { animation: { duration: 0 } } },
    plugins: {
      legend: {
        position: "bottom",
        labels: {
          color: getCssVar("--text") || "#1a1820",
          font: { size: 11, family: "system-ui, sans-serif" },
          boxWidth: 12, boxHeight: 12, padding: 8,
        },
      },
      tooltip: {
        backgroundColor: getCssVar("--panel") || "#f5f3f7",
        borderColor: getCssVar("--accent") || "#8b2090",
        borderWidth: 1,
        titleColor: getCssVar("--accent") || "#8b2090",
        bodyColor: getCssVar("--text") || "#1a1820",
        padding: 8,
        callbacks: {
          label: (ctx) => {
            const v = ctx.parsed;
            const pct = total > 0 ? ((v / total) * 100).toFixed(1) : "0.0";
            return ctx.label + ": " + v.toLocaleString() + " (" + pct + "%)";
          },
        },
      },
    },
    onClick: (_evt, elements) => {
      if (!elements.length) return;
      const idx = elements[0].index;
      const label = visible[idx].label;
      const delta = filterFor(label);
      if (!delta) return;
      // On the chart view, drilldowns compose into the chart filter chain
      // and re-render. The "View these documents" button is the escape hatch
      // to the documents table.
      if (state.view === "charts") chartDrillIn(delta);
      else drillToDocuments(delta);
    },
  };

  // Inline plugin: draw the total in the doughnut hole.
  // We render in afterDatasetsDraw (NOT afterDraw): in Chart.js v4 the
  // tooltip renders between afterDatasetsDraw and afterDraw, so afterDraw
  // would paint the total on top of the tooltip. Drawing here puts the
  // center text above the slices but below the tooltip.
  const centerTotalPlugin = {
    id: "centerTotal",
    afterDatasetsDraw(chart) {
      const { ctx, chartArea } = chart;
      if (!chartArea) return;
      const cx = (chartArea.left + chartArea.right) / 2;
      const cy = (chartArea.top + chartArea.bottom) / 2;
      // Inner radius: chart-area shorter dimension * cutout fraction / 2.
      const dim = Math.min(
        chartArea.right - chartArea.left,
        chartArea.bottom - chartArea.top,
      );
      const inner = (dim * 0.58) / 2;
      const numSize = Math.max(14, Math.min(28, inner * 0.42));
      const labelSize = Math.max(9, Math.min(13, inner * 0.20));
      ctx.save();
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = getCssVar("--text") || "#1a1820";
      ctx.font = "600 " + numSize + "px system-ui, sans-serif";
      ctx.fillText(total.toLocaleString(), cx, cy - labelSize * 0.4);
      ctx.fillStyle = getCssVar("--muted") || "#6c6478";
      ctx.font = labelSize + "px system-ui, sans-serif";
      ctx.fillText("total", cx, cy + numSize * 0.55);
      ctx.restore();
    },
  };

  liveCharts[canvasId] = new Chart(canvas, {
    type: "doughnut",
    data,
    options,
    plugins: [centerTotalPlugin],
  });
}

// Switch to the documents table with the given filter delta applied.
function drillToDocuments(delta) {
  state.view = "table";
  state.table = "documents";
  state.offset = 0;
  state.filter = "";
  state.classifiedFilter = "all";
  state.minConfidence = "all";
  state.exactConfidence = "";
  state.fileTypeFilters = [];
  state.documentTypeFilters = [];
  state.productFilter = "";
  state.hasProductFilter = "";
  state.sdtFilter = "";
  state.buildingFilter = "";
  state.hasBuildingFilter = "";
  Object.assign(state, delta);

  document.getElementById("filter").value = "";
  document.getElementById("classified-filter").value = state.classifiedFilter;
  refreshMultiButton("filetype");
  refreshMultiButton("doctype");
  document.getElementById("sdt-filter").value = state.sdtFilter || "";

  document.querySelectorAll(".table-row").forEach((el) => {
    el.classList.toggle("active",
      el.dataset.table === "documents" && el.dataset.view !== "charts");
  });
  document.getElementById("charts-row").classList.remove("active");
  document.querySelectorAll(".chart-preset").forEach((el) => el.classList.remove("active"));
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.remove("hidden");
  }
  showTableView();
  loadRows();

  // Surface what filter we landed in via the log strip.
  const summary = [];
  if (delta.classifiedFilter && delta.classifiedFilter !== "all") summary.push(delta.classifiedFilter);
  if (delta.exactConfidence) summary.push("confidence=" + delta.exactConfidence);
  else if (delta.minConfidence && delta.minConfidence !== "all") summary.push("min confidence=" + delta.minConfidence);
  if (delta.fileTypeFilters     && delta.fileTypeFilters.length)     summary.push("file_type=" + delta.fileTypeFilters.join(","));
  if (delta.documentTypeFilters && delta.documentTypeFilters.length) summary.push("document_type=" + delta.documentTypeFilters.join(","));
  if (delta.productFilter)    summary.push("product=" + delta.productFilter);
  if (delta.hasProductFilter) summary.push("has_product=" + delta.hasProductFilter);
  if (delta.sdtFilter)        summary.push("SDT=" + delta.sdtFilter);
  log("drilled to documents · " + summary.join(" · "), "info");
}

// Returns true iff every chart card would be hidden under this filter
// chain. Mirrors the visibility logic in loadCharts() — keep in sync.
// When this fires after a drilldown click, the chart page would render
// blank and the user can't drill further; we route to the documents table
// instead.
function chartFilterHidesAllPies(cf) {
  const classifiedHidden = cf.classifiedFilter !== "all";
  const confidenceHidden = !!cf.exactConfidence
    || cf.classifiedFilter === "unclassified";
  const filetypeHidden   = !!(cf.fileTypeFilters && cf.fileTypeFilters.length);
  const doctypeHidden    = !!(cf.documentTypeFilters && cf.documentTypeFilters.length)
    || cf.classifiedFilter === "unclassified";
  // Product pies hide when productFilter is set. Coverage pie hides when
  // hasProductFilter is set (one side only).
  const productHidden    = !!cf.productFilter;
  const coverageHidden   = !!cf.productFilter || !!cf.hasProductFilter;
  const sdtHidden        = !!cf.sdtFilter;
  return classifiedHidden && confidenceHidden && filetypeHidden && doctypeHidden
    && productHidden && coverageHidden && sdtHidden;
}

// Apply a chart-page drilldown delta into state.chartFilter and reload.
// This composes filters (c1) instead of replacing them. If the resulting
// filter chain would hide every pie (no useful breakdown left), short-
// circuit to the documents table — saves the user a click on the empty
// chart page's "View these documents" button.
function chartDrillIn(delta) {
  const composed = { ...state.chartFilter, ...delta };
  if (chartFilterHidesAllPies(composed)) {
    log("filter chain narrows to a single subset — jumping to documents", "info");
    drillToDocuments({
      classifiedFilter:    composed.classifiedFilter,
      exactConfidence:     composed.exactConfidence,
      fileTypeFilters:     (composed.fileTypeFilters || []).slice(),
      documentTypeFilters: (composed.documentTypeFilters || []).slice(),
      productFilter:       composed.productFilter || "",
      hasProductFilter:    composed.hasProductFilter || "",
      sdtFilter:           composed.sdtFilter || "",
    });
    return;
  }
  state.chartFilter = composed;
  loadCharts();
}

function clearChartFilter() {
  state.chartFilter = {
    classifiedFilter: "all",
    exactConfidence: "",
    fileTypeFilters: [],
    documentTypeFilters: [],
    productFilter: "",
    hasProductFilter: "",
    sdtFilter: "",
    buildingFilter: "",
    hasBuildingFilter: "",
  };
  loadCharts();
}

// Take the user from the chart page to the documents table with the
// current chart-filter chain applied.
function chartViewDocuments() {
  const cf = state.chartFilter;
  drillToDocuments({
    classifiedFilter:    cf.classifiedFilter,
    exactConfidence:     cf.exactConfidence,
    fileTypeFilters:     cf.fileTypeFilters.slice(),
    documentTypeFilters: cf.documentTypeFilters.slice(),
    productFilter:       cf.productFilter || "",
    hasProductFilter:    cf.hasProductFilter || "",
    sdtFilter:           cf.sdtFilter || "",
    buildingFilter:      cf.buildingFilter || "",
    hasBuildingFilter:   cf.hasBuildingFilter || "",
  });
}

function renderBreadcrumb() {
  const cf = state.chartFilter;
  // Each chip: dimension -> clearDelta to merge into chartFilter when × clicks.
  // Multi-select dimensions render as one chip with comma-joined values; the
  // × clears the whole array in one click (per user spec).
  const chips = [];
  if (cf.classifiedFilter && cf.classifiedFilter !== "all") {
    chips.push({ label: cf.classifiedFilter, clear: { classifiedFilter: "all" } });
  }
  if (cf.sdtFilter) {
    chips.push({ label: "SDT=" + cf.sdtFilter, clear: { sdtFilter: "" } });
  }
  if (cf.fileTypeFilters && cf.fileTypeFilters.length) {
    chips.push({ label: "file_type=" + cf.fileTypeFilters.join(", "),
                 clear: { fileTypeFilters: [] } });
  }
  if (cf.documentTypeFilters && cf.documentTypeFilters.length) {
    chips.push({ label: "document_type=" + cf.documentTypeFilters.join(", "),
                 clear: { documentTypeFilters: [] } });
  }
  if (cf.exactConfidence) {
    chips.push({ label: "confidence=" + cf.exactConfidence, clear: { exactConfidence: "" } });
  }
  if (cf.productFilter) {
    chips.push({ label: "product=" + cf.productFilter, clear: { productFilter: "" } });
  }
  if (cf.hasProductFilter) {
    chips.push({ label: "has_product=" + cf.hasProductFilter, clear: { hasProductFilter: "" } });
  }
  if (cf.buildingFilter) {
    // Show first 8 chars of the UID so the chip stays compact; the full
    // UID is the value being filtered on. The Buildings table view shows
    // the human-readable address.
    chips.push({
      label: "building=" + cf.buildingFilter.slice(0, 12) + "…",
      clear: { buildingFilter: "" },
    });
  }
  if (cf.hasBuildingFilter) {
    chips.push({ label: "has_building=" + cf.hasBuildingFilter, clear: { hasBuildingFilter: "" } });
  }

  const crumb = document.getElementById("chart-crumb");
  crumb.innerHTML = "";
  // Root chip stays unclosable — it's the "All documents" anchor.
  const root = document.createElement("span");
  root.className = "chip root";
  root.textContent = "All documents";
  crumb.appendChild(root);

  for (const c of chips) {
    const sep = document.createElement("span");
    sep.className = "chip-sep";
    sep.textContent = "›";
    crumb.appendChild(sep);

    const chip = document.createElement("span");
    chip.className = "chip";
    chip.appendChild(document.createTextNode(c.label));
    const x = document.createElement("button");
    x.className = "chip-x";
    x.type = "button";
    x.title = "Remove this filter";
    x.textContent = "×";
    x.addEventListener("click", () => {
      Object.assign(state.chartFilter, c.clear);
      loadCharts();
    });
    chip.appendChild(x);
    crumb.appendChild(chip);
  }

  const filtered = chips.length > 0;
  document.getElementById("chart-clear").style.display      = filtered ? "" : "none";
  document.getElementById("chart-view-docs").style.display  = filtered ? "" : "none";
}

async function loadCharts() {
  const r = await fetch("/api/stats", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(state.chartFilter),
  }).then((res) => res.json());
  if (!r.ok) { log(r.error || "stats failed", "err"); return; }

  renderBreadcrumb();

  const cf = state.chartFilter;
  const isScoped = cf.classifiedFilter !== "all"
    || cf.exactConfidence
    || (cf.fileTypeFilters && cf.fileTypeFilters.length)
    || (cf.documentTypeFilters && cf.documentTypeFilters.length)
    || cf.productFilter
    || cf.hasProductFilter
    || cf.sdtFilter;
  document.getElementById("chart-meta").textContent = isScoped
    ? "Showing " + r.total + " documents matching the filter chain."
    : "Snapshot of all " + r.total + " documents and " + (r.totalFiles ?? 0) +
      " non-dir files.";

  // Pie 1 — classified vs unclassified. Hidden once the filter has already
  // narrowed to one side (would just be a 100% slice).
  const classifiedCard = document.getElementById("card-classified");
  if (cf.classifiedFilter !== "all") {
    classifiedCard.style.display = "none";
    if (liveCharts["chart-classified"]) {
      liveCharts["chart-classified"].destroy();
      delete liveCharts["chart-classified"];
    }
  } else {
    classifiedCard.style.display = "";
    makeDoughnut(
      "chart-classified",
      [
        { label: "Classified",   value: r.classified,   color: "#4a9d4a" },
        { label: "Unclassified", value: r.unclassified, color: "#5a5563" },
      ],
      (label) => ({
        classifiedFilter: label === "Classified" ? "classified" : "unclassified",
      }),
    );
  }

  // SDT pie — Vendor / JobFiles / Sales / Any / unstamped. Hidden once an
  // SDT filter is active (would just be a 100% slice).
  const sdtCard = document.getElementById("card-sdt");
  const sdtSlices = (r.bySdt || []);
  if (cf.sdtFilter || sdtSlices.length === 0) {
    sdtCard.style.display = "none";
    if (liveCharts["chart-sdt"]) {
      liveCharts["chart-sdt"].destroy();
      delete liveCharts["chart-sdt"];
    }
  } else {
    sdtCard.style.display = "";
    // Stable color per SDT label so Vendor doesn't swap colors when
    // counts change ordering.
    const SDT_COLORS = {
      "Vendor":   "#8b2090",
      "JobFiles": "#3a8fd6",
      "Sales":    "#d68a3a",
      "Any":      "#4a9d4a",
      "(none)":   "#5a5563",
    };
    makeDoughnut(
      "chart-sdt",
      sdtSlices.map((row, i) => ({
        label: row.sdt,
        value: row.n,
        color: SDT_COLORS[row.sdt] || EXT_PALETTE[i % EXT_PALETTE.length],
      })),
      (label) => ({ sdtFilter: label }),
    );
  }

  // Pie 2 — by confidence. Hidden once a specific confidence is selected,
  // and skipped entirely under classifiedFilter='unclassified' (no data).
  const confidenceCard = document.getElementById("card-confidence");
  if (cf.exactConfidence || cf.classifiedFilter === "unclassified") {
    confidenceCard.style.display = "none";
    if (liveCharts["chart-confidence"]) {
      liveCharts["chart-confidence"].destroy();
      delete liveCharts["chart-confidence"];
    }
  } else {
    confidenceCard.style.display = "";
    makeDoughnut(
      "chart-confidence",
      [
        { label: "high",   value: r.byConfidence.high,   color: "#4a9d4a" },
        { label: "medium", value: r.byConfidence.medium, color: "#d6a23a" },
        { label: "low",    value: r.byConfidence.low,    color: "#c43a3a" },
      ],
      // Click "high" → only the rows with confidence='high', not "≥ high".
      (label) => ({ classifiedFilter: "classified", exactConfidence: label }),
    );
  }

  // Pies 3 & 4 — filetype split. Hidden once any fileType is selected.
  // (With multi-select, even one filter narrows the corpus enough that the
  //  filetype pie just shows the selected types as 100% of the subset.)
  const filetypeCard      = document.getElementById("card-filetype");
  const filetypeOtherCard = document.getElementById("card-filetype-other");
  if (cf.fileTypeFilters && cf.fileTypeFilters.length) {
    filetypeCard.style.display      = "none";
    filetypeOtherCard.style.display = "none";
    for (const id of ["chart-filetype", "chart-filetype-other"]) {
      if (liveCharts[id]) { liveCharts[id].destroy(); delete liveCharts[id]; }
    }
  } else {
    filetypeCard.style.display      = "";
    filetypeOtherCard.style.display = (r.byFileTypeOther || []).length ? "" : "none";
    makeDoughnut(
      "chart-filetype",
      (r.byFileTypeTop || []).map((row, i) => ({
        label: row.ext,
        value: row.n,
        color: EXT_PALETTE[i % EXT_PALETTE.length],
      })),
      (label) => ({ fileTypeFilters: addToArray(cf.fileTypeFilters, label) }),
    );
    if ((r.byFileTypeOther || []).length) {
      makeDoughnut(
        "chart-filetype-other",
        (r.byFileTypeOther || []).map((row, i) => ({
          label: row.ext,
          value: row.n,
          color: EXT_PALETTE[i % EXT_PALETTE.length],
        })),
        (label) => ({ fileTypeFilters: addToArray(cf.fileTypeFilters, label) }),
      );
    }
  }

  // Pies 5 & 6 — document type split. Skipped when:
  //   - documentTypeFilter is already set (would be one slice)
  //   - classifiedFilter='unclassified' (no doc-type data to show)
  const doctypeCard      = document.getElementById("card-doctype");
  const doctypeOtherCard = document.getElementById("card-doctype-other");
  const skipDoctype = (cf.documentTypeFilters && cf.documentTypeFilters.length)
    || cf.classifiedFilter === "unclassified";
  if (skipDoctype) {
    doctypeCard.style.display      = "none";
    doctypeOtherCard.style.display = "none";
    for (const id of ["chart-doctype", "chart-doctype-other"]) {
      if (liveCharts[id]) { liveCharts[id].destroy(); delete liveCharts[id]; }
    }
  } else {
    doctypeCard.style.display      = "";
    doctypeOtherCard.style.display = (r.byDocumentTypeOther || []).length ? "" : "none";
    makeDoughnut(
      "chart-doctype",
      (r.byDocumentTypeTop || []).map((row, i) => ({
        label: row.dtype,
        value: row.n,
        color: EXT_PALETTE[i % EXT_PALETTE.length],
      })),
      (label) => ({ documentTypeFilters: addToArray(cf.documentTypeFilters, label) }),
    );
    if ((r.byDocumentTypeOther || []).length) {
      makeDoughnut(
        "chart-doctype-other",
        (r.byDocumentTypeOther || []).map((row, i) => ({
          label: row.dtype,
          value: row.n,
          color: EXT_PALETTE[i % EXT_PALETTE.length],
        })),
        (label) => ({ documentTypeFilters: addToArray(cf.documentTypeFilters, label) }),
      );
    }
  }

  // Pies 7-9 — product family ----------------------------------------
  // All three product pies hide on JobFiles AND Sales: those corpora are
  // per-job/per-deal records (PO, RFI, lien releases, quotes) — products
  // are a manufacturer concept and never link there. Showing "100% without
  // products" is just noise.
  const isJobFilesSdt = cf.sdtFilter === "JobFiles" || cf.sdtFilter === "Sales";

  // (c) Coverage: docs with at least one product link vs without.
  //     Hidden once a productFilter narrows everything to one side.
  const coverageCard = document.getElementById("card-product-coverage");
  if (cf.productFilter || isJobFilesSdt) {
    coverageCard.style.display = "none";
    if (liveCharts["chart-product-coverage"]) {
      liveCharts["chart-product-coverage"].destroy();
      delete liveCharts["chart-product-coverage"];
    }
  } else {
    coverageCard.style.display = "";
    makeDoughnut(
      "chart-product-coverage",
      [
        { label: "with products",    value: r.docsWithProducts ?? 0,    color: "#4a9d4a" },
        { label: "without products", value: r.docsWithoutProducts ?? 0, color: "#5a5563" },
      ],
      (label) => ({ hasProductFilter: label === "with products" ? "yes" : "no" }),
    );
  }

  // (a) Top 20 products. When a productFilter is already set, this pie is
  //     redundant (single slice — the chosen product) so we hide it.
  const productsTopCard = document.getElementById("card-products-top");
  if (cf.productFilter || isJobFilesSdt || !(r.byProductTop || []).length) {
    productsTopCard.style.display = "none";
    if (liveCharts["chart-products-top"]) {
      liveCharts["chart-products-top"].destroy();
      delete liveCharts["chart-products-top"];
    }
  } else {
    productsTopCard.style.display = "";
    makeDoughnut(
      "chart-products-top",
      r.byProductTop.map((row, i) => ({
        // Show "Vendor • Product" as the label so two products with the
        // same model id from different vendors don't collapse into one
        // slice. Tooltip + drilldown still use just the product.
        label: row.vendor + " • " + row.product,
        value: row.n,
        color: EXT_PALETTE[i % EXT_PALETTE.length],
      })),
      // Drilldown: filter to that product. The label includes the vendor
      // prefix; we strip it back to the bare product name for the filter.
      (label) => {
        const i = label.indexOf(" • ");
        const product = i >= 0 ? label.slice(i + 3) : label;
        return { productFilter: product };
      },
    );
  }

  // (b) Products discovered per vendor. Static / informational —
  //     non-clickable. Hidden once productFilter narrows the corpus.
  const productsVendorCard = document.getElementById("card-products-vendor");
  if (cf.productFilter || isJobFilesSdt || !(r.productsPerVendor || []).length) {
    productsVendorCard.style.display = "none";
    if (liveCharts["chart-products-vendor"]) {
      liveCharts["chart-products-vendor"].destroy();
      delete liveCharts["chart-products-vendor"];
    }
  } else {
    productsVendorCard.style.display = "";
    makeDoughnut(
      "chart-products-vendor",
      r.productsPerVendor.map((row, i) => ({
        label: row.vendor,
        value: row.n,
        color: EXT_PALETTE[i % EXT_PALETTE.length],
      })),
      // Vendor-filter not yet supported — return null to disable drilldown.
      () => null,
    );
  }

  // Building cards. These hide outside JobFiles + Sales (Vendor docs don't
  // link to buildings), and the coverage card hides once a buildingFilter
  // or hasBuildingFilter has narrowed the corpus to one side.
  const showBuildings = cf.sdtFilter === "JobFiles"
    || cf.sdtFilter === "Sales"
    || cf.hasBuildingFilter
    || cf.buildingFilter;
  // (a) Coverage: docs with vs. without a building link.
  const buildingCoverageCard = document.getElementById("card-building-coverage");
  if (!showBuildings || cf.buildingFilter || cf.hasBuildingFilter) {
    buildingCoverageCard.style.display = "none";
    if (liveCharts["chart-building-coverage"]) {
      liveCharts["chart-building-coverage"].destroy();
      delete liveCharts["chart-building-coverage"];
    }
  } else {
    buildingCoverageCard.style.display = "";
    makeDoughnut(
      "chart-building-coverage",
      [
        { label: "with buildings",    value: r.docsWithBuildings ?? 0,    color: "#4a9d4a" },
        { label: "without buildings", value: r.docsWithoutBuildings ?? 0, color: "#5a5563" },
      ],
      (label) => ({ hasBuildingFilter: label === "with buildings" ? "yes" : "no" }),
    );
  }

  // (b) Top 20 buildings by doc count. Drilldown sets buildingFilter
  // to the buildings.id so the next chart pass narrows to that site.
  const buildingsTopCard = document.getElementById("card-buildings-top");
  if (!showBuildings || cf.buildingFilter || !(r.byBuildingTop || []).length) {
    buildingsTopCard.style.display = "none";
    if (liveCharts["chart-buildings-top"]) {
      liveCharts["chart-buildings-top"].destroy();
      delete liveCharts["chart-buildings-top"];
    }
  } else {
    buildingsTopCard.style.display = "";
    // Map label → buildings.id so the drilldown can recover the id from
    // the visible label (makeDoughnut's filterFor only sees the label).
    const labelToId = new Map(r.byBuildingTop.map((b) => [b.label, b.building_id]));
    makeDoughnut(
      "chart-buildings-top",
      r.byBuildingTop.map((row, i) => ({
        label: row.label,
        value: row.n,
        color: EXT_PALETTE[i % EXT_PALETTE.length],
      })),
      (label) => {
        const id = labelToId.get(label);
        return id ? { buildingFilter: String(id) } : null;
      },
    );
  }
}

// Append value to arr (returning a new array) unless already present.
// Used by chart drilldowns to compose multi-select filters.
function addToArray(arr, value) {
  const list = (arr || []).slice();
  if (!list.includes(value)) list.push(value);
  return list;
}

// --- Listing file picker (server-side directory browser) ----------------
const LISTING_KEY = "listing:path";

// Update the sidebar display from state.listingPath.
function refreshListingDisplay() {
  const empty = document.getElementById("listing-display-empty");
  const set   = document.getElementById("listing-display-set");
  if (state.listingPath) {
    const parts = state.listingPath.split(/[\\/]/);
    document.getElementById("listing-name").textContent = parts[parts.length - 1] || state.listingPath;
    document.getElementById("listing-path").textContent = state.listingPath;
    empty.style.display = "none";
    set.style.display = "";
  } else {
    empty.style.display = "";
    set.style.display = "none";
  }
}

function setListingPath(p) {
  state.listingPath = p || "";
  if (p) localStorage.setItem(LISTING_KEY, p);
  else   localStorage.removeItem(LISTING_KEY);
  refreshListingDisplay();
  // Auto-set the active SDT based on the chosen folder. Mirrors the
  // ingest-time path detection: if the path sits under a JobFiles or
  // vendors segment, switch SDT to match. No-op for "Other" paths so
  // the user's manual SDT pick isn't trampled.
  if (p) maybeAutoSwitchSdtFromPath(p);
}

// Inspect a path and auto-switch state.activeSdt if the path clearly
// belongs to one corpus. Case-insensitive segment match. Idempotent —
// silent when SDT already matches.
function maybeAutoSwitchSdtFromPath(p) {
  const segs = String(p).split(/[\\/]/).map((s) => s.toLowerCase());
  let target = null;
  if (segs.includes("jobfiles")) target = "JobFiles";
  else if (segs.includes("vendors")) target = "Vendor";
  else if (segs.includes("sales")) target = "Sales";
  if (!target) return;
  if (state.activeSdt === target) return;
  setActiveSdt(target);  // setActiveSdt logs "Using X Rules" for us
}

// Folder picker modal: navigate the filesystem and pick a "vendors" root.
// Clicking a folder descends into it; "Use this folder" selects the
// current dir. The selection POSTs to /api/choose-folder, which returns
// either the existing basic_listing.txt or a freshly-generated one.
async function openListingPicker(startPath) {
  const backdrop = document.createElement("div");
  backdrop.className = "modal-backdrop";

  const panel = document.createElement("div");
  panel.className = "modal-panel";
  panel.innerHTML =
    '<h3><span>Choose vendors folder</span>' +
    '<button class="close" type="button">×</button></h3>' +
    '<div class="modal-cwd" id="modal-cwd"></div>' +
    '<div class="modal-actions">' +
      '<button class="primary" id="modal-use-this" type="button">Use this folder</button>' +
      '<span class="modal-hint">Subfolders are interpreted as vendor names.</span>' +
    '</div>' +
    '<div class="modal-list" id="modal-list"></div>';
  backdrop.appendChild(panel);
  document.body.appendChild(backdrop);

  function close() { backdrop.remove(); }
  backdrop.addEventListener("click", (e) => {
    if (e.target === backdrop) close();
  });
  panel.querySelector(".close").addEventListener("click", close);

  const cwdEl    = panel.querySelector("#modal-cwd");
  const listEl   = panel.querySelector("#modal-list");
  const useBtn   = panel.querySelector("#modal-use-this");
  let currentPath = "";

  useBtn.addEventListener("click", async () => {
    if (!currentPath) {
      log("Pick a folder first (you're at the drives view).", "err");
      return;
    }
    useBtn.disabled = true;
    const orig = useBtn.textContent;
    useBtn.textContent = "Resolving…";
    try {
      const r = await api("/api/choose-folder", { folder: currentPath });
      if (!r.ok) {
        log("choose-folder failed: " + r.error, "err");
        return;
      }
      setListingPath(r.listingPath);
      const verb = r.generated ? "generated" : "found existing";
      log(
        verb + " listing for " + r.folder + " → " + r.listingPath +
        " (" + r.lineCount + " lines)",
        "ok",
      );
      close();
    } catch (err) {
      log("choose-folder failed: " + String(err), "err");
    } finally {
      useBtn.disabled = false;
      useBtn.textContent = orig;
    }
  });

  async function navigate(p) {
    listEl.innerHTML = '<div class="empty">Loading…</div>';
    cwdEl.textContent = p || "(drives)";
    currentPath = p;
    useBtn.disabled = !p;
    const r = await api("/api/list-dir", { path: p });
    if (!r.ok) {
      listEl.innerHTML = '<div class="err">' + escapeHtml(r.error) + '</div>';
      return;
    }
    cwdEl.textContent = r.path || "(drives)";
    currentPath = r.path;
    useBtn.disabled = !r.path;
    listEl.innerHTML = "";

    if (r.parent !== null) {
      const row = document.createElement("div");
      row.className = "row dir";
      row.innerHTML = '<span class="icon">↰</span><span class="name">..</span><span class="meta"></span>';
      row.addEventListener("click", () => navigate(r.parent));
      listEl.appendChild(row);
    }

    for (const d of r.dirs) {
      const row = document.createElement("div");
      row.className = "row dir";
      row.innerHTML =
        '<span class="icon">📁</span>' +
        '<span class="name"></span>' +
        '<span class="meta"></span>';
      row.querySelector(".name").textContent = d.name;
      row.addEventListener("click", () => navigate(d.path));
      listEl.appendChild(row);
    }

    if (r.dirs.length === 0) {
      const e = document.createElement("div");
      e.className = "empty";
      e.textContent = r.parent === null ? "(no drives)" : "(no subfolders)";
      listEl.appendChild(e);
    }
  }

  navigate(startPath || "");
}

function formatSize(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / (1024 * 1024)).toFixed(1) + " MB";
}

function getCssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

// Reads the "Apply ignore lists" checkbox in the Ingest menu. Defaults to
// true when the element doesn't exist (defensive — legacy callers).
function getUseIgnores() {
  const el = document.getElementById("use-ignores");
  return el ? !!el.checked : true;
}

async function runIngest(which) {
  const source = state.listingPath;
  if (!source) { log("No listing selected. Click 'Choose file…' first.", "err"); return; }
  const useIgnores = getUseIgnores();
  log("Running " + which + " ingest" + (useIgnores ? "" : " (ignore lists OFF)") + "…", "info");
  setIngestEnabled(false);
  try {
    const r = await api("/api/ingest", { which, source, useIgnores, sourceDataType: state.activeSdt });
    if (!r.ok) { log(r.error, "err"); return; }
    if (which === "vendors") {
      log("vendors: +" + r.addedVendors + " new, " + r.skippedPaths + " paths skipped", "ok");
    } else if (which === "files") {
      const ignoreNote = (r.folderSkipped || r.docsSkipped)
        ? " · ignore: " + (r.folderSkipped || 0) + " by folder, " + (r.docsSkipped || 0) + " no-doc by ext"
        : "";
      // Incremental ingest report: emphasize what's new vs. what was
      // already in the table (and therefore kept its prior classification
      // and building-link rows).
      log("files: +" + (r.filesAdded || 0) + " new, " + (r.filesUnchanged || 0) +
          " unchanged, " + r.skippedPaths + " paths skipped" + ignoreNote, "ok");
      if (r.bySourceDataType) {
        const parts = [];
        for (const [k, v] of Object.entries(r.bySourceDataType)) {
          if (v > 0) parts.push(k + ": " + v);
        }
        if (parts.length) log("by source data type: " + parts.join(" · "), "info");
      }
    } else if (which === "full") {
      log("full: vendors +" + r.vendors.addedVendors + ", files " + r.files.files + " rows", "ok");
    }
    await refreshStatus();
    await loadRows();
    refreshChartsIfActive();
  } catch (e) {
    log(String(e), "err");
  } finally {
    setIngestEnabled(true);
  }
}

async function runPurge(target) {
  // Tables wiped per target (must match handlePurge in server.js):
  //   all     → vendors, files (cascades to documents, document_extracts, document_products), products
  //   files   → files (cascades to documents, document_extracts, document_products)
  //   vendors → vendors only (FK-checked: requires files+products empty)
  //   hashes  → clears documents.sha256 only (no table wiped)
  // Preserved across all purges: document_types, ignored_file_types, ignored_folders.
  const PURGED_TABLES = {
    all:     ["vendors", "files", "documents", "document_extracts", "products", "document_products"],
    files:   ["files", "documents", "document_extracts", "document_products"],
    vendors: ["vendors"],
  };
  if (target === "hashes") {
    if (!window.confirm("Clear all sha256 hashes from documents? On-disk .hash sidecars stay (use \"Purge text backups\" to also delete those).")) return;
    log("Purging hashes…", "info");
    const r = await api("/api/purge", { target });
    if (!r.ok) { log(r.error, "err"); return; }
    log("purged hashes — cleared documents.sha256 on " + (r.cleared || 0) + " rows. .hash sidecars preserved on disk.", "ok");
    await refreshStatus();
    await loadRows();
    return;
  }
  const label = target === "all"
    ? "the 8 ephemeral tables (vendors, files, documents, document_extracts, products, document_products, buildings, document_buildings) — canonical_buildings, building_addresses, and ignored_* are preserved"
    : target;
  if (!window.confirm("Really delete every row from " + label + "?")) return;
  log("Purging " + label + "…", "info");
  const r = await api("/api/purge", { target });
  if (!r.ok) { log(r.error, "err"); return; }

  const wiped = PURGED_TABLES[target] || [];
  const wipedSummary = wiped.length
    ? "wiped: " + wiped.join(", ")
    : "purge complete";
  // Show post-purge row count for each table that was touched.
  const remaining = wiped
    .map((t) => t + "=" + (r.counts[t] ?? "?"))
    .join(" · ");
  log("purged " + label + ". " + wipedSummary + ". now: " + remaining, "ok");
  await refreshStatus();
  await loadRows();
}

async function runPurgeTextBackups() {
  if (!window.confirm("Really delete the <Vendors>_text folder(s) on disk? This removes the filesystem extraction cache.")) return;
  log("Purging text-extract backups…", "info");
  const r = await api("/api/purge-text-backups", {});
  if (!r.ok) { log(r.error || "purge-text-backups failed", "err"); return; }
  if (r.count === 0) {
    log("no backup folders found to purge", "info");
    return;
  }
  for (const t of r.targets) {
    const kind = t.status.startsWith("error") ? "err"
      : t.status === "deleted" ? "ok" : "info";
    log("  " + t.status + ": " + t.path, kind);
  }
}

// SDT-specific Full Process step list — surfaced under the action row's
// description and re-used as the confirm() prompt. The fixed prefix is
// the same for every SDT (ingest → hash → dedup → classify → convert2txt
// → content classify); the entity-extractor steps that follow depend on
// which corpus the user is in:
//   Vendor   → extract vendors, extract products
//   JobFiles → extract buildings
//   Any/All  → no entity extractors (the rule editors can still be run
//              manually for any subset).
function fullProcessDescription(sdt) {
  const fixed = "Files + documents + hash + dedup + classify + convert2txt + content classify";
  let suffix;
  if (sdt === "Vendor")                            suffix = " + extract vendors + extract products";
  else if (sdt === "JobFiles" || sdt === "Sales")  suffix = " + extract buildings";
  else                                             suffix = "";
  return fixed + suffix + " (SDT: " + (sdt || "—") + ")";
}

// Full Process: files → hash → dedup → classify-by-filename →
// extract → classify-by-content → SDT-specific entity extractors.
// Hash and dedup happen up front so classification and the slow PDF
// extract never run on duplicate copies. Each stage waits for the
// previous to finish; extract and hash poll until idle. Runs every
// stage every time (idempotent by design — only-missing flags keep
// re-runs cheap).
async function runFullProcess() {
  if (!window.confirm("Run the full process?\n\n" + fullProcessDescription(state.activeSdt))) return;
  if (!state.listingPath) {
    log("No listing chosen. Click 'Choose folder…' first.", "err");
    return;
  }
  setIngestEnabled(false);
  log("══ Full Process: starting (SDT: " + state.activeSdt + ")", "info");
  // Keep the home dashboard live-refreshing for the full duration of the
  // pipeline. The extract/hash status pollers handle on/off transitions
  // for those individual stages, but ingest/classify are too short to
  // trip those flags — start polling unconditionally here, stop in finally.
  startDashboardPolling();
  try {
    // 1. Files ingest. Honours ignored_files from prior runs, so paths
    //    we de-duplicated last time stay out of documents this time.
    //    (Vendor creation no longer happens here — see step 5 below.)
    const useIgnores = getUseIgnores();
    log("[1/8] files ingest" + (useIgnores ? "" : " (ignore lists OFF)") + "…", "info");
    {
      const r = await api("/api/ingest", { which: "files", source: state.listingPath, useIgnores, sourceDataType: state.activeSdt });
      if (!r.ok) { log("files ingest failed: " + r.error, "err"); return; }
      log("  +" + (r.filesAdded || 0) + " new files, " + (r.filesUnchanged || 0) +
          " unchanged · " + r.docsCreated + " new documents" +
          ((r.folderSkipped || r.docsSkipped) ? (" · ignore: " + (r.folderSkipped || 0) + " by folder, " + (r.docsSkipped || 0) + " no-doc by ext") : ""), "ok");
    }
    await refreshStatus();
    await loadRows();

    // 2. Hash files (background). Done early so dedup can run before any
    //    classification or extraction work — no point processing copies
    //    that we'll mark ignored a moment later.
    log("[2/8] hash files (background)…", "info");
    {
      const r = await api("/api/hash-start", { onlyMissing: true });
      if (!r.ok && !/already running/i.test(r.error || "")) {
        log("hash-start failed: " + (r.error || "unknown"), "err");
        return;
      }
    }
    await waitForHashIdle();
    log("  hashing phase complete", "ok");
    await refreshStatus();

    // 3. De-duplicate: pull clusters, keep the shortest path in each,
    //    add the rest to ignored_files with note "de-duplicated".
    //    Each /api/ignored-files/add deletes the matching documents row,
    //    so subsequent stages skip the duplicates automatically.
    log("[3/8] de-duplicate by hash…", "info");
    {
      const dr = await fetch("/api/duplicates").then((res) => res.json());
      if (!dr.ok) { log("duplicates lookup failed: " + dr.error, "err"); return; }
      if (dr.clusterCount === 0) {
        log("  no duplicates found", "ok");
      } else {
        let losers = [];
        for (const c of dr.clusters) {
          const paths = c.docs.map((d) => d.file_path).filter(Boolean);
          if (paths.length < 2) continue;
          // Shortest path wins; lex tie-break for determinism.
          paths.sort((a, b) => a.length - b.length || a.localeCompare(b));
          losers = losers.concat(paths.slice(1));
        }
        if (losers.length === 0) {
          log("  " + dr.clusterCount + " cluster(s) found but nothing to ignore", "ok");
        } else {
          const ar = await api("/api/ignored-files/add", { paths: losers, notes: "de-duplicated" });
          if (!ar.ok) { log("ignore-add failed: " + ar.error, "err"); return; }
          log("  " + dr.clusterCount + " cluster(s) · ignored " + ar.added +
              " duplicate path(s) · dropped " + ar.dropped + " documents row(s)", "ok");
        }
      }
    }
    await refreshStatus();
    await loadRows();

    // 4. Classify by filename (now skips duplicates we just dropped)
    log("[4/8] classify by filename…", "info");
    {
      const r = await api("/api/classify", {});
      if (!r.ok) { log("classify failed: " + r.error, "err"); return; }
      log("  classified " + r.updated + ", kept " + (r.kept || 0) +
          " (high " + r.byConfidence.high + " · medium " + r.byConfidence.medium +
          " · low " + r.byConfidence.low + ")", "ok");
    }
    await refreshStatus();

    // 5. Vendor extraction — only meaningful for the Vendor corpus.
    //    Stamps documents.vendor_id from rule matches (path/name/content).
    //    Other SDTs skip; vendors are a manufacturer concept that doesn't
    //    apply to JobFiles.
    if (state.activeSdt === "Vendor") {
      log("[5/8] vendor extractor (rules)…", "info");
      const r = await api("/api/classify-vendors", { mode: "all" });
      if (!r.ok) { log("vendor extract failed: " + r.error, "err"); return; }
      log("  " + r.distinctVendors + " distinct vendors · " +
          r.docsMatched + "/" + r.docsScanned + " docs matched", "ok");
    } else {
      log("[5/8] vendor extractor — skipped (SDT is " + state.activeSdt + ", not Vendor)", "info");
    }
    await refreshStatus();
    await loadRows();

    // 6. Extract PDFs (background, slow). Same dedup-aware corpus.
    log("[6/8] extract document text — PDF + .docx (background, this is the slow stage)…", "info");
    {
      const r = await api("/api/extract-start", { onlyMissing: true });
      if (!r.ok && !/already running/i.test(r.error || "")) {
        log("extract-start failed: " + (r.error || "unknown"), "err");
        return;
      }
    }
    await waitForExtractIdle();
    log("  extraction phase complete", "ok");
    await refreshStatus();

    // 7. Classify by content
    log("[7/8] classify by extract content (low/none only)…", "info");
    {
      const r = await api("/api/classify-by-content", {});
      if (!r.ok) { log("content classify failed: " + r.error, "err"); return; }
      log("  +" + r.updated + " classified from content, " + r.unmatched + " unmatched", "ok");
    }
    await refreshStatus();

    // 8. SDT-specific entity extractor:
    //      Vendor             → product extractor (vendor-scoped rules; M:N)
    //      JobFiles or Sales  → building extractor (both corpora reference
    //                           job sites / projects)
    //      other              → skipped
    if (state.activeSdt === "Vendor") {
      log("[8/8] product extractor…", "info");
      const r = await api("/api/classify-products", {});
      if (!r.ok) { log("product classify failed: " + r.error, "err"); return; }
      log("  " + r.distinctProducts + " products · " + r.totalLinks + " doc-product links · " +
          r.docsWithProducts + "/" + r.docsScanned + " docs matched", "ok");
    } else if (state.activeSdt === "JobFiles" || state.activeSdt === "Sales") {
      log("[8/8] building extractor (background, filename + content)…", "info");
      const r = await api("/api/buildings-match-start", { onlyMissing: true, sdt: ["JobFiles", "Sales"], mode: "all" });
      if (!r.ok && !/already running/i.test(r.error || "")) {
        log("buildings match start failed: " + (r.error || "unknown"), "err");
        return;
      }
      startBuildingsMatchPolling();
      await waitForBuildingsMatchIdle();
      log("  building extraction phase complete", "ok");
    } else {
      log("[8/8] entity extractor — skipped (SDT is " + state.activeSdt + ")", "info");
    }
    await refreshStatus();
    await loadRows();

    log("══ Full Process: done", "ok");
  } catch (e) {
    log("Full Process aborted: " + String(e), "err");
  } finally {
    setIngestEnabled(true);
    // The extract/hash status pollers may have left dashboard polling on
    // if their workers finished mid-process; full-process is over now,
    // so shut it off (the next user action can restart it).
    if (!anyWorkerRunning()) stopDashboardPolling();
  }
}

// Poll extract-status until running flips to false. Bounded by a hard
// timeout (12 hours) so a runaway never hangs the orchestrator forever.
async function waitForExtractIdle() {
  const HARD_TIMEOUT_MS = 12 * 60 * 60 * 1000;
  const POLL_MS = 2000;
  const started = Date.now();
  while (Date.now() - started < HARD_TIMEOUT_MS) {
    let r;
    try {
      r = await fetch("/api/extract-status").then((res) => res.json());
    } catch { /* network blip — retry */ r = null; }
    if (r && r.ok && r.status && !r.status.running) return;
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
  }
  throw new Error("extraction did not finish within 12 hours");
}

async function waitForHashIdle() {
  // Hashing is much faster than extraction (no PDF parsing) so we cap
  // at 1 hour. If a real-world corpus needs more, raise this.
  const HARD_TIMEOUT_MS = 60 * 60 * 1000;
  const POLL_MS = 1000;
  const started = Date.now();
  while (Date.now() - started < HARD_TIMEOUT_MS) {
    let r;
    try {
      r = await fetch("/api/hash-status").then((res) => res.json());
    } catch { r = null; }
    if (r && r.ok && r.status && !r.status.running) return;
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
  }
  throw new Error("hashing did not finish within 1 hour");
}

async function waitForBuildingsMatchIdle() {
  // Building extraction reads already-extracted text + filenames, so it
  // runs in tens of minutes at most for the corpora we expect. 2 hours
  // is a safe ceiling.
  const HARD_TIMEOUT_MS = 2 * 60 * 60 * 1000;
  const POLL_MS = 1500;
  const started = Date.now();
  while (Date.now() - started < HARD_TIMEOUT_MS) {
    let r;
    try {
      r = await fetch("/api/buildings-match-status").then((res) => res.json());
    } catch { r = null; }
    if (r && r.ok && r.status && !r.status.running) return;
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
  }
  throw new Error("buildings match did not finish within 2 hours");
}

// Returns the row's full path string when this cell should render as a
// clickable "open the file" link, or null otherwise.
//
// documents view: every row is a non-dir file by construction. Path lives
//   in row.file_path; we expose links on file_path AND document_name (the
//   stripped/displayed name).
// files table: only rows with is_dir=0 are openable. Path lives in
//   row.path; we expose links on path AND name.
function openablePathFor(table, column, row) {
  if (table === "documents") {
    if (column === "file_path" || column === "document_name") {
      return row.file_path || null;
    }
    return null;
  }
  if (table === "files" && Number(row.is_dir) === 0) {
    if (column === "path" || column === "name") {
      return row.path || null;
    }
  }
  if (table === "document_products") {
    if (column === "file_path") return row.file_path || null;
  }
  return null;
}

async function openFile(path) {
  try {
    const r = await api("/api/open-file", { path });
    if (!r.ok) {
      log("open failed: " + (r.error || "unknown error"), "err");
      return;
    }
    log("opened: " + path, "ok");
  } catch (e) {
    log("open failed: " + String(e), "err");
  }
}

async function openFolder(path) {
  try {
    const r = await api("/api/open-folder", { path });
    if (!r.ok) {
      log("open folder failed: " + (r.error || "unknown error"), "err");
      return;
    }
    log("revealed: " + path, "ok");
  } catch (e) {
    log("open folder failed: " + String(e), "err");
  }
}

// Per-row action menu. anchor is the button the user clicked; items is an
// array of { label, onClick, disabled?, title? }. Renders a single popup
// element that's reused across rows; only one menu is open at a time.
let _rowMenuEl = null;
function closeRowMenu() {
  if (_rowMenuEl && _rowMenuEl.parentNode) _rowMenuEl.parentNode.removeChild(_rowMenuEl);
  _rowMenuEl = null;
}
function openRowMenu(anchor, items) {
  // Toggle off if the same anchor's menu is already open.
  if (_rowMenuEl && _rowMenuEl.dataset.anchorId === anchor.dataset.anchorId) {
    closeRowMenu();
    return;
  }
  closeRowMenu();
  if (!anchor.dataset.anchorId) anchor.dataset.anchorId = "rm-" + Math.random().toString(36).slice(2);

  const menu = document.createElement("div");
  menu.className = "row-menu";
  menu.dataset.anchorId = anchor.dataset.anchorId;
  for (const it of items) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "row-menu-item";
    btn.textContent = it.label;
    if (it.title) btn.title = it.title;
    if (it.disabled) {
      btn.disabled = true;
    } else {
      btn.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        closeRowMenu();
        try { it.onClick(); } catch (e) { log("menu action failed: " + String(e), "err"); }
      });
    }
    menu.appendChild(btn);
  }

  // Position below the anchor, right-aligned. If it would clip the right
  // edge of the viewport, flip to anchor.left.
  document.body.appendChild(menu);
  const ar = anchor.getBoundingClientRect();
  const mr = menu.getBoundingClientRect();
  let left = ar.right - mr.width;
  if (left < 6) left = ar.left;
  let top = ar.bottom + 4;
  if (top + mr.height > window.innerHeight - 6) {
    top = Math.max(6, ar.top - mr.height - 4);
  }
  menu.style.left = Math.round(left) + "px";
  menu.style.top  = Math.round(top)  + "px";
  _rowMenuEl = menu;

  // Click-outside / esc / scroll closes the menu.
  setTimeout(() => {
    document.addEventListener("click", _rowMenuOutsideClick, { once: true });
  }, 0);
  document.addEventListener("keydown", _rowMenuKeyDown);
}
function _rowMenuOutsideClick(ev) {
  if (!_rowMenuEl) return;
  if (_rowMenuEl.contains(ev.target)) return;
  closeRowMenu();
}
function _rowMenuKeyDown(ev) {
  if (ev.key === "Escape") {
    closeRowMenu();
    document.removeEventListener("keydown", _rowMenuKeyDown);
  }
}

// Modal showing the full extracted text + metadata for one document.
async function showExtract(documentId) {
  let r;
  try {
    r = await api("/api/get-extract", { documentId });
  } catch (e) {
    log("get-extract failed: " + String(e), "err");
    return;
  }
  if (!r.ok) {
    log("get-extract failed: " + r.error, "err");
    return;
  }

  const backdrop = document.createElement("div");
  backdrop.className = "modal-backdrop";

  const panel = document.createElement("div");
  panel.className = "modal-panel extract-panel";
  panel.innerHTML =
    '<h3><span></span><button class="close" type="button">×</button></h3>' +
    '<div class="extract-meta"></div>' +
    '<div class="extract-body"></div>';
  backdrop.appendChild(panel);
  document.body.appendChild(panel.parentElement);

  panel.querySelector("h3 span").textContent = r.file_name;
  panel.querySelector(".close").addEventListener("click", () => backdrop.remove());
  backdrop.addEventListener("click", (e) => {
    if (e.target === backdrop) backdrop.remove();
  });

  const meta = panel.querySelector(".extract-meta");
  const metaRows = [
    ["pages", r.page_count ?? "—"],
    ["chars", (r.text || "").length.toLocaleString()],
    ["extracted", r.extracted_at || "—"],
    ["path", r.file_path],
  ];
  if (r.error) metaRows.push(["error", r.error]);
  if (r.metadata && typeof r.metadata === "object") {
    for (const k of ["Title", "Author", "Subject", "Keywords", "Creator", "Producer", "CreationDate"]) {
      if (r.metadata[k]) metaRows.push([k.toLowerCase(), String(r.metadata[k])]);
    }
  }
  for (const [k, v] of metaRows) {
    const row = document.createElement("div");
    row.className = "meta-row";
    row.innerHTML = '<span class="k"></span><span class="v"></span>';
    row.querySelector(".k").textContent = k;
    row.querySelector(".v").textContent = String(v);
    meta.appendChild(row);
  }

  const body = panel.querySelector(".extract-body");
  if (r.error) {
    body.innerHTML = '<div class="extract-err"></div>';
    body.querySelector(".extract-err").textContent = r.error;
  } else if (!r.text) {
    body.innerHTML = '<div class="empty">(no text extracted)</div>';
  } else {
    const pre = document.createElement("pre");
    pre.className = "extract-text";
    pre.textContent = r.text;
    body.appendChild(pre);
  }
}

function setIngestEnabled(on) {
  // Pipeline-action rows in the sidebar's Actions menu get a "disabled"
  // class while a long-running run is in flight. The click delegator
  // ignores rows with that class. Stop extraction stays clickable — it's
  // the way out.
  const PIPELINE_ACTIONS = [
    "run-full-process", "run-vendors", "run-files",
    "classify-all", "classify-by-content", "classify-products",
  ];
  for (const action of PIPELINE_ACTIONS) {
    const el = document.querySelector('.action-row[data-action="' + action + '"]');
    if (el) el.classList.toggle("disabled", !on);
  }
}

// --- PDF extraction (background) -----------------------------------------
let extractPollTimer = 0;

// Tracks whether the poller saw a running extractor on its previous tick.
// On the first tick after running flips to false, refresh once more so
// the final counts (including the trailing few extracts that landed
// during the wind-down) make it into the sidebar.
// --- Unified worker status -------------------------------------------------
// The sidebar's Status section has one dynamic progress bar that flips
// label + bar fill based on which worker is currently running. Each
// per-worker poller below writes its latest payload into workerSnapshot
// and calls renderWorkerStatus(), which picks the active worker (or the
// most-recently-finished one for the idle summary).
const workerSnapshot = {
  extract:   null,   // { ok, status:{running,done,total,cached,failed,skipped,currentDoc,finishedAt}, extractedCount, totalPdfs }
  hash:      null,   // { ok, status:{...}, hashedCount, totalDocs }
  buildings: null,   // { ok, status:{...}, docBuildings, totalBuildings, extractedBuildings }
};

function renderWorkerStatus() {
  const el    = document.getElementById("worker-progress");
  const stopB = document.getElementById("worker-stop");
  if (!el) return;

  // Pick the currently-running worker. Priority is arbitrary — only one
  // background worker should be running at a time today, but in case of
  // overlap we surface extract first (longest-running typically).
  const running =
    (workerSnapshot.extract   && workerSnapshot.extract.status.running)   ? "extract" :
    (workerSnapshot.hash      && workerSnapshot.hash.status.running)      ? "hash" :
    (workerSnapshot.buildings && workerSnapshot.buildings.status.running) ? "buildings" :
    null;

  if (running) {
    const r = workerSnapshot[running];
    const s = r.status;
    let label, processed, total;
    if (running === "extract") {
      label = "Extracting content";
      processed = (s.done || 0) + (s.cached || 0) + (s.failed || 0) + (s.skipped || 0);
      total = s.total;
    } else if (running === "hash") {
      label = "Hashing files";
      processed = (s.done || 0) + (s.cached || 0) + (s.failed || 0) + (s.skipped || 0);
      total = s.total;
    } else {
      label = "Matching buildings";
      processed = s.done || 0;
      total = s.total;
    }
    const pct = total > 0
      ? Math.min(100, Math.max(0, (processed / total) * 100))
      : 0;
    let msg = label + ": " + processed + " / " + total + " (" + pct.toFixed(1) + "%)";
    const parts = [];
    if (running === "buildings") {
      if (s.matched)    parts.push(s.matched + " matched");
      if (s.linksAdded) parts.push(s.linksAdded + " links");
    } else {
      if (s.done)    parts.push(s.done + " done");
      if (s.cached)  parts.push(s.cached + " cached");
      if (s.failed)  parts.push(s.failed + " failed");
      if (s.skipped) parts.push(s.skipped + " skipped");
    }
    if (parts.length) msg += "  ·  " + parts.join(" · ");
    el.classList.add("running");
    el.classList.add("with-progress");
    el.style.setProperty("--progress", pct.toFixed(1) + "%");
    if (s.currentDoc) {
      el.innerHTML = "";
      const top = document.createElement("div"); top.textContent = msg; el.appendChild(top);
      const cur = document.createElement("div"); cur.className = "current"; cur.textContent = "current: " + s.currentDoc.path;
      el.appendChild(cur);
    } else {
      el.textContent = msg;
    }
    // Stop button binds to whichever worker is running.
    stopB.style.display = "";
    stopB.dataset.worker = running;
    stopB.textContent = running === "extract"   ? "Stop extraction"
                      : running === "hash"      ? "Stop hashing"
                      : "Stop matching";
  } else {
    // Idle. Show whichever idle summary has data; default to the
    // extract corpus summary (most useful default).
    el.classList.remove("running");
    el.classList.remove("with-progress");
    el.style.removeProperty("--progress");
    stopB.style.display = "none";
    stopB.dataset.worker = "";
    let msg = "Idle";
    if (workerSnapshot.extract) {
      const r = workerSnapshot.extract;
      const pct = r.totalPdfs > 0 ? (r.extractedCount / r.totalPdfs) * 100 : 0;
      msg = "Extracted: " + r.extractedCount + " / " + r.totalPdfs +
            " docs (" + pct.toFixed(1) + "%)";
    }
    el.textContent = msg;
  }
}

// Format a worker run's elapsed time + per-1000 rate for the
// running→idle transition log line. Falls back gracefully when timestamps
// or counts are missing (e.g. a no-op run that finished with total=0).
function formatRunTiming(startedAt, finishedAt, processed, label) {
  if (!startedAt || !finishedAt || processed <= 0) return null;
  const startMs  = Date.parse(startedAt);
  const finishMs = Date.parse(finishedAt);
  if (!Number.isFinite(startMs) || !Number.isFinite(finishMs) || finishMs < startMs) {
    return null;
  }
  const elapsedMs = finishMs - startMs;
  // Human-friendly elapsed: ms < 1s, "1.2s", "37s", "2m 14s"
  let elapsedStr;
  if (elapsedMs < 1000) elapsedStr = elapsedMs + "ms";
  else if (elapsedMs < 60000) elapsedStr = (elapsedMs / 1000).toFixed(elapsedMs < 10000 ? 2 : 1) + "s";
  else {
    const m = Math.floor(elapsedMs / 60000);
    const s = Math.round((elapsedMs - m * 60000) / 1000);
    elapsedStr = m + "m " + s + "s";
  }
  // Rate per 1000 files. Use the same formatter for consistency.
  const per1000Ms = (elapsedMs / processed) * 1000;
  let rateStr;
  if (per1000Ms < 1000) rateStr = per1000Ms.toFixed(0) + "ms";
  else if (per1000Ms < 60000) rateStr = (per1000Ms / 1000).toFixed(per1000Ms < 10000 ? 2 : 1) + "s";
  else {
    const m = Math.floor(per1000Ms / 60000);
    const s = Math.round((per1000Ms - m * 60000) / 1000);
    rateStr = m + "m " + s + "s";
  }
  return label + ": " + processed.toLocaleString() + " file" +
         (processed === 1 ? "" : "s") + " in " + elapsedStr +
         " (" + rateStr + " per 1,000)";
}

let extractWasRunning = false;

async function refreshExtractStatus() {
  let r;
  try {
    r = await fetch("/api/extract-status").then((res) => res.json());
  } catch { return; }
  if (!r.ok) return;
  const s = r.status;
  workerSnapshot.extract = r;
  // Mirror running state to the action-menu row's enabled/disabled look —
  // grey out "Stop extraction" when no extraction is in flight.
  const stopRow = document.getElementById("action-row-extract-stop");
  if (stopRow) {
    stopRow.classList.toggle("disabled", !s.running);
    stopRow.title = s.running ? "" : "No extraction running";
  }
  if (s.running) refreshTableCounts();
  else if (extractWasRunning) refreshTableCounts();
  renderWorkerStatus();
  // If running state flipped, restart the poll loop with the appropriate
  // cadence (fast while running, slow when idle), and switch the home
  // dashboard's auto-refresh on/off so the user can monitor progress
  // without leaving the home view.
  if (s.running !== extractWasRunning) {
    // Just-finished log: emit elapsed + per-1000 rate so the user has
    // a permanent record after the live status pill rolls over.
    if (extractWasRunning && !s.running) {
      const processed = (s.done || 0) + (s.cached || 0) + (s.failed || 0) + (s.skipped || 0);
      const line = formatRunTiming(s.startedAt, s.finishedAt, processed, "Extracted");
      if (line) log(line, "ok");
    }
    extractWasRunning = s.running;
    restartExtractPolling();
    if (anyWorkerRunning()) startDashboardPolling();
    else stopDashboardPolling();
  } else {
    extractWasRunning = s.running;
  }
}

const EXTRACT_POLL_FAST = 250;   // ms while running — bar moves on cached runs
const EXTRACT_POLL_IDLE = 1500;  // ms while idle — pick up new runs cheaply
function startExtractPolling() {
  if (extractPollTimer) return;
  extractPollTimer = setInterval(refreshExtractStatus, EXTRACT_POLL_IDLE);
}
function restartExtractPolling() {
  if (extractPollTimer) clearInterval(extractPollTimer);
  const ms = extractWasRunning ? EXTRACT_POLL_FAST : EXTRACT_POLL_IDLE;
  extractPollTimer = setInterval(refreshExtractStatus, ms);
}

// --- File hashing (background) -------------------------------------------
let hashPollTimer = 0;
let hashWasRunning = false;
async function refreshHashStatus() {
  let r;
  try {
    r = await fetch("/api/hash-status").then((res) => res.json());
  } catch { return; }
  if (!r.ok) return;
  const s = r.status;
  workerSnapshot.hash = r;
  // Mirror running state to the action-menu row's enabled/disabled look —
  // grey out "Stop hashing" when no hash run is in flight.
  const stopRow = document.getElementById("action-row-hash-stop");
  if (stopRow) {
    stopRow.classList.toggle("disabled", !s.running);
    stopRow.title = s.running ? "" : "No hashing running";
  }
  renderWorkerStatus();
  if (s.running !== hashWasRunning) {
    if (hashWasRunning && !s.running) {
      const processed = (s.done || 0) + (s.cached || 0) + (s.failed || 0) + (s.skipped || 0);
      const line = formatRunTiming(s.startedAt, s.finishedAt, processed, "Hashed");
      if (line) log(line, "ok");
    }
    hashWasRunning = s.running;
    restartHashPolling();
    if (anyWorkerRunning()) startDashboardPolling();
    else stopDashboardPolling();
  } else {
    hashWasRunning = s.running;
  }
}
const HASH_POLL_FAST = 500;
const HASH_POLL_IDLE = 3000;
function startHashPolling() {
  if (hashPollTimer) return;
  hashPollTimer = setInterval(refreshHashStatus, HASH_POLL_IDLE);
}
function restartHashPolling() {
  if (hashPollTimer) clearInterval(hashPollTimer);
  const ms = hashWasRunning ? HASH_POLL_FAST : HASH_POLL_IDLE;
  hashPollTimer = setInterval(refreshHashStatus, ms);
}

async function runExtractStart() {
  log("Starting PDF extraction in the background…", "info");
  try {
    const r = await api("/api/extract-start", { onlyMissing: true });
    if (!r.ok) {
      log("extract-start failed: " + (r.error || "unknown"), "err");
      return;
    }
    log("Extraction started. Status updates below.", "ok");
    await refreshExtractStatus();
  } catch (e) {
    log("extract-start failed: " + String(e), "err");
  }
}

async function runExtractStop() {
  try {
    const r = await api("/api/extract-stop", {});
    if (r.stopped) log("Extraction stop requested — finishing current PDF.", "info");
    else           log("Extraction wasn't running.", "info");
    await refreshExtractStatus();
  } catch (e) {
    log("extract-stop failed: " + String(e), "err");
  }
}

async function runHashStart() {
  log("Starting file hashing in the background…", "info");
  try {
    const r = await api("/api/hash-start", { onlyMissing: true });
    if (!r.ok) {
      log("hash-start failed: " + (r.error || "unknown"), "err");
      return;
    }
    log("Hashing started. Status updates below.", "ok");
    await refreshHashStatus();
  } catch (e) {
    log("hash-start failed: " + String(e), "err");
  }
}

async function runHashStop() {
  try {
    const r = await api("/api/hash-stop", {});
    if (r.stopped) log("Hash stop requested — finishing current file.", "info");
    else           log("Hashing wasn't running.", "info");
    await refreshHashStatus();
  } catch (e) {
    log("hash-stop failed: " + String(e), "err");
  }
}

async function runFindDuplicates() {
  log("Looking for duplicates…", "info");
  try {
    const r = await fetch("/api/duplicates").then((res) => res.json());
    if (!r.ok) { log("duplicates failed: " + r.error, "err"); return; }
    if (r.clusterCount === 0) {
      log("No duplicates found across hashed documents.", "ok");
      return;
    }
    log(
      r.clusterCount + " cluster(s) of duplicates · " +
      r.totalDocs + " total docs involved · " +
      r.wastedCopies + " redundant cop" + (r.wastedCopies === 1 ? "y" : "ies"),
      "ok",
    );
    for (const c of r.clusters.slice(0, 50)) {
      log("  " + c.sha256.slice(0, 12) + "… × " + c.count, "info");
      for (const d of c.docs) {
        log("    [" + (d.vendor || "?") + "/" + (d.document_type || "?") + "] " + d.file_path, "info");
      }
    }
    if (r.clusters.length > 50) {
      log("  …and " + (r.clusters.length - 50) + " more cluster(s) (truncated)", "info");
    }
  } catch (e) {
    log("duplicates failed: " + String(e), "err");
  }
}

async function runBuildingsSnapshot() {
  log("Snapshotting buildings from Snowflake (Kent's CE_HISTORICAL.BUILDING_IDENTITY)…", "info");
  let r;
  try {
    r = await api("/api/buildings-snapshot", {});
  } catch (e) {
    log("buildings snapshot failed: " + String(e), "err");
    return;
  }
  if (!r.ok) {
    log("buildings snapshot failed: " + r.error, "err");
    return;
  }
  log(
    "buildings: " + r.buildings.fetched + " fetched · " +
    r.buildings.inserted + " new · " + r.buildings.updated + " updated",
    "ok",
  );
  log(
    "addresses: " + r.addresses.fetched + " fetched · " +
    r.addresses.inserted + " new · " + r.addresses.updated + " updated · " +
    r.addresses.orphaned + " orphaned",
    "ok",
  );
  if (r.ignored) {
    log(
      "ignored_buildings (Pyrocomm seed): " + r.ignored.resolved +
      " resolved · " + r.ignored.inserted + " newly added",
      "info",
    );
  }
  refreshTableCounts();
  refreshBuildingsMatchStatus();
}

async function runBuildingsMatchStart(mode) {
  const m = mode || "all";
  const r = await api("/api/buildings-match-start", { onlyMissing: true, sdt: ["JobFiles", "Sales"], mode: m });
  if (!r.ok) { log("buildings match start failed: " + r.error, "err"); return; }
  const label = m === "file"    ? "filename / path tokens only"
              : m === "content" ? "extract-text tokens only"
              : "filename + extract text";
  log("Building extractor: " + label + " (background)…", "info");
  startBuildingsMatchPolling();
}

async function runBuildingsMatchStop() {
  const r = await api("/api/buildings-match-stop", {});
  if (!r.ok) { log("buildings match stop failed: " + r.error, "err"); return; }
  log("buildings match: stop signal sent", "info");
}

let buildingsMatchPollTimer = 0;
function startBuildingsMatchPolling() {
  if (buildingsMatchPollTimer) return;
  buildingsMatchPollTimer = setInterval(refreshBuildingsMatchStatus, 1000);
}
function stopBuildingsMatchPolling() {
  if (buildingsMatchPollTimer) { clearInterval(buildingsMatchPollTimer); buildingsMatchPollTimer = 0; }
}
async function refreshBuildingsMatchStatus() {
  let r;
  try { r = await fetch("/api/buildings-match-status").then((res) => res.json()); }
  catch { return; }
  if (!r.ok) return;
  const s = r.status;
  workerSnapshot.buildings = r;
  const stopRow = document.getElementById("action-row-buildings-match-stop");
  if (stopRow) {
    stopRow.classList.toggle("disabled", !s.running);
    stopRow.title = s.running ? "" : "No buildings match running";
  }
  renderWorkerStatus();
  if (!s.running && buildingsMatchPollTimer) {
    stopBuildingsMatchPolling();
    refreshTableCounts();
  }
}

async function runClassify() {
  // Note: the run itself is SDT-agnostic — every file is classified using
  // rules that match its OWN source_data_type. The "Using X Rules" hint
  // here just tells the user which rules they were viewing; the classifier
  // backend runs the full rule set against each file's matching subset.
  log("Classifying all files (filename rules) — Using " + state.activeSdt + " Rules in editor view", "info");
  setIngestEnabled(false);
  try {
    const r = await api("/api/classify", {});
    if (!r.ok) { log(r.error, "err"); return; }
    log(
      "classified " + r.updated + "/" + r.totalFiles + " files " +
      "(high " + r.byConfidence.high + " · medium " + r.byConfidence.medium +
      " · low " + r.byConfidence.low + " · unmatched " + r.byConfidence.none +
      (r.kept ? " · kept " + r.kept : "") + ")",
      "ok",
    );
    if (r.unknownDocType > 0) {
      log("warning: " + r.unknownDocType + " rule(s) referenced an unknown document_type — skipped", "err");
    }
    // Top types
    const top = Object.entries(r.byType).sort((a, b) => b[1] - a[1]).slice(0, 5);
    if (top.length) {
      log("top: " + top.map(([k, v]) => k + " " + v).join(" · "), "info");
    }
    await refreshStatus();
    await loadRows();
    refreshChartsIfActive();
  } catch (e) {
    log(String(e), "err");
  } finally {
    setIngestEnabled(true);
  }
}

async function runClassifyVendors(mode) {
  const m = mode || "all";
  const label = m === "file"    ? "filename / path rules only"
              : m === "content" ? "content rules only"
              : "all rules";
  log("Vendor extractor: " + label, "info");
  setIngestEnabled(false);
  try {
    const r = await api("/api/classify-vendors", { mode: m });
    if (!r.ok) { log(r.error, "err"); return; }
    log(
      "vendors: " + r.distinctVendors + " distinct · " +
      r.docsMatched + "/" + r.docsScanned + " docs matched · " +
      r.rulesLoaded + " rules fired" +
      (r.rulesAvailable && r.rulesAvailable !== r.rulesLoaded
        ? " (of " + r.rulesAvailable + " total)"
        : ""),
      "ok",
    );
    const top = Object.entries(r.byVendor).sort((a, b) => b[1] - a[1]).slice(0, 5);
    if (top.length) {
      log("top vendors by hits: " +
        top.map(([k, v]) => k + " " + v).join(" · "), "info");
    }
    await refreshStatus();
    await loadRows();
    refreshChartsIfActive();
  } catch (e) {
    log(String(e), "err");
  } finally {
    setIngestEnabled(true);
  }
}

async function runClassifyProducts(mode) {
  // Default mode is "all" so legacy callers (Full Process, the old
  // single-button action) keep firing every rule.
  const m = mode || "all";
  const label = m === "file"    ? "filename / path rules only"
              : m === "content" ? "content rules only"
              : "all rules";
  log("Product extractor: " + label + " — Using " + state.activeSdt + " Rules in editor view", "info");
  setIngestEnabled(false);
  try {
    const r = await api("/api/classify-products", { mode: m });
    if (!r.ok) { log(r.error, "err"); return; }
    log(
      "products: " + r.distinctProducts + " distinct · " +
      r.totalLinks + " links · " +
      r.docsWithProducts + "/" + r.docsScanned + " documents matched · " +
      r.rulesLoaded + " rules fired" +
      (r.rulesAvailable && r.rulesAvailable !== r.rulesLoaded
        ? " (of " + r.rulesAvailable + " total)"
        : ""),
      "ok",
    );
    if (r.unknownVendors && r.unknownVendors.length) {
      log("warning: rules reference unknown vendors: " + r.unknownVendors.join(", "), "err");
    }
    const top = Object.entries(r.byVendor).sort((a, b) => b[1] - a[1]).slice(0, 5);
    if (top.length) {
      log("top vendors by product hits: " +
        top.map(([k, v]) => k + " " + v).join(" · "), "info");
    }
    await refreshStatus();
    await loadRows();
    refreshChartsIfActive();
  } catch (e) {
    log(String(e), "err");
  } finally {
    setIngestEnabled(true);
  }
}

async function runClassifyByContent() {
  log("Classifying by extract content (low + unclassified only) — Using " + state.activeSdt + " Rules in editor view", "info");
  setIngestEnabled(false);
  try {
    const r = await api("/api/classify-by-content", {});
    if (!r.ok) { log(r.error, "err"); return; }
    log(
      "content-classified " + r.updated + "/" + r.candidates + " candidates " +
      "(high " + r.byConfidence.high + " · medium " + r.byConfidence.medium +
      " · low " + r.byConfidence.low +
      " · unmatched " + r.unmatched + " · kept " + r.kept + ")",
      "ok",
    );
    if (r.unknownDocType > 0) {
      log("warning: " + r.unknownDocType + " rule(s) referenced an unknown document_type — skipped", "err");
    }
    const top = Object.entries(r.byType).sort((a, b) => b[1] - a[1]).slice(0, 5);
    if (top.length) {
      log("top: " + top.map(([k, v]) => k + " " + v).join(" · "), "info");
    }
    await refreshStatus();
    await loadRows();
    refreshChartsIfActive();
  } catch (e) {
    log(String(e), "err");
  } finally {
    setIngestEnabled(true);
  }
}

// Wire up

// --- Actions menu (inline, nested <details>) -----------------------------
// All action rows in the sidebar carry a data-action attribute mapping to
// a handler in this table. Click delegation on the document root keeps
// wiring trivial — the action rows are static markup, no per-row listener.
const ACTIONS = {
  "purge-all":           () => runPurge("all"),
  "purge-hashes":        () => runPurge("hashes"),
  "purge-text-backups":  runPurgeTextBackups,
  "run-vendors":         () => runIngest("vendors"),
  "run-files":           () => runIngest("files"),
  "run-full-process":    runFullProcess,
  "classify-all":        runClassify,
  "classify-by-content": runClassifyByContent,
  "classify-products":   runClassifyProducts,             // legacy alias = Run All
  "extract-products-all":     () => runClassifyProducts("all"),
  "extract-products-file":    () => runClassifyProducts("file"),
  "extract-products-content": () => runClassifyProducts("content"),
  "extract-vendors-all":      () => runClassifyVendors("all"),
  "extract-vendors-file":     () => runClassifyVendors("file"),
  "extract-vendors-content":  () => runClassifyVendors("content"),
  "extract-start":       runExtractStart,
  "extract-stop":        runExtractStop,
  "hash-start":          runHashStart,
  "hash-stop":           runHashStop,
  "find-duplicates":     runFindDuplicates,
  "buildings-snapshot":         runBuildingsSnapshot,
  "buildings-match-start":      runBuildingsMatchStart,    // legacy alias = Run All
  "extract-buildings-all":      () => runBuildingsMatchStart("all"),
  "extract-buildings-file":     () => runBuildingsMatchStart("file"),
  "extract-buildings-content":  () => runBuildingsMatchStart("content"),
  "buildings-match-stop":       runBuildingsMatchStop,
};

document.addEventListener("click", (e) => {
  const row = e.target.closest(".action-row");
  if (!row || row.classList.contains("disabled")) return;
  const action = row.dataset.action;
  const handler = ACTIONS[action];
  if (handler) handler();
  else log("unknown action: " + action, "err");
});

// Unified Stop button — its data-worker attribute is set by
// renderWorkerStatus() to the currently-running worker. Click dispatches
// to the matching runner.
document.getElementById("worker-stop").addEventListener("click", () => {
  const which = document.getElementById("worker-stop").dataset.worker;
  if (which === "extract")   runExtractStop();
  else if (which === "hash") runHashStop();
  else if (which === "buildings") runBuildingsMatchStop();
});
document.getElementById("refresh").addEventListener("click", async () => {
  await refreshStatus();
  await loadRows();
});
document.getElementById("sdt-switcher").addEventListener("change", (e) => {
  setActiveSdt(e.target.value);
});
// Brand title in the header → home page (All-documents dashboard).
document.getElementById("brand").addEventListener("click", () => {
  setActiveHelp("");
});
document.getElementById("prev").addEventListener("click", () => {
  state.offset = Math.max(0, state.offset - state.limit);
  loadRows();
});
document.getElementById("next").addEventListener("click", () => {
  state.offset += state.limit;
  loadRows();
});
let filterTimer = 0;
document.getElementById("filter").addEventListener("input", (e) => {
  clearTimeout(filterTimer);
  filterTimer = setTimeout(() => {
    state.filter = e.target.value;
    state.offset = 0;
    loadRows();
  }, 200);
});
document.getElementById("classified-filter").addEventListener("change", (e) => {
  state.classifiedFilter = e.target.value;
  state.offset = 0;
  loadRows();
});
document.getElementById("ignored-filter").addEventListener("change", (e) => {
  state.ignoredFilter = e.target.value;
  state.offset = 0;
  loadRows();
});
// --- Multi-select dropdowns (file type + document type) ---------------
// Each dropdown is a button + panel of checkboxes. The button label
// summarizes selection (e.g. "Any file type" / ".pdf" / "3 file types").
// Selections live in state.fileTypeFilters / state.documentTypeFilters
// (arrays). A toggle dispatches to loadRows().
const MULTI_CONFIGS = {
  filetype: {
    stateKey: "fileTypeFilters",
    endpoint: "/api/file-types",
    pluck:    (r) => r.fileTypes.map((ft) => ({ key: ft.ext, label: ft.ext, count: ft.n })),
    emptyLabel: "Any file type",
    singleNoun: "file type",
    pluralNoun: "file types",
  },
  doctype: {
    stateKey: "documentTypeFilters",
    endpoint: "/api/document-types",
    pluck:    (r) => r.documentTypes.map((dt) => ({ key: dt.name, label: dt.name, count: dt.n })),
    emptyLabel: "Any document type",
    singleNoun: "document type",
    pluralNoun: "document types",
  },
};

// Cached option lists so opening the panel doesn't refetch every time.
const multiOptions = { filetype: [], doctype: [] };

async function loadMultiOptions(name) {
  const cfg = MULTI_CONFIGS[name];
  const r = await fetch(cfg.endpoint).then((res) => res.json());
  if (!r.ok) return;
  multiOptions[name] = cfg.pluck(r);
}

function refreshMultiButton(name) {
  const cfg = MULTI_CONFIGS[name];
  const btn   = document.getElementById(name + "-filter-btn");
  const label = document.getElementById(name + "-filter-label");
  const selected = state[cfg.stateKey] || [];
  if (selected.length === 0) {
    label.textContent = cfg.emptyLabel;
    btn.classList.remove("active");
  } else if (selected.length === 1) {
    label.textContent = selected[0];
    btn.classList.add("active");
  } else {
    label.textContent = selected.length + " " + cfg.pluralNoun;
    btn.classList.add("active");
  }
}

function renderMultiPanel(name) {
  const cfg = MULTI_CONFIGS[name];
  const panel = document.getElementById(name + "-filter-panel");
  panel.innerHTML = "";

  // Quick actions: Clear / All — handy when option lists are long.
  const actions = document.createElement("div");
  actions.className = "actions";
  const clearBtn = document.createElement("button");
  clearBtn.type = "button";
  clearBtn.textContent = "Clear";
  clearBtn.addEventListener("click", () => {
    state[cfg.stateKey] = [];
    refreshMultiButton(name);
    renderMultiPanel(name);
    state.offset = 0;
    loadRows();
  });
  actions.appendChild(clearBtn);
  panel.appendChild(actions);

  const selected = new Set(state[cfg.stateKey] || []);
  for (const opt of multiOptions[name]) {
    const row = document.createElement("label");
    row.className = "row";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = selected.has(opt.key);
    cb.addEventListener("change", () => {
      const list = state[cfg.stateKey].slice();
      if (cb.checked) {
        if (!list.includes(opt.key)) list.push(opt.key);
      } else {
        const i = list.indexOf(opt.key);
        if (i >= 0) list.splice(i, 1);
      }
      state[cfg.stateKey] = list;
      refreshMultiButton(name);
      state.offset = 0;
      loadRows();
    });
    const lbl = document.createElement("span");
    lbl.textContent = opt.label;
    const count = document.createElement("span");
    count.className = "count";
    count.textContent = opt.count;
    row.appendChild(cb);
    row.appendChild(lbl);
    row.appendChild(count);
    panel.appendChild(row);
  }
}

function wireMultiDropdown(name) {
  const btn = document.getElementById(name + "-filter-btn");
  const panel = document.getElementById(name + "-filter-panel");
  btn.addEventListener("click", (e) => {
    e.stopPropagation();
    const opening = panel.style.display === "none";
    // Close any other open panels first.
    for (const k of Object.keys(MULTI_CONFIGS)) {
      document.getElementById(k + "-filter-panel").style.display = "none";
    }
    if (opening) {
      renderMultiPanel(name);
      panel.style.display = "";
    }
  });
  // Click outside to close.
  document.addEventListener("click", (e) => {
    if (panel.style.display === "none") return;
    if (panel.contains(e.target) || btn.contains(e.target)) return;
    panel.style.display = "none";
  });
}
document.getElementById("sdt-filter").addEventListener("change", (e) => {
  state.sdtFilter = e.target.value || "";
  state.offset = 0;
  loadRows();
});
document.getElementById("charts-row").addEventListener("click", () => setActiveCharts());
// Every dashboard row carries data-sdt — empty string = "all docs", or
// one of "Vendor" / "JobFiles" / "Any". One handler covers them all.
for (const row of document.querySelectorAll('.table-row[data-view="help"]')) {
  row.addEventListener("click", () => setActiveHelp(row.dataset.sdt || ""));
}
// Same pattern for the three Classification report rows under Results →
// Reports — each carries the SDT scope on its data-sdt attribute.
for (const row of document.querySelectorAll('.table-row[data-view="report-classification"]')) {
  row.addEventListener("click", () => setActiveClassificationReport(row.dataset.sdt || ""));
}
document.getElementById("ignored-row").addEventListener("click", setActiveIgnored);
document.getElementById("ignored-folders-row").addEventListener("click", setActiveIgnoredFolders);
{
  const ifr = document.getElementById("ignored-files-row");
  if (ifr) ifr.addEventListener("click", setActiveIgnoredFiles);
}
{
  const dupRow = document.getElementById("duplicates-row");
  if (dupRow) dupRow.addEventListener("click", setActiveDuplicates);
}
document.getElementById("doctypes-row").addEventListener("click", () => setActiveTable("document_types"));
{
  const cbr = document.getElementById("canonical-buildings-row");
  if (cbr) cbr.addEventListener("click", () => setActiveTable("canonical_buildings"));
  const bar = document.getElementById("building-addresses-row");
  if (bar) bar.addEventListener("click", () => setActiveTable("building_addresses"));
  const ibr = document.getElementById("ignored-buildings-row");
  if (ibr) ibr.addEventListener("click", () => setActiveTable("ignored_buildings"));
}

// Classifier editor sidebar rows + editor toolbar.
// Document-type kinds (filename / content) are flat. Entity extractors
// each have a file-side and a content-side editor that share storage —
// see CLASSIFIER_KINDS in classifier.js.
for (const kind of [
  "filename", "content",
  "vendor_file", "vendor_content",
  "product_file", "product_content",
  "building_file", "building_content",
]) {
  const row = document.getElementById("classifier-row-" + kind);
  if (row) row.addEventListener("click", () => setActiveClassifierEditor(kind));
}
document.getElementById("cedit-add").addEventListener("click", () => {
  if (!cedit.meta) return;
  ceditAddRule();
});
document.getElementById("cedit-save").addEventListener("click", ceditSave);
document.getElementById("cedit-discard").addEventListener("click", ceditDiscard);
document.getElementById("cedit-expand-all").addEventListener("click", () => {
  for (const r of cedit.rules) ceditOpenVendors.add(r.vendor || "(no vendor)");
  renderCeditTable();
});
document.getElementById("cedit-collapse-all").addEventListener("click", () => {
  ceditOpenVendors.clear();
  renderCeditTable();
});

// Browser-level guard: warn if the user closes the tab with unsaved edits.
window.addEventListener("beforeunload", (e) => {
  if (cedit.kind && ceditIsDirty()) {
    e.preventDefault();
    e.returnValue = "";
  }
});

// Persist the "Apply ignore lists" checkbox state. Default ON.
(function initUseIgnoresToggle() {
  const el = document.getElementById("use-ignores");
  if (!el) return;
  const KEY = "enlogosgrag.useIgnores";
  const saved = localStorage.getItem(KEY);
  if (saved !== null) el.checked = saved === "1";
  el.addEventListener("change", () => {
    localStorage.setItem(KEY, el.checked ? "1" : "0");
  });
})();

// Add-ignored-type form. Enter in the ext input also submits.
(function initIgnoredForm() {
  const input = document.getElementById("ignored-input");
  const notes = document.getElementById("ignored-notes");
  const btn   = document.getElementById("ignored-add");
  if (!input || !btn) return;
  async function submit() {
    const ext = input.value.trim();
    if (!ext) return;
    const r = await api("/api/ignored-types/add", { ext, notes: notes.value.trim() });
    if (!r.ok) { log("add failed: " + r.error, "err"); return; }
    log("added " + r.ext + " to ignored types", "ok");
    input.value = "";
    notes.value = "";
    loadIgnoredTypes();
    refreshTableCounts();
  }
  btn.addEventListener("click", submit);
  input.addEventListener("keydown", (e) => { if (e.key === "Enter") submit(); });
  notes.addEventListener("keydown", (e) => { if (e.key === "Enter") submit(); });
})();

(function initIgnoredFoldersForm() {
  const input = document.getElementById("ignored-folders-input");
  const notes = document.getElementById("ignored-folders-notes");
  const btn   = document.getElementById("ignored-folders-add");
  if (!input || !btn) return;
  async function submit() {
    const name = input.value.trim();
    if (!name) return;
    const r = await api("/api/ignored-folders/add", { name, notes: notes.value.trim() });
    if (!r.ok) { log("add failed: " + r.error, "err"); return; }
    log("added folder " + r.name + " to ignore list", "ok");
    input.value = "";
    notes.value = "";
    loadIgnoredFolders();
    refreshTableCounts();
  }
  btn.addEventListener("click", submit);
  input.addEventListener("keydown", (e) => { if (e.key === "Enter") submit(); });
  notes.addEventListener("keydown", (e) => { if (e.key === "Enter") submit(); });
})();

(function initIgnoredFilesForm() {
  const input = document.getElementById("ignored-files-input");
  const notes = document.getElementById("ignored-files-notes");
  const btn   = document.getElementById("ignored-files-add");
  if (!input || !btn) return;
  async function submit() {
    const path = input.value.trim();
    if (!path) return;
    const r = await api("/api/ignored-files/add", { paths: [path], notes: notes.value.trim() || "manual" });
    if (!r.ok) { log("add failed: " + r.error, "err"); return; }
    log("ignored " + path + (r.dropped ? " (also dropped " + r.dropped + " documents row)" : ""), "ok");
    input.value = "";
    if (typeof loadIgnoredFiles === "function") loadIgnoredFiles();
    refreshTableCounts();
  }
  btn.addEventListener("click", submit);
  input.addEventListener("keydown", (e) => { if (e.key === "Enter") submit(); });
  notes.addEventListener("keydown", (e) => { if (e.key === "Enter") submit(); });
})();

// Log strip collapse/expand. Persisted in localStorage so the user's choice
// survives page reloads.
(function initLogToggle() {
  const wrap = document.getElementById("log-wrap");
  const header = document.getElementById("log-header");
  if (!wrap || !header) return;
  const KEY = "enlogosgrag.logCollapsed";
  if (localStorage.getItem(KEY) === "1") wrap.classList.add("collapsed");
  header.addEventListener("click", () => {
    wrap.classList.toggle("collapsed");
    localStorage.setItem(KEY, wrap.classList.contains("collapsed") ? "1" : "0");
  });
})();
for (const el of document.querySelectorAll(".chart-preset")) {
  el.addEventListener("click", () => {
    const preset = CHART_PRESETS[el.dataset.preset];
    if (!preset) return;
    setActiveCharts(preset);
    // Highlight the chosen preset, deactivate the parent "All documents".
    document.querySelectorAll(".chart-preset").forEach((x) => x.classList.remove("active"));
    document.getElementById("charts-row").classList.remove("active");
    el.classList.add("active");
  });
}
document.getElementById("chart-clear").addEventListener("click", clearChartFilter);
document.getElementById("chart-view-docs").addEventListener("click", chartViewDocuments);

// --- Sidebar collapse/expand --------------------------------------------
const SIDEBAR_KEY = "sidebar:collapsed";
function applySidebarState(collapsed) {
  const aside = document.getElementById("sidebar");
  const btn   = document.getElementById("sidebar-toggle");
  aside.classList.toggle("collapsed", collapsed);
  btn.textContent = collapsed ? "›" : "‹";
  btn.title = collapsed ? "Expand sidebar" : "Collapse sidebar";
  // Chart.js re-fits to its container on window resize. Triggering a
  // resize event lets the visible charts grow/shrink to fill the new width.
  window.dispatchEvent(new Event("resize"));
}
document.getElementById("sidebar-toggle").addEventListener("click", () => {
  const next = !document.getElementById("sidebar").classList.contains("collapsed");
  applySidebarState(next);
  localStorage.setItem(SIDEBAR_KEY, next ? "1" : "0");
});

// --- Sidebar resize -----------------------------------------------------
// Drag handle on the sidebar's right edge sets aside.style.width during
// a pointermove, persists the final value in localStorage, and clamps to
// [SIDEBAR_MIN, SIDEBAR_MAX]. The aside.resizing class disables the
// width transition so the panel follows the pointer 1:1.
const SIDEBAR_WIDTH_KEY = "sidebar:width";
const SIDEBAR_MIN = 160;   // narrower than this and the row labels truncate ugly
const SIDEBAR_MAX_FRAC = 0.5; // max half the viewport
function applySidebarWidth(px) {
  const aside = document.getElementById("sidebar");
  const max = Math.floor(window.innerWidth * SIDEBAR_MAX_FRAC);
  const w = Math.max(SIDEBAR_MIN, Math.min(max, px));
  aside.style.width = w + "px";
  // Same trick as collapse: kick a resize so Chart.js re-fits.
  window.dispatchEvent(new Event("resize"));
  return w;
}
// Restore saved width on boot (only when not collapsed — a collapsed
// sidebar's width is fixed at 24px regardless of saved value).
(() => {
  const saved = Number(localStorage.getItem(SIDEBAR_WIDTH_KEY));
  if (Number.isFinite(saved) && saved >= SIDEBAR_MIN) {
    const aside = document.getElementById("sidebar");
    if (!aside.classList.contains("collapsed")) {
      applySidebarWidth(saved);
    }
  }
})();
{
  const handle = document.getElementById("sidebar-resizer");
  const aside  = document.getElementById("sidebar");
  if (handle && aside) {
    handle.addEventListener("pointerdown", (e) => {
      e.preventDefault();
      handle.setPointerCapture(e.pointerId);
      handle.classList.add("dragging");
      aside.classList.add("resizing");
      document.body.classList.add("resizing-sidebar");
      const startX = e.clientX;
      const startW = aside.getBoundingClientRect().width;
      function onMove(ev) {
        applySidebarWidth(startW + (ev.clientX - startX));
      }
      function onUp(ev) {
        handle.removeEventListener("pointermove", onMove);
        handle.removeEventListener("pointerup", onUp);
        handle.removeEventListener("pointercancel", onUp);
        handle.classList.remove("dragging");
        aside.classList.remove("resizing");
        document.body.classList.remove("resizing-sidebar");
        try { handle.releasePointerCapture(ev.pointerId); } catch { /* ignore */ }
        const finalW = aside.getBoundingClientRect().width;
        localStorage.setItem(SIDEBAR_WIDTH_KEY, String(Math.round(finalW)));
      }
      handle.addEventListener("pointermove", onMove);
      handle.addEventListener("pointerup", onUp);
      handle.addEventListener("pointercancel", onUp);
    });
    // Double-click resets to the default width.
    handle.addEventListener("dblclick", () => {
      applySidebarWidth(232);
      localStorage.setItem(SIDEBAR_WIDTH_KEY, "232");
    });
  }
}

// Active source-data-type — UI filter that controls which corpus the user
// is "in" (Vendor / JobFiles / Sales / Any). Filters editor + log surfaces;
// every classify call still routes by the file's own source_data_type.
const SDT_KEY = "enlogosgrag.activeSdt";
const VALID_ACTIVE_SDTS = ["Vendor", "JobFiles", "Sales", "Any", "All"];
function loadActiveSdt() {
  const v = localStorage.getItem(SDT_KEY);
  if (v && VALID_ACTIVE_SDTS.includes(v)) return v;
  return "Vendor";  // default — preserves existing single-corpus behavior
}
function setActiveSdt(sdt) {
  if (!VALID_ACTIVE_SDTS.includes(sdt)) return;
  state.activeSdt = sdt;
  localStorage.setItem(SDT_KEY, sdt);
  applyActiveSdtToUi();
  log("Using " + sdt + " Rules", "info");
}
// Apply state.activeSdt to all the UI surfaces that show it: header
// switcher value, "Using X Rules" banner, classifier editor filter, etc.
// Cheap idempotent re-render — call after any mutation.
function applyActiveSdtToUi() {
  const sw = document.getElementById("sdt-switcher");
  if (sw && sw.value !== state.activeSdt) sw.value = state.activeSdt;
  const text = state.activeSdt === "All"
    ? "Showing All Rules (no filter)"
    : "Using " + state.activeSdt + " Rules";
  for (const id of ["sdt-banner", "sdt-sidebar-banner", "sdt-status-banner"]) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }
  // (Legacy "Ingest Vendors" SDT-gating removed — that row no longer
  // exists in the menu. Vendor extraction is now rule-driven via the
  // Vendor extractor sub-menu and runs only on Vendor-SDT documents
  // automatically.)
  // Full Process description is SDT-aware: which entity extractors fire
  // depends on the active SDT, so we surface the actual step list.
  const fpDesc = document.getElementById("full-process-desc");
  if (fpDesc) fpDesc.textContent = fullProcessDescription(state.activeSdt);
  // If the classifier editor is currently open, re-render so the rule list
  // filters by the new SDT — and refresh the in-editor reminder banner.
  if (cedit && cedit.kind && document.getElementById("classifier-editor-view").style.display !== "none") {
    renderCeditTable();
    updateCeditSdtReminder();
  }
  // If the document_types browse table is currently active, reload so the
  // server-side SDT filter applies to the visible rows.
  if (state.view === "table" && state.table === "document_types") {
    state.offset = 0;
    loadRows();
  }
}

// In-editor reminder banner — sits between the description and the
// save/discard toolbar on the rule editor. Tells the user the rule list
// is filtered by the active source-data-type so they don't think rules
// have gone missing.
function updateCeditSdtReminder() {
  const el = document.getElementById("cedit-sdt-reminder");
  if (!el) return;
  if (!cedit || !cedit.kind) {
    el.style.display = "none";
    return;
  }
  const sdt = state.activeSdt || "Vendor";
  let text;
  if (sdt === "Any") {
    text = "Showing all rules (Source Data Type: Any). Switch in the header to filter.";
  } else {
    text = "Showing rules for Source Data Type \"" + sdt + "\" + Any. Switch in the header to see other rules.";
  }
  el.textContent = text;
  el.style.display = "block";
}

// Boot
(async () => {
  // Force every sidebar <details> closed on page load. The browser's bfcache
  // can otherwise restore a previously-open state when navigating back.
  for (const d of document.querySelectorAll("aside details")) d.open = false;
  applySidebarState(localStorage.getItem(SIDEBAR_KEY) === "1");
  state.activeSdt = loadActiveSdt();
  await refreshStatus();
  // Hide per-table controls until the user picks a table that exposes them.
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only, .toolbar .files-only")) {
    el.classList.add("hidden");
  }
  for (const name of Object.keys(MULTI_CONFIGS)) {
    wireMultiDropdown(name);
    await loadMultiOptions(name);
    refreshMultiButton(name);
  }
  // Listing picker button + restore previously-chosen path.
  document.getElementById("pick-listing").addEventListener("click", () => {
    // Start at the directory of the current selection if any, otherwise default source.
    let start = "";
    if (state.listingPath) {
      const i = Math.max(state.listingPath.lastIndexOf("\\"), state.listingPath.lastIndexOf("/"));
      start = i > 0 ? state.listingPath.slice(0, i) : "";
    }
    openListingPicker(start);
  });

  // Rescan: delete the existing basic_listing.txt and walk the folder again.
  // Operates on whatever folder the current listing lives in. Doesn't touch
  // the DB — user runs files-ingest separately to push the new content.
  document.getElementById("rescan-listing").addEventListener("click", async () => {
    if (!state.listingPath) {
      log("No folder chosen yet — click 'Choose folder…' first.", "err");
      return;
    }
    const i = Math.max(state.listingPath.lastIndexOf("\\"), state.listingPath.lastIndexOf("/"));
    const folder = i > 0 ? state.listingPath.slice(0, i) : "";
    if (!folder) {
      log("Cannot resolve folder from listing path.", "err");
      return;
    }
    if (!window.confirm("Delete " + state.listingPath + " and regenerate it by walking " + folder + "?")) return;
    log("Rescanning " + folder + "…", "info");
    try {
      const r = await api("/api/rescan-listing", { folder });
      if (!r.ok) { log("rescan failed: " + r.error, "err"); return; }
      setListingPath(r.listingPath);
      log(
        (r.deletedExisting ? "regenerated" : "generated") +
          " listing: " + r.listingPath + " (" + r.lineCount + " lines)",
        "ok",
      );
      log("Run 'files ingest' next to push the new listing into the DB.", "info");
    } catch (e) {
      log("rescan failed: " + String(e), "err");
    }
  });
  setListingPath(localStorage.getItem(LISTING_KEY) || "");

  // Reflect the loaded activeSdt to the UI now that all elements exist.
  applyActiveSdtToUi();

  // Replay the persisted log ring buffer before any live entries write —
  // keeps prior-session activity visible after a page reload. Live
  // entries from this session prepend on top.
  restoreLog();

  // Initial extract status fetch + start polling so the running state
  // picks up automatically after a page reload.
  await refreshExtractStatus();
  startExtractPolling();
  await refreshHashStatus();
  startHashPolling();
  // One-shot buildings status so the sidebar reflects the current state
  // without waiting for the user to kick off a match. The matcher poller
  // only runs while a match is in flight.
  await refreshBuildingsMatchStatus();

  // Default landing page is Home (dashboard). Charts/Tables remain one click away.
  setActiveHelp();
})();
</script>
</body>
</html>
`;

// --- Server ----------------------------------------------------------------

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === "GET" && (req.url === "/" || req.url === "/index.html")) {
      sendHtml(res, PAGE);
      return;
    }
    if (req.method === "GET" && (req.url === "/help" || req.url === "/help.html")) {
      const helpPath = path.join(PUBLIC_DIR, "help.html");
      if (fs.existsSync(helpPath)) {
        sendHtml(res, fs.readFileSync(helpPath, "utf8"));
      } else {
        res.writeHead(404, { "content-type": "text/plain" });
        res.end("help.html not found");
      }
      return;
    }
    if (req.method === "GET" && req.url === "/api/status") {
      sendJson(res, 200, handleStatus());
      return;
    }
    if (req.method === "POST" && req.url === "/api/browse") {
      sendJson(res, 200, handleBrowse(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/resolve-source") {
      sendJson(res, 200, handleResolveSource(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/ingest") {
      const body = await readBody(req);
      sendJson(res, 200, handleIngest(body, body.which));
      return;
    }
    if (req.method === "POST" && req.url === "/api/purge") {
      sendJson(res, 200, handlePurge(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/purge-text-backups") {
      sendJson(res, 200, handlePurgeTextBackups());
      return;
    }
    if (req.method === "POST" && req.url === "/api/classify") {
      sendJson(res, 200, handleClassify());
      return;
    }
    if (req.method === "POST" && req.url === "/api/classify-by-content") {
      sendJson(res, 200, handleClassifyByContent());
      return;
    }
    if (req.method === "POST" && req.url === "/api/classify-vendors") {
      sendJson(res, 200, handleClassifyVendors(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/classify-products") {
      sendJson(res, 200, handleClassifyProducts(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/extract-start") {
      sendJson(res, 200, handleExtractStart(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/extract-stop") {
      sendJson(res, 200, handleExtractStop());
      return;
    }
    if (req.method === "GET" && req.url === "/api/extract-status") {
      sendJson(res, 200, handleExtractStatus());
      return;
    }
    if (req.method === "POST" && req.url === "/api/hash-start") {
      sendJson(res, 200, handleHashStart(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/hash-stop") {
      sendJson(res, 200, handleHashStop());
      return;
    }
    if (req.method === "GET" && req.url === "/api/hash-status") {
      sendJson(res, 200, handleHashStatus());
      return;
    }
    if (req.method === "POST" && req.url === "/api/buildings-snapshot") {
      sendJson(res, 200, await handleBuildingsSnapshot());
      return;
    }
    if (req.method === "POST" && req.url === "/api/buildings-match-start") {
      sendJson(res, 200, handleBuildingsMatchStart(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/buildings-match-stop") {
      sendJson(res, 200, handleBuildingsMatchStop());
      return;
    }
    if (req.method === "GET" && req.url === "/api/buildings-match-status") {
      sendJson(res, 200, handleBuildingsMatchStatus());
      return;
    }
    if (req.method === "GET" && req.url === "/api/duplicates") {
      sendJson(res, 200, handleDuplicates());
      return;
    }
    if (req.method === "POST" && req.url === "/api/get-extract") {
      sendJson(res, 200, handleGetExtract(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/open-file") {
      sendJson(res, 200, await handleOpenFile(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/open-folder") {
      sendJson(res, 200, await handleOpenFolder(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/upload-listing") {
      sendJson(res, 200, handleUploadListing(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/list-dir") {
      sendJson(res, 200, handleListDir(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/choose-folder") {
      sendJson(res, 200, handleChooseFolder(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/rescan-listing") {
      sendJson(res, 200, handleRescanListing(await readBody(req)));
      return;
    }
    if (req.method === "GET" && req.url === "/api/stats") {
      sendJson(res, 200, handleStats());
      return;
    }
    if (req.method === "GET" && req.url === "/api/file-types") {
      sendJson(res, 200, handleFileTypes());
      return;
    }
    if (req.method === "GET" && req.url === "/api/document-types") {
      sendJson(res, 200, handleDocumentTypeOptions());
      return;
    }
    if (req.method === "POST" && req.url === "/api/stats") {
      sendJson(res, 200, handleStats(await readBody(req)));
      return;
    }
    if (req.method === "GET" && req.url.startsWith("/api/dashboard")) {
      // Accepts an optional ?sdt=Vendor|JobFiles|Sales|Any|All filter.
      // Anything unrecognized is treated as "no filter".
      const u = new URL(req.url, "http://localhost");
      const sdt = u.searchParams.get("sdt") || "";
      const allowed = ["Vendor", "JobFiles", "Sales", "Any"];
      const safe = allowed.includes(sdt) ? sdt : "";
      sendJson(res, 200, handleDashboard(safe));
      return;
    }
    if (req.method === "GET" && req.url === "/api/ignored-types") {
      sendJson(res, 200, handleListIgnoredTypes());
      return;
    }
    if (req.method === "POST" && req.url === "/api/ignored-types/add") {
      sendJson(res, 200, handleAddIgnoredType(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/ignored-types/remove") {
      sendJson(res, 200, handleRemoveIgnoredType(await readBody(req)));
      return;
    }
    if (req.method === "GET" && req.url === "/api/ignored-folders") {
      sendJson(res, 200, handleListIgnoredFolders());
      return;
    }
    if (req.method === "POST" && req.url === "/api/ignored-folders/add") {
      sendJson(res, 200, handleAddIgnoredFolder(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/ignored-folders/remove") {
      sendJson(res, 200, handleRemoveIgnoredFolder(await readBody(req)));
      return;
    }
    if (req.method === "GET" && req.url === "/api/ignored-files") {
      sendJson(res, 200, handleIgnoredFiles());
      return;
    }
    if (req.method === "POST" && req.url === "/api/ignored-files/add") {
      sendJson(res, 200, handleAddIgnoredFiles(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/ignored-files/remove") {
      sendJson(res, 200, handleRemoveIgnoredFile(await readBody(req)));
      return;
    }
    // Classifier rule editor: /api/classifier-rules/<kind> [GET=list, POST=save]
    //                         /api/classifier-rules/<kind>/coverage [GET]
    if (req.url && req.url.startsWith("/api/classifier-rules/")) {
      const tail = req.url.slice("/api/classifier-rules/".length);
      const slash = tail.indexOf("/");
      const kind = slash >= 0 ? tail.slice(0, slash) : tail;
      const sub  = slash >= 0 ? tail.slice(slash + 1) : "";
      if (sub === "coverage" && req.method === "GET") {
        sendJson(res, 200, handleClassifierRuleCoverage(kind));
        return;
      }
      if (sub === "" && req.method === "GET") {
        sendJson(res, 200, handleListClassifierRules(kind));
        return;
      }
      if (sub === "" && req.method === "POST") {
        sendJson(res, 200, handleSaveClassifierRules(kind, await readBody(req)));
        return;
      }
    }
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("Not found");
  } catch (e) {
    sendJson(res, 500, { ok: false, error: e.message });
  }
});

server.listen(PORT, () => {
  console.log(`EnLogosGRAG web app running at http://localhost:${PORT}/`);
});
