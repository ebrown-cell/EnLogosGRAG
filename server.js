// Web app for EnLogosGRAG: browse the test.db tables and run ingest actions.
// Single-page UI bundled inline — open http://localhost:8780.

import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { openDb } from "./db.js";
import { readListing, ingestVendors, ingestFiles } from "./listing.js";
import { classifyAll, classifyAllByContent, classifyAllByProduct } from "./classifier.js";
import * as extractor from "./extractor.js";

// The extractor needs to translate canonical S:\ paths to local paths
// using the same remap table the open-file feature uses. Wire that up
// once at module load.
extractor.setPathRemapper(remapPath);

const SERVER_DIR = path.dirname(fileURLToPath(import.meta.url));

const PORT = Number(process.env.PORT) || 8780;
const DEFAULT_SOURCE = "C:\\data\\PyroCommData";
const DEFAULT_LISTING_NAME = "basic_listing.txt";

const TABLES = ["vendors", "document_types", "files", "documents", "document_extracts", "products", "document_products"];

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
    vendors:           db.prepare("SELECT COUNT(*) AS n FROM vendors").get().n,
    document_types:    db.prepare("SELECT COUNT(*) AS n FROM document_types").get().n,
    files:             db.prepare("SELECT COUNT(*) AS n FROM files").get().n,
    documents:         db.prepare("SELECT COUNT(*) AS n FROM documents").get().n,
    document_extracts: db.prepare("SELECT COUNT(*) AS n FROM document_extracts").get().n,
    products:          db.prepare("SELECT COUNT(*) AS n FROM products").get().n,
    document_products: db.prepare("SELECT COUNT(*) AS n FROM document_products").get().n,
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
      });
    }
    if (table === "document_extracts") {
      return browseExtracts(db, { limit, offset, filter });
    }
    if (table === "products") {
      return browseProducts(db, { limit, offset, filter });
    }
    if (table === "document_products") {
      return browseDocumentProducts(db, { limit, offset, filter });
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
    if (table === "files" && Array.isArray(body.fileTypeFilters) && body.fileTypeFilters.length) {
      const placeholders = body.fileTypeFilters.map(() => "?").join(",");
      clauses.push(`file_type IN (${placeholders})`);
      args.push(...body.fileTypeFilters);
    }
    const where = clauses.length ? "WHERE " + clauses.join(" AND ") : "";
    const totalRow = db
      .prepare(`SELECT COUNT(*) AS n FROM ${table} ${where}`)
      .get(...args);
    const total = totalRow.n;

    const rows = db
      .prepare(`SELECT * FROM ${table} ${where} LIMIT ? OFFSET ?`)
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
function browseDocuments(db, { limit, offset, filter, classifiedFilter, minConfidence, exactConfidence, fileTypeFilters, documentTypeFilters, productFilter, hasProductFilter }) {
  const { from, where, args } = buildDocumentsWhere({
    filter, classifiedFilter, minConfidence, exactConfidence, fileTypeFilters, documentTypeFilters, productFilter, hasProductFilter,
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
              d.confidence    AS confidence,
              f.file_type     AS file_type,
              CASE
                WHEN e.document_id IS NULL THEN ''
                WHEN e.error IS NOT NULL   THEN 'err'
                ELSE 'ok'
              END             AS extract,
              f.path          AS file_path
       ${fromWithExtract} ${where}
       ORDER BY d.id
       LIMIT ? OFFSET ?`,
    )
    .all(...args, limit, offset);

  const columns = [
    "id",
    "document_name",
    "vendor_name",
    "document_type",
    "confidence",
    "file_type",
    "extract",
    "file_path",
  ];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// Joined view of document_extracts with the file name + char count, since
// the raw row would just show document_id (an int) and a giant text blob.
function browseExtracts(db, { limit, offset, filter }) {
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
     ORDER BY e.document_id
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
function browseProducts(db, { limit, offset, filter }) {
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
     ORDER BY v.name, p.name
     LIMIT ? OFFSET ?`,
  ).all(...args, limit, offset);

  const columns = ["id", "vendor_name", "name", "aliases", "notes", "docs"];
  return { ok: true, total, limit, offset, filter, columns, rows };
}

// Joined view of document_products. Replaces the raw two-int row with
// readable names (same document_name treatment as the documents view —
// extension stripped).
function browseDocumentProducts(db, { limit, offset, filter }) {
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
     ORDER BY p.name, v.name, dp.document_id
     LIMIT ? OFFSET ?`,
  ).all(...args, limit, offset);

  const columns = [
    "product_name", "vendor_name", "document_id", "document_name",
    "confidence", "source", "file_path",
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
function buildDocumentsWhere({ filter, classifiedFilter, minConfidence, exactConfidence, fileTypeFilters, documentTypeFilters, productFilter, hasProductFilter }) {
  const from = `
    FROM documents d
    JOIN files f          ON f.id = d.file_id
    JOIN vendors v        ON v.id = f.vendor_id
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
  const listing = resolveListingPath(source);
  if (!listing) {
    return {
      ok: false,
      error: `No listing file found for source: ${source || "(empty)"}`,
    };
  }
  const db = openDb();
  try {
    const paths = readListing(listing);
    if (which === "vendors") {
      const r = ingestVendors(db, paths);
      return { ok: true, action: "vendors", listing, ...r, counts: tableCounts(db) };
    }
    if (which === "files") {
      const r = ingestFiles(db, paths);
      return { ok: true, action: "files", listing, ...r, counts: tableCounts(db) };
    }
    if (which === "full") {
      const v = ingestVendors(db, paths);
      const f = ingestFiles(db, paths);
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
function handleDashboard() {
  const db = openDb();
  try {
    const counts = tableCounts(db);

    const totalDocs = counts.documents;
    const classifiedTotal = db
      .prepare("SELECT COUNT(*) AS n FROM documents WHERE document_type_id IS NOT NULL")
      .get().n;

    // Per-doctype confidence breakdown. LEFT JOIN so types with zero documents
    // still appear (count = 0); the client decides whether to dim/hide them.
    const rows = db
      .prepare(
        `SELECT dt.name AS name,
                COUNT(d.id)                                              AS total,
                SUM(CASE WHEN d.confidence = 'high'   THEN 1 ELSE 0 END) AS high,
                SUM(CASE WHEN d.confidence = 'medium' THEN 1 ELSE 0 END) AS medium,
                SUM(CASE WHEN d.confidence = 'low'    THEN 1 ELSE 0 END) AS low
         FROM document_types dt
         LEFT JOIN documents d ON d.document_type_id = dt.id
         GROUP BY dt.id, dt.name
         ORDER BY total DESC, dt.name ASC`,
      )
      .all();

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
    };

    return {
      ok: true,
      counts,
      classification: {
        totalDocuments: totalDocs,
        classified: classifiedTotal,
        unclassified: totalDocs - classifiedTotal,
        pctClassified: totalDocs > 0 ? (classifiedTotal / totalDocs) * 100 : 0,
      },
      ignored,
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

    // (b) Distinct products discovered per vendor — informational chart.
    //     Counts how many product rows each vendor has in the products
    //     table (independent of doc-link counts).
    const productsPerVendor = db
      .prepare(
        `SELECT v.name AS vendor, COUNT(DISTINCT p.id) AS n
         FROM products p JOIN vendors v ON v.id = p.vendor_id
         GROUP BY v.id ORDER BY n DESC, v.name ASC LIMIT 30`,
      )
      .all();

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

    return {
      ok: true,
      total,
      classified,
      unclassified,
      byConfidence,
      byFileTypeTop,
      byFileTypeOther,
      byDocumentTypeTop,
      byDocumentTypeOther,
      byProductTop,
      byProductOther,
      productsPerVendor,
      docsWithProducts,
      docsWithoutProducts,
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

// Walk a folder recursively, writing one path per line to
// <folder>/basic_listing.txt. Paths are canonicalized to the S:\vendors\
// form regardless of the chosen folder's actual location, so the existing
// vendorOf() parser (which expects ['S:', 'vendors', vendor, ...]) keeps
// working. Caller verified `folder` is a real directory.
//
// The chosen folder is treated as the equivalent of S:\vendors\ — i.e.
// each immediate subfolder is interpreted as a vendor.
function generateBasicListing(folder) {
  const lines = [];
  const stack = [{ rel: [], abs: folder }];
  while (stack.length) {
    const { rel, abs } = stack.pop();
    let entries;
    try {
      entries = fs.readdirSync(abs, { withFileTypes: true });
    } catch (e) {
      // Surface the first failure but keep walking — bad ACL on one
      // subdir shouldn't kill the whole scan.
      console.warn("[generateBasicListing]", abs, e.code || e.message);
      continue;
    }
    // Sort for stable output across runs.
    entries.sort((a, b) => a.name.localeCompare(b.name));
    for (const entry of entries) {
      if (entry.name === "basic_listing.txt" || entry.name === "listing.txt") continue;
      const childRel = rel.concat(entry.name);
      const canonical = "S:\\vendors\\" + childRel.join("\\");
      lines.push(canonical);
      if (entry.isDirectory()) {
        stack.push({ rel: childRel, abs: path.join(abs, entry.name) });
      }
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

  const uploadsDir = path.join(SERVER_DIR, "uploads");
  fs.mkdirSync(uploadsDir, { recursive: true });
  const dest = path.join(uploadsDir, safe);

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

function handleClassifyProducts() {
  const db = openDb();
  try {
    const result = classifyAllByProduct(db);
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
  // show "X / Y of N PDFs extracted" even between runs.
  const db = openDb();
  let extractedCount = 0, totalPdfs = 0;
  try {
    extractedCount = db.prepare(
      "SELECT COUNT(*) AS n FROM document_extracts WHERE error IS NULL"
    ).get().n;
    totalPdfs = db.prepare(
      "SELECT COUNT(*) AS n FROM documents d JOIN files f ON f.id = d.file_id WHERE f.file_type = '.pdf'"
    ).get().n;
  } finally {
    db.close();
  }
  return { ok: true, status: extractor.getStatus(), extractedCount, totalPdfs };
}

function handlePurge(body) {
  const target = body.target;
  const db = openDb();
  try {
    if (target === "all") {
      // Order matters: child tables first to satisfy FK constraints.
      //   files     → cascades to documents, document_extracts, document_products
      //   products  → owned by vendors via FK; clear before deleting vendors
      //   vendors   → safe to delete once products is empty
      // Intentionally preserved:
      //   document_types     one-time taxonomy snapshot, no reason to reseed
      //   ignored_file_types user-curated ingest config, survives data wipes
      //   ignored_folders    user-curated ingest config, survives data wipes
      db.exec("DELETE FROM files");
      db.exec("DELETE FROM products");
      db.exec("DELETE FROM vendors");
      db.exec(
        "DELETE FROM sqlite_sequence WHERE name IN " +
        "('vendors', 'files', 'documents', 'products')",
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
    --panel: #f5f3f7;
    --inset: #fafafc;        /* inputs, code blocks, log strip */
    --hover: #ebe7ef;        /* row hover, button hover */
    --hover-strong: #ddd5e3; /* deeper hover for nested controls */
    --border: #c8c2d0;
    --border-row: #e2dde7;   /* horizontal table row separators */
    --border-col: #ccc3d4;   /* vertical column separators */
    --text: #1a1820;
    --muted: #6a6573;
    --accent: #8b2090;
    --accent-bg: #f0d8f3;
    --danger: #b03030;
    --danger-bg: #fbe5e5;
    --danger-text: #6a1010;
    --ok: #2c7a2c;
    --error-text: #b03030;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 0;
    font-family: system-ui, -apple-system, sans-serif;
    background: var(--bg); color: var(--text);
    height: 100vh; display: flex; flex-direction: column; overflow: hidden;
  }
  header {
    padding: 8px 14px; background: var(--panel);
    border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 14px; flex-wrap: wrap;
  }
  header h1 { font-size: 14px; font-weight: 600; margin: 0; color: var(--accent); }
  header .meta { font-size: 12px; color: var(--muted); }
  header .spacer { flex: 1; }
  header a.help-link {
    font-size: 12px; color: var(--accent); text-decoration: none;
    padding: 3px 8px; border: 1px solid var(--border); border-radius: 3px;
    background: var(--inset);
  }
  header a.help-link:hover { background: var(--hover); border-color: var(--accent); }
  main { flex: 1; display: flex; min-height: 0; }
  aside {
    width: 280px; border-right: 1px solid var(--border);
    padding: 12px; overflow-y: auto;
    background: var(--panel);
    position: relative;
    transition: width 0.15s ease;
  }
  /* Collapsed: narrow strip just wide enough for the toggle button.
     All children except the toggle are hidden. */
  aside.collapsed { width: 28px; padding: 8px 4px; overflow: hidden; }
  aside.collapsed > :not(#sidebar-toggle) { display: none; }
  #sidebar-toggle {
    position: absolute; top: 8px; right: 6px;
    width: 18px; height: 22px; padding: 0;
    background: transparent; color: var(--muted);
    border: 1px solid var(--border); border-radius: 3px;
    cursor: pointer; font-size: 12px; line-height: 1;
  }
  #sidebar-toggle:hover { color: var(--text); border-color: var(--accent); }
  aside.collapsed #sidebar-toggle {
    /* Centered when collapsed, with a different glyph. */
    top: 8px; left: 50%; right: auto; transform: translateX(-50%);
  }
  aside h2 {
    font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
    color: var(--muted); margin: 14px 0 6px 0;
  }
  aside h2:first-child { margin-top: 0; padding-right: 26px; }
  aside .table-row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 6px 8px; cursor: pointer; border-radius: 4px;
    font-size: 13px;
  }
  aside .table-row:hover { background: var(--hover); }
  aside .table-row.active { background: var(--accent-bg); color: var(--text); }
  aside .table-row .count { color: var(--muted); font-variant-numeric: tabular-nums; }

  /* Compact 2-col summary table used inside the Source data section to show
     document_types / ignored types / ignored folders with their counts. Each
     row behaves like a .table-row (hover, active highlight, click). */
  aside table.sidebar-summary {
    width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px;
  }
  aside table.sidebar-summary tr { cursor: pointer; }
  aside table.sidebar-summary tr:hover td { background: var(--hover); }
  aside table.sidebar-summary tr.active td { background: var(--accent-bg); color: var(--text); }
  aside table.sidebar-summary td {
    padding: 6px 8px; border-radius: 4px;
  }
  aside table.sidebar-summary td.count {
    color: var(--muted); font-variant-numeric: tabular-nums;
    text-align: right; width: 40px;
  }
  /* Chart-preset shortcut links sit under "📊 All documents" and apply a
     pre-built filter chain when clicked. Lightly muted + slightly smaller
     so the parent (📊 All documents) stays visually primary. */
  aside .chart-preset {
    padding: 4px 8px 4px 24px; cursor: pointer; border-radius: 4px;
    font-size: 12px; color: var(--muted);
  }
  aside .chart-preset:hover { background: var(--hover); color: var(--text); }
  aside .chart-preset.active { background: var(--accent-bg); color: var(--text); }
  aside label { display: block; font-size: 12px; color: var(--muted); margin: 6px 0 3px 0; }
  aside input[type=text] {
    width: 100%; padding: 5px 7px; font-size: 12px;
    background: var(--inset); color: var(--text);
    border: 1px solid var(--border); border-radius: 3px;
    font-family: monospace;
  }
  #listing-display {
    padding: 8px;
    border: 1px solid var(--border); border-radius: 4px;
    background: var(--inset); font-size: 12px;
    word-break: break-all;
  }
  .extract-status {
    padding: 6px 8px; margin-bottom: 4px;
    background: var(--inset); border: 1px solid var(--border); border-radius: 3px;
    font-size: 11px; color: var(--muted); font-family: monospace;
  }
  .extract-status.running { color: var(--accent); border-color: var(--accent); }
  .extract-status .current {
    margin-top: 4px; word-break: break-all; color: var(--text);
    font-size: 10px; opacity: 0.8;
  }
  #listing-display .listing-empty { color: var(--muted); font-style: italic; }
  #listing-display .listing-name { color: var(--accent); font-weight: 600; }
  #listing-display .listing-path {
    color: var(--muted); font-family: monospace; font-size: 11px; margin-top: 3px;
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
  .modal-actions .primary:hover { background: var(--accent); color: #000; }
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
    width: 100%; padding: 7px; margin-top: 5px; font-size: 12px;
    background: var(--accent-bg); color: var(--text);
    border: 1px solid var(--accent); border-radius: 3px;
    cursor: pointer;
  }
  aside button:hover { background: var(--accent); color: #000; }
  aside button.danger { background: var(--danger-bg); border-color: var(--danger); color: var(--danger-text); }
  aside button.danger:hover { background: var(--danger); color: #fff; }
  aside button.secondary { background: var(--inset); border-color: var(--border); color: var(--text); }
  aside button.secondary:hover { background: var(--hover); }
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
  }
  .multi-panel .row:hover { background: var(--hover); }
  .multi-panel .row input { cursor: pointer; }
  .multi-panel .row .count { color: var(--muted); margin-left: auto; font-variant-numeric: tabular-nums; }
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
  .empty { padding: 20px; text-align: center; color: var(--muted); font-size: 13px; }

  /* Collapsible sidebar sections. <summary> acts as the section header
     (mirroring the existing h2 styling) and clicking toggles the body. */
  aside details {
    margin: 14px 0 0 0;
  }
  aside details > summary {
    list-style: none;
    cursor: pointer;
    font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
    color: var(--muted); margin: 0 0 6px 0;
    user-select: none;
    display: flex; align-items: center; gap: 6px;
  }
  aside details > summary::-webkit-details-marker { display: none; }
  aside details > summary::before {
    content: "▸";
    font-size: 9px;
    transition: transform 0.1s;
    display: inline-block;
    width: 10px;
  }
  aside details[open] > summary::before { transform: rotate(90deg); }
  aside details > summary:hover { color: var(--text); }
  /* "Full Process" gets its own visual distinction — it's the headline
     action, runs the whole pipeline. */
  aside button.headline {
    background: var(--accent); color: var(--bg);
    border-color: var(--accent); font-weight: 600;
  }
  aside button.headline:hover { background: var(--accent-bg); color: var(--accent); }

  /* Click-to-open menus (Purge, Ingest). The trigger looks like an aside
     button; the panel pops out below and floats above following content. */
  aside .menu-wrap { position: relative; }
  aside .menu-trigger {
    width: 100%; padding: 7px; margin-top: 5px; font-size: 12px;
    background: var(--inset); color: var(--text);
    border: 1px solid var(--border); border-radius: 3px;
    cursor: pointer; text-align: left;
    display: flex; justify-content: space-between; align-items: center;
  }
  aside .menu-trigger:hover { background: var(--hover); border-color: var(--accent); }
  aside .menu-trigger.open  { background: var(--hover); border-color: var(--accent); }
  aside .menu-trigger .caret { color: var(--muted); font-size: 10px; }
  aside .menu-panel {
    position: absolute; top: 100%; left: 0; right: 0;
    margin-top: 2px; padding: 4px;
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 3px;
    z-index: 60;
    box-shadow: 0 4px 12px rgba(0,0,0,0.25);
  }
  aside .menu-item {
    padding: 6px 10px; cursor: pointer; border-radius: 3px;
    font-size: 12px; color: var(--text);
  }
  aside .menu-item:hover { background: var(--hover); }
  aside .menu-item.danger { color: var(--danger-text); }
  aside .menu-item.danger:hover { background: var(--danger); color: #fff; }
  aside .menu-item.disabled { opacity: 0.4; cursor: not-allowed; pointer-events: none; }

  /* Inline action menu inside the sidebar. Outer details is the "Actions"
     section; nested details.action-sub are the three groups (Purge /
     Ingest / Classify & Extract). Action rows look the same in any group. */
  aside details.action-sub { margin: 4px 0 4px 12px; }
  aside details.action-sub > summary {
    padding: 4px 6px; border-radius: 3px;
    text-transform: none; letter-spacing: 0; font-size: 12px;
    color: var(--text);
  }
  aside details.action-sub > summary:hover { background: var(--hover); }
  aside .action-row {
    padding: 6px 8px 6px 26px; cursor: pointer; border-radius: 3px;
    font-size: 12px; color: var(--text);
  }
  aside .action-row:hover { background: var(--hover); }
  aside .action-row.danger { color: var(--danger-text); }
  aside .action-row.danger:hover { background: var(--danger); color: #fff; }
  aside .action-row.headline { color: var(--accent); font-weight: 600; }
  aside .action-row.disabled { opacity: 0.4; cursor: not-allowed; pointer-events: none; }
  aside .action-row .desc {
    display: block; color: var(--muted); font-size: 10px; font-weight: normal; margin-top: 2px;
  }
  aside .action-row.danger:hover .desc { color: rgba(255,255,255,0.85); }
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
  #dash-counts {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
    gap: 8px; margin-bottom: 8px;
  }
  #dash-counts .dash-card {
    background: var(--inset); border: 1px solid var(--border);
    border-radius: 4px; padding: 10px 12px;
  }
  #dash-counts .dash-card .label {
    color: var(--muted); font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  #dash-counts .dash-card .value {
    color: var(--text); font-size: 22px; font-weight: 600; margin-top: 4px;
    font-variant-numeric: tabular-nums;
  }
  #dash-class-summary {
    background: var(--inset); border: 1px solid var(--border);
    border-radius: 4px; padding: 10px 12px; margin-bottom: 6px;
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

  /* Help / welcome page styling. Readable prose, not a data grid. */
  #help-view { max-width: 760px; }
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
  <h1>EnLogosGRAG</h1>
  <span class="meta" id="status">loading…</span>
  <span class="spacer"></span>
  <a class="help-link" href="/help" target="_blank" rel="noopener">Help</a>
</header>
<main>
  <aside id="sidebar">
    <button id="sidebar-toggle" type="button" title="Collapse sidebar">‹</button>
    <div id="help-row" class="table-row" data-view="help">
      <span>🏠 Home</span>
    </div>
    <details open>
      <summary>Charts</summary>
      <div id="charts-row" class="table-row" data-view="charts">
        <span>📊 All documents</span>
      </div>
      <div class="chart-preset" data-preset="classified">
        <span>↳ Classified only</span>
      </div>
      <div class="chart-preset" data-preset="unclassified">
        <span>↳ Unclassified only</span>
      </div>
      <div class="chart-preset" data-preset="pdfs">
        <span>↳ PDFs only</span>
      </div>
      <div class="chart-preset" data-preset="classified-pdfs">
        <span>↳ Classified PDFs only</span>
      </div>
      <div class="chart-preset" data-preset="unclassified-pdfs">
        <span>↳ Unclassified PDFs only</span>
      </div>
      <div class="chart-preset" data-preset="with-products">
        <span>↳ Documents with products</span>
      </div>
      <div class="chart-preset" data-preset="without-products">
        <span>↳ Documents without products</span>
      </div>
    </details>

    <details open>
      <summary>Tables</summary>
      <div id="table-list"></div>
    </details>

    <details>
      <summary>Source data</summary>
      <div id="listing-display">
        <div id="listing-display-empty" class="listing-empty">No folder chosen.</div>
        <div id="listing-display-set" class="listing-set" style="display:none;">
          <div class="listing-name" id="listing-name"></div>
          <div class="listing-path" id="listing-path"></div>
        </div>
      </div>
      <button id="pick-listing" class="secondary">Choose folder…</button>
      <button id="rescan-listing" class="secondary">Rescan listing (regenerate basic_listing.txt)</button>
      <table class="sidebar-summary">
        <tbody>
          <tr id="doctypes-row" data-table="document_types">
            <td>📚 Document types</td>
            <td class="count" id="doctypes-count">0</td>
          </tr>
          <tr id="ignored-row" data-view="ignored">
            <td>🚫 Ignored types</td>
            <td class="count" id="ignored-types-count">0</td>
          </tr>
          <tr id="ignored-folders-row" data-view="ignored-folders">
            <td>📁 Ignored folders</td>
            <td class="count" id="ignored-folders-count">0</td>
          </tr>
        </tbody>
      </table>
    </details>

    <details>
      <summary>Actions</summary>
      <details class="action-sub">
        <summary>Purge</summary>
        <div class="action-row danger" data-action="purge-vendors">Purge vendors</div>
        <div class="action-row danger" data-action="purge-files">Purge files</div>
        <div class="action-row danger" data-action="purge-all">Purge ALL tables</div>
        <div class="action-row danger" data-action="purge-text-backups">
          Purge text backups
          <span class="desc">Delete the &lt;Vendors&gt;_text filesystem cache</span>
        </div>
      </details>
      <details class="action-sub">
        <summary>Ingest</summary>
        <div class="action-row headline" data-action="run-full-process">
          Full Process
          <span class="desc">Vendors + files + classify + extract + content classify</span>
        </div>
        <div class="action-row" data-action="run-vendors">Run vendors ingest</div>
        <div class="action-row" data-action="run-files">Run files ingest</div>
        <div class="action-row" data-action="run-full">Full Run (vendors + files)</div>
      </details>
      <details class="action-sub">
        <summary>Classify &amp; Extract</summary>
        <div class="action-row" data-action="classify-all">
          Classify all files
          <span class="desc">Filename / path rules</span>
        </div>
        <div class="action-row" data-action="extract-start">
          Extract PDF text
          <span class="desc">Background; uses filesystem cache when present</span>
        </div>
        <div class="action-row" data-action="extract-stop">
          Stop extraction
          <span class="desc">Signals the worker to stop after current file</span>
        </div>
        <div class="action-row" data-action="classify-by-content">
          Classify by extract content
          <span class="desc">PDF-text rules; only fills empty / upgrades low</span>
        </div>
        <div class="action-row" data-action="classify-products">
          Classify products
          <span class="desc">Vendor-scoped rules; many-to-many to documents</span>
        </div>
      </details>
    </details>

    <details open>
      <summary>Status</summary>
      <button id="refresh" class="secondary">Refresh status</button>
      <div id="classify-status" class="extract-status">Classified: …</div>
      <div id="extract-status" class="extract-status">Extracted: …</div>
      <button id="extract-stop" class="secondary" style="display:none; margin-top: 4px;">Stop extraction</button>
    </details>
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
      <select id="confidence-filter" class="docs-only" title="Minimum confidence threshold (excludes unclassified for any threshold above 'all')">
        <option value="all">Any confidence</option>
        <option value="low">≥ low</option>
        <option value="medium">≥ medium</option>
        <option value="high">high only</option>
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
        <h2 class="dash-h">Dashboard</h2>
        <p class="dash-sub" id="dash-empty" style="display:none;">No data yet. Run an ingest to populate the database.</p>

        <h3 class="dash-h">Record counts</h3>
        <div id="dash-counts"></div>

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
      </div>
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

<script>
const state = {
  view: "table",         // "table" | "charts"
  table: "vendors",
  offset: 0,
  listingPath: "",        // absolute path to a listing .txt file on disk

  limit: 100,
  filter: "",
  classifiedFilter: "all",
  minConfidence: "all",
  exactConfidence: "",      // set by drilldown click; wins over minConfidence
  fileTypeFilters: [],      // multi-select: ['.pdf', '.dwg', …]
  documentTypeFilters: [],  // multi-select: ['installation_manual', …]
  productFilter: "",        // single-select: 'NFS2-3030' (chart drilldown)
  hasProductFilter: "",     // 'yes' | 'no' | '' — chart drilldown only
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
  },
};

async function api(path, body) {
  const opts = body
    ? { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }
    : { method: "GET" };
  const res = await fetch(path, opts);
  return res.json();
}

function log(msg, kind = "info") {
  const el = document.getElementById("log");
  const e = document.createElement("div");
  e.className = "entry " + kind;
  const t = new Date().toTimeString().slice(0, 8);
  e.textContent = "[" + t + "] " + msg;
  el.insertBefore(e, el.firstChild);
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
  document.getElementById("filter").value = "";
  document.getElementById("classified-filter").value = "all";
  document.getElementById("confidence-filter").value = "all";
  refreshMultiButton("filetype");
  refreshMultiButton("doctype");
  // Highlight only the matching row across both Views and Tables groups.
  document.querySelectorAll(".table-row").forEach((el) => {
    el.classList.toggle("active",
      el.dataset.table === name && el.dataset.view !== "charts");
  });
  document.getElementById("charts-row").classList.remove("active");
  document.querySelectorAll(".chart-preset").forEach((el) => el.classList.remove("active"));
  // Visibility of per-table controls.
  // docs-only: classified + confidence dropdowns (documents only).
  // ext-only:  file-type dropdown (files + documents — anything with extensions).
  const showDocsControls = name === "documents";
  const showExtControls  = name === "documents" || name === "files";
  for (const el of document.querySelectorAll(".toolbar .docs-only")) {
    el.classList.toggle("hidden", !showDocsControls);
  }
  for (const el of document.querySelectorAll(".toolbar .ext-only")) {
    el.classList.toggle("hidden", !showExtControls);
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
  };
  if (preset && typeof preset === "object") {
    Object.assign(state.chartFilter, preset);
  }
  document.querySelectorAll(".table-row, .chart-preset").forEach((el) => {
    el.classList.remove("active");
  });
  document.getElementById("charts-row").classList.add("active");
  // Hide all per-table filters on the chart view — charts have their own breadcrumb.
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only")) {
    el.classList.add("hidden");
  }
  showChartView();
  loadCharts();
}

// Preset shortcuts: each clicks-through to setActiveCharts() with a
// starting filter chain. Defined here so the wiring at the bottom of
// the file stays declarative.
const CHART_PRESETS = {
  classified:           { classifiedFilter: "classified" },
  unclassified:         { classifiedFilter: "unclassified" },
  pdfs:                 { fileTypeFilters: [".pdf"] },
  "classified-pdfs":    { classifiedFilter: "classified",   fileTypeFilters: [".pdf"] },
  "unclassified-pdfs":  { classifiedFilter: "unclassified", fileTypeFilters: [".pdf"] },
  "with-products":      { hasProductFilter: "yes" },
  "without-products":   { hasProductFilter: "no" },
};

function hideAllViews() {
  document.getElementById("table-view").style.display           = "none";
  document.getElementById("chart-view").style.display           = "none";
  document.getElementById("help-view").style.display            = "none";
  document.getElementById("ignored-view").style.display         = "none";
  document.getElementById("ignored-folders-view").style.display = "none";
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

function setActiveHelp() {
  state.view = "help";
  document.querySelectorAll(".table-row, .chart-preset").forEach((el) => {
    el.classList.remove("active");
  });
  const helpRow = document.getElementById("help-row");
  if (helpRow) helpRow.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only")) {
    el.classList.add("hidden");
  }
  showHelpView();
  loadDashboard();
}

function setActiveIgnored() {
  state.view = "ignored";
  document.querySelectorAll(".table-row, .chart-preset").forEach((el) => {
    el.classList.remove("active");
  });
  const row = document.getElementById("ignored-row");
  if (row) row.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only")) {
    el.classList.add("hidden");
  }
  showIgnoredView();
  loadIgnoredTypes();
}

function setActiveIgnoredFolders() {
  state.view = "ignored-folders";
  document.querySelectorAll(".table-row, .chart-preset").forEach((el) => {
    el.classList.remove("active");
  });
  const row = document.getElementById("ignored-folders-row");
  if (row) row.classList.add("active");
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only")) {
    el.classList.add("hidden");
  }
  showIgnoredFoldersView();
  loadIgnoredFolders();
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
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// Renders the Home dashboard: per-table counts + per-doctype confidence mix.
// Idempotent — called every time Home is opened so numbers reflect current DB.
async function loadDashboard() {
  const countsEl = document.getElementById("dash-counts");
  const tbody    = document.querySelector("#dash-types-table tbody");
  const summary  = document.getElementById("dash-class-summary");
  const empty    = document.getElementById("dash-empty");
  if (!countsEl || !tbody || !summary) return;

  let r;
  try {
    r = await api("/api/dashboard");
  } catch (e) {
    summary.textContent = "Failed to load dashboard: " + (e.message || e);
    return;
  }

  // Counts grid — table counts (alphabetized, like the sidebar) + the two
  // ignore-list sizes appended at the end so they're visually grouped.
  countsEl.innerHTML = "";
  const entries = Object.entries(r.counts).sort(([a], [b]) => a.localeCompare(b));
  if (r.ignored) {
    entries.push(["ignored types",   r.ignored.types   || 0]);
    entries.push(["ignored folders", r.ignored.folders || 0]);
  }
  for (const [tbl, n] of entries) {
    const card = document.createElement("div");
    card.className = "dash-card";
    card.innerHTML =
      '<div class="label">' + tbl + '</div>' +
      '<div class="value">' + Number(n).toLocaleString() + '</div>';
    countsEl.appendChild(card);
  }

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
}

async function refreshStatus() {
  const r = await api("/api/status");
  document.getElementById("status").textContent =
    "vendors: " + r.counts.vendors + " · files: " + r.counts.files;

  const list = document.getElementById("table-list");
  list.innerHTML = "";
  // Alphabetize the sidebar so users find tables by name, not by the order
  // tableCounts() happened to enumerate them.
  const entries = Object.entries(r.counts).sort(
    ([a], [b]) => a.localeCompare(b),
  );
  for (const [tbl, n] of entries) {
    const row = document.createElement("div");
    row.className = "table-row" + (state.table === tbl ? " active" : "");
    row.dataset.table = tbl;
    row.innerHTML = '<span>' + tbl + '</span><span class="count">' + n + '</span>';
    row.addEventListener("click", () => setActiveTable(tbl));
    list.appendChild(row);
  }

  updateClassifyStatus(r.classification);
  updateIgnoredCounts(r.ignored);
}

// Light-weight version: just refresh the row counts beside each table.
// Avoids rebuilding DOM / event listeners while polling during a long
// extraction run.
async function refreshTableCounts() {
  let r;
  try { r = await api("/api/status"); } catch { return; }
  if (!r || !r.counts) return;
  document.getElementById("status").textContent =
    "vendors: " + r.counts.vendors + " · files: " + r.counts.files;
  for (const row of document.querySelectorAll("#table-list .table-row")) {
    const tbl = row.dataset.table;
    if (!tbl || !(tbl in r.counts)) continue;
    const span = row.querySelector(".count");
    if (span) span.textContent = String(r.counts[tbl]);
  }
  updateClassifyStatus(r.classification);
  updateIgnoredCounts(r.ignored);
}

function updateIgnoredCounts(ig) {
  if (!ig) return;
  const t = document.getElementById("ignored-types-count");
  const f = document.getElementById("ignored-folders-count");
  if (t) t.textContent = String(ig.types || 0);
  if (f) f.textContent = String(ig.folders || 0);
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
  // header — each th gets explicit width (saved or default) plus a resizer grip.
  const hr = document.createElement("tr");
  for (const c of r.columns) {
    const th = document.createElement("th");
    th.textContent = c;
    th.title = c;
    const w = getSavedColWidth(state.table, c) ?? COL_DEFAULT_WIDTH;
    th.style.width = w + "px";
    attachResizer(th, state.table, c);
    hr.appendChild(th);
  }
  thead.appendChild(hr);
  // rows
  if (r.rows.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = r.columns.length || 1;
    td.className = "empty";
    td.textContent = state.filter
      ? "No rows match the filter."
      : "Table is empty. Run an ingest from the sidebar.";
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    for (const row of r.rows) {
      const tr = document.createElement("tr");
      for (const c of r.columns) {
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

  liveCharts[canvasId] = new Chart(canvas, { type: "doughnut", data, options });
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
  Object.assign(state, delta);

  document.getElementById("filter").value = "";
  document.getElementById("classified-filter").value = state.classifiedFilter;
  refreshMultiButton("filetype");
  refreshMultiButton("doctype");
  // The dropdown can only express "Any" / "≥ low" / "≥ medium" / "high only".
  // exactConfidence (drilldown) doesn't map cleanly onto that, so when an
  // exact filter is active we show "Any" — the log line carries the truth.
  document.getElementById("confidence-filter").value =
    state.exactConfidence ? "all" : state.minConfidence;

  document.querySelectorAll(".table-row").forEach((el) => {
    el.classList.toggle("active",
      el.dataset.table === "documents" && el.dataset.view !== "charts");
  });
  document.getElementById("charts-row").classList.remove("active");
  document.querySelectorAll(".chart-preset").forEach((el) => el.classList.remove("active"));
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only")) {
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
  return classifiedHidden && confidenceHidden && filetypeHidden && doctypeHidden
    && productHidden && coverageHidden;
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
  });
}

function renderBreadcrumb() {
  const cf = state.chartFilter;
  const parts = ["All documents"];
  if (cf.classifiedFilter && cf.classifiedFilter !== "all") parts.push(cf.classifiedFilter);
  if (cf.fileTypeFilters     && cf.fileTypeFilters.length)     parts.push("file_type=" + cf.fileTypeFilters.join(", "));
  if (cf.documentTypeFilters && cf.documentTypeFilters.length) parts.push("document_type=" + cf.documentTypeFilters.join(", "));
  if (cf.exactConfidence)    parts.push("confidence=" + cf.exactConfidence);
  if (cf.productFilter)      parts.push("product=" + cf.productFilter);
  if (cf.hasProductFilter)   parts.push("has_product=" + cf.hasProductFilter);
  document.getElementById("chart-crumb").textContent = parts.join(" › ");
  const filtered = parts.length > 1;
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
    || cf.hasProductFilter;
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
  // (c) Coverage: docs with at least one product link vs without.
  //     Hidden once a productFilter narrows everything to one side.
  const coverageCard = document.getElementById("card-product-coverage");
  if (cf.productFilter) {
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
  if (cf.productFilter || !(r.byProductTop || []).length) {
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
  if (cf.productFilter || !(r.productsPerVendor || []).length) {
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

async function runIngest(which) {
  const source = state.listingPath;
  if (!source) { log("No listing selected. Click 'Choose file…' first.", "err"); return; }
  log("Running " + which + " ingest…", "info");
  setIngestEnabled(false);
  try {
    const r = await api("/api/ingest", { which, source });
    if (!r.ok) { log(r.error, "err"); return; }
    if (which === "vendors") {
      log("vendors: +" + r.addedVendors + " new, " + r.skippedPaths + " paths skipped", "ok");
    } else if (which === "files") {
      log("files: " + r.files + " rows written, " + r.skippedPaths + " paths skipped", "ok");
    } else if (which === "full") {
      log("full: vendors +" + r.vendors.addedVendors + ", files " + r.files.files + " rows", "ok");
    }
    await refreshStatus();
    await loadRows();
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
  // Preserved across all purges: document_types, ignored_file_types, ignored_folders.
  const PURGED_TABLES = {
    all:     ["vendors", "files", "documents", "document_extracts", "products", "document_products"],
    files:   ["files", "documents", "document_extracts", "document_products"],
    vendors: ["vendors"],
  };
  const label = target === "all" ? "ALL data tables" : target;
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

// Full Process: vendors → files → classify by filename → extract PDFs →
// classify by content. Each stage waits for the previous to finish; the
// extraction phase polls extract-status until idle. Buttons stay disabled
// throughout. Runs every stage every time (idempotent by design).
async function runFullProcess() {
  if (!window.confirm("Run the full process? Vendors + files ingest, then classify by filename, then extract PDFs (slow), then classify by content.")) return;
  if (!state.listingPath) {
    log("No listing chosen. Click 'Choose folder…' first.", "err");
    return;
  }
  setIngestEnabled(false);
  log("══ Full Process: starting", "info");
  try {
    // 1. Vendors ingest
    log("[1/6] vendors ingest…", "info");
    {
      const r = await api("/api/ingest", { which: "vendors", source: state.listingPath });
      if (!r.ok) { log("vendors ingest failed: " + r.error, "err"); return; }
      log("  +" + r.addedVendors + " new vendors, " + r.skippedPaths + " skipped", "ok");
    }

    // 2. Files ingest
    log("[2/6] files ingest…", "info");
    {
      const r = await api("/api/ingest", { which: "files", source: state.listingPath });
      if (!r.ok) { log("files ingest failed: " + r.error, "err"); return; }
      log("  " + r.files + " file rows, " + r.docsCreated + " documents", "ok");
    }
    await refreshStatus();
    await loadRows();

    // 3. Classify by filename
    log("[3/6] classify by filename…", "info");
    {
      const r = await api("/api/classify", {});
      if (!r.ok) { log("classify failed: " + r.error, "err"); return; }
      log("  classified " + r.updated + ", kept " + (r.kept || 0) +
          " (high " + r.byConfidence.high + " · medium " + r.byConfidence.medium +
          " · low " + r.byConfidence.low + ")", "ok");
    }
    await refreshStatus();

    // 4. Extract PDFs (background) — kick off, then poll until idle.
    log("[4/6] extract PDF text (background, this is the slow stage)…", "info");
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

    // 5. Classify by content
    log("[5/6] classify by extract content (low/none only)…", "info");
    {
      const r = await api("/api/classify-by-content", {});
      if (!r.ok) { log("content classify failed: " + r.error, "err"); return; }
      log("  +" + r.updated + " classified from content, " + r.unmatched + " unmatched", "ok");
    }
    await refreshStatus();

    // 6. Classify products (vendor-scoped rules; M:N to documents)
    log("[6/6] classify products…", "info");
    {
      const r = await api("/api/classify-products", {});
      if (!r.ok) { log("product classify failed: " + r.error, "err"); return; }
      log("  " + r.distinctProducts + " products · " + r.totalLinks + " doc-product links · " +
          r.docsWithProducts + "/" + r.docsScanned + " docs matched", "ok");
    }
    await refreshStatus();
    await loadRows();

    log("══ Full Process: done", "ok");
  } catch (e) {
    log("Full Process aborted: " + String(e), "err");
  } finally {
    setIngestEnabled(true);
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
    "run-full-process", "run-vendors", "run-files", "run-full",
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
let extractWasRunning = false;

async function refreshExtractStatus() {
  let r;
  try {
    r = await fetch("/api/extract-status").then((res) => res.json());
  } catch { return; }
  if (!r.ok) return;
  const s = r.status;
  const el    = document.getElementById("extract-status");
  const stopB = document.getElementById("extract-stop");

  // Cumulative corpus progress: how much of the total has been extracted
  // overall (across all start/stop cycles). 0 when there are no PDFs.
  const pct = r.totalPdfs > 0
    ? Math.min(100, Math.max(0, (r.extractedCount / r.totalPdfs) * 100))
    : 0;

  let msg = "Extracted: " + r.extractedCount + " / " + r.totalPdfs + " PDFs (" + pct.toFixed(1) + "%)";
  if (s.running) {
    msg += "  ·  this run: " + s.done + " done";
    if (s.cached)  msg += " · " + s.cached + " cached";
    if (s.failed)  msg += " · " + s.failed + " failed";
    if (s.skipped) msg += " · " + s.skipped + " skipped";
    msg += " of " + s.total;
    el.classList.add("running");
    el.classList.add("with-progress");
    el.style.setProperty("--progress", pct.toFixed(1) + "%");
    stopB.style.display = "";
    if (s.currentDoc) {
      el.innerHTML = msg + '<div class="current"></div>';
      el.querySelector(".current").textContent = "current: " + s.currentDoc.path;
    } else {
      el.textContent = msg;
    }
    refreshTableCounts();
  } else {
    el.classList.remove("running");
    el.classList.remove("with-progress");
    el.style.removeProperty("--progress");
    stopB.style.display = "none";
    if (s.finishedAt && s.total > 0) {
      msg += "  ·  last run: " + s.done + " done";
      if (s.failed) msg += ", " + s.failed + " failed";
    }
    el.textContent = msg;
    if (extractWasRunning) refreshTableCounts();
  }
  extractWasRunning = s.running;
}

function startExtractPolling() {
  if (extractPollTimer) return;
  extractPollTimer = setInterval(refreshExtractStatus, 1500);
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

async function runClassify() {
  log("Classifying all files (filename rules)…", "info");
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
  } catch (e) {
    log(String(e), "err");
  } finally {
    setIngestEnabled(true);
  }
}

async function runClassifyProducts() {
  log("Classifying products (vendor-scoped rules)…", "info");
  setIngestEnabled(false);
  try {
    const r = await api("/api/classify-products", {});
    if (!r.ok) { log(r.error, "err"); return; }
    log(
      "products: " + r.distinctProducts + " distinct · " +
      r.totalLinks + " links · " +
      r.docsWithProducts + "/" + r.docsScanned + " documents matched · " +
      r.rulesLoaded + " rules",
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
  } catch (e) {
    log(String(e), "err");
  } finally {
    setIngestEnabled(true);
  }
}

async function runClassifyByContent() {
  log("Classifying by extract content (low + unclassified only)…", "info");
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
  "purge-vendors":       () => runPurge("vendors"),
  "purge-files":         () => runPurge("files"),
  "purge-all":           () => runPurge("all"),
  "purge-text-backups":  runPurgeTextBackups,
  "run-vendors":         () => runIngest("vendors"),
  "run-files":           () => runIngest("files"),
  "run-full":            () => runIngest("full"),
  "run-full-process":    runFullProcess,
  "classify-all":        runClassify,
  "classify-by-content": runClassifyByContent,
  "classify-products":   runClassifyProducts,
  "extract-start":       runExtractStart,
  "extract-stop":        runExtractStop,
};

document.addEventListener("click", (e) => {
  const row = e.target.closest(".action-row");
  if (!row || row.classList.contains("disabled")) return;
  const action = row.dataset.action;
  const handler = ACTIONS[action];
  if (handler) handler();
  else log("unknown action: " + action, "err");
});

document.getElementById("extract-stop").addEventListener("click", runExtractStop);
document.getElementById("refresh").addEventListener("click", async () => {
  await refreshStatus();
  await loadRows();
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
document.getElementById("confidence-filter").addEventListener("change", (e) => {
  // Toolbar dropdown wins back over any drilldown-set exactConfidence.
  state.exactConfidence = "";
  state.minConfidence = e.target.value;
  state.offset = 0;
  loadRows();
});
document.getElementById("charts-row").addEventListener("click", () => setActiveCharts());
document.getElementById("help-row").addEventListener("click", setActiveHelp);
document.getElementById("ignored-row").addEventListener("click", setActiveIgnored);
document.getElementById("ignored-folders-row").addEventListener("click", setActiveIgnoredFolders);
document.getElementById("doctypes-row").addEventListener("click", () => setActiveTable("document_types"));

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

// Boot
(async () => {
  applySidebarState(localStorage.getItem(SIDEBAR_KEY) === "1");
  await refreshStatus();
  // Hide per-table controls until the user picks a table that exposes them.
  for (const el of document.querySelectorAll(".toolbar .docs-only, .toolbar .ext-only")) {
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

  // Initial extract status fetch + start polling so the running state
  // picks up automatically after a page reload.
  await refreshExtractStatus();
  startExtractPolling();

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
      const helpPath = path.join(SERVER_DIR, "help.html");
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
    if (req.method === "POST" && req.url === "/api/classify-products") {
      sendJson(res, 200, handleClassifyProducts());
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
    if (req.method === "POST" && req.url === "/api/get-extract") {
      sendJson(res, 200, handleGetExtract(await readBody(req)));
      return;
    }
    if (req.method === "POST" && req.url === "/api/open-file") {
      sendJson(res, 200, await handleOpenFile(await readBody(req)));
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
    if (req.method === "GET" && req.url === "/api/dashboard") {
      sendJson(res, 200, handleDashboard());
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
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("Not found");
  } catch (e) {
    sendJson(res, 500, { ok: false, error: e.message });
  }
});

server.listen(PORT, () => {
  console.log(`EnLogosGRAG web app running at http://localhost:${PORT}/`);
});
