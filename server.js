// Web app for EnLogosGRAG: browse the test.db tables and run ingest actions.
// Single-page UI bundled inline — open http://localhost:8780.

import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { openDb } from "./db.js";
import { readListing, ingestVendors, ingestFiles } from "./listing.js";
import { classifyAll, classifyAllByContent } from "./classifier.js";
import * as extractor from "./extractor.js";

// The extractor needs to translate canonical S:\ paths to local paths
// using the same remap table the open-file feature uses. Wire that up
// once at module load.
extractor.setPathRemapper(remapPath);

const SERVER_DIR = path.dirname(fileURLToPath(import.meta.url));

const PORT = Number(process.env.PORT) || 8780;
const DEFAULT_SOURCE = "C:\\data\\PyroCommData";
const DEFAULT_LISTING_NAME = "basic_listing.txt";

const TABLES = ["vendors", "document_types", "files", "documents", "document_extracts"];

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
    return {
      counts,
      defaultSource: DEFAULT_SOURCE,
      classification: {
        total: totalDocs,
        classified,
        unclassified: totalDocs - classified,
        byConfidence,
      },
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
      });
    }
    if (table === "document_extracts") {
      return browseExtracts(db, { limit, offset, filter });
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
function browseDocuments(db, { limit, offset, filter, classifiedFilter, minConfidence, exactConfidence, fileTypeFilters, documentTypeFilters }) {
  const { from, where, args } = buildDocumentsWhere({
    filter, classifiedFilter, minConfidence, exactConfidence, fileTypeFilters, documentTypeFilters,
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
function buildDocumentsWhere({ filter, classifiedFilter, minConfidence, exactConfidence, fileTypeFilters, documentTypeFilters }) {
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

function handleStats(body = {}) {
  const filterArgs = {
    filter:              body.filter || "",
    classifiedFilter:    body.classifiedFilter || "all",
    minConfidence:       body.minConfidence || "all",
    exactConfidence:     body.exactConfidence || "",
    fileTypeFilters:     Array.isArray(body.fileTypeFilters) ? body.fileTypeFilters : [],
    documentTypeFilters: Array.isArray(body.documentTypeFilters) ? body.documentTypeFilters : [],
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
      // `start` is a cmd builtin, not a separate exe. The empty string is
      // the window title (start treats the first quoted arg as title).
      const child = spawn("cmd.exe", ["/c", "start", "", localPath], {
        detached: true,
        stdio: "ignore",
        windowsVerbatimArguments: false,
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
      // documents cascades from files via ON DELETE CASCADE; document_types
      // is intentionally preserved (one-time taxonomy snapshot).
      db.exec("DELETE FROM files");
      db.exec("DELETE FROM vendors");
      db.exec(
        "DELETE FROM sqlite_sequence WHERE name IN ('vendors', 'files', 'documents')",
      );
      return { ok: true, target, counts: tableCounts(db) };
    }
    if (target === "files") {
      // Cascade wipes documents too.
      db.exec("DELETE FROM files");
      db.exec("DELETE FROM sqlite_sequence WHERE name IN ('files', 'documents')");
      return { ok: true, target, counts: tableCounts(db) };
    }
    if (target === "vendors") {
      const filesCount = db.prepare("SELECT COUNT(*) AS n FROM files").get().n;
      if (filesCount > 0) {
        return {
          ok: false,
          error:
            `Cannot purge vendors while files has ${filesCount} rows ` +
            `(FK constraint). Purge files first, or use "Purge ALL".`,
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
  #log {
    margin-top: 8px; padding: 8px 10px; max-height: 120px; overflow-y: auto;
    font-size: 12px; font-family: monospace;
    background: var(--inset); border: 1px solid var(--border); border-radius: 3px;
  }
  #log .entry { margin: 0 0 2px 0; }
  #log .entry.ok { color: var(--ok); }
  #log .entry.err { color: var(--error-text); }
  #log .entry.info { color: var(--muted); }
  .empty { padding: 20px; text-align: center; color: var(--muted); font-size: 13px; }
</style>
</head>
<body>
<header>
  <h1>EnLogosGRAG</h1>
  <span class="meta" id="status">loading…</span>
</header>
<main>
  <aside id="sidebar">
    <button id="sidebar-toggle" type="button" title="Collapse sidebar">‹</button>
    <h2>Views</h2>
    <div id="charts-row" class="table-row" data-view="charts">
      <span>📊 Charts</span>
    </div>

    <h2>Tables</h2>
    <div id="table-list"></div>

    <h2>Source listing</h2>
    <div id="listing-display">
      <div id="listing-display-empty" class="listing-empty">No folder chosen.</div>
      <div id="listing-display-set" class="listing-set" style="display:none;">
        <div class="listing-name" id="listing-name"></div>
        <div class="listing-path" id="listing-path"></div>
      </div>
    </div>
    <button id="pick-listing" class="secondary">Choose folder…</button>

    <h2>Ingest</h2>
    <button id="run-vendors">Run vendors ingest</button>
    <button id="run-files">Run files ingest</button>
    <button id="run-full">Full Run (vendors + files)</button>

    <h2>Classify</h2>
    <div id="classify-status" class="extract-status">Classified: …</div>
    <button id="classify-all">Classify all files (filename rules)</button>
    <button id="classify-by-content">Classify by extract content (low/none only)</button>

    <h2>Extract</h2>
    <div id="extract-status" class="extract-status">Extracted: …</div>
    <button id="extract-start">Extract PDF text (background)</button>
    <button id="extract-stop" class="secondary" style="display:none;">Stop extraction</button>

    <h2>Purge</h2>
    <button id="purge-vendors" class="danger">Purge vendors</button>
    <button id="purge-files" class="danger">Purge files</button>
    <button id="purge-all" class="danger">Purge ALL tables</button>

    <h2>&nbsp;</h2>
    <button id="refresh" class="secondary">Refresh status</button>
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
      </div>
    </div>
    <div id="log"></div>
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
  total: 0,
  // Chart-page drilldown filter chain. Independent from the table filters
  // above — switching to charts clears it. Drilldowns compose into this.
  chartFilter: {
    classifiedFilter: "all",
    exactConfidence: "",
    fileTypeFilters: [],
    documentTypeFilters: [],
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

function setActiveCharts() {
  state.view = "charts";
  // Reset chart-page drilldown chain — entering charts is a fresh start.
  state.chartFilter = {
    classifiedFilter: "all",
    exactConfidence: "",
    fileTypeFilters: [],
    documentTypeFilters: [],
  };
  document.querySelectorAll(".table-row").forEach((el) => {
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

function showTableView() {
  document.getElementById("table-view").style.display = "";
  document.getElementById("chart-view").style.display = "none";
  // Re-show toolbar pagination controls (hidden in chart mode would be wrong;
  // they're already there — but disabling them keeps them inert).
  document.querySelectorAll(".toolbar input, .toolbar button").forEach((el) => {
    el.style.display = "";
  });
  document.getElementById("page-meta").style.display = "";
}

function showChartView() {
  document.getElementById("table-view").style.display = "none";
  document.getElementById("chart-view").style.display = "block";
  // Hide toolbar inputs that don't apply to the chart view.
  for (const id of ["filter", "prev", "next"]) {
    document.getElementById(id).style.display = "none";
  }
  document.getElementById("page-meta").style.display = "none";
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
  return classifiedHidden && confidenceHidden && filetypeHidden && doctypeHidden;
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
  });
}

function renderBreadcrumb() {
  const cf = state.chartFilter;
  const parts = ["All documents"];
  if (cf.classifiedFilter && cf.classifiedFilter !== "all") parts.push(cf.classifiedFilter);
  if (cf.fileTypeFilters     && cf.fileTypeFilters.length)     parts.push("file_type=" + cf.fileTypeFilters.join(", "));
  if (cf.documentTypeFilters && cf.documentTypeFilters.length) parts.push("document_type=" + cf.documentTypeFilters.join(", "));
  if (cf.exactConfidence)    parts.push("confidence=" + cf.exactConfidence);
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
    || (cf.documentTypeFilters && cf.documentTypeFilters.length);
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
  const label = target === "all" ? "BOTH tables" : target;
  if (!window.confirm("Really delete every row from " + label + "?")) return;
  log("Purging " + label + "…", "info");
  const r = await api("/api/purge", { target });
  if (!r.ok) { log(r.error, "err"); return; }
  log("purged " + label + ". now: " + r.counts.vendors + " vendors, " + r.counts.files + " files", "ok");
  await refreshStatus();
  await loadRows();
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
  for (const id of ["run-vendors", "run-files", "run-full", "classify-all", "classify-by-content"]) {
    document.getElementById(id).disabled = !on;
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
  const el     = document.getElementById("extract-status");
  const startB = document.getElementById("extract-start");
  const stopB  = document.getElementById("extract-stop");

  let msg = "Extracted: " + r.extractedCount + " / " + r.totalPdfs + " PDFs";
  if (s.running) {
    msg += "  ·  this run: " + s.done + " done";
    if (s.failed)  msg += " · " + s.failed + " failed";
    if (s.skipped) msg += " · " + s.skipped + " skipped";
    msg += " of " + s.total;
    el.classList.add("running");
    startB.disabled = true;
    stopB.style.display = "";
    if (s.currentDoc) {
      el.innerHTML = msg + '<div class="current"></div>';
      el.querySelector(".current").textContent = "current: " + s.currentDoc.path;
    } else {
      el.textContent = msg;
    }
    // Live update of sidebar table counts while the run is in flight.
    refreshTableCounts();
  } else {
    el.classList.remove("running");
    startB.disabled = false;
    stopB.style.display = "none";
    if (s.finishedAt && s.total > 0) {
      msg += "  ·  last run: " + s.done + " done";
      if (s.failed) msg += ", " + s.failed + " failed";
    }
    el.textContent = msg;
    // One last sidebar refresh on the running→idle transition so the
    // final document_extracts count settles correctly.
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
      " · low " + r.byConfidence.low + " · unmatched " + r.byConfidence.none + ")",
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
document.getElementById("run-vendors").addEventListener("click", () => runIngest("vendors"));
document.getElementById("run-files").addEventListener("click", () => runIngest("files"));
document.getElementById("run-full").addEventListener("click", () => runIngest("full"));
document.getElementById("purge-vendors").addEventListener("click", () => runPurge("vendors"));
document.getElementById("purge-files").addEventListener("click", () => runPurge("files"));
document.getElementById("purge-all").addEventListener("click", () => runPurge("all"));
document.getElementById("classify-all").addEventListener("click", runClassify);
document.getElementById("classify-by-content").addEventListener("click", runClassifyByContent);
document.getElementById("extract-start").addEventListener("click", runExtractStart);
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
document.getElementById("charts-row").addEventListener("click", setActiveCharts);
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
  setListingPath(localStorage.getItem(LISTING_KEY) || "");

  // Initial extract status fetch + start polling so the running state
  // picks up automatically after a page reload.
  await refreshExtractStatus();
  startExtractPolling();
  await loadRows();
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
    if (req.method === "POST" && req.url === "/api/classify") {
      sendJson(res, 200, handleClassify());
      return;
    }
    if (req.method === "POST" && req.url === "/api/classify-by-content") {
      sendJson(res, 200, handleClassifyByContent());
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
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("Not found");
  } catch (e) {
    sendJson(res, 500, { ok: false, error: e.message });
  }
});

server.listen(PORT, () => {
  console.log(`EnLogosGRAG web app running at http://localhost:${PORT}/`);
});
