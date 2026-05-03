// Listing parser + ingest logic. One Windows path per line.
// Vendor = the segment after "vendors\" (path part index 2).
// Skips paths whose vendor segment is _-prefixed or has a file extension
// (filters archive folders + listing self-references like basic_listing.txt).

import fs from "node:fs";
import path from "node:path";

const FILE_EXTS = new Set([
  ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt",
  ".csv", ".zip", ".rar", ".7z", ".jpg", ".jpeg", ".png", ".gif", ".bmp",
  ".tif", ".tiff", ".dwg", ".dxf", ".rtf", ".msg", ".eml", ".html", ".htm",
  ".xml", ".json", ".log", ".cfg", ".ini", ".bak", ".mp4", ".mov", ".avi",
  // Executables / installers / scripts. Recognized as files so the
  // ignore-extension path can drop them; not allowing them here means
  // they'd be misclassified as directories at ingest.
  ".exe", ".dll", ".msi", ".dmg", ".pkg", ".bat", ".cmd", ".ps1",
  ".sh", ".vbs", ".bin",
]);

function looksLikeFile(name) {
  return FILE_EXTS.has(path.win32.extname(name).toLowerCase());
}

// Split a Windows path into segments. path.win32.parse only gives us root +
// dir + base; we want every segment between separators.
function winParts(p) {
  // path.win32.normalize collapses "S:\\vendors\\..." → "S:\\vendors\\..."
  // We split on \ after stripping trailing separators.
  const parts = p.replace(/\\+$/, "").split("\\").filter(Boolean);
  return parts;
}

// Source-data-type roots: which segment name marks the start of each
// corpus. Match is case-insensitive; the segment AFTER this one is the
// "owner" (vendor for Vendor corpus, job/site for JobFiles).
const SOURCE_DATA_ROOTS = [
  { segment: "vendors",  type: "Vendor"   },
  { segment: "jobfiles", type: "JobFiles" },
  // Sales: per-salesperson archives. Owner segment is the salesperson's
  // folder name ("Blanca Varney", "Brandon Fopma", …). Personal-archive
  // subfolders prefixed with "_" (_2018, _back up) are skipped by the
  // existing _-prefix guard in vendorOf().
  { segment: "sales",    type: "Sales"    },
];

// Find the source-data-type root segment in a path. Returns the index of
// the root segment (e.g. 'vendors' or 'JobFiles') and its type, or null
// if the path doesn't sit under any known root.
function findSourceDataRoot(parts) {
  for (let i = 0; i < parts.length; i++) {
    const lower = parts[i].toLowerCase();
    for (const root of SOURCE_DATA_ROOTS) {
      if (lower === root.segment) return { idx: i, type: root.type };
    }
  }
  return null;
}

// Source-data-type for a given path. 'Vendor' for paths under a vendors/
// segment, 'JobFiles' for paths under a JobFiles/ segment, 'Sales' for
// paths under a sales/ segment, 'Other' otherwise. Stamped onto each row
// in files.source_data_type at ingest.
export function sourceDataTypeOf(p) {
  const root = findSourceDataRoot(winParts(p));
  return root ? root.type : "Other";
}

// Find the "owner" segment of a path — the folder immediately after the
// source-data-type root. Meaning depends on the corpus:
//   Vendor:   manufacturer name (Notifier, Potter, ...)
//   JobFiles: job/site folder    (1 Ambroise-Newport-Coast-Bordeaux Apts, ...)
//   Sales:    salesperson folder (Blanca Varney, Brandon Fopma, ...)
// Returns null if the path doesn't sit under any known root, or if the
// candidate is _-prefixed (archive) or has a file extension (stray file
// at the root level).
export function vendorOf(p) {
  const parts = winParts(p);
  const root = findSourceDataRoot(parts);
  if (!root) return null;
  const ownerIdx = root.idx + 1;
  if (ownerIdx >= parts.length) return null;
  const seg = parts[ownerIdx];
  if (seg.startsWith("_")) return null;
  if (path.win32.extname(seg)) return null;
  return seg;
}

// Parent dir, or null if this path is at the source-data-type root level.
// Works across both Vendor and JobFiles corpora.
export function parentOf(p) {
  const parts = winParts(p);
  const root = findSourceDataRoot(parts);
  if (!root) return null;
  // Path must be at least one level below the owner (vendor / job folder)
  // for it to have a parent we care about.
  if (parts.length <= root.idx + 2) return null;
  return parts.slice(0, -1).join("\\");
}

export function parseListing(text) {
  const seen = new Set();
  const out = [];
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || seen.has(line)) continue;
    seen.add(line);
    out.push(line);
  }
  return out;
}

export function readListing(filePath) {
  const text = fs.readFileSync(filePath, "utf8");
  return parseListing(text);
}

function withTx(db, fn) {
  db.exec("BEGIN");
  try {
    const r = fn();
    db.exec("COMMIT");
    return r;
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
}

// --- Vendors-only ingest ----------------------------------------------------
// Inserts any vendor segment from the listing that isn't already in `vendors`.
// Idempotent. Returns counts.
export function ingestVendors(db, paths) {
  const existing = new Map(
    db.prepare("SELECT id, name FROM vendors").all().map((r) => [r.name, r.id])
  );
  const insert = db.prepare("INSERT INTO vendors (name) VALUES (?)");

  let added = 0;
  let skipped = 0;
  const seenInThisRun = new Set();

  withTx(db, () => {
    for (const p of paths) {
      const v = vendorOf(p);
      if (!v) {
        skipped++;
        continue;
      }
      if (existing.has(v) || seenInThisRun.has(v)) continue;
      insert.run(v);
      seenInThisRun.add(v);
      added++;
    }
  });

  return { lines: paths.length, addedVendors: added, skippedPaths: skipped };
}

// --- Files-only ingest ------------------------------------------------------
// Wipes `files` (and via ON DELETE CASCADE, `documents`) and rebuilds from
// the listing. Vendors are no longer a precondition — vendor association
// lives on `documents.vendor_id` (nullable) and is auto-created on demand
// for Vendor-SDT paths. JobFiles paths leave it NULL.
//
// We can't use INSERT OR REPLACE here: the files schema has an integer
// PK, so REPLACE would churn ids on every re-ingest and break parent_id
// references mid-flight. Clean rebuild is the simpler, correct path.
//
// Eager: every non-dir file gets a paired `documents` row (NULL classification).
export function ingestFiles(db, paths, opts = {}) {
  // applyIgnores: when true (default), drop ignored folders entirely and
  // skip the documents row for ignored extensions. Off → fully untouched
  // ingest, useful when the user wants to see everything in one shot.
  const applyIgnores = opts.applyIgnores !== false;
  // sourceDataType: which corpus this run owns. Re-running an ingest only
  // clears rows tagged with this SDT — leaves the OTHER corpus alone so
  // Vendor and JobFiles can coexist in one DB. If absent (legacy callers),
  // we fall back to the all-corpus wipe behavior so behavior stays defined.
  const sourceDataType = opts.sourceDataType || null;
  // corpusRoot: absolute path of the chosen folder. Stamped onto each
  // files row so the extract/hash caches can be routed to the sibling
  // <corpusRoot>_text directory without segment-magic.
  const corpusRoot = opts.corpusRoot || null;

  // Vendor association is now soft + lives on documents (not files). Cache
  // existing vendor name→id lookups so we can stamp them onto documents
  // without a per-row roundtrip. New vendors auto-created on demand for
  // Vendor-SDT paths only — JobFiles paths leave documents.vendor_id NULL.
  const vendorIdByName = new Map(
    db.prepare("SELECT id, name FROM vendors").all().map((r) => [r.name, r.id])
  );
  const insertVendor = db.prepare("INSERT INTO vendors (name) VALUES (?)");
  function getOrCreateVendorId(name) {
    if (!name) return null;
    let id = vendorIdByName.get(name);
    if (id != null) return id;
    const r = insertVendor.run(name);
    id = Number(r.lastInsertRowid);
    vendorIdByName.set(name, id);
    return id;
  }

  // First pass: just count usable paths. No vendor validation — vendor
  // association is soft now, not a precondition.
  let usable = 0;
  let skipped = 0;
  for (const p of paths) {
    if (vendorOf(p) === null) { skipped++; continue; }
    usable++;
  }

  // Folders to drop entirely (the folder + all descendants). Match is
  // exact, case-insensitive, against any segment of the path. See
  // db.js / ignored_folders for the contract.
  const ignoredFolders = new Set(
    applyIgnores
      ? db.prepare("SELECT name FROM ignored_folders").all()
          .map((r) => r.name.toLowerCase())
      : [],
  );
  const hasIgnoredSegment = (p) => {
    if (ignoredFolders.size === 0) return false;
    for (const seg of winParts(p)) {
      if (ignoredFolders.has(seg.toLowerCase())) return true;
    }
    return false;
  };
  let folderSkipped = 0;

  // Sort paths by depth ascending so a parent is always inserted before
  // its children — lets us look up parent_id from the in-memory Map as we go.
  const usablePaths = paths
    .filter((p) => {
      if (vendorOf(p) === null) return false;
      if (hasIgnoredSegment(p)) { folderSkipped++; return false; }
      return true;
    })
    .map((p) => ({ p, depth: winParts(p).length - 1 }))
    .sort((a, b) => a.depth - b.depth)
    .map((x) => x.p);

  // ON CONFLICT(path) DO NOTHING: a duplicate path in the source listing
  // (or a stray collision from a partial prior run) shouldn't abort the
  // whole ingest. We skip the row and look up the existing id below so
  // descendants still find their parent.
  const insertFile = db.prepare(
    `INSERT INTO files (path, name, parent_id, file_type, is_dir, depth, source_data_type, corpus_root)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(path) DO NOTHING`,
  );
  const lookupFileId = db.prepare("SELECT id FROM files WHERE path = ?");
  const insertDoc = db.prepare(
    "INSERT INTO documents (file_id, document_type_id, confidence, vendor_id) VALUES (?, NULL, NULL, ?)",
  );

  // Extensions to ingest as files but NOT create documents rows for.
  // See db.js / ignored_file_types for the contract.
  const ignoredExts = new Set(
    applyIgnores
      ? db.prepare("SELECT ext FROM ignored_file_types").all().map((r) => r.ext)
      : [],
  );
  // Specific paths the user has ignored (manually or via the de-duplicate
  // action). Ingested as files but no documents row. See db.js / ignored_files.
  const ignoredPaths = new Set(
    applyIgnores
      ? db.prepare("SELECT path FROM ignored_files").all().map((r) => r.path)
      : [],
  );

  let docsCreated = 0;
  let docsSkipped = 0;
  // Counter pair under incremental ingest:
  //   filesAdded     — new files row inserted this run
  //   filesUnchanged — path already existed; row + downstream classifications
  //                    preserved. Ingest is additive; only Purge wipes.
  let filesAdded = 0;
  let filesUnchanged = 0;
  // Per-SDT file count, populated as we walk usablePaths. Surfaced in the
  // return value so the UI can log "Vendor: 4880, JobFiles: 2958, Sales: 142"
  // and the user can see at a glance which corpus came in.
  const bySourceDataType = { Vendor: 0, JobFiles: 0, Sales: 0, Other: 0 };

  withTx(db, () => {
    // Incremental ingest: do NOT wipe. New paths are inserted via
    // INSERT … ON CONFLICT(path) DO NOTHING below; paths that already
    // exist keep their id (and therefore their downstream rows in
    // documents / document_extracts / document_buildings / etc).
    // Use the Purge actions to wipe — ingest is now additive only.
    //
    // Pre-seed idByPath with existing rows so a fresh insert of a child
    // file finds its existing parent directory's id without a per-row
    // SELECT. Scope to the active SDT (or all rows for a global ingest)
    // so the map stays bounded.
    const idByPath = new Map();
    {
      const seedSql = sourceDataType
        ? "SELECT id, path FROM files WHERE source_data_type = ?"
        : "SELECT id, path FROM files";
      const stmt = db.prepare(seedSql);
      const rows = sourceDataType ? stmt.all(sourceDataType) : stmt.all();
      for (const r of rows) idByPath.set(r.path, r.id);
    }

    for (const p of usablePaths) {
      const v = vendorOf(p);
      const parts = winParts(p);
      const name = parts[parts.length - 1] || p;
      const isFile = looksLikeFile(name);
      const ext = isFile ? path.win32.extname(name).toLowerCase() : null;
      const parentPath = parentOf(p);
      const parentId = parentPath ? idByPath.get(parentPath) ?? null : null;

      const sdt = sourceDataTypeOf(p);
      bySourceDataType[sdt] = (bySourceDataType[sdt] || 0) + 1;
      const result = insertFile.run(
        p,
        name,
        parentId,
        ext,
        isFile ? 0 : 1,
        parts.length - 1,
        sdt,
        corpusRoot,
      );
      // changes === 0 means the path already existed in files. Under
      // incremental ingest that's the common case, not an error. Look up
      // the existing row so descendants can still resolve their parent_id,
      // and skip creating a second documents row for it (any prior
      // documents/document_buildings/etc rows are intentionally preserved).
      let fileId;
      let isNewFile;
      if (result.changes === 0) {
        // Pre-seeded idByPath usually has it; lookup is the fallback for
        // paths under a different SDT seen mid-loop.
        fileId = idByPath.get(p) ?? lookupFileId.get(p)?.id ?? null;
        isNewFile = false;
        filesUnchanged++;
      } else {
        fileId = Number(result.lastInsertRowid);
        isNewFile = true;
        filesAdded++;
      }
      if (fileId !== null) idByPath.set(p, fileId);

      if (isFile && isNewFile && fileId !== null) {
        if (ignoredExts.has(ext) || ignoredPaths.has(p)) {
          docsSkipped++;
        } else {
          // Vendor association: only for Vendor-SDT paths. JobFiles paths
          // (and any 'Other') leave documents.vendor_id NULL — the owner
          // segment in those paths is a job/site name, not a manufacturer.
          const docVendorId = sdt === "Vendor" ? getOrCreateVendorId(v) : null;
          insertDoc.run(fileId, docVendorId);
          docsCreated++;
        }
      }
    }
  });

  return {
    lines: paths.length,
    // Total files rows touched by this listing (added + unchanged), kept
    // under the legacy `files` key so existing UI logging keeps working.
    files: filesAdded + filesUnchanged,
    filesAdded,
    filesUnchanged,
    docsCreated,
    docsSkipped,
    folderSkipped,
    skippedPaths: skipped,
    applyIgnores,
    bySourceDataType,
  };
}
