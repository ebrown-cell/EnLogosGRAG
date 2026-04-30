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

// Find the vendor segment of a path. Robust to either form:
//   canonical:  S:\vendors\<vendor>\...
//   local:      C:\data\PyroCommData\PyroCommSubset\Vendors\<vendor>\...
// We locate any segment whose name (case-insensitive) is "vendors", then
// take the segment immediately after it. Returns null if there isn't
// such a segment, or if the candidate vendor is _-prefixed (archive
// folder) or has a file extension (a stray .txt at the vendors-root).
export function vendorOf(p) {
  const parts = winParts(p);
  let vendorIdx = -1;
  for (let i = 0; i < parts.length - 1; i++) {
    if (parts[i].toLowerCase() === "vendors") {
      vendorIdx = i + 1;
      break;
    }
  }
  if (vendorIdx < 0 || vendorIdx >= parts.length) return null;
  const seg = parts[vendorIdx];
  if (seg.startsWith("_")) return null;
  if (path.win32.extname(seg)) return null;
  return seg;
}

// Parent dir, or null if this path is a vendor root (its parent would
// be the "vendors" folder which we don't model as a node).
export function parentOf(p) {
  const parts = winParts(p);
  let vendorsIdx = -1;
  for (let i = 0; i < parts.length; i++) {
    if (parts[i].toLowerCase() === "vendors") { vendorsIdx = i; break; }
  }
  if (vendorsIdx < 0) return null;
  // Path must be at least one level below the vendors-root for it to
  // have a parent we care about (the vendor itself, or deeper).
  if (parts.length <= vendorsIdx + 2) return null;
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
// the listing. FAILS LOUDLY if any path's vendor segment is missing from
// `vendors` — caller is expected to run vendors first.
//
// We can't use INSERT OR REPLACE here: the new files schema has an integer
// PK, so REPLACE would churn ids on every re-ingest and break parent_id
// references mid-flight. Clean rebuild is the simpler, correct path.
//
// Eager: every non-dir file gets a paired `documents` row (NULL classification).
export function ingestFiles(db, paths, opts = {}) {
  // applyIgnores: when true (default), drop ignored folders entirely and
  // skip the documents row for ignored extensions. Off → fully untouched
  // ingest, useful when the user wants to see everything in one shot.
  const applyIgnores = opts.applyIgnores !== false;
  const vendorIdByName = new Map(
    db.prepare("SELECT id, name FROM vendors").all().map((r) => [r.name, r.id])
  );

  // First pass: validate every path resolves to a known vendor.
  const missing = new Set();
  let usable = 0;
  let skipped = 0;
  for (const p of paths) {
    const v = vendorOf(p);
    if (!v) {
      skipped++;
      continue;
    }
    if (!vendorIdByName.has(v)) missing.add(v);
    else usable++;
  }
  if (missing.size > 0) {
    const sample = [...missing].slice(0, 10).join(", ");
    throw new Error(
      `Files ingest aborted: ${missing.size} vendor segment(s) not in vendors ` +
        `table. Run vendors ingest first. Missing: ${sample}` +
        (missing.size > 10 ? `, ...` : ""),
    );
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

  const insertFile = db.prepare(
    `INSERT INTO files (path, name, parent_id, vendor_id, file_type, is_dir, depth)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
  );
  const insertDoc = db.prepare(
    "INSERT INTO documents (file_id, document_type_id, confidence) VALUES (?, NULL, NULL)",
  );

  // Extensions to ingest as files but NOT create documents rows for.
  // See db.js / ignored_file_types for the contract.
  const ignoredExts = new Set(
    applyIgnores
      ? db.prepare("SELECT ext FROM ignored_file_types").all().map((r) => r.ext)
      : [],
  );

  let docsCreated = 0;
  let docsSkipped = 0;

  withTx(db, () => {
    // Wipe and start clean. ON DELETE CASCADE drops documents alongside files.
    db.exec("DELETE FROM files");
    db.exec("DELETE FROM documents"); // belt-and-braces; cascade should already handle it
    db.exec("DELETE FROM sqlite_sequence WHERE name IN ('files', 'documents')");

    const idByPath = new Map();

    for (const p of usablePaths) {
      const v = vendorOf(p);
      const parts = winParts(p);
      const name = parts[parts.length - 1] || p;
      const isFile = looksLikeFile(name);
      const ext = isFile ? path.win32.extname(name).toLowerCase() : null;
      const parentPath = parentOf(p);
      const parentId = parentPath ? idByPath.get(parentPath) ?? null : null;

      const result = insertFile.run(
        p,
        name,
        parentId,
        vendorIdByName.get(v),
        ext,
        isFile ? 0 : 1,
        parts.length - 1,
      );
      const fileId = Number(result.lastInsertRowid);
      idByPath.set(p, fileId);

      if (isFile) {
        if (ignoredExts.has(ext)) {
          docsSkipped++;
        } else {
          insertDoc.run(fileId);
          docsCreated++;
        }
      }
    }
  });

  return {
    lines: paths.length,
    files: usable - folderSkipped,
    docsCreated,
    docsSkipped,
    folderSkipped,
    skippedPaths: skipped,
    applyIgnores,
  };
}
