// PDF text extractor using pdfjs-dist. Pulls text content + per-item
// bounding boxes from each page so future table-reconstruction code has
// the spatial context to work with.
//
// Phase 1 (this file): flat text + per-page item list with bboxes.
// Phase 2 (later): layer table detection on top of pages_json.
//
// The extractor runs as a singleton background loop. Calling start()
// while it's already running is a no-op; stop() flips a flag and the
// loop exits at the next file boundary.

import fs from "node:fs";
import path from "node:path";

// pdfjs ships an ESM build at build/pdf.mjs. We import it lazily inside
// the worker so module load doesn't slow server startup for users who
// never run an extraction.
let pdfjsLib = null;
async function loadPdfjs() {
  if (pdfjsLib) return pdfjsLib;
  // pdfjs's "modern" build assumes browser globals (DOMMatrix, etc.).
  // The "legacy" build is the Node-friendly one.
  pdfjsLib = await import("pdfjs-dist/legacy/build/pdf.mjs");
  // Point at the legacy worker file. pdfjs requires *some* workerSrc
  // value or it throws "No GlobalWorkerOptions.workerSrc specified".
  // We resolve the worker file inside the package and pass its file URL.
  const { createRequire } = await import("node:module");
  const req = createRequire(import.meta.url);
  const workerPath = req.resolve("pdfjs-dist/legacy/build/pdf.worker.mjs");
  pdfjsLib.GlobalWorkerOptions.workerSrc = "file:///" + workerPath.replace(/\\/g, "/");
  return pdfjsLib;
}

// mammoth handles .docx text extraction. Lazy-load for the same reason
// as pdfjs — never paid by users who don't run an extraction. .doc (the
// legacy OLE binary format) is NOT supported by mammoth; we route only
// .docx files through here.
let mammothLib = null;
async function loadMammoth() {
  if (mammothLib) return mammothLib;
  mammothLib = (await import("mammoth")).default;
  return mammothLib;
}

const state = {
  running: false,
  shouldStop: false,
  total: 0,         // pdfs in this batch
  done: 0,          // newly extracted via pdfjs this batch
  cached: 0,        // restored from filesystem backup this batch (no pdfjs run)
  failed: 0,        // extraction errors this batch
  skipped: 0,       // already extracted, skipped this batch
  currentDoc: null, // {id, path} or null
  startedAt: null,
  finishedAt: null,
  lastError: null,
};

export function getStatus() {
  return {
    running: state.running,
    total: state.total,
    done: state.done,
    cached: state.cached,
    failed: state.failed,
    skipped: state.skipped,
    currentDoc: state.currentDoc,
    startedAt: state.startedAt,
    finishedAt: state.finishedAt,
    lastError: state.lastError,
  };
}

export function stop() {
  if (!state.running) return false;
  state.shouldStop = true;
  return true;
}

// Translate a canonical S:\vendors\... path to its local equivalent using
// the same PATH_REMAPS the open-file feature uses. Imported via callback
// so we don't take a hard dep on server.js layout.
let pathRemapper = (p) => p;
export function setPathRemapper(fn) { pathRemapper = fn; }

// Start the extraction worker. opts.openDb is the same factory the rest
// of the app uses; opts.onlyMissing controls whether already-extracted
// docs get reprocessed (default: only-missing = true).
export function start(opts) {
  if (state.running) return false;
  state.running = true;
  state.shouldStop = false;
  state.total = 0;
  state.done = 0;
  state.cached = 0;
  state.failed = 0;
  state.skipped = 0;
  state.currentDoc = null;
  state.startedAt = new Date().toISOString();
  state.finishedAt = null;
  state.lastError = null;

  // Kick off the loop without awaiting — caller returns immediately and
  // /api/extract-status reports progress.
  runWorker(opts).catch((e) => {
    state.lastError = String(e);
  }).finally(() => {
    state.running = false;
    state.shouldStop = false;
    state.currentDoc = null;
    state.finishedAt = new Date().toISOString();
  });
  return true;
}

// File types this extractor knows how to process. Each entry maps an
// extension (lowercase, with leading dot) to the per-file extract fn.
// Adding a new format = add an entry here and a matching extractFoo() —
// the rest of the worker (queueing, caching, DB write) is format-agnostic.
const EXTRACTORS = {
  ".pdf":  (path) => extractPdf(path),
  ".docx": (path) => extractDocx(path),
};
const EXTRACTABLE_EXTS = Object.keys(EXTRACTORS);

async function runWorker(opts) {
  // Lazy-load every backend up front. Cheap (just module imports) and
  // keeps us from interleaving init with the extraction loop.
  await loadPdfjs();
  await loadMammoth();

  // Pull the work queue once at the start of the run. New files added
  // mid-run won't get picked up — caller can re-click after the run ends.
  const db = opts.openDb();
  let queue;
  try {
    const placeholders = EXTRACTABLE_EXTS.map(() => "?").join(",");
    const sql = opts.onlyMissing
      ? `SELECT d.id AS doc_id, f.path AS file_path, f.file_type AS file_type, f.corpus_root AS corpus_root
           FROM documents d
           JOIN files f ON f.id = d.file_id
           LEFT JOIN document_extracts e ON e.document_id = d.id
          WHERE e.document_id IS NULL AND f.file_type IN (${placeholders})
          ORDER BY d.id`
      : `SELECT d.id AS doc_id, f.path AS file_path, f.file_type AS file_type, f.corpus_root AS corpus_root
           FROM documents d
           JOIN files f ON f.id = d.file_id
          WHERE f.file_type IN (${placeholders})
          ORDER BY d.id`;
    queue = db.prepare(sql).all(...EXTRACTABLE_EXTS);
  } finally {
    db.close();
  }
  state.total = queue.length;

  const upsert = "INSERT OR REPLACE INTO document_extracts " +
    "(document_id, extracted_at, page_count, text, pages_json, metadata, error) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?)";

  for (const row of queue) {
    if (state.shouldStop) break;
    state.currentDoc = { id: row.doc_id, path: row.file_path };

    const localPath = pathRemapper(row.file_path);
    // After remapping, the file might live under a different folder than
    // its stored corpus_root (path-remap layer). Compute the cache path
    // against the LOCAL root: replace the canonical-root prefix in
    // corpus_root with the remapped one. Easiest: just remap corpus_root
    // through the same remapper.
    const localRoot = row.corpus_root ? pathRemapper(row.corpus_root) : null;
    const backupPath = backupPathFor(localRoot, localPath);
    const ts = new Date().toISOString();

    let pageCount = null, text = null, pagesJson = null, metadata = null, errMsg = null;
    let fromCache = false;

    try {
      // 1. Try the filesystem cache first. A successful prior extract
      //    sits at <Vendors_text>/<rel-path>.json. Restoring is just a
      //    JSON parse — no pdfjs invocation.
      if (backupPath && fs.existsSync(backupPath)) {
        const cached = JSON.parse(fs.readFileSync(backupPath, "utf8"));
        pageCount = cached.pageCount ?? null;
        text      = cached.text ?? null;
        pagesJson = cached.pages ? JSON.stringify(cached.pages) : null;
        metadata  = cached.metadata ? JSON.stringify(cached.metadata) : null;
        fromCache = true;
      } else {
        if (!fs.existsSync(localPath)) {
          throw new Error("Local file not found: " + localPath);
        }
        // Dispatch by extension. The queue query already filtered to
        // EXTRACTABLE_EXTS so we should always have a handler — but if
        // the queue ever drifts (e.g. mixed-case .PDF the schema lower-
        // cased), throw a clear error instead of silently writing nulls.
        const ext = (row.file_type || "").toLowerCase();
        const handler = EXTRACTORS[ext];
        if (!handler) {
          throw new Error("No extractor registered for file_type: " + ext);
        }
        const result = await handler(localPath);
        pageCount = result.pageCount;
        text      = result.text;
        pagesJson = result.pages ? JSON.stringify(result.pages) : null;
        metadata  = result.metadata ? JSON.stringify(result.metadata) : null;

        // 2. Persist the fresh extract to the filesystem cache. Best-effort:
        //    if write fails (read-only volume, etc.) we still have the
        //    DB row, so don't propagate the error.
        if (backupPath) {
          try {
            fs.mkdirSync(path.dirname(backupPath), { recursive: true });
            fs.writeFileSync(backupPath, JSON.stringify({
              extractedAt: ts,
              pageCount: result.pageCount,
              text: result.text,
              pages: result.pages,
              metadata: result.metadata,
            }), "utf8");
          } catch (e) {
            console.warn("[extractor] backup write failed:", backupPath, e.message);
          }
        }
      }
    } catch (e) {
      errMsg = String(e.message || e);
    }

    // Open a fresh DB connection per write — node:sqlite doesn't share
    // well across long-lived async work and we want the changes durable
    // even if a subsequent page crashes.
    const wdb = opts.openDb();
    try {
      wdb.prepare(upsert).run(
        row.doc_id, ts, pageCount, text, pagesJson, metadata, errMsg,
      );
    } finally {
      wdb.close();
    }

    if (errMsg) {
      state.failed++;
      state.lastError = errMsg;
    } else if (fromCache) {
      state.cached++;
    } else {
      state.done++;
    }

    // Yield to the event loop every 25 items so /api/extract-status can
    // respond mid-batch. Without this, all-cached runs never expose
    // progress — the loop body for cached items is synchronous JSON+SQLite
    // work that completes too fast for the client poll to catch.
    const processed = state.done + state.cached + state.failed + state.skipped;
    if (processed % 25 === 0) {
      await new Promise((resolve) => setImmediate(resolve));
    }
  }
}

// Map a (corpus_root, localPath, suffix) triple to its sidecar cache path.
// The cache lives in <corpus_root>_text/, mirroring the relative path of
// the source under corpus_root. Returns null if either input is missing
// or the file isn't actually under the corpus root.
//
// Example (suffix='.json'):
//   corpus_root: C:\data\PyroCommData\PyroCommSubset\Vendors
//   localPath:   C:\data\PyroCommData\PyroCommSubset\Vendors\Notifier\Manuals\foo.pdf
//   out:         C:\data\PyroCommData\PyroCommSubset\Vendors_text\Notifier\Manuals\foo.pdf.json
//
// Used by extractor (.json) and hasher (.hash). Exported so hasher.js can
// reuse the same routing.
export function cachePathFor(corpusRoot, localPath, suffix) {
  if (!corpusRoot || !localPath) return null;
  const sep = localPath.includes("\\") ? "\\" : "/";
  // Trim a trailing separator on corpusRoot so prefix matching is exact.
  const root = corpusRoot.replace(/[\\/]+$/, "");
  // Case-insensitive prefix check — Windows paths can mix Vendors/vendors.
  if (localPath.toLowerCase().indexOf(root.toLowerCase() + sep) !== 0) {
    // The file isn't under the recorded corpus root. Don't cache — we
    // don't know where to put it, and inventing a location would scatter
    // sidecars unpredictably.
    return null;
  }
  const rel = localPath.slice(root.length + 1);  // skip the separator too
  return root + "_text" + sep + rel + suffix;
}

// Back-compat shim used by the extractor loop below. Wraps cachePathFor
// for the .json suffix.
function backupPathFor(corpusRoot, localPath) {
  return cachePathFor(corpusRoot, localPath, ".json");
}

// Extract one PDF. Returns { pageCount, text, pages: [...], metadata }.
// Each page in `pages` is { pageNumber, items: [{ str, bbox: [x,y,w,h] }] }.
// bbox is in PDF user space (origin bottom-left, units = points = 1/72").
async function extractPdf(filePath) {
  const data = new Uint8Array(fs.readFileSync(filePath));
  const loadingTask = pdfjsLib.getDocument({
    data,
    // Quiet pdfjs's console output for this run.
    verbosity: 0,
  });
  const doc = await loadingTask.promise;

  let metadata = null;
  try {
    const m = await doc.getMetadata();
    if (m && m.info) metadata = m.info;
  } catch { /* metadata is best-effort */ }

  const pages = [];
  const textChunks = [];
  for (let p = 1; p <= doc.numPages; p++) {
    const page = await doc.getPage(p);
    const content = await page.getTextContent();
    const items = [];
    const lineParts = [];
    for (const it of content.items) {
      // pdfjs gives us the transform matrix for each text run. The last
      // two entries (a[4], a[5]) are the x/y origin in user space. The
      // width is `it.width` and the height is `it.height` (font size).
      const t = it.transform || [1, 0, 0, 1, 0, 0];
      const x = t[4], y = t[5];
      const w = it.width || 0, h = it.height || 0;
      items.push({ str: it.str, bbox: [x, y, w, h] });
      lineParts.push(it.str);
      if (it.hasEOL) lineParts.push("\n");
    }
    pages.push({ pageNumber: p, items });
    textChunks.push(lineParts.join(""));
  }
  await doc.cleanup();
  await doc.destroy();

  return {
    pageCount: doc.numPages,
    text: textChunks.join("\n\n=== PAGE BREAK ===\n\n"),
    pages,
    metadata,
  };
}

// Extract one .docx via mammoth. Returns the same shape as extractPdf so
// the worker / cache writers are unchanged. .docx has no native page
// concept (pagination is done at render time), so:
//   pageCount: null  — unknown
//   pages: null      — no per-page item list
// The text body is the only thing we get; that's all the content rules
// need (they match against `extract`, never against `pages_json`).
async function extractDocx(filePath) {
  // mammoth.extractRawText returns plain text with paragraph breaks but
  // strips formatting, footnotes, and embedded images. That's the right
  // tradeoff for content-rule matching — rules anchor on words, not
  // styling. For first-page-only matching, the content classifier already
  // takes whatever the first ~600 chars happen to be.
  const buffer = fs.readFileSync(filePath);
  const result = await mammothLib.extractRawText({ buffer });
  // mammoth surfaces parser warnings on `result.messages`. Most are
  // benign (unrecognized styles, unsupported elements). We capture the
  // first few in metadata so they're inspectable from the extract modal,
  // but never throw — partial text is better than no text.
  const warnings = (result.messages || [])
    .slice(0, 10)
    .map((m) => m.type + ": " + m.message);
  return {
    pageCount: null,
    text: result.value || "",
    pages: null,
    metadata: warnings.length ? { warnings } : null,
  };
}
