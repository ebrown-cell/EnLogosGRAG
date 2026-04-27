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

const state = {
  running: false,
  shouldStop: false,
  total: 0,         // pdfs in this batch
  done: 0,          // successfully extracted this batch
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

async function runWorker(opts) {
  await loadPdfjs();

  // Pull the work queue once at the start of the run. New PDFs added
  // mid-run won't get picked up — caller can re-click after the run ends.
  const db = opts.openDb();
  let queue;
  try {
    const sql = `
      SELECT d.id AS doc_id, f.path AS file_path
      FROM documents d
      JOIN files f ON f.id = d.file_id
      ${opts.onlyMissing
        ? "LEFT JOIN document_extracts e ON e.document_id = d.id WHERE e.document_id IS NULL AND f.file_type = '.pdf'"
        : "WHERE f.file_type = '.pdf'"}
      ORDER BY d.id
    `;
    queue = db.prepare(sql).all();
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
    const ts = new Date().toISOString();

    let pageCount = null, text = null, pagesJson = null, metadata = null, errMsg = null;
    try {
      if (!fs.existsSync(localPath)) {
        throw new Error("Local file not found: " + localPath);
      }
      const result = await extractPdf(localPath);
      pageCount = result.pageCount;
      text      = result.text;
      pagesJson = JSON.stringify(result.pages);
      metadata  = result.metadata ? JSON.stringify(result.metadata) : null;
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
    } else {
      state.done++;
    }
  }
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
