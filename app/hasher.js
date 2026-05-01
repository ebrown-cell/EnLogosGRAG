// Computes SHA-256 hashes for non-ignored files (the document set) so we
// can detect duplicates across the corpus. Mirrors extractor.js's pattern:
// singleton background worker with start/stop/status, kicked off via the
// HTTP API and polled by the UI.
//
// Hashes live on the documents table (column added by db.js). Files with
// excluded extensions or in excluded folders never get a documents row,
// so they're naturally skipped.

import fs from "node:fs";
import crypto from "node:crypto";

const state = {
  running: false,
  shouldStop: false,
  total: 0,           // documents to consider this batch
  done: 0,            // newly hashed this batch
  skipped: 0,         // already had sha256 (only-missing mode)
  failed: 0,          // file open / read errors
  currentDoc: null,   // {id, path}
  startedAt: null,
  finishedAt: null,
  lastError: null,
};

export function getStatus() {
  return { ...state };
}

export function stop() {
  if (!state.running) return false;
  state.shouldStop = true;
  return true;
}

let pathRemapper = (p) => p;
export function setPathRemapper(fn) { pathRemapper = fn; }

// Start the hashing worker.
//   opts.openDb     factory; same one the rest of the app uses
//   opts.onlyMissing default true; false rehashes everything (e.g. after
//                    moving canonical paths around)
export function start(opts) {
  if (state.running) return false;
  state.running = true;
  state.shouldStop = false;
  state.total = 0;
  state.done = 0;
  state.skipped = 0;
  state.failed = 0;
  state.currentDoc = null;
  state.startedAt = new Date().toISOString();
  state.finishedAt = null;
  state.lastError = null;

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
  const onlyMissing = opts.onlyMissing !== false; // default true

  // Snapshot the queue once. Files added mid-run won't be picked up;
  // user re-clicks if needed.
  const db = opts.openDb();
  let queue;
  try {
    const sql = onlyMissing
      ? `SELECT d.id AS doc_id, f.path AS file_path
           FROM documents d
           JOIN files f ON f.id = d.file_id
          WHERE d.sha256 IS NULL AND f.is_dir = 0
          ORDER BY d.id`
      : `SELECT d.id AS doc_id, f.path AS file_path
           FROM documents d
           JOIN files f ON f.id = d.file_id
          WHERE f.is_dir = 0
          ORDER BY d.id`;
    queue = db.prepare(sql).all();
  } finally {
    db.close();
  }
  state.total = queue.length;

  for (const row of queue) {
    if (state.shouldStop) break;
    state.currentDoc = { id: row.doc_id, path: row.file_path };

    const localPath = pathRemapper(row.file_path);
    try {
      const digest = await sha256OfFile(localPath);
      const wdb = opts.openDb();
      try {
        wdb.prepare("UPDATE documents SET sha256 = ? WHERE id = ?")
           .run(digest, row.doc_id);
      } finally {
        wdb.close();
      }
      state.done += 1;
    } catch (e) {
      state.failed += 1;
      state.lastError = `${row.file_path}: ${e.message}`;
    }
  }
}

function sha256OfFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const stream = fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 });
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(hash.digest("hex")));
  });
}

// Find groups of documents that share a sha256. Returns one entry per
// duplicate cluster: { sha256, count, docs: [{doc_id, file_id, path,
// vendor, document_type}] }. The first doc in each cluster is the
// "primary" (smallest path — stable, deterministic).
export function findDuplicates(openDb) {
  const db = openDb();
  try {
    const groups = db.prepare(
      `SELECT sha256, COUNT(*) AS n
         FROM documents
        WHERE sha256 IS NOT NULL
        GROUP BY sha256
        HAVING COUNT(*) > 1
        ORDER BY n DESC, sha256`,
    ).all();
    const detail = db.prepare(
      `SELECT d.id   AS doc_id,
              f.id   AS file_id,
              f.path AS file_path,
              v.name AS vendor,
              dt.name AS document_type
         FROM documents d
         JOIN files f          ON f.id = d.file_id
         LEFT JOIN vendors v   ON v.id = f.vendor_id
         LEFT JOIN document_types dt ON dt.id = d.document_type_id
        WHERE d.sha256 = ?
        ORDER BY f.path`,
    );
    const clusters = groups.map((g) => ({
      sha256: g.sha256,
      count: g.n,
      docs: detail.all(g.sha256),
    }));
    const totalDocs = clusters.reduce((acc, c) => acc + c.count, 0);
    const wastedCopies = clusters.reduce((acc, c) => acc + (c.count - 1), 0);
    return { clusters, clusterCount: clusters.length, totalDocs, wastedCopies };
  } finally {
    db.close();
  }
}
