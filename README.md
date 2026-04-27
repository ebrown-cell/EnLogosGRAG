# EnLogosGRAG

A local web app for cataloging a corpus of fire-safety vendor documents — extracting metadata, vendors, and document types **without AI**, then surfacing the results through filtered tables and drilldown pie charts.

The pipeline is two stages of extraction, both rule-based and free of API costs:

1. **Filename + path rules.** Reads a flat listing of file paths and classifies documents by what their name and parent folders say. Cheap and fast — typically catches half the corpus on its own.
2. **PDF text content rules.** Extracts the text of every PDF (via `pdfjs-dist`, no Java/Tika dependency) and runs a second pass of regex rules against the cover page and full body. Picks up the docs the filename rules missed.

Both rule sets live in editable YAML files, reload on every classify run, and store hits in the same `documents` table with a `confidence` column so high-confidence matches survive when lower-quality rules try to overwrite them.

## What you get

- **Browsable tables** for `vendors`, `document_types`, `files`, `documents`, `document_extracts`, all in the sidebar with live row counts. Each table view supports text-filter, file-type filter, document-type filter, classified/unclassified filter, confidence threshold, column resize, and sort.
- **Pie chart drilldown.** Six donut charts: Classified vs unclassified · By confidence · Top ten filetypes · Other filetypes · By document type · Other document types. Click any slice to filter the chart page and compose; click "View these documents →" to drop into the table view with the same filter chain.
- **Per-document extract viewer.** Click `view` in the documents table or the document_extracts table to read the extracted text alongside PDF metadata (title, author, page count, char count).
- **Open the file.** Click any path or filename in the documents/files views to launch the PDF in the OS default app.
- **Test different folders.** "Choose folder…" picks a vendors-root on disk; the server uses an existing `basic_listing.txt` if present or generates one. The `vendors` segment is found dynamically (case-insensitive), so the same DB schema works against any local copy of the corpus or a totally different vendor tree.
- **Background extraction.** PDF text extraction runs as a singleton worker, processes ~2-3 PDFs/sec, persists each result before moving on, and resumes from where it left off if you stop or restart.

## How it works

```
listing.txt  ──►  vendors / files            (listing.js)
files        ──►  documents (1 per non-dir)  (listing.js)
documents    ──►  document_extracts          (extractor.js, pdfjs-dist)
documents  ◄──    classifier.yaml            (filename rules)
documents  ◄──    content_classifier.yaml    (PDF-text rules, only fills empty / upgrades low)
```

All five tables are SQLite, stored in `test.db` next to the server. Schema lives in [db.js](db.js) with idempotent migrations; the database starts empty and rebuilds on demand.

## Running it

Requires Node 22+ (for the built-in `node:sqlite` module).

```
git clone https://github.com/ebrown-cell/EnLogosGRAG.git
cd EnLogosGRAG
npm install
npm start
```

Open http://localhost:8780.

The first time:
1. **Choose folder…** in the sidebar → pick the directory whose subfolders are vendor names. Server generates `basic_listing.txt` inside it (skipped if one already exists).
2. **Full Run (vendors + files)** to populate the DB.
3. **Classify all files (filename rules)** — fast, picks up the easy wins.
4. **Extract PDF text (background)** — slower, churns through every PDF in the corpus. Status updates live in the sidebar.
5. **Classify by extract content (low/none only)** — second pass against the extracted text, only fills documents the filename rules missed or weren't sure about.
6. **📊 Charts** in the sidebar to see the breakdown.

## Editing the rules

- [classifier.yaml](classifier.yaml) — filename / path rules. Each rule has `id`, `document_type`, `confidence` (high/medium/low), `match` (name / parent / path / file_type), and `pattern` (JS regex, case-insensitive). First match wins.
- [content_classifier.yaml](content_classifier.yaml) — same shape but `match` is `first_page` or `extract`. Only fires against documents that have a successful extract and are unclassified or low-confidence.

Edit either YAML and click the corresponding Classify button — rules reload from disk on every run.

## Endpoints

The server exposes a small JSON API at the same port. Useful for scripting:

```
GET  /api/status               table counts + classification breakdown
POST /api/browse               filtered/paginated row fetch for any table
POST /api/stats                aggregate counts for the chart page
GET  /api/file-types           distinct file_type values
GET  /api/document-types       all document_types with counts
POST /api/list-dir             server-side directory browser (folder picker)
POST /api/choose-folder        resolve a folder to its listing file (use existing or generate)
POST /api/ingest               run vendors / files / full
POST /api/classify             filename-rule pass
POST /api/classify-by-content  PDF-content-rule pass
POST /api/extract-start        kick off background PDF extraction
GET  /api/extract-status       poll the extraction worker
POST /api/extract-stop         signal the worker to stop at next file boundary
POST /api/get-extract          fetch one document's extracted text + metadata
POST /api/open-file            launch a file in its OS default app (whitelisted by files.path)
POST /api/purge                clear vendors / files (cascades to documents) / all
```

## Project layout

```
server.js             HTTP server + inline single-page UI
db.js                 schema + migrations + openDb()
listing.js            listing parser, vendor extraction, file ingest
extractor.js          pdfjs-dist worker (singleton, resumable)
classifier.js         filename + content classifiers
classifier.yaml       filename rules
content_classifier.yaml  content rules
test.db               local SQLite DB (gitignored)
uploads/              dropped-file landing zone (gitignored)
```

## Why no AI

AI classification works but costs per call and per token. For a corpus where the filename and the PDF cover page already give you most of the answer, regex rules are essentially free, deterministic, and editable in seconds when a vendor changes how they name something. The result: ~70% classified at zero ongoing cost on the test corpus, with high-confidence rules driving the vast majority of those hits.

The architecture leaves room to layer AI on the unclassified remainder later — `document_extracts` already stores per-page bounding-box positions for every text run, so future code can reconstruct tables, feed structured content to a model, or build a vector index without re-extracting the PDFs.
