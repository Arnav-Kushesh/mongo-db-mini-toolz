const express = require("express");
const http = require("http");
const path = require("path");
const { MongoClient } = require("mongodb");
const fs = require("fs-extra");
const archiver = require("archiver");
const multer = require("multer");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const UPLOAD_DIR = path.join(__dirname, "uploads");
fs.ensureDirSync(UPLOAD_DIR);

// cleanup scheduling: remove created paths after TTL (milliseconds)
const CLEANUP_TTL_MIN = parseInt(process.env.CLEANUP_TTL_MIN || "60", 10);
const scheduledCleanups = new Map();
function scheduleCleanup(absPath, ttlMs = CLEANUP_TTL_MIN * 60 * 1000) {
  try {
    if (scheduledCleanups.has(absPath)) return;
    const t = setTimeout(async () => {
      try {
        await fs.remove(absPath);
      } catch (e) {
        console.warn("cleanup remove failed", absPath, e.message);
      }
      scheduledCleanups.delete(absPath);
    }, ttlMs);
    scheduledCleanups.set(absPath, t);
  } catch (e) {
    console.warn("schedule cleanup failed", e.message);
  }
}

// count lines in a file without loading whole file
function countLinesStream(filePath) {
  return new Promise((resolve, reject) => {
    let count = 0;
    const rs = fs.createReadStream(filePath, { encoding: "utf8" });
    rs.on("data", (chunk) => {
      for (let i = 0; i < chunk.length; i++) if (chunk[i] === "\n") count++;
    });
    rs.on("end", () => resolve(count));
    rs.on("error", reject);
  });
}

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

app.use(express.static(path.join(__dirname, "public")));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// Home UI
app.get("/", (req, res) => {
  res.render("index");
});

// Helper: emit progress to client
function emitProgress(socketId, event, payload) {
  if (!socketId) return;
  io.to(socketId).emit(event, payload);
}

// Backup route -> creates folder with collection JSON files and zips them
app.post("/api/backup", async (req, res) => {
  const { uri, dbName, socketId } = req.body;
  if (!uri || !dbName)
    return res.status(400).json({ error: "Missing uri or dbName" });

  const timestamp = Date.now();
  const outDir = path.join(UPLOAD_DIR, `${dbName}-${timestamp}`);
  await fs.ensureDir(outDir);

  let client;
  try {
    client = new MongoClient(uri);
    await client.connect();
    const db = client.db(dbName);
    const collections = await db.listCollections().toArray();

    // allow client to configure batch size
    const BATCH_SIZE = parseInt(req.body.batchSize, 10) || 1000;

    emitProgress(socketId, "backup-start", {
      totalCollections: collections.length,
      collections: collections.map((c) => c.name),
      batchSize: BATCH_SIZE,
    });
    let collIndex = 0;
    for (const collInfo of collections) {
      const name = collInfo.name;
      const coll = db.collection(name);
      // get count if possible
      let totalDocs = 0;
      try {
        totalDocs = await coll.countDocuments();
      } catch (e) {
        totalDocs = 0;
      }

      const outFile = path.join(outDir, `${name}.ndjson`);
      const ws = fs.createWriteStream(outFile, { flags: "w" });

      emitProgress(socketId, "backup-collection-start", {
        collection: name,
        index: collIndex,
        totalDocs,
      });

      const cursor = coll.find();
      let docsDone = 0;
      const start = Date.now();
      while (await cursor.hasNext()) {
        const batch = [];
        for (let i = 0; i < BATCH_SIZE && (await cursor.hasNext()); i++) {
          const doc = await cursor.next();
          batch.push(doc);
        }
        // write batch as ndjson lines
        for (const d of batch) {
          ws.write(JSON.stringify(d) + "\n");
        }
        docsDone += batch.length;
        const elapsedSec = Math.max(1, (Date.now() - start) / 1000);
        const speed = Math.round(docsDone / elapsedSec); // docs/sec
        const percent = totalDocs
          ? Math.min(100, Math.round((docsDone / totalDocs) * 100))
          : null;
        const etaSec =
          totalDocs && speed
            ? Math.max(0, Math.round((totalDocs - docsDone) / speed))
            : null;
        emitProgress(socketId, "backup-progress", {
          collection: name,
          docsDone,
          totalDocs,
          percent,
          speed,
          etaSec,
        });
      }
      ws.end();
      collIndex++;
      emitProgress(socketId, "backup-collection-done", {
        collection: name,
        index: collIndex,
        docsDone: undefined,
      });
    }

    // zip
    const zipPath = path.join(UPLOAD_DIR, `${dbName}-${timestamp}.zip`);
    await new Promise((resolve, reject) => {
      const output = fs.createWriteStream(zipPath);
      const archive = archiver("zip");
      output.on("close", resolve);
      archive.on("error", reject);
      archive.pipe(output);
      archive.directory(outDir, false);
      archive.finalize();
    });

    emitProgress(socketId, "backup-done", {
      zip: `/download/${path.basename(zipPath)}`,
    });

    res.json({ ok: true, zip: `/download/${path.basename(zipPath)}` });
  } catch (err) {
    console.error(err);
    emitProgress(socketId, "backup-error", { message: err.message });
    res.status(500).json({ error: err.message });
  } finally {
    if (client) await client.close();
  }
});

app.get("/download/:name", (req, res) => {
  const file = path.join(UPLOAD_DIR, req.params.name);
  res.download(file);
});

// Data transfer: copy collections from source to target
app.post("/api/transfer", async (req, res) => {
  const { srcUri, srcDb, dstUri, dstDb, socketId } = req.body;
  if (!srcUri || !srcDb || !dstUri || !dstDb)
    return res.status(400).json({ error: "Missing params" });

  let srcClient, dstClient;
  try {
    srcClient = new MongoClient(srcUri);
    dstClient = new MongoClient(dstUri);
    await srcClient.connect();
    await dstClient.connect();
    const sdb = srcClient.db(srcDb);
    const ddb = dstClient.db(dstDb);

    const collections = await sdb.listCollections().toArray();
    emitProgress(socketId, "transfer-start", {
      totalCollections: collections.length,
    });

    const BATCH_SIZE = parseInt(req.body.batchSize, 10) || 1000;
    let migratedCollections = 0;
    for (const c of collections) {
      const name = c.name;
      const srcColl = sdb.collection(name);
      const dstColl = ddb.collection(name);
      // attempt to get document count for percent reporting
      let totalDocs = 0;
      try {
        totalDocs = await srcColl.countDocuments();
      } catch (e) {
        totalDocs = 0;
      }
      await dstColl.deleteMany({});

      emitProgress(socketId, "transfer-collection-start", {
        collection: name,
        migratedCollections,
        totalDocs,
      });

      const cursor = srcColl.find();
      let transferred = 0;
      const tstart = Date.now();
      while (await cursor.hasNext()) {
        const batch = [];
        for (let i = 0; i < BATCH_SIZE && (await cursor.hasNext()); i++) {
          batch.push(await cursor.next());
        }
        if (batch.length) {
          await dstColl.insertMany(batch);
          transferred += batch.length;
          const elapsedSec = Math.max(1, (Date.now() - tstart) / 1000);
          const speed = Math.round(transferred / elapsedSec);
          const percent = totalDocs
            ? Math.min(100, Math.round((transferred / totalDocs) * 100))
            : null;
          const etaSec =
            totalDocs && speed
              ? Math.max(0, Math.round((totalDocs - transferred) / speed))
              : null;
          emitProgress(socketId, "transfer-progress", {
            collection: name,
            transferred,
            totalDocs,
            percent,
            speed,
            etaSec,
          });
        }
      }
      migratedCollections++;
      emitProgress(socketId, "transfer-collection-done", {
        collection: name,
        migratedCollections,
      });
    }
    emitProgress(socketId, "transfer-done", { migratedCollections });
    res.json({ ok: true, migratedCollections });
  } catch (err) {
    console.error(err);
    emitProgress(socketId, "transfer-error", { message: err.message });
    res.status(500).json({ error: err.message });
  } finally {
    if (srcClient) await srcClient.close();
    if (dstClient) await dstClient.close();
  }
});

// Upload: expects zipped folder or folder upload
const upload = multer({ dest: UPLOAD_DIR });
app.post("/api/upload", upload.single("file"), async (req, res) => {
  const { uri, dbName, socketId } = req.body;
  if (!req.file) return res.status(400).json({ error: "Missing file" });

  const filePath = req.file.path;
  const extractDir = path.join(UPLOAD_DIR, `extract-${Date.now()}`);
  await fs.ensureDir(extractDir);

  // If zip, extract; otherwise assume it's a folder uploaded as files (multer handles file only)
  // For simplicity, assume uploaded is a zip created by backup
  const unzip = require("unzipper");
  try {
    await fs
      .createReadStream(filePath)
      .pipe(unzip.Extract({ path: extractDir }))
      .promise();

    let client;
    try {
      client = new MongoClient(uri);
      await client.connect();
      const db = client.db(dbName);

      const files = await fs.readdir(extractDir);
      const ndjsonFiles = files.filter(
        (f) => f.endsWith(".ndjson") || f.endsWith(".json")
      );
      emitProgress(socketId, "upload-start", {
        totalFiles: ndjsonFiles.length,
      });

      const BATCH_SIZE = parseInt(req.body.batchSize, 10) || 1000;
      let importedCollections = 0;
      for (const f of ndjsonFiles) {
        const collName = f.replace(/\.(ndjson|json)$/, "");
        const rs = fs.createReadStream(path.join(extractDir, f), {
          encoding: "utf8",
        });
        let buffer = "";
        const dst = db.collection(collName);
        await dst.deleteMany({});
        let docsBatch = [];
        let importedCount = 0;

        // estimate totalDocs by streaming newline count (memory friendly)
        let totalDocs = null;
        try {
          totalDocs = await countLinesStream(path.join(extractDir, f));
        } catch (e) {
          totalDocs = null;
        }

        emitProgress(socketId, "upload-collection-start", {
          collection: collName,
          importedCollections,
          totalDocs,
        });

        const ustart = Date.now();
        for await (const chunk of rs) {
          buffer += chunk;
          let idx;
          while ((idx = buffer.indexOf("\n")) >= 0) {
            const line = buffer.slice(0, idx).trim();
            buffer = buffer.slice(idx + 1);
            if (!line) continue;
            try {
              const obj = JSON.parse(line);
              docsBatch.push(obj);
            } catch (e) {
              // skip parse error
              continue;
            }
            if (docsBatch.length >= BATCH_SIZE) {
              await dst.insertMany(docsBatch);
              importedCount += docsBatch.length;
              docsBatch = [];
              const elapsedSec = Math.max(1, (Date.now() - ustart) / 1000);
              const speed = Math.round(importedCount / elapsedSec);
              const percent = totalDocs
                ? Math.min(100, Math.round((importedCount / totalDocs) * 100))
                : null;
              const etaSec =
                totalDocs && speed
                  ? Math.max(0, Math.round((totalDocs - importedCount) / speed))
                  : null;
              emitProgress(socketId, "upload-progress", {
                collection: collName,
                importedCount,
                totalDocs,
                percent,
                speed,
                etaSec,
              });
            }
          }
        }
        if (docsBatch.length) {
          await dst.insertMany(docsBatch);
          importedCount += docsBatch.length;
          const elapsedSec = Math.max(1, (Date.now() - ustart) / 1000);
          const speed = Math.round(importedCount / elapsedSec);
          const percent = totalDocs
            ? Math.min(100, Math.round((importedCount / totalDocs) * 100))
            : null;
          const etaSec =
            totalDocs && speed
              ? Math.max(0, Math.round((totalDocs - importedCount) / speed))
              : null;
          emitProgress(socketId, "upload-progress", {
            collection: collName,
            importedCount,
            totalDocs,
            percent,
            speed,
            etaSec,
          });
        }
        importedCollections++;
        emitProgress(socketId, "upload-collection-done", {
          collection: collName,
          importedCount,
        });
      }

      // schedule cleanup of extracted dir and zip file (if exists)
      scheduleCleanup(extractDir);
      scheduleCleanup(filePath);

      emitProgress(socketId, "upload-done", { importedCollections });
      res.json({ ok: true, importedCollections });
    } finally {
      if (client) await client.close();
    }
  } catch (err) {
    console.error(err);
    emitProgress(socketId, "upload-error", { message: err.message });
    res.status(500).json({ error: err.message });
  } finally {
    // cleanup
    try {
      await fs.remove(filePath);
    } catch (e) {}
    // don't immediately remove extractDir so user can inspect; could remove later
  }
});

// Websocket handshake — send socket id to client
io.on("connection", (socket) => {
  console.log("ws connect", socket.id);
  socket.emit("welcome", { socketId: socket.id });
});

const PORT = process.env.PORT || 4040;
server.listen(PORT, () =>
  console.log(`MongoDB Mini Toolz listening on http://localhost:${PORT}`)
);
