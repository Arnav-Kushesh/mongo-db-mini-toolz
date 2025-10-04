// Client-side socket UI and handlers (cleaned final)
(function () {
  const socket = io();
  let socketId = null;

  socket.on("welcome", (d) => {
    socketId = d.socketId;
    appendLog("backupLog", "Connected socketId=" + socketId);
    appendLog("transferLog", "Connected socketId=" + socketId);
    appendLog("uploadLog", "Connected socketId=" + socketId);
  });

  function appendLog(id, msg) {
    const el = document.getElementById(id);
    if (!el) return;
    el.insertAdjacentHTML(
      "beforeend",
      new Date().toLocaleTimeString() +
        " - " +
        (typeof msg === "string" ? msg : JSON.stringify(msg)) +
        "<br>"
    );
    el.scrollTop = el.scrollHeight;
  }

  function setFormsDisabled(disabled) {
    ["backupForm", "transferForm", "uploadForm"].forEach((id) => {
      const f = document.getElementById(id);
      if (!f) return;
      Array.from(f.querySelectorAll("input,button,select")).forEach(
        (el) => (el.disabled = disabled)
      );
    });
  }

  function renderCollectionList(containerId, collections) {
    const container = document.getElementById(containerId);
    if (!container) return;
    container.innerHTML = "<h4>Collections</h4>";
    const ul = document.createElement("ul");
    collections.forEach((name) => {
      const li = document.createElement("li");
      li.setAttribute("data-coll", name);
      li.classList.add("state-running");
      li.innerHTML = `
        <div class="status">
          <svg viewBox="0 0 24 24" width="18" height="18"><circle cx="12" cy="12" r="10" stroke="#888" stroke-width="1.5" fill="none"></circle></svg>
        </div>
        <div style="flex:1">
          <div style="display:flex;align-items:center;justify-content:space-between"><strong>${name}</strong><span class="percent">0%</span></div>
          <div class="meta small"></div>
          <div class="progress-track"><div class="bar" style="width:0%"></div></div>
        </div>
      `;
      ul.appendChild(li);
    });
    container.appendChild(ul);
  }

  // helper to safely update a collection item
  function updateCollectionProgress(
    containerPrefix,
    collection,
    percent,
    speed,
    etaSec
  ) {
    const sel = `#${containerPrefix} [data-coll="${collection}"]`;
    const item = document.querySelector(sel);
    if (!item) return;
    item.classList.remove("state-error");
    item.classList.add("state-running");
    if (percent != null) {
      const bar = item.querySelector(".bar");
      if (bar) bar.style.width = percent + "%";
      const pct = item.querySelector(".percent");
      if (pct) pct.textContent = percent + "%";
      if (percent >= 100) {
        item.classList.remove("state-running");
        item.classList.add("state-done");
      }
    }
    const meta = [];
    if (speed != null) meta.push(speed + " docs/s");
    if (etaSec != null) meta.push("ETA: " + etaSec + "s");
    const m = item.querySelector(".meta");
    if (m) m.textContent = meta.join(" | ");
  }

  // bind forms
  const backupForm = document.getElementById("backupForm");
  if (backupForm) {
    backupForm.addEventListener("submit", (e) => {
      e.preventDefault();

      const form = backupForm;

      const fd = new FormData(form);

      setFormsDisabled(true);

      for (const [key, value] of fd.entries()) {
        console.log(key, value);
      }

      const uriVal = String(fd.get("uri") || "").trim();
      const dbNameVal = String(fd.get("dbName") || "").trim();
      const batch = Number(fd.get("batchSize") || 1000);

      console.log(fd, uriVal, dbNameVal, batch);

      if (!uriVal || !dbNameVal) {
        appendLog(
          "backupLog",
          "Missing Mongo URI or Database name. Please fill both fields."
        );
        setFormsDisabled(false);
        return;
      }

      const body = {
        uri: uriVal,
        dbName: dbNameVal,
        socketId,
        batchSize: batch,
      };
      appendLog("backupLog", "Starting backup...");
      fetch("/api/backup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      })
        .then(async (r) => {
          const j = await r.json().catch(() => null);
          if (!r.ok) {
            appendLog("backupLog", j && j.error ? j.error : "HTTP " + r.status);
            setFormsDisabled(false);
            return;
          }
          appendLog("backupLog", j);
        })
        .catch((err) => {
          appendLog("backupLog", err.message || err);
          setFormsDisabled(false);
        });
    });
  }

  const transferForm = document.getElementById("transferForm");
  if (transferForm) {
    transferForm.addEventListener("submit", (e) => {
      e.preventDefault();

      const fd = new FormData(e.target);
      setFormsDisabled(true);
      const srcUri = String(fd.get("srcUri") || "").trim();
      const srcDb = String(fd.get("srcDb") || "").trim();
      const dstUri = String(fd.get("dstUri") || "").trim();
      const dstDb = String(fd.get("dstDb") || "").trim();
      const batch = Number(fd.get("batchSize") || 1000);

      if (!srcUri || !srcDb || !dstUri || !dstDb) {
        appendLog(
          "transferLog",
          "Missing source/target URI or DB. Please fill all fields."
        );
        setFormsDisabled(false);
        return;
      }

      const body = { srcUri, srcDb, dstUri, dstDb, socketId, batchSize: batch };
      appendLog("transferLog", "Starting transfer...");
      fetch("/api/transfer", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      })
        .then(async (r) => {
          const j = await r.json().catch(() => null);
          if (!r.ok) {
            appendLog(
              "transferLog",
              j && j.error ? j.error : "HTTP " + r.status
            );
            setFormsDisabled(false);
            return;
          }
          appendLog("transferLog", j);
        })
        .catch((err) => {
          appendLog("transferLog", err.message || err);
          setFormsDisabled(false);
        });
    });
  }

  const uploadForm = document.getElementById("uploadForm");
  if (uploadForm) {
    uploadForm.addEventListener("submit", (e) => {
      e.preventDefault();

      const form = e.target;
      const fd = new FormData(form);

      setFormsDisabled(true);
      const fileInput = form.querySelector('input[name="file"]');
      const uriVal = String(fd.get("uri") || "").trim();
      const dbNameVal = String(fd.get("dbName") || "").trim();
      fd.append("socketId", socketId);
      fd.append("batchSize", form.batchSize.value || 1000);

      if (!fileInput || !fileInput.files || !fileInput.files.length) {
        appendLog("uploadLog", "Please choose a zip file to upload.");
        setFormsDisabled(false);
        return;
      }
      if (!uriVal || !dbNameVal) {
        appendLog(
          "uploadLog",
          "Missing target Mongo URI or DB name. Please fill both fields."
        );
        setFormsDisabled(false);
        return;
      }

      appendLog("uploadLog", "Uploading...");
      fetch("/api/upload", { method: "POST", body: fd })
        .then(async (r) => {
          const j = await r.json().catch(() => null);
          if (!r.ok) {
            appendLog("uploadLog", j && j.error ? j.error : "HTTP " + r.status);
            setFormsDisabled(false);
            return;
          }
          appendLog("uploadLog", j);
        })
        .catch((err) => {
          appendLog("uploadLog", err.message || err);
          setFormsDisabled(false);
        });
    });
  }

  // socket events - backup
  socket.on("backup-start", (d) =>
    appendLog("backupLog", Object.assign({ event: "start" }, d))
  );
  socket.on("backup-collection-start", (d) => {
    if (d.collections) renderCollectionList("backupCollections", d.collections);
  });
  socket.on("backup-progress", (d) => {
    updateCollectionProgress(
      "backupCollections",
      d.collection,
      d.percent,
      d.speed,
      d.etaSec
    );
    appendLog("backupLog", Object.assign({ event: "progress" }, d));
  });
  socket.on("backup-collection-done", (d) => {
    appendLog("backupLog", Object.assign({ event: "collection-done" }, d));
    updateCollectionProgress("backupCollections", d.collection, 100);
  });
  socket.on("backup-done", (d) => {
    appendLog("backupLog", Object.assign({ event: "done" }, d));
    setFormsDisabled(false);
  });
  socket.on("backup-error", (d) => {
    appendLog("backupLog", Object.assign({ event: "error" }, d));
    setFormsDisabled(false);
    if (d.collection) {
      const item = document.querySelector(
        `#backupCollections [data-coll="${d.collection}"]`
      );
      if (item) {
        item.classList.remove("state-running");
        item.classList.add("state-error");
      }
    }
  });

  // socket events - transfer
  socket.on("transfer-start", (d) => {
    if (d.collections)
      renderCollectionList("transferCollections", d.collections);
    appendLog("transferLog", Object.assign({ event: "start" }, d));
  });
  socket.on("transfer-progress", (d) => {
    updateCollectionProgress(
      "transferCollections",
      d.collection,
      d.percent,
      d.speed,
      d.etaSec
    );
    appendLog("transferLog", Object.assign({ event: "progress" }, d));
  });
  socket.on("transfer-collection-done", (d) => {
    appendLog("transferLog", Object.assign({ event: "collection-done" }, d));
    updateCollectionProgress("transferCollections", d.collection, 100);
  });
  socket.on("transfer-done", (d) => {
    appendLog("transferLog", Object.assign({ event: "done" }, d));
    setFormsDisabled(false);
  });
  socket.on("transfer-error", (d) => {
    appendLog("transferLog", Object.assign({ event: "error" }, d));
    setFormsDisabled(false);
    if (d.collection) {
      const item = document.querySelector(
        `#transferCollections [data-coll="${d.collection}"]`
      );
      if (item) {
        item.classList.remove("state-running");
        item.classList.add("state-error");
      }
    }
  });

  // socket events - upload
  socket.on("upload-start", (d) => {
    appendLog("uploadLog", Object.assign({ event: "start" }, d));
    setFormsDisabled(true);
  });
  socket.on("upload-collection-start", (d) => {
    renderCollectionList("uploadCollections", [d.collection]);
    appendLog("uploadLog", Object.assign({ event: "collection-start" }, d));
  });
  socket.on("upload-progress", (d) => {
    updateCollectionProgress(
      "uploadCollections",
      d.collection,
      d.percent,
      d.speed,
      d.etaSec
    );
    appendLog("uploadLog", Object.assign({ event: "progress" }, d));
  });
  socket.on("upload-collection-done", (d) => {
    appendLog("uploadLog", Object.assign({ event: "collection-done" }, d));
    updateCollectionProgress("uploadCollections", d.collection, 100);
  });
  socket.on("upload-done", (d) => {
    appendLog("uploadLog", Object.assign({ event: "done" }, d));
    setFormsDisabled(false);
  });
  socket.on("upload-error", (d) => {
    appendLog("uploadLog", Object.assign({ event: "error" }, d));
    setFormsDisabled(false);
    if (d.collection) {
      const item = document.querySelector(
        `#uploadCollections [data-coll="${d.collection}"]`
      );
      if (item) {
        item.classList.remove("state-running");
        item.classList.add("state-error");
      }
    }
  });
})();
