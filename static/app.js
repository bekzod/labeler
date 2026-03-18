const state = {
  index: 0,
  totalRows: 0,
  row: null,
  selectedSource: "text",
  deleted: false,
  dirty: false,
  isLoading: false,
  autosaveTimer: null,
  checked: false,
};

const els = {
  jsonlPath: document.getElementById("jsonlPath"),
  commitBtn: document.getElementById("commitBtn"),
  prevBtn: document.getElementById("prevBtn"),
  nextBtn: document.getElementById("nextBtn"),
  playBtn: document.getElementById("playBtn"),
  deleteBtn: document.getElementById("deleteBtn"),
  revertRowBtn: document.getElementById("revertRowBtn"),
  revertAllBtn: document.getElementById("revertAllBtn"),
  autoplayToggle: document.getElementById("autoplayToggle"),
  pickTextBtn: document.getElementById("pickTextBtn"),
  pickHypBtn: document.getElementById("pickHypBtn"),
  pickLikelyTextBtn: document.getElementById("pickLikelyTextBtn"),
  pickLikelyHypBtn: document.getElementById("pickLikelyHypBtn"),
  indexInput: document.getElementById("indexInput"),
  positionLabel: document.getElementById("positionLabel"),
  outputPathInput: document.getElementById("outputPathInput"),
  deletedOutputInput: document.getElementById("deletedOutputInput"),
  audioPath: document.getElementById("audioPath"),
  duration: document.getElementById("duration"),
  wer: document.getElementById("wer"),
  cer: document.getElementById("cer"),
  bucket: document.getElementById("bucket"),
  suspicion: document.getElementById("suspicion"),
  textSource: document.getElementById("textSource"),
  hypSource: document.getElementById("hypSource"),
  likelyBadTextSource: document.getElementById("likelyBadTextSource"),
  likelyBadModelSource: document.getElementById("likelyBadModelSource"),
  editInput: document.getElementById("editInput"),
  statusText: document.getElementById("statusText"),
  countText: document.getElementById("countText"),
  audioPlayer: document.getElementById("audioPlayer"),
};

function isTypingTarget(target) {
  if (!target) return false;
  const tag = target.tagName;
  return tag === "TEXTAREA" || tag === "INPUT" || tag === "SELECT";
}

function clampIndex(index, totalRows) {
  if (totalRows <= 0) return 0;
  return Math.max(0, Math.min(totalRows - 1, index));
}

function parseRequestedRow(rawValue, totalRows) {
  if (totalRows <= 0) return 1;
  const fallback = 1;
  const maxRow = totalRows;
  const raw = String(rawValue ?? "").trim();
  if (!raw) return fallback;

  // Accept only integer-like input; decimals are truncated for compatibility.
  const numeric = Number(raw);
  if (!Number.isFinite(numeric)) {
    // Very large integer strings can parse as Infinity; clamp to final row.
    if (/^\d+$/.test(raw)) return maxRow;
    return fallback;
  }

  const requested = Math.trunc(numeric);
  if (requested < 1) return 1;
  if (requested > maxRow) return maxRow;
  return requested;
}

function getRequestedRowIndex(totalRows) {
  const params = new URLSearchParams(window.location.search);
  const rawRow = params.get("row");
  const requestedRow = parseRequestedRow(rawRow, totalRows);
  return requestedRow - 1;
}

function syncRowQuery(index) {
  const rowValue = String(index + 1);
  const params = new URLSearchParams(window.location.search);

  if (params.get("row") === rowValue) return;

  params.set("row", rowValue);
  const query = params.toString();
  const nextUrl = `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`;
  window.history.replaceState(null, "", nextUrl);
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  const raw = await res.text();
  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    throw new Error(`Invalid JSON from ${path}`);
  }

  if (!res.ok || !data.ok) {
    throw new Error(data.error || `Request failed: ${res.status}`);
  }

  return data;
}

function sourceText(row, source) {
  if (!row) return "";
  if (source === "model_hypothesis") return String(row.model_hypothesis ?? "");
  if (source === "likely_bad_text") return String(row.likely_bad_text ?? "");
  if (source === "likely_bad_model_text")
    return String(row.likely_bad_model_text ?? "");
  return String(row.text ?? "");
}

function escapeHtml(text) {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function highlightWholeWordMarkup(textValue, tokenValue) {
  const text = String(textValue ?? "");
  const token = String(tokenValue ?? "");

  if (!token.trim()) {
    return escapeHtml(text);
  }

  let html = "";
  let start = 0;
  let idx = text.indexOf(token, start);

  while (idx !== -1) {
    html += escapeHtml(text.slice(start, idx));
    html += `<span class="likely-bad-word">${escapeHtml(token)}</span>`;
    start = idx + token.length;
    idx = text.indexOf(token, start);
  }

  html += escapeHtml(text.slice(start));
  return html;
}

function isTextHypothesisMatch(textValue, hypValue) {
  return String(textValue ?? "").trim() === String(hypValue ?? "").trim();
}

function setStatus(text) {
  els.statusText.textContent = text;
}

function setCounts(edited, deleted, checked = 0) {
  els.countText.textContent = `edited: ${edited}, deleted: ${deleted}, checked: ${checked}`;
}

function updateDeleteButton() {
  els.deleteBtn.classList.toggle("active", state.deleted);
  els.deleteBtn.textContent = state.deleted ? "Undelete (D)" : "Delete (D)";
}

function maybeAutoPlayCurrentRow() {
  if (!els.autoplayToggle || !els.autoplayToggle.checked) return;
  if (!els.audioPlayer.src) return;

  els.audioPlayer.currentTime = 0;
  const playPromise = els.audioPlayer.play();
  if (playPromise && typeof playPromise.catch === "function") {
    playPromise.catch(() => {
      setStatus("Autoplay blocked by browser. Press Play or Space.");
    });
  }
}

function updateSourceChoiceHighlight() {
  els.pickTextBtn.classList.toggle("active", state.selectedSource === "text");
  els.pickHypBtn.classList.toggle(
    "active",
    state.selectedSource === "model_hypothesis",
  );
  els.pickLikelyTextBtn.classList.toggle(
    "active",
    state.selectedSource === "likely_bad_text",
  );
  els.pickLikelyHypBtn.classList.toggle(
    "active",
    state.selectedSource === "likely_bad_model_text",
  );
}

function updateMatchHighlight(isMatch) {
  els.pickTextBtn.classList.toggle("match-ok", isMatch);
  els.pickHypBtn.classList.toggle("match-ok", isMatch);
}

function updatePosition() {
  const total = state.totalRows || 0;
  const rowLabel = `${Math.min(state.index + 1, total)} / ${total}`;
  els.positionLabel.textContent = state.checked
    ? `${rowLabel} (checked)`
    : rowLabel;
  els.indexInput.value = state.index + 1;
}

function applySourceChoice(source, prefill = true) {
  state.selectedSource = source;
  updateSourceChoiceHighlight();

  if (prefill) {
    els.editInput.value = sourceText(state.row, source);
    state.dirty = true;
    const shouldMarkChecked =
      source === "text" || source === "model_hypothesis";
    queueAutosave(100, { markChecked: shouldMarkChecked });
    setStatus(
      shouldMarkChecked
        ? `Prefilled from ${source} and marked checked`
        : `Prefilled from ${source}`,
    );
  }
}

async function applyPrimarySourceAndAdvance(source) {
  if (!state.row) return;
  if (source !== "text" && source !== "model_hypothesis") return;

  clearTimeout(state.autosaveTimer);

  state.selectedSource = source;
  updateSourceChoiceHighlight();
  els.editInput.value = sourceText(state.row, source);
  state.dirty = true;
  setStatus(`Prefilled from ${source} and marked checked`);

  const hasNext = state.index + 1 < state.totalRows;
  if (hasNext) {
    await moveBy(1);
  } else {
    await saveCurrentRow(true, { markChecked: true });
    setStatus(`Marked row ${state.index + 1} as checked`);
  }
}

function renderRowPayload(payload) {
  state.index = payload.index;
  state.totalRows = payload.total_rows;
  state.row = payload.row;
  state.deleted = Boolean(payload.state.deleted);
  state.checked = Boolean(payload.state.checked);

  const selected = payload.state.selected_source || "text";
  applySourceChoice(selected, false);

  if (payload.state.has_saved_edit) {
    els.editInput.value = String(payload.state.edited_text ?? "");
  } else {
    els.editInput.value = sourceText(payload.row, selected);
  }

  const row = payload.row;
  els.audioPath.textContent = String(row.audio_filepath ?? "");
  els.duration.textContent =
    row.duration != null ? Number(row.duration).toFixed(3) : "";
  els.wer.textContent = row.wer != null ? String(row.wer) : "";
  els.cer.textContent = row.cer != null ? String(row.cer) : "";
  els.bucket.textContent = String(row.bucket ?? "");
  els.suspicion.textContent =
    row.suspicion_score != null ? String(row.suspicion_score) : "";

  const textValue = sourceText(row, "text");
  const hypValue = sourceText(row, "model_hypothesis");
  const likelyText = sourceText(row, "likely_bad_text");
  const likelyHyp = sourceText(row, "likely_bad_model_text");
  const textHypMatch = isTextHypothesisMatch(textValue, hypValue);

  els.textSource.innerHTML = highlightWholeWordMarkup(textValue, likelyText);
  els.hypSource.innerHTML = highlightWholeWordMarkup(hypValue, likelyHyp);
  els.likelyBadTextSource.textContent = likelyText;
  els.likelyBadModelSource.textContent = likelyHyp;
  updateMatchHighlight(textHypMatch);

  const audioPath = encodeURIComponent(String(row.audio_filepath ?? ""));
  els.audioPlayer.src = `/api/audio?path=${audioPath}`;
  els.playBtn.textContent = "Play (Space)";
  maybeAutoPlayCurrentRow();

  updatePosition();
  updateDeleteButton();
  syncRowQuery(state.index);
  state.dirty = false;
  setStatus(`Loaded row ${state.index + 1}`);
}

async function loadRow(index) {
  if (index < 0 || index >= state.totalRows) return;
  if (state.isLoading) return;

  state.isLoading = true;
  try {
    setStatus(`Loading row ${index + 1}...`);
    const data = await api(`/api/row?index=${index}`);
    renderRowPayload(data);
  } catch (err) {
    setStatus(`Load failed: ${err.message}`);
  } finally {
    state.isLoading = false;
  }
}

async function saveCurrentRow(silent = false, options = {}) {
  if (!state.row) return;
  const markChecked = Boolean(options.markChecked);

  const payload = {
    index: state.index,
    selected_source: state.selectedSource,
    edited_text: els.editInput.value,
    deleted: state.deleted,
    mark_checked: markChecked,
  };

  try {
    const data = await api("/api/row/save", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.checked = Boolean(data.result.checked);
    updatePosition();
    setCounts(
      data.result.edited_rows,
      data.result.deleted_rows,
      data.result.checked_rows,
    );
    state.dirty = false;
    if (!silent) setStatus(`Saved row ${state.index + 1}`);
  } catch (err) {
    setStatus(`Save failed: ${err.message}`);
    throw err;
  }
}

function queueAutosave(delayMs = 450, options = {}) {
  clearTimeout(state.autosaveTimer);
  state.autosaveTimer = setTimeout(() => {
    saveCurrentRow(true, options).catch(() => undefined);
  }, delayMs);
}

async function moveBy(delta) {
  const next = state.index + delta;
  if (next < 0 || next >= state.totalRows) return;
  const shouldMarkChecked =
    delta > 0 &&
    !state.deleted &&
    (state.selectedSource === "text" ||
      state.selectedSource === "model_hypothesis");
  if (state.dirty || shouldMarkChecked) {
    await saveCurrentRow(true, { markChecked: shouldMarkChecked });
  }
  await loadRow(next);
}

async function jumpToIndex() {
  const requestedRow = parseRequestedRow(els.indexInput.value, state.totalRows);
  const next = requestedRow - 1;
  els.indexInput.value = String(requestedRow);
  if (next === state.index) return;

  if (state.dirty) await saveCurrentRow(true);
  await loadRow(next);
}

function toggleDelete() {
  state.deleted = !state.deleted;
  updateDeleteButton();
  state.dirty = true;
  queueAutosave(120);
  setStatus(state.deleted ? "Marked as deleted" : "Row restored");
}

async function revertCurrentRow() {
  if (!state.row) return;

  clearTimeout(state.autosaveTimer);
  state.dirty = false;

  try {
    const data = await api("/api/row/reset", {
      method: "POST",
      body: JSON.stringify({ index: state.index }),
    });

    setCounts(
      data.meta.edited_rows,
      data.meta.deleted_rows,
      data.meta.checked_rows,
    );
    await loadRow(state.index);
    setStatus(`Reverted row ${state.index + 1}`);
  } catch (err) {
    setStatus(`Revert row failed: ${err.message}`);
  }
}

async function revertAllRows() {
  const ok = window.confirm("Revert all uncommitted edits and deletions?");
  if (!ok) return;

  clearTimeout(state.autosaveTimer);
  state.dirty = false;

  try {
    const data = await api("/api/reset_all", {
      method: "POST",
      body: JSON.stringify({}),
    });

    setCounts(
      data.meta.edited_rows,
      data.meta.deleted_rows,
      data.meta.checked_rows,
    );
    await loadRow(clampIndex(state.index, state.totalRows));
    setStatus("Reverted all uncommitted changes");
  } catch (err) {
    setStatus(`Revert all failed: ${err.message}`);
  }
}

async function writeJsonl() {
  if (!state.row) return;
  if (state.dirty) await saveCurrentRow(true);

  const inPlace = !els.outputPathInput.value.trim();
  if (inPlace) {
    const ok = window.confirm(
      "Write in-place to source JSONL? A timestamped backup will be created automatically.",
    );
    if (!ok) return;
  }

  setStatus("Writing JSONL files...");

  try {
    const data = await api("/api/commit", {
      method: "POST",
      body: JSON.stringify({
        output_path: els.outputPathInput.value.trim(),
        deleted_output_path: els.deletedOutputInput.value.trim(),
      }),
    });

    const { result, meta } = data;
    setCounts(meta.edited_rows, meta.deleted_rows, meta.checked_rows);

    const backup = result.backup_path ? ` backup: ${result.backup_path}` : "";
    setStatus(
      `Wrote ${result.kept_rows} rows, deleted ${result.deleted_rows}, changed ${result.changed_rows}.${backup}`,
    );

    state.totalRows = meta.total_rows;
    els.jsonlPath.textContent = meta.jsonl_path;

    if (state.index >= state.totalRows) {
      state.index = Math.max(0, state.totalRows - 1);
    }
    await loadRow(state.index);
  } catch (err) {
    setStatus(`Write failed: ${err.message}`);
  }
}

function bindEvents() {
  els.prevBtn.addEventListener("click", () =>
    moveBy(-1).catch(() => undefined),
  );
  els.nextBtn.addEventListener("click", () => moveBy(1).catch(() => undefined));
  els.commitBtn.addEventListener("click", () =>
    writeJsonl().catch(() => undefined),
  );
  els.deleteBtn.addEventListener("click", toggleDelete);
  els.revertRowBtn.addEventListener("click", () =>
    revertCurrentRow().catch(() => undefined),
  );
  els.revertAllBtn.addEventListener("click", () =>
    revertAllRows().catch(() => undefined),
  );

  els.autoplayToggle.addEventListener("change", () => {
    if (els.autoplayToggle.checked) {
      maybeAutoPlayCurrentRow();
      setStatus("Autoplay enabled");
    } else {
      setStatus("Autoplay disabled");
    }
  });

  els.pickTextBtn.addEventListener("click", () =>
    applyPrimarySourceAndAdvance("text").catch(() => undefined),
  );
  els.pickHypBtn.addEventListener("click", () =>
    applyPrimarySourceAndAdvance("model_hypothesis").catch(() => undefined),
  );
  els.pickLikelyTextBtn.addEventListener("click", () =>
    applySourceChoice("likely_bad_text", true),
  );
  els.pickLikelyHypBtn.addEventListener("click", () =>
    applySourceChoice("likely_bad_model_text", true),
  );

  els.editInput.addEventListener("input", () => {
    state.dirty = true;
    setStatus("Editing...");
    queueAutosave(450, { markChecked: false });
  });

  els.indexInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      jumpToIndex().catch(() => undefined);
    }
  });
  els.indexInput.addEventListener("change", () => {
    jumpToIndex().catch(() => undefined);
  });
  els.indexInput.addEventListener("blur", () => {
    jumpToIndex().catch(() => undefined);
  });

  els.playBtn.addEventListener("click", () => {
    if (!els.audioPlayer.src) return;
    if (els.audioPlayer.paused) {
      els.audioPlayer.play().catch(() => undefined);
    } else {
      els.audioPlayer.pause();
    }
  });

  els.audioPlayer.addEventListener("play", () => {
    els.playBtn.textContent = "Pause (Space)";
  });

  els.audioPlayer.addEventListener("pause", () => {
    els.playBtn.textContent = "Play (Space)";
  });

  window.addEventListener("keydown", (event) => {
    const mod = event.metaKey || event.ctrlKey;
    const key = event.key.toLowerCase();
    const typing = isTypingTarget(event.target);

    if (mod && event.key === "Enter") {
      event.preventDefault();
      writeJsonl().catch(() => undefined);
      return;
    }

    if (event.code === "Space" && !typing) {
      event.preventDefault();
      if (els.audioPlayer.paused) {
        els.audioPlayer.play().catch(() => undefined);
      } else {
        els.audioPlayer.pause();
      }
      return;
    }

    if (typing || mod || event.altKey) return;

    if (key === "k") {
      event.preventDefault();
      moveBy(1).catch(() => undefined);
      return;
    }

    if (key === "j") {
      event.preventDefault();
      moveBy(-1).catch(() => undefined);
      return;
    }

    if (event.key === "ArrowLeft") {
      event.preventDefault();
      applySourceChoice("text", true);
      return;
    }

    if (event.key === "ArrowRight") {
      event.preventDefault();
      applySourceChoice("model_hypothesis", true);
      return;
    }

    if (key === "a") {
      event.preventDefault();
      applyPrimarySourceAndAdvance("text").catch(() => undefined);
      return;
    }

    if (key === "s") {
      event.preventDefault();
      applyPrimarySourceAndAdvance("model_hypothesis").catch(() => undefined);
      return;
    }

    if (key === "q") {
      event.preventDefault();
      applySourceChoice("likely_bad_text", true);
      return;
    }

    if (key === "w") {
      event.preventDefault();
      applySourceChoice("likely_bad_model_text", true);
      return;
    }

    if (key === "d") {
      event.preventDefault();
      toggleDelete();
      return;
    }

    if (key === "r") {
      event.preventDefault();
      if (event.shiftKey) {
        revertAllRows().catch(() => undefined);
      } else {
        revertCurrentRow().catch(() => undefined);
      }
    }
  });

  window.addEventListener("beforeunload", (event) => {
    if (state.dirty) {
      event.preventDefault();
      event.returnValue = "";
    }
  });
}

async function init() {
  setStatus("Loading metadata...");

  try {
    const data = await api("/api/meta");
    const meta = data.meta;

    state.totalRows = meta.total_rows;
    setCounts(meta.edited_rows, meta.deleted_rows, meta.checked_rows);

    els.jsonlPath.textContent = meta.jsonl_path;
    els.indexInput.min = "1";
    els.indexInput.max = String(meta.total_rows);
    els.deletedOutputInput.value = meta.default_deleted_output_path;

    bindEvents();

    if (state.totalRows > 0) {
      const initialIndex = getRequestedRowIndex(state.totalRows);
      await loadRow(initialIndex);
    } else {
      setStatus("No rows found in JSONL");
    }
  } catch (err) {
    setStatus(`Init failed: ${err.message}`);
  }
}

init();
