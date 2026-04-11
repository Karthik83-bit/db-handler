// =============================================================================
//  Cross-DB Merge Studio — merge.js
//
//  Mental model:
//    1. LOOKUP VALUE  — who/what you're searching for (e.g. user_id = "42")
//    2. MAPPINGS      — how tables relate (e.g. postgres.users.id → mongo.orders.user_id)
//
//  Engine steps:
//    a. Fetch the seed row(s) from the starting table matching the lookup value.
//    b. For each mapping, extract the join-key value(s) from already-fetched rows
//       and fetch matching rows from the other side.
//    c. Repeat until every connected table has been resolved (BFS over mappings).
//    d. Assemble one unified result object per seed row, carrying all related data.
// =============================================================================

const DATABASES = ["postgres", "mongodb", "cassandra"];
const CAST_TYPES = ["auto", "string", "number", "boolean", "date"];

// ── DOM refs ──────────────────────────────────────────────────────────────────
const environmentSelect        = document.getElementById("mergeEnvironment");
const connectAllBtn            = document.getElementById("connectAllBtn");
const connectResult            = document.getElementById("connectResult");
const seedSourceSelect         = document.getElementById("seedSource");
const seedFieldSelect          = document.getElementById("seedField");
const seedValueInput           = document.getElementById("seedValue");
const addMappingBtn            = document.getElementById("addMappingBtn");
const mappingRowsEl            = document.getElementById("mappingRows");
const runCrossMergeBtn         = document.getElementById("runCrossMergeBtn");
const mergeSummary             = document.getElementById("mergeSummary");
const mergeDebug               = document.getElementById("mergeDebug");
const outputFieldDropdownBtn   = document.getElementById("outputFieldDropdownBtn");
const outputFieldDropdownPanel = document.getElementById("outputFieldDropdownPanel");
const outputFieldSearchInput   = document.getElementById("outputFieldSearch");
const outputFieldCheckboxes    = document.getElementById("outputFieldCheckboxes");
const selectAllOutputFieldsBtn = document.getElementById("selectAllOutputFieldsBtn");
const clearOutputFieldsBtn     = document.getElementById("clearOutputFieldsBtn");
const sourceSummary            = document.getElementById("sourceSummary");
const mergedTableWrapper       = document.getElementById("mergedTableWrapper");
const mergedJson               = document.getElementById("mergedJson");

// ── App state ─────────────────────────────────────────────────────────────────
const state = {
  entitiesByDatabase:     {},
  fieldsBySource:         {},
  connectedDatabases:     new Set(),
  sourceCounter:          0,
  selectedOutputFieldIds: new Set()
};

// =============================================================================
//  JOIN ENGINE
// =============================================================================

function castValue(value, castType) {
  if (value === null || value === undefined) return null;
  switch (castType) {
    case "string":  return String(value);
    case "number":  return Number(value);
    case "boolean": return value === "true" || value === true || value === 1;
    case "date":    return new Date(value).toISOString();
    default:        return value; // "auto"
  }
}

/**
 * Fetch all rows for one source from the server.
 */
async function fetchSourceRows(environment, database, entity) {
  const url =
    `/api/entity-rows?environment=${encodeURIComponent(environment)}` +
    `&database=${encodeURIComponent(database)}` +
    `&entity=${encodeURIComponent(entity)}`;
  const data = await fetchJson(url);
  return data.rows || [];
}

/**
 * Master join function — BFS over mappings.
 *
 * @param {string}   seedSourceKey  — compound key of the starting table
 * @param {string}   seedField      — field to filter on
 * @param {string}   seedValue      — lookup value
 * @param {object[]} mappings       — [{leftSourceKey, leftField, rightSourceKey, rightField, castType}]
 * @param {string}   environment
 *
 * Returns { unifiedRows, rowsBySource, debugSteps }
 */
async function runJoin({ seedSourceKey, seedField, seedValue, mappings, environment }) {
  // resolved: sourceKey → filtered rows[]
  const resolved   = new Map();
  const debugSteps = [];

  // ── Step 1: seed table ──────────────────────────────────────────────────
  const { database: seedDb, entity: seedEntity } = parseSourceKey(seedSourceKey);
  const allSeedRows = await fetchSourceRows(environment, seedDb, seedEntity);

  const seedRows = (seedField && seedValue !== "")
    ? allSeedRows.filter(row => String(row[seedField]) === String(seedValue))
    : allSeedRows;

  resolved.set(seedSourceKey, seedRows);
  debugSteps.push({
    step: "seed", sourceKey: seedSourceKey,
    field: seedField, value: seedValue,
    totalRows: allSeedRows.length, matchedRows: seedRows.length
  });

  // ── Step 2: BFS — follow each mapping outward ───────────────────────────
  let progress = true;
  while (progress) {
    progress = false;
    for (const mapping of mappings) {
      const lk = mapping.leftSourceKey;
      const rk = mapping.rightSourceKey;
      const lResolved = resolved.has(lk);
      const rResolved = resolved.has(rk);

      if (lResolved === rResolved) continue; // both done, or neither ready yet

      const knownKey     = lResolved ? lk : rk;
      const unknownKey   = lResolved ? rk : lk;
      const knownField   = lResolved ? mapping.leftField  : mapping.rightField;
      const unknownField = lResolved ? mapping.rightField : mapping.leftField;

      // Collect distinct join-key values from the known side
      const knownRows  = resolved.get(knownKey);
      const joinValues = new Set(
        knownRows
          .map(row => castValue(row[knownField], mapping.castType))
          .filter(v => v !== null && v !== undefined)
          .map(String)
      );

      if (joinValues.size === 0) {
        resolved.set(unknownKey, []);
        debugSteps.push({ step: "join", knownKey, unknownKey, knownField, unknownField, joinValues: [], fetchedRows: 0, matchedRows: 0 });
        progress = true;
        continue;
      }

      // Fetch the unknown side and filter to matching rows
      const { database: uDb, entity: uEntity } = parseSourceKey(unknownKey);
      const allRows     = await fetchSourceRows(environment, uDb, uEntity);
      const matchedRows = allRows.filter(row => {
        const v = castValue(row[unknownField], mapping.castType);
        return v !== null && joinValues.has(String(v));
      });

      resolved.set(unknownKey, matchedRows);
      debugSteps.push({
        step: "join", knownKey, unknownKey, knownField, unknownField,
        castType: mapping.castType,
        joinValues: [...joinValues].slice(0, 10),
        fetchedRows: allRows.length, matchedRows: matchedRows.length
      });
      progress = true;
    }
  }

  // ── Step 3: assemble one unified record per seed row ────────────────────
  const unifiedRows = seedRows.map(seedRow => {
    const unified    = {};
    const seedLabel  = labelForKey(seedSourceKey);

    // Seed fields at top level
    for (const [k, v] of Object.entries(seedRow)) {
      unified[`${seedLabel}.${k}`] = v;
    }

    // Related sources: attach rows that belong to this specific seed row
    for (const [sourceKey, rows] of resolved.entries()) {
      if (sourceKey === seedSourceKey) continue;
      const relatedRows = findRelatedRows(seedRow, seedSourceKey, sourceKey, rows, mappings, resolved);
      const label       = labelForKey(sourceKey);

      if (relatedRows.length === 1) {
        for (const [k, v] of Object.entries(relatedRows[0])) unified[`${label}.${k}`] = v;
      } else if (relatedRows.length > 1) {
        unified[`${label}[]`] = relatedRows;
      }
    }

    return unified;
  });

  // Summary for UI
  const rowsBySource = {};
  for (const [key, rows] of resolved.entries()) {
    const { database, entity } = parseSourceKey(key);
    rowsBySource[key] = { database, entity, rows, columns: rows.length > 0 ? Object.keys(rows[0]) : [] };
  }

  return { unifiedRows, rowsBySource, debugSteps };
}

/**
 * Given one seed row, find which rows from a target source belong to it.
 * Checks direct mappings first, then one-hop indirect mappings.
 */
function findRelatedRows(seedRow, seedSourceKey, targetSourceKey, targetRows, mappings, resolved) {
  // Direct link
  for (const m of mappings) {
    if (m.leftSourceKey === seedSourceKey && m.rightSourceKey === targetSourceKey) {
      const kv = String(castValue(seedRow[m.leftField], m.castType));
      return targetRows.filter(r => String(castValue(r[m.rightField], m.castType)) === kv);
    }
    if (m.rightSourceKey === seedSourceKey && m.leftSourceKey === targetSourceKey) {
      const kv = String(castValue(seedRow[m.rightField], m.castType));
      return targetRows.filter(r => String(castValue(r[m.leftField], m.castType)) === kv);
    }
  }

  // One-hop indirect: seed → intermediate → target
  for (const m1 of mappings) {
    let intermKey = null, seedJoinField = null, intermJoinField = null;

    if (m1.leftSourceKey === seedSourceKey  && resolved.has(m1.rightSourceKey)) {
      intermKey = m1.rightSourceKey; seedJoinField = m1.leftField; intermJoinField = m1.rightField;
    } else if (m1.rightSourceKey === seedSourceKey && resolved.has(m1.leftSourceKey)) {
      intermKey = m1.leftSourceKey;  seedJoinField = m1.rightField; intermJoinField = m1.leftField;
    }

    if (!intermKey) continue;

    const seedKV      = String(castValue(seedRow[seedJoinField], m1.castType));
    const intermRows  = (resolved.get(intermKey) || []).filter(
      r => String(castValue(r[intermJoinField], m1.castType)) === seedKV
    );

    for (const m2 of mappings) {
      if (m2.leftSourceKey === intermKey && m2.rightSourceKey === targetSourceKey) {
        const keys = new Set(intermRows.map(r => String(castValue(r[m2.leftField], m2.castType))));
        return targetRows.filter(r => keys.has(String(castValue(r[m2.rightField], m2.castType))));
      }
      if (m2.rightSourceKey === intermKey && m2.leftSourceKey === targetSourceKey) {
        const keys = new Set(intermRows.map(r => String(castValue(r[m2.rightField], m2.castType))));
        return targetRows.filter(r => keys.has(String(castValue(r[m2.leftField], m2.castType))));
      }
    }
  }

  return targetRows; // fallback — return all if path not found
}

function labelForKey(sourceKey) {
  const { database, entity } = parseSourceKey(sourceKey);
  return `${database}.${entity}`;
}

// =============================================================================
//  RUN MERGE HANDLER
// =============================================================================

async function handleRunMerge() {
  const selectedSources = getSourcesWithSelectedOutputFields();
  const totalSelected   = selectedSources.reduce((t, s) => t + s.selectedFields.length, 0);

  if (totalSelected === 0) {
    hideMergeResults();
    mergeSummary.textContent = "Select at least one output field before merging.";
    return;
  }
  if (!seedSourceSelect.value) {
    hideMergeResults();
    mergeSummary.textContent = "Choose a starting table / collection in Step 2.";
    return;
  }

  runCrossMergeBtn.disabled = true;
  hideMergeResults();
  mergeSummary.textContent = "Fetching and joining data across databases…";

  try {
    const { unifiedRows, rowsBySource, debugSteps } = await runJoin({
      seedSourceKey: seedSourceSelect.value,
      seedField:     seedFieldSelect.value,
      seedValue:     seedValueInput.value.trim(),
      mappings:      getMappings(),
      environment:   environmentSelect.value
    });

    const outputRows = projectOutputFields(unifiedRows, selectedSources);

    mergeSummary.textContent =
      `${outputRows.length} unified record(s) assembled from ${Object.keys(rowsBySource).length} source(s).`;

    renderMergeDebug(debugSteps, rowsBySource);
    renderSourceSummary(rowsBySource);
    renderMergedRows(outputRows);
    renderJsonResult(mergedJson, { unifiedCount: outputRows.length, unifiedRows: outputRows, debugSteps });

  } catch (err) {
    mergeSummary.textContent = "Join failed: " + err.message;
    renderJsonResult(mergedJson, { success: false, error: err.message });
  } finally {
    runCrossMergeBtn.disabled = false;
  }
}

/**
 * Keep only the fields the user ticked in the output selector.
 * Fields are addressed as "db.entity.field" keys in the unified row.
 */
function projectOutputFields(unifiedRows, selectedSources) {
  if (state.selectedOutputFieldIds.size === 0) return unifiedRows;

  const wantedKeys = new Set();
  for (const itemId of state.selectedOutputFieldIds) {
    const [sourceId, field] = itemId.split("::");
    if (!sourceId || !field) continue;
    const source = selectedSources.find(s => s.sourceId === sourceId);
    if (!source) continue;
    wantedKeys.add(`${source.database}.${source.entity}.${field}`);
  }

  return unifiedRows.map(row => {
    const out = {};
    for (const [k, v] of Object.entries(row)) {
      if (wantedKeys.has(k)) out[k] = v;
    }
    return Object.keys(out).length > 0 ? out : row;
  });
}

// =============================================================================
//  DEBUG & RENDER
// =============================================================================

function renderMergeDebug(debugSteps, rowsBySource) {
  if (!debugSteps || debugSteps.length === 0) {
    mergeDebug.classList.add("hidden"); mergeDebug.innerHTML = ""; return;
  }

  const seedStep  = debugSteps.find(s => s.step === "seed");
  const joinSteps = debugSteps.filter(s => s.step === "join");

  const joinRows = joinSteps.map(s => `
    <tr>
      <td><code>${s.knownKey}</code>.${s.knownField}</td>
      <td style="text-align:center">→</td>
      <td><code>${s.unknownKey}</code>.${s.unknownField}</td>
      <td>${s.castType || "auto"}</td>
      <td>${(s.joinValues || []).join(", ") || "—"}</td>
      <td>${s.fetchedRows}</td>
      <td>${s.matchedRows}</td>
    </tr>`).join("");

  const srcRows = Object.entries(rowsBySource).map(([k, s]) =>
    `<li><code>${k}</code>: ${s.rows.length} row(s), ${s.columns.length} field(s)</li>`
  ).join("");

  mergeDebug.innerHTML = `
    <p class="eyebrow">Debug — Join trace</p>
    ${seedStep
      ? `<p><strong>Seed:</strong> <code>${seedStep.sourceKey}</code> — <code>${seedStep.field} = "${seedStep.value}"</code> → ${seedStep.matchedRows} of ${seedStep.totalRows} row(s) matched</p>`
      : ""}
    <ul>${srcRows}</ul>
    <div class="table-wrapper">
      <table class="data-table">
        <thead>
          <tr><th>Known side</th><th></th><th>Fetched side</th><th>Cast</th><th>Key values</th><th>Total fetched</th><th>Matched</th></tr>
        </thead>
        <tbody>${joinRows || "<tr><td colspan='7'>No join steps — add at least one mapping.</td></tr>"}</tbody>
      </table>
    </div>`;
  mergeDebug.classList.remove("hidden");
}

function hideMergeResults() {
  mergeSummary.textContent = "";
  mergeDebug.classList.add("hidden"); mergeDebug.innerHTML = "";
  sourceSummary.innerHTML = "";
  mergedTableWrapper.classList.add("hidden"); mergedTableWrapper.innerHTML = "";
  mergedJson.classList.add("hidden"); mergedJson.textContent = "";
}

function renderMergedRows(rows) {
  if (!rows.length) { mergedTableWrapper.classList.add("hidden"); mergedTableWrapper.innerHTML = ""; return; }

  const list = document.createElement("div");
  list.className = "merged-row-list";

  rows.forEach((row, index) => {
    const card = document.createElement("article");
    card.className = "merged-row-card";
    const header = document.createElement("h4");
    header.textContent = `Record ${index + 1}`;
    card.appendChild(header);

    const fields = document.createElement("div");
    fields.className = "merged-row-fields";
    Object.entries(row).forEach(([key, value]) => {
      const item    = document.createElement("div");
      item.className = "merged-field-item";
      const label   = document.createElement("span");
      label.className = "merged-field-key";
      label.textContent = key;
      const content = document.createElement("span");
      content.className = "merged-field-value";
      content.textContent = value && typeof value === "object" ? JSON.stringify(value) : String(value ?? "");
      item.appendChild(label);
      item.appendChild(content);
      fields.appendChild(item);
    });

    card.appendChild(fields);
    list.appendChild(card);
  });

  mergedTableWrapper.innerHTML = "";
  mergedTableWrapper.appendChild(list);
  mergedTableWrapper.classList.remove("hidden");
}

function renderSourceSummary(rowsBySource) {
  sourceSummary.innerHTML = "";
  Object.values(rowsBySource).forEach(source => {
    const card = document.createElement("article");
    card.className = "summary-card";
    card.innerHTML = `
      <p class="eyebrow">${source.database.toUpperCase()}</p>
      <h3>${source.entity}</h3>
      <p>${source.rows.length} row(s) matched</p>
      <p>${source.columns.length} field(s) available</p>`;
    sourceSummary.appendChild(card);
  });
}

function renderJsonResult(element, payload) {
  element.classList.remove("hidden");
  element.textContent = JSON.stringify(payload, null, 2);
}

// =============================================================================
//  API HELPERS
// =============================================================================

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const payload  = await response.json();
  if (!response.ok) throw new Error(payload.error || payload.message || "Request failed.");
  return payload;
}

async function loadDatabaseOptions() {
  try {
    const data = await fetchJson("/api/databases");
    populateSelect(environmentSelect, data.environments, "Choose environment");
    connectResult.textContent = "Choose an environment, then connect all databases.";
  } catch (error) {
    connectResult.textContent = JSON.stringify({ success: false, error: error.message }, null, 2);
  }
}

async function loadEntities(database) {
  const data = await fetchJson(
    `/api/entities?environment=${encodeURIComponent(environmentSelect.value)}&database=${encodeURIComponent(database)}`
  );
  state.entitiesByDatabase[database] = data.entities;
  getAddSourceButton(database).disabled = false;
  resetDatabaseSources(database);
}

async function handleConnectAll() {
  if (!environmentSelect.value) {
    connectResult.textContent = JSON.stringify({ success: false, message: "Choose an environment first." }, null, 2);
    return;
  }
  connectAllBtn.disabled = true;
  hideMergeResults();
  connectResult.textContent = "Connecting to PostgreSQL, MongoDB, and Cassandra…";
  state.connectedDatabases.clear();
  const statuses = [];

  try {
    await Promise.all(DATABASES.map(async database => {
      updateStatus(database, "Connecting…", "pending");
      try {
        const result = await fetchJson("/api/connect", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ environment: environmentSelect.value, database })
        });
        state.connectedDatabases.add(database);
        updateStatus(database, "Connected", "success");
        statuses.push(result);
        await loadEntities(database);
      } catch (error) {
        updateStatus(database, "Failed", "error");
        getAddSourceButton(database).disabled = true;
        getSourceContainer(database).innerHTML = "";
        statuses.push({ database, success: false, error: error.message });
      }
    }));
    refreshMappingRowSources();
    refreshSeedSourceOptions();
    refreshOutputFieldSelector();
    connectResult.textContent = JSON.stringify(statuses, null, 2);
  } finally {
    connectAllBtn.disabled = false;
  }
}

async function loadFieldsForCard(card) {
  const entity   = card.querySelector('[data-role="entity"]').value;
  if (!entity) {
    clearSourceCard(card);
    refreshMappingRowSources(); refreshSeedSourceOptions(); refreshOutputFieldSelector();
    return;
  }
  const database = card.dataset.database;
  const sourceId = card.dataset.sourceId;
  const data = await fetchJson(
    `/api/entity-fields?environment=${encodeURIComponent(environmentSelect.value)}&database=${encodeURIComponent(database)}&entity=${encodeURIComponent(entity)}`
  );
  state.fieldsBySource[getSourceKey(database, entity, sourceId)] = data.fields;
  renderAvailableFieldOptions(card.querySelector('[data-role="available-fields"]'), data.fields);
  refreshMappingRowSources(); refreshSeedSourceOptions(); refreshOutputFieldSelector();
}

// =============================================================================
//  MAPPING ROWS
// =============================================================================

function getMappings() {
  return Array.from(mappingRowsEl.querySelectorAll(".mapping-row")).map(row => ({
    leftSourceKey:  row.querySelector('[data-role="left-source"]').value,
    rightSourceKey: row.querySelector('[data-role="right-source"]').value,
    leftField:      row.querySelector('[data-role="left-field"]').value,
    rightField:     row.querySelector('[data-role="right-field"]').value,
    castType:       row.querySelector('[data-role="cast-type"]').value
  })).filter(m => m.leftSourceKey && m.rightSourceKey && m.leftField && m.rightField);
}

function createMappingRow() {
  const row = document.createElement("div");
  row.className = "mapping-row";
  row.innerHTML = `
    <select class="select" data-role="left-source"></select>
    <select class="select" data-role="left-field"></select>
    <span style="padding:0 6px;align-self:center;color:var(--color-text-secondary);font-size:13px;white-space:nowrap">=</span>
    <select class="select" data-role="right-source"></select>
    <select class="select" data-role="right-field"></select>
    <select class="select" data-role="cast-type"></select>
    <button type="button" class="button secondary" data-role="remove-row">Remove</button>
  `;
  CAST_TYPES.forEach(type => {
    const o = document.createElement("option");
    o.value = type; o.textContent = type;
    row.querySelector('[data-role="cast-type"]').appendChild(o);
  });
  row.querySelector('[data-role="left-source"]').addEventListener("change",  () => refreshMappingFieldOptions(row));
  row.querySelector('[data-role="right-source"]').addEventListener("change", () => refreshMappingFieldOptions(row));
  row.querySelector('[data-role="remove-row"]').addEventListener("click",    () => row.remove());
  mappingRowsEl.appendChild(row);
  refreshMappingRowSources();
  const options = getSourceOptions();
  if (options.length >= 2) {
    row.querySelector('[data-role="left-source"]').value  = options[0].value;
    row.querySelector('[data-role="right-source"]').value = options[1].value;
    refreshMappingFieldOptions(row);
  }
}

function refreshMappingFieldOptions(row) {
  [["left-source", "left-field"], ["right-source", "right-field"]].forEach(([srcRole, fldRole]) => {
    const sourceKey = row.querySelector(`[data-role="${srcRole}"]`).value;
    const select    = row.querySelector(`[data-role="${fldRole}"]`);
    const current   = select.value;
    select.innerHTML = "";
    getSourceFields(sourceKey).forEach(field => {
      const o = document.createElement("option");
      o.value = field.name; o.textContent = `${field.name} (${field.type})`;
      select.appendChild(o);
    });
    if (current) select.value = current;
  });
}

function refreshMappingRowSources() {
  const sourceOptions = getSourceOptions();
  Array.from(mappingRowsEl.querySelectorAll(".mapping-row")).forEach(row => {
    ["left-source", "right-source"].forEach(role => {
      const sel     = row.querySelector(`[data-role="${role}"]`);
      const current = sel.value;
      sel.innerHTML = "";
      createOptionElements(sourceOptions).forEach(o => sel.appendChild(o));
      if (current) sel.value = current;
    });
    refreshMappingFieldOptions(row);
  });
}

// =============================================================================
//  SOURCE CARDS
// =============================================================================

function createSourceCard(database) {
  state.sourceCounter += 1;
  const entityLabel = database === "mongodb" ? "Collection" : "Table";
  const card = document.createElement("div");
  card.className        = "source-card";
  card.dataset.database = database;
  card.dataset.sourceId = `${database}-${state.sourceCounter}`;
  card.innerHTML = `
    <div class="source-card-header">
      <h4>${entityLabel} Source</h4>
      <button type="button" class="button secondary" data-role="remove-source">Remove</button>
    </div>
    <label class="label">${entityLabel}</label>
    <select class="select" data-role="entity"></select>
    <label class="label">Available fields</label>
    <select class="select" data-role="available-fields" disabled></select>
  `;
  populateSelect(
    card.querySelector('[data-role="entity"]'),
    state.entitiesByDatabase[database] || [],
    `Choose a ${database === "mongodb" ? "collection" : "table"}`
  );
  card.querySelector('[data-role="entity"]').addEventListener("change", async () => {
    hideMergeResults(); await loadFieldsForCard(card);
  });
  card.querySelector('[data-role="remove-source"]').addEventListener("click", () => {
    const ev = card.querySelector('[data-role="entity"]').value;
    if (ev) delete state.fieldsBySource[getSourceKey(database, ev, card.dataset.sourceId)];
    card.remove();
    refreshMappingRowSources(); refreshSeedSourceOptions(); refreshOutputFieldSelector(); hideMergeResults();
  });
  clearSourceCard(card);
  return card;
}

function clearSourceCard(card) {
  renderAvailableFieldOptions(card.querySelector('[data-role="available-fields"]'), []);
}

function ensureAtLeastOneSourceCard(database) {
  const container = getSourceContainer(database);
  if (!container.children.length) container.appendChild(createSourceCard(database));
}

function resetDatabaseSources(database) {
  getSourceContainer(database).innerHTML = "";
  if (state.connectedDatabases.has(database)) ensureAtLeastOneSourceCard(database);
}

// =============================================================================
//  SEED SOURCE / FIELD SELECTORS
// =============================================================================

function refreshSeedSourceOptions() {
  const current = seedSourceSelect.value;
  seedSourceSelect.innerHTML = "";
  createOptionElements(getSourceOptions()).forEach(o => seedSourceSelect.appendChild(o));
  if (current) seedSourceSelect.value = current;
  refreshSeedFieldOptions();
}

function refreshSeedFieldOptions() {
  const current = seedFieldSelect.value;
  seedFieldSelect.innerHTML = "";
  createOptionElements(
    getSourceFields(seedSourceSelect.value).map(f => ({ value: f.name, label: `${f.name} (${f.type})` }))
  ).forEach(o => seedFieldSelect.appendChild(o));
  if (current) seedFieldSelect.value = current;
}

// =============================================================================
//  OUTPUT FIELD DROPDOWN
// =============================================================================

function updateOutputFieldDropdownLabel() {
  const count = state.selectedOutputFieldIds.size;
  outputFieldDropdownBtn.textContent = count > 0 ? `Output fields selected: ${count}` : "Select output fields";
}

function getAllOutputFieldEntries() {
  return getSelectedSources().flatMap(source => {
    const sourceKey = getSourceKey(source.database, source.entity, source.sourceId);
    return getSourceFields(sourceKey).map(field => ({
      id:       `${source.sourceId}::${field.name}`,
      sourceId: source.sourceId,
      database: source.database,
      entity:   source.entity,
      field:    field.name,
      label:    `${source.database.toUpperCase()} / ${source.entity} / ${field.name}`
    }));
  });
}

function refreshOutputFieldSelector() {
  const term    = outputFieldSearchInput.value.trim().toLowerCase();
  const entries = getAllOutputFieldEntries();
  const knownIds = new Set(entries.map(e => e.id));
  state.selectedOutputFieldIds = new Set(
    Array.from(state.selectedOutputFieldIds).filter(id => knownIds.has(id))
  );
  outputFieldCheckboxes.innerHTML = "";
  const visible = entries.filter(e => !term || e.label.toLowerCase().includes(term));
  if (!visible.length) {
    outputFieldCheckboxes.innerHTML = '<p class="muted-note">No fields match your search.</p>';
    updateOutputFieldDropdownLabel(); return;
  }
  visible.forEach(entry => {
    const label     = document.createElement("label");
    label.className = "checkbox-row";
    const checkbox  = document.createElement("input");
    checkbox.type    = "checkbox";
    checkbox.value   = entry.id;
    checkbox.checked = state.selectedOutputFieldIds.has(entry.id);
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) state.selectedOutputFieldIds.add(entry.id);
      else state.selectedOutputFieldIds.delete(entry.id);
      updateOutputFieldDropdownLabel(); hideMergeResults();
    });
    const text = document.createElement("span");
    text.textContent = entry.label;
    label.appendChild(checkbox); label.appendChild(text);
    outputFieldCheckboxes.appendChild(label);
  });
  updateOutputFieldDropdownLabel();
}

function toggleOutputFieldDropdown() { outputFieldDropdownPanel.classList.toggle("hidden"); }

function handleClickOutsideOutputDropdown(event) {
  if (!event.target.closest("#outputFieldDropdown")) outputFieldDropdownPanel.classList.add("hidden");
}

function selectAllVisibleOutputFields() {
  Array.from(outputFieldCheckboxes.querySelectorAll('input[type="checkbox"]')).forEach(cb => {
    cb.checked = true; state.selectedOutputFieldIds.add(cb.value);
  });
  updateOutputFieldDropdownLabel(); hideMergeResults();
}

function clearAllVisibleOutputFields() {
  Array.from(outputFieldCheckboxes.querySelectorAll('input[type="checkbox"]')).forEach(cb => {
    cb.checked = false; state.selectedOutputFieldIds.delete(cb.value);
  });
  updateOutputFieldDropdownLabel(); hideMergeResults();
}

// =============================================================================
//  DOM UTILITIES
// =============================================================================

function getStatusPill(database)      { return document.getElementById(`status-${database}`); }
function getSourceContainer(database) { return document.getElementById(`sources-${database}`); }
function getAddSourceButton(database) { return document.getElementById(`add-source-${database}`); }

function getSourceKey(database, entity, sourceId = "") { return `${database}:${entity}:${sourceId}`; }
function parseSourceKey(sourceKey) {
  const parts = String(sourceKey || "").split(":");
  return { database: parts[0] || "", entity: parts[1] || "", sourceId: parts[2] || "" };
}
function getSourceFields(sourceKey) { return state.fieldsBySource[sourceKey] || []; }

function getSelectedSourceCards() {
  return Array.from(document.querySelectorAll(".source-card")).filter(
    card => card.querySelector('[data-role="entity"]').value
  );
}

function getSelectedSources() {
  return getSelectedSourceCards().map(card => ({
    sourceId: card.dataset.sourceId,
    database: card.dataset.database,
    entity:   card.querySelector('[data-role="entity"]').value
  }));
}

function getSourceOptions() {
  return getSelectedSources().map(source => ({
    value: getSourceKey(source.database, source.entity, source.sourceId),
    label: `${source.database.toUpperCase()} / ${source.entity}`
  }));
}

function getSourcesWithSelectedOutputFields() {
  const fieldMapBySource = {};
  Array.from(state.selectedOutputFieldIds).forEach(itemId => {
    const [sourceId, field] = itemId.split("::");
    if (!sourceId || !field) return;
    if (!fieldMapBySource[sourceId]) fieldMapBySource[sourceId] = [];
    fieldMapBySource[sourceId].push(field);
  });
  return getSelectedSources().map(source => ({
    ...source, selectedFields: fieldMapBySource[source.sourceId] || []
  }));
}

function updateStatus(database, text, tone = "idle") {
  const pill = getStatusPill(database);
  pill.textContent  = text;
  pill.dataset.tone = tone;
}

function populateSelect(selectElement, items, placeholder) {
  selectElement.innerHTML = "";
  const ph = document.createElement("option");
  ph.value = ""; ph.textContent = placeholder;
  selectElement.appendChild(ph);
  items.forEach(item => {
    const o = document.createElement("option");
    o.value = item.value; o.textContent = item.label;
    selectElement.appendChild(o);
  });
}

function createOptionElements(items) {
  return items.map(item => {
    const o = document.createElement("option");
    o.value = item.value; o.textContent = item.label;
    return o;
  });
}

function renderAvailableFieldOptions(selectElement, fields) {
  selectElement.innerHTML = "";
  selectElement.disabled  = !fields.length;
  const ph = document.createElement("option");
  ph.value = ""; ph.textContent = fields.length ? "Open to view fields" : "No fields found";
  selectElement.appendChild(ph);
  fields.forEach(field => {
    const o = document.createElement("option");
    o.value = field.name; o.textContent = `${field.name} (${field.type})`;
    selectElement.appendChild(o);
  });
}

// =============================================================================
//  BOOTSTRAP
// =============================================================================

DATABASES.forEach(database => {
  getAddSourceButton(database).addEventListener("click", () => {
    getSourceContainer(database).appendChild(createSourceCard(database));
    refreshMappingRowSources(); refreshSeedSourceOptions(); refreshOutputFieldSelector(); hideMergeResults();
  });
});

connectAllBtn.addEventListener("click",            handleConnectAll);
addMappingBtn.addEventListener("click",            createMappingRow);
runCrossMergeBtn.addEventListener("click",         handleRunMerge);
seedSourceSelect.addEventListener("change",        refreshSeedFieldOptions);
outputFieldDropdownBtn.addEventListener("click",   toggleOutputFieldDropdown);
outputFieldSearchInput.addEventListener("input",   refreshOutputFieldSelector);
selectAllOutputFieldsBtn.addEventListener("click", selectAllVisibleOutputFields);
clearOutputFieldsBtn.addEventListener("click",     clearAllVisibleOutputFields);
document.addEventListener("click",                handleClickOutsideOutputDropdown);

updateOutputFieldDropdownLabel();
loadDatabaseOptions();