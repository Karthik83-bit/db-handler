const DATABASES = ["postgres", "mongodb", "cassandra"];
const CAST_TYPES = ["auto", "string", "number", "boolean", "date"];
const environmentSelect = document.getElementById("mergeEnvironment");
const connectAllBtn = document.getElementById("connectAllBtn");
const connectResult = document.getElementById("connectResult");
const seedSourceSelect = document.getElementById("seedSource");
const seedFieldSelect = document.getElementById("seedField");
const seedValueInput = document.getElementById("seedValue");
const addMappingBtn = document.getElementById("addMappingBtn");
const mappingRows = document.getElementById("mappingRows");
const runCrossMergeBtn = document.getElementById("runCrossMergeBtn");
const mergeSummary = document.getElementById("mergeSummary");
const outputFieldDropdownBtn = document.getElementById("outputFieldDropdownBtn");
const outputFieldDropdownPanel = document.getElementById("outputFieldDropdownPanel");
const outputFieldSearchInput = document.getElementById("outputFieldSearch");
const outputFieldCheckboxes = document.getElementById("outputFieldCheckboxes");
const selectAllOutputFieldsBtn = document.getElementById("selectAllOutputFieldsBtn");
const clearOutputFieldsBtn = document.getElementById("clearOutputFieldsBtn");
const sourceSummary = document.getElementById("sourceSummary");
const mergedTableWrapper = document.getElementById("mergedTableWrapper");
const mergedJson = document.getElementById("mergedJson");

const state = {
  entitiesByDatabase: {},
  fieldsBySource: {},
  connectedDatabases: new Set(),
  sourceCounter: 0,
  selectedOutputFieldIds: new Set()
};

function renderJsonResult(element, payload) {
  element.classList.remove("hidden");
  element.textContent = JSON.stringify(payload, null, 2);
}

function hideMergeResults() {
  mergeSummary.textContent = "";
  sourceSummary.innerHTML = "";
  mergedTableWrapper.classList.add("hidden");
  mergedTableWrapper.innerHTML = "";
  mergedJson.classList.add("hidden");
  mergedJson.textContent = "";
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || payload.message || "Request failed.");
  return payload;
}

function populateSelect(selectElement, items, placeholder) {
  selectElement.innerHTML = "";
  const placeholderOption = document.createElement("option");
  placeholderOption.value = "";
  placeholderOption.textContent = placeholder;
  selectElement.appendChild(placeholderOption);
  items.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    selectElement.appendChild(option);
  });
}

function createOptionElements(items) {
  return items.map((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    return option;
  });
}

function getStatusPill(database) { return document.getElementById(`status-${database}`); }
function getSourceContainer(database) { return document.getElementById(`sources-${database}`); }
function getAddSourceButton(database) { return document.getElementById(`add-source-${database}`); }
function getSourceKey(database, entity, sourceId = "") {
  return `${database}:${entity}:${sourceId}`;
}
function parseSourceKey(sourceKey) {
  const [database = "", entity = "", sourceId = ""] = String(sourceKey || "").split(":");
  return { database, entity, sourceId };
}
function getSourceFields(sourceKey) { return state.fieldsBySource[sourceKey] || []; }

function getSelectedSourceCards() {
  return Array.from(document.querySelectorAll(".source-card")).filter((card) => card.querySelector('[data-role="entity"]').value);
}

function getSelectedSources() {
  return getSelectedSourceCards().map((card) => ({
    sourceId: card.dataset.sourceId,
    database: card.dataset.database,
    entity: card.querySelector('[data-role="entity"]').value
  }));
}

function getSourceOptions() {
  return getSelectedSources().map((source) => ({
    value: getSourceKey(source.database, source.entity, source.sourceId),
    label: `${source.database.toUpperCase()} / ${source.entity}`
  }));
}

function renderAvailableFieldOptions(selectElement, fields) {
  selectElement.innerHTML = "";
  selectElement.disabled = !fields.length;
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = fields.length ? "Open to view fields" : "No fields found";
  selectElement.appendChild(placeholder);
  fields.forEach((field) => {
    const option = document.createElement("option");
    option.value = field.name;
    option.textContent = `${field.name} (${field.type})`;
    selectElement.appendChild(option);
  });
}

function updateOutputFieldDropdownLabel() {
  const count = state.selectedOutputFieldIds.size;
  outputFieldDropdownBtn.textContent = count > 0 ? `Output fields selected: ${count}` : "Select output fields";
}

function getAllOutputFieldEntries() {
  return getSelectedSources().flatMap((source) => {
    const sourceKey = getSourceKey(source.database, source.entity, source.sourceId);
    const fields = getSourceFields(sourceKey);
    return fields.map((field) => ({
      id: `${source.sourceId}::${field.name}`,
      sourceId: source.sourceId,
      database: source.database,
      entity: source.entity,
      field: field.name,
      label: `${source.database.toUpperCase()} / ${source.entity} / ${field.name}`
    }));
  });
}

function refreshOutputFieldSelector() {
  const term = outputFieldSearchInput.value.trim().toLowerCase();
  const entries = getAllOutputFieldEntries();
  const knownIds = new Set(entries.map((entry) => entry.id));
  state.selectedOutputFieldIds = new Set(
    Array.from(state.selectedOutputFieldIds).filter((id) => knownIds.has(id))
  );

  outputFieldCheckboxes.innerHTML = "";
  const visible = entries.filter((entry) => !term || entry.label.toLowerCase().includes(term));
  if (!visible.length) {
    outputFieldCheckboxes.innerHTML = '<p class="muted-note">No fields match your search.</p>';
    updateOutputFieldDropdownLabel();
    return;
  }

  visible.forEach((entry) => {
    const label = document.createElement("label");
    label.className = "checkbox-row";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = entry.id;
    checkbox.checked = state.selectedOutputFieldIds.has(entry.id);
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) state.selectedOutputFieldIds.add(entry.id);
      else state.selectedOutputFieldIds.delete(entry.id);
      updateOutputFieldDropdownLabel();
      hideMergeResults();
    });
    const text = document.createElement("span");
    text.textContent = entry.label;
    label.appendChild(checkbox);
    label.appendChild(text);
    outputFieldCheckboxes.appendChild(label);
  });

  updateOutputFieldDropdownLabel();
}

function getSourcesWithSelectedOutputFields() {
  const fieldMapBySource = {};
  Array.from(state.selectedOutputFieldIds).forEach((itemId) => {
    const [sourceId, field] = itemId.split("::");
    if (!sourceId || !field) return;
    if (!fieldMapBySource[sourceId]) fieldMapBySource[sourceId] = [];
    fieldMapBySource[sourceId].push(field);
  });

  return getSelectedSources().map((source) => ({
    ...source,
    selectedFields: fieldMapBySource[source.sourceId] || []
  }));
}

function updateStatus(database, text, tone = "idle") {
  const pill = getStatusPill(database);
  pill.textContent = text;
  pill.dataset.tone = tone;
}

function refreshFieldOptions(row) {
  [["left-source", "left-field"], ["right-source", "right-field"]].forEach(([sourceRole, fieldRole]) => {
    const sourceValue = row.querySelector(`[data-role="${sourceRole}"]`).value;
    const select = row.querySelector(`[data-role="${fieldRole}"]`);
    const current = select.value;
    select.innerHTML = "";
    createOptionElements(getSourceFields(sourceValue).map((field) => ({
      value: field.name,
      label: `${field.name} (${field.type})`
    }))).forEach((option) => select.appendChild(option));
    if (current) select.value = current;
  });
}

function refreshMappingRowSources() {
  const sourceOptions = getSourceOptions();
  Array.from(mappingRows.querySelectorAll(".mapping-row")).forEach((row) => {
    const left = row.querySelector('[data-role="left-source"]');
    const right = row.querySelector('[data-role="right-source"]');
    const currentLeft = left.value;
    const currentRight = right.value;
    left.innerHTML = "";
    right.innerHTML = "";
    createOptionElements(sourceOptions).forEach((option) => left.appendChild(option));
    createOptionElements(sourceOptions).forEach((option) => right.appendChild(option));
    if (currentLeft) left.value = currentLeft;
    if (currentRight) right.value = currentRight;
    refreshFieldOptions(row);
  });
}

function refreshSeedFieldOptions() {
  const current = seedFieldSelect.value;
  seedFieldSelect.innerHTML = "";
  createOptionElements(getSourceFields(seedSourceSelect.value).map((field) => ({
    value: field.name,
    label: `${field.name} (${field.type})`
  }))).forEach((option) => seedFieldSelect.appendChild(option));
  if (current) seedFieldSelect.value = current;
}

function refreshSeedSourceOptions() {
  const current = seedSourceSelect.value;
  seedSourceSelect.innerHTML = "";
  createOptionElements(getSourceOptions()).forEach((option) => seedSourceSelect.appendChild(option));
  if (current) seedSourceSelect.value = current;
  refreshSeedFieldOptions();
}

function clearSourceCard(card) {
  renderAvailableFieldOptions(card.querySelector('[data-role="available-fields"]'), []);
}

async function loadFieldsForCard(card) {
  const entity = card.querySelector('[data-role="entity"]').value;
  if (!entity) {
    clearSourceCard(card);
    refreshMappingRowSources();
    refreshSeedSourceOptions();
    refreshOutputFieldSelector();
    return;
  }
  const database = card.dataset.database;
  const sourceId = card.dataset.sourceId;
  const data = await fetchJson(`/api/entity-fields?environment=${encodeURIComponent(environmentSelect.value)}&database=${encodeURIComponent(database)}&entity=${encodeURIComponent(entity)}`);
  state.fieldsBySource[getSourceKey(database, entity, sourceId)] = data.fields;
  renderAvailableFieldOptions(card.querySelector('[data-role="available-fields"]'), data.fields);
  refreshMappingRowSources();
  refreshSeedSourceOptions();
  refreshOutputFieldSelector();
}

function createSourceCard(database) {
  state.sourceCounter += 1;
  const entityLabel = database === "mongodb" ? "Collection" : "Table";
  const card = document.createElement("div");
  card.className = "source-card";
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
  populateSelect(card.querySelector('[data-role="entity"]'), state.entitiesByDatabase[database] || [], `Choose a ${database === "mongodb" ? "collection" : "table"}`);
  card.querySelector('[data-role="entity"]').addEventListener("change", async () => {
    hideMergeResults();
    await loadFieldsForCard(card);
  });
  card.querySelector('[data-role="remove-source"]').addEventListener("click", () => {
    const entityValue = card.querySelector('[data-role="entity"]').value;
    if (entityValue) {
      delete state.fieldsBySource[getSourceKey(database, entityValue, card.dataset.sourceId)];
    }
    card.remove();
    refreshMappingRowSources();
    refreshSeedSourceOptions();
    refreshOutputFieldSelector();
    hideMergeResults();
  });
  clearSourceCard(card);
  return card;
}

function ensureAtLeastOneSourceCard(database) {
  const container = getSourceContainer(database);
  if (!container.children.length) container.appendChild(createSourceCard(database));
}

function resetDatabaseSources(database) {
  getSourceContainer(database).innerHTML = "";
  if (state.connectedDatabases.has(database)) ensureAtLeastOneSourceCard(database);
}

function createMappingRow() {
  const row = document.createElement("div");
  row.className = "mapping-row";
  row.innerHTML = `
    <select class="select" data-role="left-source"></select>
    <select class="select" data-role="left-field"></select>
    <select class="select" data-role="right-source"></select>
    <select class="select" data-role="right-field"></select>
    <select class="select" data-role="cast-type"></select>
    <button type="button" class="button secondary" data-role="remove-row">Remove</button>
  `;
  CAST_TYPES.forEach((type) => {
    const option = document.createElement("option");
    option.value = type;
    option.textContent = type;
    row.querySelector('[data-role="cast-type"]').appendChild(option);
  });
  row.querySelector('[data-role="left-source"]').addEventListener("change", () => refreshFieldOptions(row));
  row.querySelector('[data-role="right-source"]').addEventListener("change", () => refreshFieldOptions(row));
  row.querySelector('[data-role="remove-row"]').addEventListener("click", () => row.remove());
  mappingRows.appendChild(row);
  refreshMappingRowSources();
  const options = getSourceOptions();
  if (options.length >= 2) {
    row.querySelector('[data-role="left-source"]').value = options[0].value;
    row.querySelector('[data-role="right-source"]').value = options[1].value;
    refreshFieldOptions(row);
  }
}

function getMappings() {
  return Array.from(mappingRows.querySelectorAll(".mapping-row")).map((row) => {
    const left = parseSourceKey(row.querySelector('[data-role="left-source"]').value);
    const right = parseSourceKey(row.querySelector('[data-role="right-source"]').value);
    return {
      leftSourceId: left.sourceId,
      leftDatabase: left.database,
      leftEntity: left.entity,
      leftField: row.querySelector('[data-role="left-field"]').value,
      rightSourceId: right.sourceId,
      rightDatabase: right.database,
      rightEntity: right.entity,
      rightField: row.querySelector('[data-role="right-field"]').value,
      castType: row.querySelector('[data-role="cast-type"]').value
    };
  }).filter((mapping) => mapping.leftDatabase && mapping.leftEntity && mapping.leftField && mapping.rightDatabase && mapping.rightEntity && mapping.rightField);
}

function getSourceFilters() {
  const value = seedValueInput.value.trim();
  if (!seedSourceSelect.value || !seedFieldSelect.value || value === "") return [];
  const { database, entity, sourceId } = parseSourceKey(seedSourceSelect.value);
  return [{ sourceId, database, entity, field: seedFieldSelect.value, value }];
}

function renderMergedRows(rows) {
  if (!rows.length) {
    mergedTableWrapper.classList.add("hidden");
    mergedTableWrapper.innerHTML = "";
    return;
  }

  const list = document.createElement("div");
  list.className = "merged-row-list";

  rows.forEach((row, index) => {
    const card = document.createElement("article");
    card.className = "merged-row-card";
    const header = document.createElement("h4");
    header.textContent = `Merged Row ${index + 1}`;
    card.appendChild(header);

    const fields = document.createElement("div");
    fields.className = "merged-row-fields";
    Object.entries(row).forEach(([key, value]) => {
      const item = document.createElement("div");
      item.className = "merged-field-item";
      const label = document.createElement("span");
      label.className = "merged-field-key";
      label.textContent = key;
      const content = document.createElement("span");
      content.className = "merged-field-value";
      content.textContent =
        value && typeof value === "object" ? JSON.stringify(value) : String(value ?? "");
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
  Object.values(rowsBySource).forEach((source) => {
    const card = document.createElement("article");
    card.className = "summary-card";
    card.innerHTML = `<p class="eyebrow">${source.database.toUpperCase()}</p><h3>${source.entity}</h3><p>${source.rows.length} row(s) fetched</p><p>${source.columns.length} field(s) available</p>`;
    sourceSummary.appendChild(card);
  });
}

async function loadDatabaseOptions() {
  try {
    const data = await fetchJson("/api/databases");
    populateSelect(environmentSelect, data.environments, "Choose environment");
    connectResult.textContent = "Choose an environment, then connect all databases.";
  } catch (error) {
    connectResult.textContent = JSON.stringify({ success: false, message: "Failed to load environments.", error: error.message }, null, 2);
  }
}

async function loadEntities(database) {
  const data = await fetchJson(`/api/entities?environment=${encodeURIComponent(environmentSelect.value)}&database=${encodeURIComponent(database)}`);
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
  connectResult.textContent = "Connecting to PostgreSQL, MongoDB, and Cassandra...";
  const statuses = [];
  state.connectedDatabases.clear();
  try {
    await Promise.all(DATABASES.map(async (database) => {
      updateStatus(database, "Connecting...", "pending");
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

async function handleRunMerge() {
  const selectedSources = getSourcesWithSelectedOutputFields();
  const selectedOutputFieldsCount = selectedSources.reduce(
    (total, source) => total + source.selectedFields.length,
    0
  );
  if (selectedOutputFieldsCount === 0) {
    hideMergeResults();
    mergeSummary.textContent = "Select at least one output field before merging.";
    return;
  }

  runCrossMergeBtn.disabled = true;
  hideMergeResults();
  mergeSummary.textContent = "Merging selected data...";
  try {
    const data = await fetchJson("/api/cross-merge", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        environment: environmentSelect.value,
        sources: selectedSources,
        mappings: getMappings(),
        sourceFilters: getSourceFilters()
      })
    });
    mergeSummary.textContent = `${data.mergedCount} merged row(s) built from ${data.sources.length} selected data sources.`;
    renderSourceSummary(data.rowsBySource);
    renderMergedRows(data.mergedRows);
    renderJsonResult(mergedJson, data);
  } catch (error) {
    mergeSummary.textContent = "Merge failed.";
    renderJsonResult(mergedJson, { success: false, error: error.message });
  } finally {
    runCrossMergeBtn.disabled = false;
  }
}

function toggleOutputFieldDropdown() {
  outputFieldDropdownPanel.classList.toggle("hidden");
}

function handleClickOutsideOutputDropdown(event) {
  if (!event.target.closest("#outputFieldDropdown")) {
    outputFieldDropdownPanel.classList.add("hidden");
  }
}

function selectAllVisibleOutputFields() {
  Array.from(outputFieldCheckboxes.querySelectorAll('input[type="checkbox"]')).forEach((checkbox) => {
    checkbox.checked = true;
    state.selectedOutputFieldIds.add(checkbox.value);
  });
  updateOutputFieldDropdownLabel();
  hideMergeResults();
}

function clearAllVisibleOutputFields() {
  Array.from(outputFieldCheckboxes.querySelectorAll('input[type="checkbox"]')).forEach((checkbox) => {
    checkbox.checked = false;
    state.selectedOutputFieldIds.delete(checkbox.value);
  });
  updateOutputFieldDropdownLabel();
  hideMergeResults();
}

DATABASES.forEach((database) => {
  getAddSourceButton(database).addEventListener("click", () => {
    getSourceContainer(database).appendChild(createSourceCard(database));
    refreshMappingRowSources();
    refreshSeedSourceOptions();
    refreshOutputFieldSelector();
    hideMergeResults();
  });
});

connectAllBtn.addEventListener("click", handleConnectAll);
addMappingBtn.addEventListener("click", createMappingRow);
runCrossMergeBtn.addEventListener("click", handleRunMerge);
seedSourceSelect.addEventListener("change", refreshSeedFieldOptions);
outputFieldDropdownBtn.addEventListener("click", toggleOutputFieldDropdown);
outputFieldSearchInput.addEventListener("input", refreshOutputFieldSelector);
selectAllOutputFieldsBtn.addEventListener("click", selectAllVisibleOutputFields);
clearOutputFieldsBtn.addEventListener("click", clearAllVisibleOutputFields);
document.addEventListener("click", handleClickOutsideOutputDropdown);
updateOutputFieldDropdownLabel();
loadDatabaseOptions();
