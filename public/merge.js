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
const sourceSummary = document.getElementById("sourceSummary");
const mergedTableWrapper = document.getElementById("mergedTableWrapper");
const mergedJson = document.getElementById("mergedJson");

const state = {
  entitiesByDatabase: {},
  fieldsBySource: {},
  connectedDatabases: new Set()
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

  if (!response.ok) {
    throw new Error(payload.error || payload.message || "Request failed.");
  }

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

function getEntitySelect(database) {
  return document.getElementById(`entity-${database}`);
}

function getFieldList(database) {
  return document.getElementById(`fields-${database}`);
}

function getStatusPill(database) {
  return document.getElementById(`status-${database}`);
}

function getSelectedSources() {
  return DATABASES.map((database) => ({
    database,
    entity: getEntitySelect(database).value
  })).filter((source) => source.entity);
}

function getSourceOptions() {
  return getSelectedSources().map((source) => ({
    value: `${source.database}:${source.entity}`,
    label: `${source.database.toUpperCase()} / ${source.entity}`
  }));
}

function getSourceFields(sourceKey) {
  return state.fieldsBySource[sourceKey] || [];
}

function renderFieldList(database, fields) {
  const fieldList = getFieldList(database);
  fieldList.innerHTML = "";

  if (!fields.length) {
    fieldList.innerHTML = '<span class="field-chip">No fields found.</span>';
    return;
  }

  fields.forEach((field) => {
    const chip = document.createElement("span");
    chip.className = "field-chip";
    chip.textContent = `${field.name} - ${field.type}`;
    fieldList.appendChild(chip);
  });
}

function updateStatus(database, text, tone = "idle") {
  const pill = getStatusPill(database);
  pill.textContent = text;
  pill.dataset.tone = tone;
}

function createOptionElements(items) {
  return items.map((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    return option;
  });
}

function refreshFieldOptions(row) {
  const leftSource = row.querySelector('[data-role="left-source"]').value;
  const rightSource = row.querySelector('[data-role="right-source"]').value;
  const leftFieldSelect = row.querySelector('[data-role="left-field"]');
  const rightFieldSelect = row.querySelector('[data-role="right-field"]');
  const leftSelected = leftFieldSelect.value;
  const rightSelected = rightFieldSelect.value;

  leftFieldSelect.innerHTML = "";
  rightFieldSelect.innerHTML = "";

  createOptionElements(
    getSourceFields(leftSource).map((field) => ({
      value: field.name,
      label: `${field.name} (${field.type})`
    }))
  ).forEach((option) => leftFieldSelect.appendChild(option));

  createOptionElements(
    getSourceFields(rightSource).map((field) => ({
      value: field.name,
      label: `${field.name} (${field.type})`
    }))
  ).forEach((option) => rightFieldSelect.appendChild(option));

  if (leftSelected) {
    leftFieldSelect.value = leftSelected;
  }

  if (rightSelected) {
    rightFieldSelect.value = rightSelected;
  }
}

function refreshMappingRowSources() {
  const sourceOptions = getSourceOptions();

  Array.from(mappingRows.querySelectorAll(".mapping-row")).forEach((row) => {
    const leftSourceSelect = row.querySelector('[data-role="left-source"]');
    const rightSourceSelect = row.querySelector('[data-role="right-source"]');
    const currentLeft = leftSourceSelect.value;
    const currentRight = rightSourceSelect.value;

    leftSourceSelect.innerHTML = "";
    rightSourceSelect.innerHTML = "";

    createOptionElements(sourceOptions).forEach((option) =>
      leftSourceSelect.appendChild(option)
    );
    createOptionElements(sourceOptions).forEach((option) =>
      rightSourceSelect.appendChild(option)
    );

    if (currentLeft) {
      leftSourceSelect.value = currentLeft;
    }

    if (currentRight) {
      rightSourceSelect.value = currentRight;
    }

    refreshFieldOptions(row);
  });
}

function refreshSeedSourceOptions() {
  const sourceOptions = getSourceOptions();
  const currentSource = seedSourceSelect.value;

  seedSourceSelect.innerHTML = "";
  createOptionElements(sourceOptions).forEach((option) =>
    seedSourceSelect.appendChild(option)
  );

  if (currentSource) {
    seedSourceSelect.value = currentSource;
  }

  refreshSeedFieldOptions();
}

function createMappingRow() {
  const row = document.createElement("div");
  row.className = "mapping-row";

  const leftSourceSelect = document.createElement("select");
  leftSourceSelect.className = "select";
  leftSourceSelect.dataset.role = "left-source";

  const leftFieldSelect = document.createElement("select");
  leftFieldSelect.className = "select";
  leftFieldSelect.dataset.role = "left-field";

  const rightSourceSelect = document.createElement("select");
  rightSourceSelect.className = "select";
  rightSourceSelect.dataset.role = "right-source";

  const rightFieldSelect = document.createElement("select");
  rightFieldSelect.className = "select";
  rightFieldSelect.dataset.role = "right-field";

  const castTypeSelect = document.createElement("select");
  castTypeSelect.className = "select";
  castTypeSelect.dataset.role = "cast-type";
  CAST_TYPES.forEach((type) => {
    const option = document.createElement("option");
    option.value = type;
    option.textContent = type;
    castTypeSelect.appendChild(option);
  });

  const removeButton = document.createElement("button");
  removeButton.type = "button";
  removeButton.className = "button secondary";
  removeButton.textContent = "Remove";
  removeButton.addEventListener("click", () => {
    row.remove();
  });

  leftSourceSelect.addEventListener("change", () => refreshFieldOptions(row));
  rightSourceSelect.addEventListener("change", () => refreshFieldOptions(row));

  row.appendChild(leftSourceSelect);
  row.appendChild(leftFieldSelect);
  row.appendChild(rightSourceSelect);
  row.appendChild(rightFieldSelect);
  row.appendChild(castTypeSelect);
  row.appendChild(removeButton);

  mappingRows.appendChild(row);
  refreshMappingRowSources();

  const sourceOptions = getSourceOptions();
  if (sourceOptions.length >= 2) {
    leftSourceSelect.value = sourceOptions[0].value;
    rightSourceSelect.value = sourceOptions[1].value;
    refreshFieldOptions(row);
  }
}

function refreshSeedFieldOptions() {
  const currentField = seedFieldSelect.value;
  const selectedSource = seedSourceSelect.value;

  seedFieldSelect.innerHTML = "";
  createOptionElements(
    getSourceFields(selectedSource).map((field) => ({
      value: field.name,
      label: `${field.name} (${field.type})`
    }))
  ).forEach((option) => seedFieldSelect.appendChild(option));

  if (currentField) {
    seedFieldSelect.value = currentField;
  }
}

function getMappings() {
  return Array.from(mappingRows.querySelectorAll(".mapping-row"))
    .map((row) => {
      const leftSource = row.querySelector('[data-role="left-source"]').value;
      const leftField = row.querySelector('[data-role="left-field"]').value;
      const rightSource = row.querySelector('[data-role="right-source"]').value;
      const rightField = row.querySelector('[data-role="right-field"]').value;
      const castType = row.querySelector('[data-role="cast-type"]').value;
      const [leftDatabase, leftEntity] = leftSource.split(":");
      const [rightDatabase, rightEntity] = rightSource.split(":");

      return {
        leftDatabase,
        leftEntity,
        leftField,
        rightDatabase,
        rightEntity,
        rightField,
        castType
      };
    })
    .filter(
      (mapping) =>
        mapping.leftDatabase &&
        mapping.leftEntity &&
        mapping.leftField &&
        mapping.rightDatabase &&
        mapping.rightEntity &&
        mapping.rightField
    );
}

function getSourceFilters() {
  const source = seedSourceSelect.value;
  const field = seedFieldSelect.value;
  const value = seedValueInput.value.trim();

  if (!source || !field || value === "") {
    return [];
  }

  const [database, entity] = source.split(":");

  return [
    {
      database,
      entity,
      field,
      value
    }
  ];
}

function renderTable(columns, rows) {
  if (!rows.length) {
    mergedTableWrapper.classList.add("hidden");
    mergedTableWrapper.innerHTML = "";
    return;
  }

  const table = document.createElement("table");
  table.className = "data-table";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      const value = row[column];
      td.textContent =
        value && typeof value === "object"
          ? JSON.stringify(value)
          : value ?? "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  table.appendChild(thead);
  table.appendChild(tbody);
  mergedTableWrapper.innerHTML = "";
  mergedTableWrapper.appendChild(table);
  mergedTableWrapper.classList.remove("hidden");
}

function renderSourceSummary(rowsBySource) {
  sourceSummary.innerHTML = "";

  Object.values(rowsBySource).forEach((source) => {
    const card = document.createElement("article");
    card.className = "summary-card";
    card.innerHTML = `
      <p class="eyebrow">${source.database.toUpperCase()}</p>
      <h3>${source.entity}</h3>
      <p>${source.rows.length} row(s) fetched</p>
      <p>${source.columns.length} field(s) available</p>
    `;
    sourceSummary.appendChild(card);
  });
}

async function loadDatabaseOptions() {
  try {
    const data = await fetchJson("/api/databases");
    populateSelect(environmentSelect, data.environments, "Choose environment");
    connectResult.textContent =
      "Choose an environment, then connect all databases.";
  } catch (error) {
    connectResult.textContent = JSON.stringify(
      {
        success: false,
        message: "Failed to load environments.",
        error: error.message
      },
      null,
      2
    );
  }
}

async function loadFields(database, entity) {
  if (!entity) {
    renderFieldList(database, []);
    return;
  }

  const environment = environmentSelect.value;
  const data = await fetchJson(
    `/api/entity-fields?environment=${encodeURIComponent(environment)}&database=${encodeURIComponent(database)}&entity=${encodeURIComponent(entity)}`
  );

  const sourceKey = `${database}:${entity}`;
  state.fieldsBySource[sourceKey] = data.fields;
  renderFieldList(database, data.fields);
  refreshMappingRowSources();
  refreshSeedSourceOptions();
}

async function loadEntities(database) {
  const environment = environmentSelect.value;
  const entitySelect = getEntitySelect(database);

  const data = await fetchJson(
    `/api/entities?environment=${encodeURIComponent(environment)}&database=${encodeURIComponent(database)}`
  );

  state.entitiesByDatabase[database] = data.entities;
  entitySelect.disabled = false;
  populateSelect(
    entitySelect,
    data.entities,
    `Choose a ${database === "mongodb" ? "collection" : "table"}`
  );
  renderFieldList(database, []);
}

async function handleConnectAll() {
  const environment = environmentSelect.value;

  if (!environment) {
    connectResult.textContent = JSON.stringify(
      {
        success: false,
        message: "Choose an environment first."
      },
      null,
      2
    );
    return;
  }

  connectAllBtn.disabled = true;
  hideMergeResults();
  connectResult.textContent = "Connecting to PostgreSQL, MongoDB, and Cassandra...";

  const statuses = [];
  state.connectedDatabases.clear();

  try {
    await Promise.all(
      DATABASES.map(async (database) => {
        updateStatus(database, "Connecting...", "pending");

        try {
          const result = await fetchJson("/api/connect", {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              environment,
              database
            })
          });

          state.connectedDatabases.add(database);
          updateStatus(database, "Connected", "success");
          statuses.push(result);
          await loadEntities(database);
        } catch (error) {
          updateStatus(database, "Failed", "error");
          getEntitySelect(database).disabled = true;
          getEntitySelect(database).innerHTML = "";
          renderFieldList(database, []);
          statuses.push({
            database,
            success: false,
            error: error.message
          });
        }
      })
    );

    connectResult.textContent = JSON.stringify(statuses, null, 2);
  } finally {
    connectAllBtn.disabled = false;
  }
}

async function handleRunMerge() {
  const sources = getSelectedSources();
  const mappings = getMappings();
  const sourceFilters = getSourceFilters();

  runCrossMergeBtn.disabled = true;
  hideMergeResults();
  mergeSummary.textContent = "Merging selected data...";

  try {
    const data = await fetchJson("/api/cross-merge", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        environment: environmentSelect.value,
        sources,
        mappings,
        sourceFilters
      })
    });

    mergeSummary.textContent = `${data.mergedCount} merged row(s) built from ${data.sources.length} selected data sources.`;
    renderSourceSummary(data.rowsBySource);
    renderTable(data.mergedColumns, data.mergedRows);
    renderJsonResult(mergedJson, data);
  } catch (error) {
    mergeSummary.textContent = "Merge failed.";
    renderJsonResult(mergedJson, {
      success: false,
      error: error.message
    });
  } finally {
    runCrossMergeBtn.disabled = false;
  }
}

DATABASES.forEach((database) => {
  getEntitySelect(database).addEventListener("change", async (event) => {
    hideMergeResults();
    await loadFields(database, event.target.value);
  });
});

connectAllBtn.addEventListener("click", handleConnectAll);
addMappingBtn.addEventListener("click", () => {
  createMappingRow();
});
runCrossMergeBtn.addEventListener("click", handleRunMerge);
seedSourceSelect.addEventListener("change", refreshSeedFieldOptions);

loadDatabaseOptions();
