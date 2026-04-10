const environmentSelect = document.getElementById("environment");
const databaseSelect = document.getElementById("database");
const connectBtn = document.getElementById("connectBtn");
const resultBox = document.getElementById("result");
const explorerPanel = document.getElementById("explorerPanel");
const reloadEntitiesBtn = document.getElementById("reloadEntitiesBtn");
const entitySelect = document.getElementById("entity");
const fieldList = document.getElementById("fieldList");
const filtersContainer = document.getElementById("filters");
const addFilterBtn = document.getElementById("addFilterBtn");
const runQueryBtn = document.getElementById("runQueryBtn");
const queryMeta = document.getElementById("queryMeta");
const tableWrapper = document.getElementById("tableWrapper");
const queryJson = document.getElementById("queryJson");

const state = {
  connected: false,
  fields: [],
  entities: []
};

function renderResult(payload) {
  resultBox.textContent = JSON.stringify(payload, null, 2);
}

function renderJsonResult(payload) {
  queryJson.classList.remove("hidden");
  queryJson.textContent = JSON.stringify(payload, null, 2);
}

function hideQueryResults() {
  queryMeta.textContent = "";
  tableWrapper.innerHTML = "";
  tableWrapper.classList.add("hidden");
  queryJson.classList.add("hidden");
  queryJson.textContent = "";
}

function getSelection() {
  return {
    environment: environmentSelect.value,
    database: databaseSelect.value,
    entity: entitySelect.value
  };
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json();

  if (!response.ok) {
    throw new Error(payload.error || payload.message || "Request failed.");
  }

  return payload;
}

function populateSelect(selectElement, items) {
  selectElement.innerHTML = "";
  items.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    selectElement.appendChild(option);
  });
}

function createFilterRow() {
  const row = document.createElement("div");
  row.className = "filter-row";

  const fieldSelect = document.createElement("select");
  fieldSelect.className = "select";
  state.fields.forEach((field) => {
    const option = document.createElement("option");
    option.value = field.name;
    option.textContent = `${field.name} (${field.type})`;
    fieldSelect.appendChild(option);
  });

  const valueInput = document.createElement("input");
  valueInput.className = "input";
  valueInput.placeholder = "Value";

  const removeButton = document.createElement("button");
  removeButton.type = "button";
  removeButton.className = "button secondary";
  removeButton.textContent = "Remove";
  removeButton.addEventListener("click", () => {
    row.remove();
  });

  row.appendChild(fieldSelect);
  row.appendChild(valueInput);
  row.appendChild(removeButton);

  filtersContainer.appendChild(row);
}

function getFilters() {
  return Array.from(filtersContainer.querySelectorAll(".filter-row"))
    .map((row) => {
      const fieldSelect = row.querySelector("select");
      const valueInput = row.querySelector("input");
      return {
        field: fieldSelect.value,
        value: valueInput.value
      };
    })
    .filter((filter) => filter.field && filter.value !== "");
}

function renderFieldList(fields) {
  fieldList.innerHTML = "";

  fields.forEach((field) => {
    const chip = document.createElement("span");
    chip.className = "field-chip";
    chip.textContent = `${field.name} - ${field.type}`;
    fieldList.appendChild(chip);
  });
}

function renderTable(columns, rows) {
  if (!rows.length) {
    tableWrapper.classList.add("hidden");
    tableWrapper.innerHTML = "";
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

  tableWrapper.innerHTML = "";
  tableWrapper.appendChild(table);
  tableWrapper.classList.remove("hidden");
}

async function loadDatabaseOptions() {
  try {
    const data = await fetchJson("/api/databases");
    populateSelect(environmentSelect, data.environments);
    populateSelect(databaseSelect, data.options);
    renderResult({
      message: "Choose a database and click Connect."
    });
  } catch (error) {
    renderResult({
      success: false,
      message: "Failed to load database options.",
      error: error.message
    });
  }
}

async function loadEntities() {
  const { environment, database } = getSelection();
  const data = await fetchJson(
    `/api/entities?environment=${encodeURIComponent(environment)}&database=${encodeURIComponent(database)}`
  );

  state.entities = data.entities;
  populateSelect(entitySelect, data.entities);

  if (!data.entities.length) {
    fieldList.innerHTML = "<span class=\"field-chip\">No tables or collections found.</span>";
    filtersContainer.innerHTML = "";
    hideQueryResults();
    return;
  }

  await loadFields();
}

async function loadFields() {
  const { environment, database, entity } = getSelection();

  if (!entity) {
    state.fields = [];
    renderFieldList([]);
    filtersContainer.innerHTML = "";
    return;
  }

  const data = await fetchJson(
    `/api/entity-fields?environment=${encodeURIComponent(environment)}&database=${encodeURIComponent(database)}&entity=${encodeURIComponent(entity)}`
  );

  state.fields = data.fields;
  renderFieldList(data.fields);
  filtersContainer.innerHTML = "";

  if (data.fields.length) {
    createFilterRow();
  }
}

async function handleConnect() {
  connectBtn.disabled = true;
  explorerPanel.classList.add("hidden");
  hideQueryResults();
  renderResult({
    message: `Connecting to ${databaseSelect.value} in ${environmentSelect.value}...`
  });

  try {
    const data = await fetchJson("/api/connect", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        environment: environmentSelect.value,
        database: databaseSelect.value
      })
    });

    state.connected = true;
    renderResult(data);
    await loadEntities();
    explorerPanel.classList.remove("hidden");
  } catch (error) {
    state.connected = false;
    renderResult({
      success: false,
      message: "Connection failed.",
      error: error.message
    });
  } finally {
    connectBtn.disabled = false;
  }
}

async function handleRunQuery() {
  const { environment, database, entity } = getSelection();
  const filters = getFilters();

  runQueryBtn.disabled = true;
  hideQueryResults();
  queryMeta.textContent = `Running query on ${entity}...`;

  try {
    const data = await fetchJson("/api/query", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        environment,
        database,
        entity,
        filters
      })
    });

    queryMeta.textContent = `${data.rowCount} row(s) returned from ${data.entity}.`;
    renderTable(data.columns, data.rows);
    renderJsonResult(data);
  } catch (error) {
    queryMeta.textContent = "Query failed.";
    renderJsonResult({
      success: false,
      error: error.message
    });
  } finally {
    runQueryBtn.disabled = false;
  }
}

connectBtn.addEventListener("click", handleConnect);
reloadEntitiesBtn.addEventListener("click", async () => {
  if (!state.connected) {
    return;
  }

  await loadEntities();
});
entitySelect.addEventListener("change", async () => {
  hideQueryResults();
  await loadFields();
});
addFilterBtn.addEventListener("click", () => {
  if (state.fields.length) {
    createFilterRow();
  }
});
runQueryBtn.addEventListener("click", handleRunQuery);

loadDatabaseOptions();
