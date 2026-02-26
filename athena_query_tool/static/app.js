/* Athena Query Tool — frontend application logic */

// Module-level variable storing the latest query results for export
let currentResults = null;

// DOM element references
const configRegion = document.getElementById("config-region");
const configDatabase = document.getElementById("config-database");
const configWorkgroup = document.getElementById("config-workgroup");
const queryInput = document.getElementById("query-input");
const executeBtn = document.getElementById("execute-btn");
const loadingIndicator = document.getElementById("loading-indicator");
const errorDisplay = document.getElementById("error-display");
const resultsTable = document.getElementById("results-table");
const rowCount = document.getElementById("row-count");
const noResults = document.getElementById("no-results");
const exportButtons = document.getElementById("export-buttons");
const exportCsvBtn = document.getElementById("export-csv-btn");
const exportJsonBtn = document.getElementById("export-json-btn");

/**
 * Fetch configuration from the API and populate the Configuration_Panel.
 */
async function fetchConfig() {
  try {
    const res = await fetch("/api/config");
    const json = await res.json();
    if (json.success) {
      configRegion.textContent = json.data.region;
      configDatabase.textContent = json.data.database;
      configWorkgroup.textContent = json.data.workgroup;
    }
  } catch (_err) {
    // Config fetch failure is non-critical; panel keeps placeholder text
  }
}

/**
 * Execute the SQL query entered in the Query_Editor.
 */
async function executeQuery() {
  const sql = queryInput.value;

  // Clear previous state
  errorDisplay.hidden = true;
  errorDisplay.textContent = "";
  noResults.hidden = true;
  exportButtons.hidden = true;
  resultsTable.querySelector("thead").innerHTML = "";
  resultsTable.querySelector("tbody").innerHTML = "";
  rowCount.textContent = "";
  currentResults = null;

  // Enter loading state
  executeBtn.disabled = true;
  loadingIndicator.hidden = false;

  try {
    const res = await fetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql }),
    });

    const json = await res.json();

    if (!json.success) {
      showError(json.error);
      return;
    }

    renderResults(json.data);
  } catch (_err) {
    showError("Connection error: unable to reach the server");
  } finally {
    executeBtn.disabled = false;
    loadingIndicator.hidden = true;
  }
}

/**
 * Render query results into the Results_Panel table.
 * @param {Object} data - { columns, rows, row_count, from_cache }
 */
function renderResults(data) {
  currentResults = data;

  const { columns, rows, row_count } = data;

  // Zero-row case
  if (row_count === 0) {
    noResults.hidden = false;
    rowCount.textContent = "0 rows";
    exportButtons.hidden = true;
    return;
  }

  // Build header row
  const thead = resultsTable.querySelector("thead");
  const headerRow = document.createElement("tr");
  columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col.name;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  // Build data rows
  const tbody = resultsTable.querySelector("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    row.forEach((cell) => {
      const td = document.createElement("td");
      td.textContent = cell != null ? cell : "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });

  rowCount.textContent = row_count + " row" + (row_count !== 1 ? "s" : "");
  exportButtons.hidden = false;
}

/**
 * Generate a CSV file from the current results and trigger a download.
 */
function exportCSV() {
  if (!currentResults) return;

  const { columns, rows } = currentResults;

  const csvRows = [];
  // Header row
  csvRows.push(columns.map((c) => csvEscape(c.name)).join(","));
  // Data rows
  rows.forEach((row) => {
    csvRows.push(row.map((cell) => csvEscape(cell != null ? String(cell) : "")).join(","));
  });

  const blob = new Blob([csvRows.join("\n")], { type: "text/csv" });
  downloadBlob(blob, "results.csv");
}

/**
 * Escape a value for CSV output. Wraps in quotes if the value contains
 * a comma, double-quote, or newline.
 */
function csvEscape(value) {
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return '"' + value.replace(/"/g, '""') + '"';
  }
  return value;
}

/**
 * Generate a JSON file from the current results and trigger a download.
 */
function exportJSON() {
  if (!currentResults) return;

  const { columns, rows, row_count } = currentResults;

  const rowObjects = rows.map((row) => {
    const obj = {};
    columns.forEach((col, i) => {
      obj[col.name] = row[i] != null ? row[i] : null;
    });
    return obj;
  });

  const payload = {
    columns: columns,
    rows: rowObjects,
    row_count: row_count,
  };

  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  downloadBlob(blob, "results.json");
}

/**
 * Display an error message in the error display area.
 */
function showError(message) {
  errorDisplay.textContent = message;
  errorDisplay.hidden = false;
}

/**
 * Trigger a file download from a Blob.
 */
function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// Wire up event listeners
executeBtn.addEventListener("click", executeQuery);
exportCsvBtn.addEventListener("click", exportCSV);
exportJsonBtn.addEventListener("click", exportJSON);

// Load configuration on page load
fetchConfig();
