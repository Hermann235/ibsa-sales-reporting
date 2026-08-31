const POLL_INTERVAL_MS = 10000;

// palette categoriale validata (ordine fisso, mai riassegnata per rango) - vedi skill dataviz
const PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"];

function makeColorAssigner(palette) {
  const assigned = new Map();
  let next = 0;
  return function colorFor(key) {
    if (!assigned.has(key)) {
      assigned.set(key, palette[next % palette.length]);
      next += 1;
    }
    return assigned.get(key);
  };
}

const colorForCountry = makeColorAssigner(PALETTE);
const colorForCategory = makeColorAssigner(PALETTE);
const colorForChannel = makeColorAssigner(PALETTE);

const charts = {};

function buildTrendDatasets(rows, dimKey, valueKey, colorFor) {
  const dates = [...new Set(rows.map((r) => r.order_date))].sort();
  const dims = [...new Set(rows.map((r) => r[dimKey]))].sort();
  const datasets = dims.map((dim) => {
    const byDate = new Map(rows.filter((r) => r[dimKey] === dim).map((r) => [r.order_date, r[valueKey]]));
    const color = colorFor(dim);
    return {
      label: dim,
      data: dates.map((d) => (byDate.has(d) ? byDate.get(d) : null)),
      borderColor: color,
      backgroundColor: color,
      borderWidth: 2,
      pointRadius: 4,
      pointHoverRadius: 6,
      spanGaps: true,
      tension: 0.15,
    };
  });
  return { labels: dates, datasets };
}

function ensureLineChart(id) {
  if (charts[id]) return charts[id];
  const ctx = document.getElementById(id).getContext("2d");
  charts[id] = new Chart(ctx, {
    type: "line",
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      scales: {
        y: { beginAtZero: true, title: { display: true, text: "Netto (EUR)" } },
      },
      plugins: { legend: { display: true, position: "bottom" } },
    },
  });
  return charts[id];
}

function updateTrendChart(id, rows, dimKey, colorFor) {
  const chart = ensureLineChart(id);
  const { labels, datasets } = buildTrendDatasets(rows, dimKey, "net_amount_eur", colorFor);
  chart.data.labels = labels;
  chart.data.datasets = datasets;
  chart.update();
}

const eurCompactFormatter = new Intl.NumberFormat("it-IT", { notation: "compact", compactDisplay: "short", maximumFractionDigits: 1 });
const numberCompactFormatter = new Intl.NumberFormat("it-IT", { notation: "compact", compactDisplay: "short", maximumFractionDigits: 1 });

function latestDate(rows) {
  return rows.reduce((max, r) => (r.order_date > max ? r.order_date : max), rows[0].order_date);
}

function updateStatCard(valueId, captionId, rows, valueKey, formatter, suffix) {
  const valueEl = document.getElementById(valueId);
  const captionEl = document.getElementById(captionId);
  if (!rows.length) return;
  const total = rows.reduce((sum, r) => sum + (r[valueKey] ?? 0), 0);
  valueEl.textContent = `${formatter.format(total)}${suffix}`;
  captionEl.textContent = `Aggiornato al ${latestDate(rows)}`;
}

function ensureTopProductsChart() {
  const id = "chart-top-products";
  if (charts[id]) return charts[id];
  const ctx = document.getElementById(id).getContext("2d");
  charts[id] = new Chart(ctx, {
    type: "bar",
    data: { labels: [], datasets: [{ label: "Fatturato netto (EUR)", data: [], backgroundColor: PALETTE[0], borderRadius: 4, borderSkipped: false }] },
    options: {
      indexAxis: "y",
      responsive: true,
      scales: { x: { beginAtZero: true, title: { display: true, text: "Netto (EUR)" } } },
      plugins: { legend: { display: false } },
    },
  });
  return charts[id];
}

function updateTopProductsChart(rows) {
  const top = rows.slice(0, 10);
  const chart = ensureTopProductsChart();
  chart.data.labels = top.map((r) => `${r.product_name} (${r.product_code})`);
  chart.data.datasets[0].data = top.map((r) => r.net_amount_eur);
  chart.update();
}

function renderTable(tableId, emptyId, rows, rowRenderer) {
  const tbody = document.querySelector(`#${tableId} tbody`);
  const emptyState = document.getElementById(emptyId);
  tbody.innerHTML = "";
  if (!rows.length) {
    emptyState.style.display = "block";
    return;
  }
  emptyState.style.display = "none";
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = rowRenderer(row);
    tbody.appendChild(tr);
  });
}

function updateFreshnessBanner(batches) {
  const banner = document.getElementById("freshness-banner");
  const done = batches.filter((b) => b.status === "done" && b.processed_at);
  if (!done.length) {
    banner.textContent = "In attesa del primo batch...";
    return;
  }
  // batches e' ordinato per batch_id DESC: il primo "done" e' l'ultima country arrivata.
  const latest = done[0];
  const sameFolder = done.filter((b) => b.landing_dir === latest.landing_dir);
  const stillPending = batches.some((b) => b.landing_dir === latest.landing_dir && b.status !== "done");
  const countrySummary = stillPending
    ? `${sameFolder.length} country aggiornate finora, altre in arrivo`
    : `${sameFolder.length} country aggiornate, cartella completa`;
  banner.textContent = `Ultimo aggiornamento dati: ${latest.processed_at} — landing/${latest.landing_dir} (${countrySummary})`;
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`errore chiamando ${url}`);
  return response.json();
}

async function refreshDashboard() {
  try {
    const kpis = await fetchJson("/api/kpis");
    updateStatCard("stat-revenue-cumulative", "stat-revenue-caption", kpis.sales_overall_day, "net_amount_eur", eurCompactFormatter, " €");
    updateStatCard("stat-quantity-cumulative", "stat-quantity-caption", kpis.sales_overall_day, "total_quantity", numberCompactFormatter, " pz");
    updateTrendChart("chart-country-trend", kpis.sales_by_country_day, "country", colorForCountry);
    updateTrendChart("chart-category-trend", kpis.sales_by_category_day, "product_category", colorForCategory);
    updateTrendChart("chart-channel-trend", kpis.sales_by_channel_day, "sales_channel", colorForChannel);
    updateTopProductsChart(kpis.top_products);

    const countries = await fetchJson("/api/countries/status");
    renderTable(
      "table-countries",
      "countries-empty",
      countries,
      (row) => `<td>${row.country}</td><td>${row.last_batch_id}</td><td>${row.last_event_ts}</td><td>${row.last_updated_at}</td>`
    );

    const batches = await fetchJson("/api/batches");
    updateFreshnessBanner(batches);
    renderTable(
      "table-batches",
      "batches-empty",
      batches,
      (row) => `<td>${row.batch_id}</td><td>${row.landing_dir}</td><td>${row.source_file}</td><td>${row.status}</td>` +
        `<td>${row.rows_bronze ?? ""}</td><td>${row.rows_silver ?? ""}</td><td>${row.processed_at ?? ""}</td>`
    );
  } catch (err) {
    console.error("errore durante il refresh della dashboard", err);
  }
}

refreshDashboard();
setInterval(refreshDashboard, POLL_INTERVAL_MS);
