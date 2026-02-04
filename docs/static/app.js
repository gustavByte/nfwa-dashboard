/* global Chart */

const $ = (id) => document.getElementById(id);

class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.status = status;
  }
}

let apiMode = null; // "server" | "static"
const staticFileCache = new Map();
const eventKeyMap = new Map(); // `${gender}||${event_no}` -> event_key
let athleteIndexPromise = null;

async function fetchJson(url) {
  const res = await fetch(url, { cache: "no-store" });
  let data = null;
  try {
    data = await res.json();
  } catch (e) {
    data = null;
  }
  if (!res.ok) {
    throw new ApiError(data?.error || `HTTP ${res.status}`, res.status);
  }
  return data;
}

function apiEndpointFromUrl(u) {
  const pathname = u?.pathname || "";
  const idx = pathname.lastIndexOf("/api/");
  if (idx < 0) return null;
  return pathname.slice(idx + 5).replace(/^\/+/, "");
}

function rememberEventKeys(gender, events) {
  for (const e of events || []) {
    if (e && e.event_no && e.event_key) {
      eventKeyMap.set(`${gender}||${e.event_no}`, String(e.event_key));
    }
  }
}

function getEventKey(gender, eventNo) {
  return eventKeyMap.get(`${gender}||${eventNo}`) || null;
}

async function apiJsonStatic(path) {
  const u = new URL(path, window.location.href);
  const ep = apiEndpointFromUrl(u);
  if (!ep) throw new ApiError("Ugyldig API-sti", 400);
  const p = u.searchParams;

  if (ep === "meta") {
    return fetchJson("api/meta.json");
  }

  if (ep === "events") {
    const gender = p.get("gender");
    if (!gender) throw new ApiError("Mangler parameter: gender", 400);
    const data = await fetchJson(`api/events/${encodeURIComponent(gender)}.json`);
    rememberEventKeys(gender, data);
    return data;
  }

  if (ep === "season_summary") {
    const season = p.get("season");
    const gender = p.get("gender");
    const top = p.get("top") || "10";
    if (!season) throw new ApiError("Mangler parameter: season", 400);
    if (!gender) throw new ApiError("Mangler parameter: gender", 400);
    return fetchJson(
      `api/season_summary/${encodeURIComponent(season)}/${encodeURIComponent(gender)}/top${encodeURIComponent(top)}.json`,
    );
  }

  if (ep === "event_trend") {
    const gender = p.get("gender");
    const eventNo = p.get("event");
    const top = p.get("top") || "10";
    if (!gender) throw new ApiError("Mangler parameter: gender", 400);
    if (!eventNo) throw new ApiError("Mangler parameter: event", 400);
    const key = getEventKey(gender, eventNo);
    if (!key) throw new ApiError(`Fant ikke event_key for ${gender} / ${eventNo}`, 400);
    return fetchJson(
      `api/event_trend/${encodeURIComponent(gender)}/${encodeURIComponent(key)}/top${encodeURIComponent(top)}.json`,
    );
  }

  if (ep === "event_results") {
    const season = p.get("season");
    const gender = p.get("gender");
    const eventNo = p.get("event");
    const mode = p.get("mode") || "best";
    const limit = parseInt(p.get("limit") || "200", 10) || 200;
    const offset = parseInt(p.get("offset") || "0", 10) || 0;

    if (!season) throw new ApiError("Mangler parameter: season", 400);
    if (!gender) throw new ApiError("Mangler parameter: gender", 400);
    if (!eventNo) throw new ApiError("Mangler parameter: event", 400);
    const key = getEventKey(gender, eventNo);
    if (!key) throw new ApiError(`Fant ikke event_key for ${gender} / ${eventNo}`, 400);

    const fileUrl = `api/event_results/${encodeURIComponent(season)}/${encodeURIComponent(gender)}/${encodeURIComponent(key)}/${encodeURIComponent(mode)}.json`;
    let full = staticFileCache.get(fileUrl);
    if (!full) {
      full = await fetchJson(fileUrl);
      staticFileCache.set(fileUrl, full);
    }

    const allRows = full.rows || [];
    const total = full.total ?? allRows.length;
    const sliced = allRows.slice(offset, offset + limit).map((r, i) => ({
      ...r,
      rank: r.rank ?? offset + i + 1,
    }));

    return { ...full, season: Number(season), gender, event_no: eventNo, mode, limit, offset, total, rows: sliced };
  }

  if (ep === "athlete") {
    const id = p.get("id");
    const since = p.get("since");
    if (!id) throw new ApiError("Mangler parameter: id", 400);

    if (!athleteIndexPromise) {
      athleteIndexPromise = fetchJson("api/athlete/index.json");
    }
    const idx = await athleteIndexPromise;
    const rows0 = (idx?.by_id && idx.by_id[String(id)]) || [];
    const sinceSeason = since ? Number(since) : null;
    const rows = sinceSeason ? rows0.filter((r) => Number(r.season) >= sinceSeason) : rows0;
    return { athlete_id: Number(id), rows };
  }

  throw new ApiError("Ukjent API-endepunkt", 404);
}

async function apiJson(path) {
  // Prefer live server when available, otherwise fall back to exported JSON files.
  if (apiMode === "static") return apiJsonStatic(path);

  try {
    const data = await fetchJson(path);
    apiMode = "server";
    return data;
  } catch (e) {
    if (apiMode !== "static" && (e instanceof TypeError || e.status === 404)) {
      const data = await apiJsonStatic(path);
      apiMode = "static";
      return data;
    }
    throw e;
  }
}

function setOptions(select, options, { valueKey = "value", labelKey = "label", selectedValue = null } = {}) {
  select.innerHTML = "";
  for (const opt of options) {
    const o = document.createElement("option");
    o.value = opt[valueKey];
    o.textContent = opt[labelKey];
    if (selectedValue !== null && String(o.value) === String(selectedValue)) {
      o.selected = true;
    }
    select.appendChild(o);
  }
}

function fmt(v) {
  if (v === null || v === undefined) return "";
  return String(v);
}

function toNumber(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function formatTimeNo(seconds) {
  const v = toNumber(seconds);
  if (v === null) return "";

  // Under 60s: keep hundredths for sprint events.
  if (v < 60) {
    return v.toFixed(2).replace(".", ",");
  }

  const total = Math.round(v);
  const hours = Math.floor(total / 3600);
  const rem = total % 3600;
  const minutes = Math.floor(rem / 60);
  const secs = rem % 60;

  if (hours > 0) {
    return `${hours},${String(minutes).padStart(2, "0")},${String(secs).padStart(2, "0")}`;
  }
  return `${minutes},${String(secs).padStart(2, "0")}`;
}

function formatDecimalNo(value, decimals = 2) {
  const v = toNumber(value);
  if (v === null) return "";
  const d = Math.max(0, Number(decimals) || 0);
  return v.toFixed(d).replace(".", ",");
}

function formatHigherPerfNo(value) {
  const v = toNumber(value);
  if (v === null) return "";

  // Heuristic: combined events are points (thousands). Show as whole points.
  if (v >= 1000) return String(Math.round(v));

  return formatDecimalNo(v, 2);
}

function padRight(text, width) {
  const s = (text ?? "").toString();
  return s.length >= width ? s : s + " ".repeat(width - s.length);
}

function padLeft(text, width) {
  const s = (text ?? "").toString();
  return s.length >= width ? s : " ".repeat(width - s.length) + s;
}

function formatPct(n, d) {
  const nn = toNumber(n);
  const dd = toNumber(d);
  if (nn === null || dd === null || dd <= 0) return "-";
  return `${Math.round((nn / dd) * 100)}%`;
}

function formatEventMeta(rows) {
  if (!rows?.length) return "Ingen data.";
  const first = rows[0];
  const orient = first.orientation === "lower" ? "lavest er best" : "høyest er best";

  const lines = [];
  lines.push(`Kjønn: ${first.gender}`);
  lines.push(`Øvelse: ${first.event_no}`);
  lines.push(`WA-øvelse: ${first.wa_event || "-"}`);
  lines.push(`Orientering: ${orient}`);
  lines.push(`Top-N: ${first.top_n}`);
  lines.push("");

  lines.push(
    `${padRight("Sesong", 6)} ${padLeft("Utøvere", 7)} ${padLeft("Resultater", 9)} ${padLeft("WA-poeng", 8)} ${padLeft("Dekning", 7)}`,
  );
  for (const r of rows) {
    lines.push(
      `${padRight(r.season, 6)} ${padLeft(r.athletes_total, 7)} ${padLeft(r.results_total, 9)} ${padLeft(r.points_available, 8)} ${padLeft(formatPct(r.points_available, r.results_total), 7)}`,
    );
  }
  lines.push("");
  lines.push("Merk: Hvis «Utøvere» ≈ «Resultater» betyr det ofte at kilden allerede er en sesongbeste-liste (1 rad per utøver).");

  return lines.join("\n");
}

let pointsChart = null;
let perfChart = null;
let countChart = null;
let seasonRows = [];
let seasonSortState = { key: "event", dir: "asc" };
let resultsOffset = 0;
let resultsTotal = 0;

function destroyCharts() {
  for (const ch of [pointsChart, perfChart, countChart]) {
    if (ch) ch.destroy();
  }
  pointsChart = null;
  perfChart = null;
  countChart = null;
}

function makeLineChart(ctx, { labels, values, label, reverseY = false, tooltipText = null, yTickFormatter = null }) {
  return new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label,
          data: values,
          borderColor: "rgba(122, 162, 255, 0.95)",
          backgroundColor: "rgba(122, 162, 255, 0.25)",
          pointRadius: 4,
          pointHoverRadius: 5,
          tension: 0.25,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx2) => {
              const idx = ctx2.dataIndex;
              const v = ctx2.parsed.y;
              if (tooltipText && tooltipText[idx]) return tooltipText[idx];
              return `${label}: ${v}`;
            },
          },
        },
      },
      scales: {
        y: {
          reverse: reverseY,
          ticks: {
            color: "rgba(233, 238, 252, 0.75)",
            callback: (v) => (yTickFormatter ? yTickFormatter(v) : String(v)),
          },
          grid: { color: "rgba(233, 238, 252, 0.08)" },
        },
        x: {
          ticks: { color: "rgba(233, 238, 252, 0.75)" },
          grid: { color: "rgba(233, 238, 252, 0.08)" },
        },
      },
    },
  });
}

function makeBarChart(ctx, { labels, athletes, results }) {
  return new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Utøvere",
          data: athletes,
          backgroundColor: "rgba(122, 162, 255, 0.35)",
          borderColor: "rgba(122, 162, 255, 0.9)",
          borderWidth: 1,
        },
        {
          label: "Resultater",
          data: results,
          backgroundColor: "rgba(233, 238, 252, 0.12)",
          borderColor: "rgba(233, 238, 252, 0.4)",
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { position: "bottom", labels: { color: "rgba(233, 238, 252, 0.75)" } },
      },
      scales: {
        y: {
          ticks: { color: "rgba(233, 238, 252, 0.75)" },
          grid: { color: "rgba(233, 238, 252, 0.08)" },
        },
        x: {
          ticks: { color: "rgba(233, 238, 252, 0.75)" },
          grid: { color: "rgba(233, 238, 252, 0.08)" },
        },
      },
    },
  });
}

const DEFAULT_TREND_SELECTIONS = [
  { gender: "Men", event_no: "1500 meter" },
  { gender: "Women", event_no: "10 km gateløp" },
];

async function loadEvents({ preferredEventNo = null } = {}) {
  const genderSelect = $("gender");
  const eventSelect = $("event");

  const current = eventSelect.value;
  let preferred = preferredEventNo ?? current;

  if (preferredEventNo === null && !current) {
    const pick = DEFAULT_TREND_SELECTIONS[Math.floor(Math.random() * DEFAULT_TREND_SELECTIONS.length)];
    genderSelect.value = pick.gender;
    preferred = pick.event_no;
  }

  const gender = genderSelect.value;
  const events = await apiJson(`api/events?gender=${encodeURIComponent(gender)}`);

  const exists = preferred && events.some((e) => e.event_no === preferred);
  const selected = exists ? preferred : events[0]?.event_no || "";

  setOptions(
    eventSelect,
    events.map((e) => ({ value: e.event_no, label: e.event_no })),
    { selectedValue: selected },
  );
}

async function refreshTrend() {
  const gender = $("gender").value;
  const eventNo = $("event").value;
  const top = $("top").value;
  const rows = await apiJson(
    `api/event_trend?gender=${encodeURIComponent(gender)}&event=${encodeURIComponent(eventNo)}&top=${encodeURIComponent(top)}`,
  );

  if (!rows.length) {
    $("eventMeta").textContent = "Ingen data.";
    destroyCharts();
    return;
  }

  const labels = rows.map((r) => r.season);
  const points = rows.map((r) => (r.avg_points_top_n === null ? null : r.avg_points_top_n));
  const perf = rows.map((r) => (r.avg_value_top_n_perf === null ? null : r.avg_value_top_n_perf));
  const perfFmt = rows.map((r) => (r.avg_perf_top_n ? `Snitt resultat: ${r.avg_perf_top_n}` : "Snitt resultat: -"));
  const athletes = rows.map((r) => r.athletes_total);
  const results = rows.map((r) => r.results_total);

  $("eventMeta").textContent = formatEventMeta(rows);

  destroyCharts();

  pointsChart = makeLineChart($("pointsChart"), {
    labels,
    values: points,
    label: "Snitt WA-poeng",
    reverseY: false,
  });

  const reverseY = rows[0].orientation === "lower";
  perfChart = makeLineChart($("perfChart"), {
    labels,
    values: perf,
    label: "Snitt resultat (verdi)",
    reverseY,
    tooltipText: perfFmt,
    yTickFormatter: reverseY ? formatTimeNo : formatHigherPerfNo,
  });

  countChart = makeBarChart($("countChart"), { labels, athletes, results });
}

function compareNumbers(a, b, dir) {
  const aNull = a === null || a === undefined || Number.isNaN(a);
  const bNull = b === null || b === undefined || Number.isNaN(b);
  if (aNull && bNull) return 0;
  if (aNull) return 1;
  if (bNull) return -1;
  if (a < b) return dir === "asc" ? -1 : 1;
  if (a > b) return dir === "asc" ? 1 : -1;
  return 0;
}

function compareStrings(a, b, dir) {
  const sa = (a ?? "").toString();
  const sb = (b ?? "").toString();
  const cmp = sa.localeCompare(sb, "nb", { sensitivity: "base" });
  if (cmp === 0) return 0;
  return dir === "asc" ? cmp : -cmp;
}

function sortSeasonRows(rows, sortState) {
  const key = sortState?.key || "event";
  const dir = sortState?.dir || "asc";

  const out = [...rows];
  out.sort((a, b) => {
    if (key === "event") {
      const cmp = compareNumbers(toNumber(a.event_order), toNumber(b.event_order), dir);
      return cmp || compareStrings(a.event_no, b.event_no, "asc");
    }

    if (key === "wa_event") {
      const cmp = compareStrings(a.wa_event, b.wa_event, dir);
      return cmp || compareStrings(a.event_no, b.event_no, "asc");
    }

    if (key === "avg_value_top_n_perf") {
      const va = toNumber(a.avg_value_top_n_perf);
      const vb = toNumber(b.avg_value_top_n_perf);
      const ca = va === null ? null : a.orientation === "lower" ? va : -va;
      const cb = vb === null ? null : b.orientation === "lower" ? vb : -vb;
      const cmp = compareNumbers(ca, cb, dir);
      return cmp || compareStrings(a.event_no, b.event_no, "asc");
    }

    if (key === "avg_points_top_n") {
      const cmp = compareNumbers(toNumber(a.avg_points_top_n), toNumber(b.avg_points_top_n), dir);
      return cmp || compareStrings(a.event_no, b.event_no, "asc");
    }

    if (key === "athletes_total") {
      const cmp = compareNumbers(toNumber(a.athletes_total), toNumber(b.athletes_total), dir);
      return cmp || compareStrings(a.event_no, b.event_no, "asc");
    }

    if (key === "results_total") {
      const cmp = compareNumbers(toNumber(a.results_total), toNumber(b.results_total), dir);
      return cmp || compareStrings(a.event_no, b.event_no, "asc");
    }

    if (key === "points_available") {
      const cmp = compareNumbers(toNumber(a.points_available), toNumber(b.points_available), dir);
      return cmp || compareStrings(a.event_no, b.event_no, "asc");
    }

    return 0;
  });

  return out;
}

function renderSeasonTable(rows) {
  const tbody = $("seasonTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");

    const evTd = document.createElement("td");
    const evLink = document.createElement("a");
    evLink.href = "#";
    evLink.className = "tableLink seasonEventLink";
    evLink.dataset.season = String(r.season);
    evLink.dataset.gender = String(r.gender);
    evLink.dataset.event = String(r.event_no);
    evLink.textContent = fmt(r.event_no);
    evTd.appendChild(evLink);
    tr.appendChild(evTd);

    const waTd = document.createElement("td");
    waTd.textContent = fmt(r.wa_event || "");
    tr.appendChild(waTd);

    const ptsTd = document.createElement("td");
    ptsTd.textContent = r.avg_points_top_n === null ? "" : String(r.avg_points_top_n);
    tr.appendChild(ptsTd);

    const perfTd = document.createElement("td");
    perfTd.textContent = fmt(r.avg_perf_top_n || "");
    tr.appendChild(perfTd);

    const athTd = document.createElement("td");
    athTd.textContent = fmt(r.athletes_total);
    tr.appendChild(athTd);

    const resTd = document.createElement("td");
    resTd.className = "hideOnMobile";
    resTd.textContent = fmt(r.results_total);
    tr.appendChild(resTd);

    const availTd = document.createElement("td");
    availTd.textContent = fmt(r.points_available);
    tr.appendChild(availTd);
    tbody.appendChild(tr);
  }
}

function seasonSortFromDropdown() {
  const v = $("seasonSort").value;
  if (v === "points") return { key: "avg_points_top_n", dir: "desc" };
  if (v === "performance") return { key: "avg_value_top_n_perf", dir: "asc" };
  return { key: "event", dir: "asc" };
}

function defaultDirForSeasonKey(key) {
  if (key === "event" || key === "wa_event") return "asc";
  if (key === "avg_value_top_n_perf") return "asc";
  return "desc";
}

function setSeasonSortIndicator() {
  const headers = $("seasonTable").querySelectorAll("thead th[data-sort]");
  for (const th of headers) {
    th.classList.remove("sorted");
    th.removeAttribute("data-dir");
  }
  const active = $("seasonTable").querySelector(`thead th[data-sort="${seasonSortState.key}"]`);
  if (active) {
    active.classList.add("sorted");
    active.setAttribute("data-dir", seasonSortState.dir);
  }
}

function setupSeasonHeaderSorting() {
  const headers = $("seasonTable").querySelectorAll("thead th[data-sort]");
  for (const th of headers) {
    th.addEventListener("click", () => {
      const key = th.dataset.sort;
      if (!key) return;

      if (seasonSortState.key === key) {
        seasonSortState.dir = seasonSortState.dir === "asc" ? "desc" : "asc";
      } else {
        seasonSortState.key = key;
        seasonSortState.dir = defaultDirForSeasonKey(key);
      }

      seasonRows = sortSeasonRows(seasonRows, seasonSortState);
      renderSeasonTable(seasonRows);
      setSeasonSortIndicator();
    });
  }
}

async function refreshSeason() {
  const season = $("season").value;
  const gender = $("seasonGender").value;
  const top = $("seasonTop").value;
  const sort = $("seasonSort").value;
  const rows = await apiJson(
    `api/season_summary?season=${encodeURIComponent(season)}&gender=${encodeURIComponent(gender)}&top=${encodeURIComponent(top)}&sort=${encodeURIComponent(sort)}`,
  );

  seasonSortState = seasonSortFromDropdown();
  seasonRows = sortSeasonRows(rows, seasonSortState);
  renderSeasonTable(seasonRows);
  setSeasonSortIndicator();
}

async function loadResultsEvents({ preferredEventNo = null } = {}) {
  const gender = $("resultsGender").value;
  const events = await apiJson(`api/events?gender=${encodeURIComponent(gender)}`);

  const current = $("resultsEvent").value;
  const preferred = preferredEventNo ?? current;
  const exists = preferred && events.some((e) => e.event_no === preferred);
  const selected = exists ? preferred : events[0]?.event_no || "";

  setOptions(
    $("resultsEvent"),
    events.map((e) => ({ value: e.event_no, label: e.event_no })),
    { selectedValue: selected },
  );
}

function renderResultsTable(rows) {
  const tbody = $("resultsTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");

    const rankTd = document.createElement("td");
    rankTd.textContent = fmt(r.rank);
    tr.appendChild(rankTd);

    const nameTd = document.createElement("td");
    nameTd.textContent = fmt(r.athlete_name);
    tr.appendChild(nameTd);

    const clubTd = document.createElement("td");
    clubTd.textContent = fmt(r.club_name ?? "");
    tr.appendChild(clubTd);

    const perfTd = document.createElement("td");
    perfTd.textContent = fmt(r.performance_raw);
    tr.appendChild(perfTd);

    const waTd = document.createElement("td");
    waTd.textContent = fmt(r.wa_points ?? "");
    tr.appendChild(waTd);

    const dateTd = document.createElement("td");
    dateTd.textContent = fmt(r.result_date ?? "");
    tr.appendChild(dateTd);

    const compTd = document.createElement("td");
    compTd.textContent = fmt(r.competition_name ?? "");
    tr.appendChild(compTd);

    const cityTd = document.createElement("td");
    cityTd.textContent = fmt(r.venue_city ?? "");
    tr.appendChild(cityTd);

    const srcTd = document.createElement("td");
    if (r.source_url) {
      const a = document.createElement("a");
      a.href = String(r.source_url);
      a.target = "_blank";
      a.rel = "noreferrer";
      a.className = "tableLink";
      a.textContent = "Åpne";
      srcTd.appendChild(a);
    } else {
      srcTd.textContent = "";
    }
    tr.appendChild(srcTd);

    tbody.appendChild(tr);
  }
}

function setResultsInfo({ offset, limit, total, mode }) {
  const from = total <= 0 ? 0 : offset + 1;
  const to = total <= 0 ? 0 : Math.min(offset + limit, total);
  const modeLabel = mode === "all" ? "alle resultater" : "beste per utøver";
  $("resultsInfo").textContent = `Viser ${from}-${to} av ${total} (${modeLabel})`;

  $("resultsPrev").disabled = offset <= 0;
  $("resultsNext").disabled = offset + limit >= total;
}

async function refreshResults() {
  const season = $("resultsSeason").value;
  const gender = $("resultsGender").value;
  const eventNo = $("resultsEvent").value;
  const mode = $("resultsMode").value;
  const limit = parseInt($("resultsLimit").value, 10) || 100;

  if (!season || !gender || !eventNo) {
    renderResultsTable([]);
    $("resultsInfo").textContent = "Ingen data.";
    return;
  }

  let data;
  try {
    data = await apiJson(
      `api/event_results?season=${encodeURIComponent(season)}&gender=${encodeURIComponent(gender)}&event=${encodeURIComponent(eventNo)}&mode=${encodeURIComponent(mode)}&limit=${encodeURIComponent(limit)}&offset=${encodeURIComponent(resultsOffset)}`,
    );
  } catch (e) {
    renderResultsTable([]);
    $("resultsInfo").textContent = `Feil: ${e.message}`;
    $("resultsPrev").disabled = true;
    $("resultsNext").disabled = true;
    return;
  }

  resultsTotal = data.total ?? 0;

  // If the user changed filters, offset might end up out of range.
  if (resultsOffset >= resultsTotal && resultsTotal > 0) {
    resultsOffset = Math.max(0, resultsTotal - limit);
    return refreshResults();
  }

  renderResultsTable(data.rows || []);
  setResultsInfo({ offset: resultsOffset, limit, total: resultsTotal, mode: data.mode });
}

async function openEventResults({ season, gender, eventNo }) {
  if (season) $("resultsSeason").value = String(season);
  if (gender) $("resultsGender").value = String(gender);
  await loadResultsEvents({ preferredEventNo: eventNo });
  resultsOffset = 0;
  await refreshResults();

  const sec = document.getElementById("eventResultsSection");
  if (sec) {
    sec.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

async function searchAthlete() {
  const id = $("athleteId").value;
  if (!id) return;
  const since = $("athleteSince").value;
  const qs = since ? `&since=${encodeURIComponent(since)}` : "";
  const data = await apiJson(`api/athlete?id=${encodeURIComponent(id)}${qs}`);
  const tbody = $("athleteTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const r of data.rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmt(r.season)}</td>
      <td>${fmt(r.event_no)}</td>
      <td>${fmt(r.performance_raw)}</td>
      <td>${fmt(r.wa_points ?? "")}</td>
      <td>${fmt(r.result_date ?? "")}</td>
      <td>${fmt(r.competition_name ?? "")}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function init() {
  const meta = await apiJson("api/meta");

  setOptions(
    $("gender"),
    meta.genders.map((g) => ({ value: g, label: g })),
    { selectedValue: "Women" },
  );

  if (Array.isArray(meta.top_ns) && meta.top_ns.length) {
    const topNs = meta.top_ns.map((n) => String(n));
    const selectedTop = topNs.includes("10") ? "10" : topNs[0];
    setOptions(
      $("top"),
      topNs.map((n) => ({ value: n, label: n })),
      { selectedValue: selectedTop },
    );
    setOptions(
      $("seasonTop"),
      topNs.map((n) => ({ value: n, label: n })),
      { selectedValue: selectedTop },
    );
  }

  setOptions(
    $("season"),
    meta.seasons.map((s) => ({ value: s, label: s })),
    { selectedValue: meta.seasons[meta.seasons.length - 1] },
  );

  setOptions(
    $("resultsSeason"),
    meta.seasons.map((s) => ({ value: s, label: s })),
    { selectedValue: meta.seasons[meta.seasons.length - 1] },
  );

  await loadEvents();
  await refreshTrend();
  await refreshSeason();
  setupSeasonHeaderSorting();

  $("resultsGender").value = $("gender").value;
  await loadResultsEvents();
  await refreshResults();

  $("gender").addEventListener("change", async () => {
    const preferredEventNo = $("event").value;
    await loadEvents({ preferredEventNo });
    await refreshTrend();
  });
  $("refreshTrend").addEventListener("click", refreshTrend);
  $("refreshSeason").addEventListener("click", refreshSeason);

  $("seasonTable").addEventListener("click", async (ev) => {
    const a = ev.target?.closest?.("a.seasonEventLink");
    if (!a) return;
    ev.preventDefault();
    await openEventResults({ season: a.dataset.season, gender: a.dataset.gender, eventNo: a.dataset.event });
  });

  $("refreshResults").addEventListener("click", async () => {
    resultsOffset = 0;
    await refreshResults();
  });
  $("resultsPrev").addEventListener("click", async () => {
    const limit = parseInt($("resultsLimit").value, 10) || 100;
    resultsOffset = Math.max(0, resultsOffset - limit);
    await refreshResults();
  });
  $("resultsNext").addEventListener("click", async () => {
    const limit = parseInt($("resultsLimit").value, 10) || 100;
    if (resultsOffset + limit < resultsTotal) {
      resultsOffset += limit;
      await refreshResults();
    }
  });

  $("resultsSeason").addEventListener("change", async () => {
    resultsOffset = 0;
    await refreshResults();
  });
  $("resultsGender").addEventListener("change", async () => {
    const preferredEventNo = $("resultsEvent").value;
    await loadResultsEvents({ preferredEventNo });
    resultsOffset = 0;
    await refreshResults();
  });
  $("resultsEvent").addEventListener("change", async () => {
    resultsOffset = 0;
    await refreshResults();
  });
  $("resultsMode").addEventListener("change", async () => {
    resultsOffset = 0;
    await refreshResults();
  });
  $("resultsLimit").addEventListener("change", async () => {
    resultsOffset = 0;
    await refreshResults();
  });

  $("searchAthlete").addEventListener("click", searchAthlete);
}

init().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(e);
  $("eventMeta").textContent = `Feil: ${e.message}`;
});
