const form = document.getElementById("analyze-form");
const statusEl = document.getElementById("status");
const overviewEl = document.getElementById("overview");
const suggestionsEl = document.getElementById("suggestions");
const missingEl = document.getElementById("missing");
const distributionsEl = document.getElementById("distributions");
const histogramsEl = document.getElementById("histograms");
const anomaliesEl = document.getElementById("anomalies");
const previewEl = document.getElementById("preview");
const distLabelsEl = document.getElementById("dist-labels");
const labelDistEl = document.getElementById("label-dist");
const modelsEl = document.getElementById("models");
const aiEl = document.getElementById("ai");
const outliersEl = document.getElementById("outliers");
const rawEl = document.getElementById("raw");
const qualityEl = document.getElementById("quality-score");
const rootCauseEl = document.getElementById("root-cause");
const remediationEl = document.getElementById("remediation");
const exportCsvBtn = document.getElementById("export-csv");
const exportPdfBtn = document.getElementById("export-pdf");
const sampleBtn = document.getElementById("sample-data");
const aiBtn = document.getElementById("ai-generate");
const toggleRawBtn = document.getElementById("toggle-raw");
const transformBtn = document.getElementById("apply-transform");
const transformDownloadBtn = document.getElementById("download-transform");
const transformStatusEl = document.getElementById("transform-status");
const transformSummaryEl = document.getElementById("transform-summary");
const transformPreviewEl = document.getElementById("transform-preview");
const dropDuplicatesEl = document.getElementById("drop-duplicates");
const missingStrategyEl = document.getElementById("missing-strategy");
const fillValueEl = document.getElementById("fill-value");
const targetColumnsEl = document.getElementById("target-columns");
const normalizeTextEl = document.getElementById("normalize-text");
const capOutliersEl = document.getElementById("cap-outliers");
const transformPresetEl = document.getElementById("transform-preset");
const mlTargetEl = document.getElementById("ml-target");
const mlTimeEl = document.getElementById("ml-time");
const mlTestSizeEl = document.getElementById("ml-test-size");
const mlCvEl = document.getElementById("ml-cv");
const mlModelEls = document.querySelectorAll(".ml-model");
const mlRunBtn = document.getElementById("run-eda-ml");
const mlStatusEl = document.getElementById("ml-status");
const edaSummaryEl = document.getElementById("eda-summary");
const edaCorrEl = document.getElementById("eda-correlations");
const modelResultsEl = document.getElementById("model-results");
const featureImportanceEl = document.getElementById("feature-importance");
const pcaResultsEl = document.getElementById("pca-results");
const tsResultsEl = document.getElementById("ts-results");
const kpiGridEl = document.getElementById("kpi-grid");
const tabsEl = document.getElementById("tabs");
const unifiedChartColumnEl = document.getElementById("unified-chart-column");
const unifiedChartModeEl = document.getElementById("unified-chart-mode");
const unifiedChartCanvasEl = document.getElementById("unified-chart-canvas");
const unifiedChartEmptyEl = document.getElementById("unified-chart-empty");
const chatFormEl = document.getElementById("chat-form");
const chatInputEl = document.getElementById("chat-input");
const chatMessagesEl = document.getElementById("chat-messages");
const chatSendEl = document.getElementById("chat-send");
const chatQuickEl = document.getElementById("chat-quick");
const chatCollapseEl = document.getElementById("chat-collapse");
const chatOpenEl = document.getElementById("chat-open");
const chatOverlayEl = document.getElementById("chat-overlay");
const openContactModalEl = document.getElementById("open-contact-modal");
const closeContactModalEl = document.getElementById("close-contact-modal");
const contactModalEl = document.getElementById("contact-modal");
const contactFormEl = document.getElementById("contact-form");
const contactSubmitEl = document.getElementById("contact-submit");
const contactStatusEl = document.getElementById("contact-status");

let lastAnalysis = null;
let lastFile = null;
let lastTransformForm = null;
let rawFullText = "";
let rawExpanded = false;
let histEventsBound = false;
const histCharts = new Map();
let unifiedChart = null;
let unifiedHistograms = {};
let unifiedDistributions = {};
let unifiedOutliers = {};
let unifiedCorrelations = {};
let currentTab = "overview";
let chatHistory = [];

const isMobileViewport = () => window.matchMedia("(max-width: 720px)").matches;

const syncChatButtonLabel = () => {
  if (!chatCollapseEl) return;
  if (isMobileViewport()) {
    chatCollapseEl.textContent = "Close";
    return;
  }
  chatCollapseEl.textContent = document.body.classList.contains("chat-collapsed")
    ? "Expand"
    : "Collapse";
};

const openChatPane = () => {
  if (isMobileViewport()) {
    document.body.classList.add("chat-open-mobile");
    syncChatButtonLabel();
    return;
  }
  document.body.classList.remove("chat-collapsed");
  syncChatButtonLabel();
};

const closeChatPane = () => {
  if (isMobileViewport()) {
    document.body.classList.remove("chat-open-mobile");
    syncChatButtonLabel();
    return;
  }
  document.body.classList.add("chat-collapsed");
  syncChatButtonLabel();
};

const toggleChatPane = () => {
  if (isMobileViewport()) {
    document.body.classList.toggle("chat-open-mobile");
    syncChatButtonLabel();
    return;
  }
  if (document.body.classList.contains("chat-collapsed")) {
    openChatPane();
    return;
  }
  closeChatPane();
};

const closeContactModal = () => {
  if (!contactModalEl) return;
  contactModalEl.classList.remove("open");
  contactModalEl.setAttribute("aria-hidden", "true");
};

const openContactModal = () => {
  if (!contactModalEl) return;
  contactModalEl.classList.add("open");
  contactModalEl.setAttribute("aria-hidden", "false");
};

const setStatus = (text) => {
  statusEl.textContent = text;
};

const formatApiError = (payload, fallback) => {
  const message = payload?.error?.message || fallback;
  const code = payload?.error?.code ? ` (${payload.error.code})` : "";
  const details = payload?.error?.details ? ` - ${String(payload.error.details)}` : "";
  return `${message}${code}${details}`;
};

const escapeHtml = (value) =>
  String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const sanitizePayload = (value) => {
  if (typeof value === "string") return escapeHtml(value);
  if (Array.isArray(value)) return value.map((item) => sanitizePayload(item));
  if (value && typeof value === "object") {
    const clean = {};
    Object.entries(value).forEach(([key, val]) => {
      clean[key] = sanitizePayload(val);
    });
    return clean;
  }
  return value;
};

const setRawOutput = (data, maxLines = 10) => {
  const text = typeof data === "string" ? data : JSON.stringify(data, null, 2);
  rawFullText = text;
  rawExpanded = false;
  if (toggleRawBtn) {
    toggleRawBtn.textContent = "Show Full Output";
  }
  const lines = text.split("\n");
  if (lines.length <= maxLines) {
    rawEl.textContent = text;
    return;
  }
  rawEl.textContent = `${lines.slice(0, maxLines).join("\n")}\n... (${lines.length - maxLines} more lines)`;
};

const toggleRawOutput = () => {
  if (!rawFullText || !toggleRawBtn) return;
  rawExpanded = !rawExpanded;
  if (rawExpanded) {
    rawEl.textContent = rawFullText;
    toggleRawBtn.textContent = "Show Less";
  } else {
    setRawOutput(rawFullText);
  }
};

function destroyHistCharts() {
  histCharts.forEach((chart) => {
    chart.destroy();
  });
  histCharts.clear();
}

function destroyUnifiedChart() {
  if (unifiedChart) {
    unifiedChart.destroy();
    unifiedChart = null;
  }
}

const setTab = (tabName) => {
  currentTab = tabName;
  const cards = document.querySelectorAll(".results .card[data-tab]");
  cards.forEach((card) => {
    const show = card.getAttribute("data-tab") === tabName;
    if (show) {
      card.removeAttribute("hidden");
    } else {
      card.setAttribute("hidden", "hidden");
    }
  });
  if (!tabsEl) return;
  const buttons = tabsEl.querySelectorAll("[data-tab-btn]");
  buttons.forEach((btn) => {
    btn.classList.toggle("active", btn.getAttribute("data-tab-btn") === tabName);
  });
};

const initTabs = () => {
  if (!tabsEl) return;
  tabsEl.addEventListener("click", (event) => {
    const btn = event.target.closest("[data-tab-btn]");
    if (!btn) return;
    setTab(btn.getAttribute("data-tab-btn"));
  });
  setTab(currentTab);
};

const appendChatMessage = (role, content) => {
  if (!chatMessagesEl) return;
  const msg = document.createElement("div");
  msg.className = `chat-msg ${role}`;
  msg.textContent = content;
  chatMessagesEl.appendChild(msg);
  chatMessagesEl.scrollTop = chatMessagesEl.scrollHeight;
};

const addToChatHistory = (role, content) => {
  chatHistory.push({ role, content });
  if (chatHistory.length > 12) {
    chatHistory = chatHistory.slice(-12);
  }
};

const sendChatQuestion = async (question) => {
  if (!lastAnalysis) {
    appendChatMessage("bot", "Run an assessment first so I have dataset context.");
    return;
  }
  appendChatMessage("user", question);
  addToChatHistory("user", question);

  if (chatSendEl) chatSendEl.disabled = true;
  appendChatMessage("bot", "Thinking...");
  const pending = chatMessagesEl?.lastElementChild;

  try {
    const response = await fetch("/api/chat-assistant", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        analysis: lastAnalysis,
        question,
        history: chatHistory,
      }),
    });
    const data = await response.json();
    const answer =
      response.ok ? data.answer : `Error: ${formatApiError(data, "Chat request failed")}`;
    if (pending) pending.remove();
    appendChatMessage("bot", answer);
    addToChatHistory("assistant", answer);
  } catch (error) {
    if (pending) pending.remove();
    appendChatMessage("bot", `Network error: ${String(error)}`);
  } finally {
    if (chatSendEl) chatSendEl.disabled = false;
  }
};

const renderKpis = (data) => {
  if (!kpiGridEl) return;
  const quality = data?.data_quality_score?.score ?? "-";
  const severity = data?.root_cause_blast_radius?.risk_severity ?? "-";
  const missing = data?.data_quality_score?.missing_rate ?? "-";
  const rows = data?.rows ?? "-";
  const columns = data?.columns ?? "-";
  const duplicates = data?.duplicates ?? "-";
  const kpis = [
    { label: "Quality Score", value: quality },
    { label: "Risk Severity", value: severity },
    { label: "Rows", value: rows },
    { label: "Columns", value: columns },
    { label: "Duplicate Rows", value: duplicates },
    { label: "Missing Rate", value: missing },
  ];
  kpiGridEl.innerHTML = kpis
    .map(
      (kpi) =>
        `<div class="kpi-card"><div class="label">${kpi.label}</div><div class="value">${kpi.value}</div></div>`
    )
    .join("");
};

const renderUnifiedChart = () => {
  if (!unifiedChartColumnEl || !unifiedChartModeEl || !unifiedChartCanvasEl) return;
  destroyUnifiedChart();
  if (!window.Chart) {
    if (unifiedChartEmptyEl) unifiedChartEmptyEl.textContent = "Chart library not available.";
    return;
  }

  const col = unifiedChartColumnEl.value;
  const mode = unifiedChartModeEl.value;
  const ctx = unifiedChartCanvasEl.getContext("2d");

  if (mode === "correlation") {
    const cols = Object.keys(unifiedCorrelations || {});
    if (cols.length < 2) {
      if (unifiedChartEmptyEl) {
        unifiedChartEmptyEl.textContent = "Need at least 2 numeric columns for correlation plot.";
      }
      return;
    }

    const points = [];
    cols.forEach((rowCol, rowIdx) => {
      cols.forEach((colName, colIdx) => {
        const value = unifiedCorrelations?.[rowCol]?.[colName];
        if (typeof value !== "number" || Number.isNaN(value)) return;
        points.push({
          x: colIdx,
          y: rowIdx,
          r: Math.max(4, Math.round(Math.abs(value) * 16)),
          v: value,
        });
      });
    });

    if (!points.length) {
      if (unifiedChartEmptyEl) {
        unifiedChartEmptyEl.textContent = "No valid correlation values available.";
      }
      return;
    }

    if (unifiedChartEmptyEl) unifiedChartEmptyEl.textContent = "Correlation plot (numeric columns)";
    unifiedChart = new Chart(ctx, {
      type: "bubble",
      data: {
        datasets: [
          {
            label: "Correlation",
            data: points,
            backgroundColor: (context) => {
              const v = context.raw?.v ?? 0;
              return v >= 0 ? "rgba(26, 111, 255, 0.5)" : "rgba(245, 159, 0, 0.5)";
            },
            borderColor: (context) => {
              const v = context.raw?.v ?? 0;
              return v >= 0 ? "rgba(26, 111, 255, 0.9)" : "rgba(245, 159, 0, 0.9)";
            },
            borderWidth: 1,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => {
                const raw = context.raw || {};
                const xName = cols[raw.x] ?? "-";
                const yName = cols[raw.y] ?? "-";
                const v = typeof raw.v === "number" ? raw.v.toFixed(3) : "-";
                return `${yName} vs ${xName}: ${v}`;
              },
            },
          },
        },
        scales: {
          x: {
            min: -0.5,
            max: cols.length - 0.5,
            ticks: {
              stepSize: 1,
              callback: (value) => cols[value] ?? "",
              color: "#64748b",
              maxRotation: 45,
              minRotation: 45,
            },
            grid: { color: "rgba(148, 163, 184, 0.2)" },
          },
          y: {
            min: -0.5,
            max: cols.length - 0.5,
            reverse: true,
            ticks: {
              stepSize: 1,
              callback: (value) => cols[value] ?? "",
              color: "#64748b",
            },
            grid: { color: "rgba(148, 163, 184, 0.2)" },
          },
        },
      },
    });
    return;
  }

  const bins = unifiedHistograms[col] || [];
  if (!bins.length) {
    if (unifiedChartEmptyEl) unifiedChartEmptyEl.textContent = "No numeric distributions available yet.";
    return;
  }

  const labels = bins.map((bin) => `${bin.min ?? "-"}-${bin.max ?? "-"}`);
  const counts = bins.map((bin) => bin.count || 0);
  const baseOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false } },
  };

  if (mode === "pie") {
    if (unifiedChartEmptyEl) unifiedChartEmptyEl.textContent = `${col} distribution (pie)`;
    unifiedChart = new Chart(ctx, {
      type: "pie",
      data: {
        labels,
        datasets: [
          {
            data: counts,
            backgroundColor: labels.map((_, idx) => `hsl(${(idx * 37) % 360} 75% 55% / 0.8)`),
            borderColor: "rgba(255,255,255,0.8)",
            borderWidth: 1,
          },
        ],
      },
      options: {
        ...baseOptions,
        plugins: {
          ...baseOptions.plugins,
          legend: { display: true, position: "bottom" },
        },
      },
    });
    return;
  }

  if (mode === "stacked") {
    const total = Number(unifiedDistributions?.[col]?.count ?? counts.reduce((sum, n) => sum + n, 0));
    const outlierCount = Number(unifiedOutliers?.[col]?.iqr ?? 0);
    const cleanCount = Math.max(total - outlierCount, 0);
    if (unifiedChartEmptyEl) unifiedChartEmptyEl.textContent = `${col} clean vs outlier rows (IQR)`;
    unifiedChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: [col],
        datasets: [
          {
            label: "Clean",
            data: [cleanCount],
            backgroundColor: "rgba(18, 184, 134, 0.7)",
            stack: "quality",
          },
          {
            label: "Outliers (IQR)",
            data: [outlierCount],
            backgroundColor: "rgba(245, 159, 0, 0.75)",
            stack: "quality",
          },
        ],
      },
      options: {
        ...baseOptions,
        plugins: {
          ...baseOptions.plugins,
          legend: { display: true, position: "bottom" },
        },
        scales: {
          x: { stacked: true, ticks: { color: "#64748b" } },
          y: { stacked: true, ticks: { color: "#64748b" } },
        },
      },
    });
    return;
  }

  if (mode === "line") {
    if (unifiedChartEmptyEl) unifiedChartEmptyEl.textContent = `${col} distribution (line)`;
    unifiedChart = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Count",
            data: counts,
            borderColor: "#1a6fff",
            backgroundColor: "rgba(26, 111, 255, 0.2)",
            fill: false,
            tension: 0.25,
            pointRadius: 2,
          },
        ],
      },
      options: {
        ...baseOptions,
        scales: {
          x: { ticks: { maxRotation: 0, autoSkip: true, color: "#64748b" } },
          y: { ticks: { color: "#64748b" } },
        },
      },
    });
    return;
  }

  if (unifiedChartEmptyEl) {
    unifiedChartEmptyEl.textContent =
      mode === "density" ? `${col} distribution (density)` : `${col} distribution`;
  }
  unifiedChart = new Chart(ctx, {
    type: mode === "density" ? "line" : "bar",
    data: {
      labels,
      datasets: [
        {
          label: mode === "density" ? "Density" : "Count",
          data: counts,
          borderColor: mode === "density" ? "#f59f00" : "rgba(26, 111, 255, 0.75)",
          backgroundColor:
            mode === "density" ? "rgba(245, 159, 0, 0.25)" : "rgba(26, 111, 255, 0.55)",
          fill: mode === "density",
          tension: 0.35,
          borderRadius: mode === "density" ? 0 : 6,
          pointRadius: mode === "density" ? 2 : 0,
        },
      ],
    },
    options: {
      ...baseOptions,
      scales: {
        x: { ticks: { maxRotation: 0, autoSkip: true, color: "#64748b" } },
        y: { ticks: { color: "#64748b" } },
      },
    },
  });
};

const syncUnifiedChartColumns = (
  histograms,
  distributions = {},
  outliers = {},
  correlations = {}
) => {
  unifiedHistograms = histograms || {};
  unifiedDistributions = distributions || {};
  unifiedOutliers = outliers || {};
  unifiedCorrelations = correlations || {};
  if (!unifiedChartColumnEl) return;
  const cols = Object.keys(unifiedHistograms);
  unifiedChartColumnEl.innerHTML = cols
    .map((col) => `<option value="${col}">${col}</option>`)
    .join("");
  if (!cols.length) {
    destroyUnifiedChart();
    if (unifiedChartEmptyEl) unifiedChartEmptyEl.textContent = "No numeric distributions available yet.";
    return;
  }
  if (!cols.includes(unifiedChartColumnEl.value)) {
    unifiedChartColumnEl.value = cols[0];
  }
  renderUnifiedChart();
};

const clearResults = () => {
  overviewEl.innerHTML = "";
  suggestionsEl.innerHTML = "";
  missingEl.innerHTML = "";
  distributionsEl.innerHTML = "";
  histogramsEl.innerHTML = "";
  anomaliesEl.innerHTML = "";
  previewEl.innerHTML = "";
  distLabelsEl.innerHTML = "";
  labelDistEl.innerHTML = "";
  modelsEl.innerHTML = "";
  aiEl.innerHTML = "";
  outliersEl.innerHTML = "";
  qualityEl.innerHTML = "";
  rootCauseEl.innerHTML = "";
  remediationEl.innerHTML = "";
  rawEl.textContent = "";
  exportCsvBtn.disabled = true;
  exportPdfBtn.disabled = true;
  aiBtn.disabled = true;
  transformSummaryEl.innerHTML = "";
  transformPreviewEl.innerHTML = "";
  transformStatusEl.textContent = "";
  transformDownloadBtn.disabled = true;
  rawFullText = "";
  rawExpanded = false;
  if (toggleRawBtn) {
    toggleRawBtn.textContent = "Show Full Output";
  }
  destroyHistCharts();
  destroyUnifiedChart();
  unifiedHistograms = {};
  unifiedDistributions = {};
  unifiedOutliers = {};
  unifiedCorrelations = {};
  if (kpiGridEl) kpiGridEl.innerHTML = "";
  if (unifiedChartColumnEl) unifiedChartColumnEl.innerHTML = "";
  if (unifiedChartEmptyEl) unifiedChartEmptyEl.textContent = "No numeric distributions available yet.";
  mlStatusEl.textContent = "";
  edaSummaryEl.innerHTML = "";
  edaCorrEl.innerHTML = "";
  modelResultsEl.innerHTML = "";
  featureImportanceEl.innerHTML = "";
  pcaResultsEl.innerHTML = "";
  tsResultsEl.innerHTML = "";
};

const showSkeletons = () => {
  const skeletonLine = '<div class="skeleton"></div>';
  const skeletonBlock = '<div class="skeleton block"></div>';
  overviewEl.innerHTML = skeletonLine.repeat(6);
  suggestionsEl.innerHTML = skeletonLine.repeat(4);
  missingEl.innerHTML = skeletonLine.repeat(4);
  anomaliesEl.innerHTML = skeletonLine.repeat(4);
  distributionsEl.innerHTML = skeletonBlock;
  histogramsEl.innerHTML = skeletonBlock;
  outliersEl.innerHTML = skeletonLine.repeat(4);
  previewEl.innerHTML = skeletonBlock;
  distLabelsEl.innerHTML = skeletonLine.repeat(4);
  labelDistEl.innerHTML = skeletonLine.repeat(4);
  modelsEl.innerHTML = skeletonLine.repeat(3);
  aiEl.innerHTML = skeletonBlock;
  statusEl.innerHTML = 'Analyzing...<div class="progress"><span></span></div>';
};

const renderOverview = (data) => {
  const items = [
    { label: "File", value: data.file_name || "-" },
    { label: "Nature", value: data.nature || "-" },
    { label: "Type", value: data.data_type || "-" },
    { label: "Rows", value: data.rows ?? "-" },
    { label: "Columns", value: data.columns ?? "-" },
    { label: "Duplicate Rows", value: data.duplicates ?? 0 },
    { label: "Grain", value: data.grain || "-" },
    { label: "Analysis Fit", value: data.analysis_fit || "-" },
  ];

  overviewEl.innerHTML = items
    .map(
      (item) =>
        `<div class="metric"><strong>${item.label}</strong><div>${item.value}</div></div>`
    )
    .join("");
};

const renderSuggestions = (suggestions) => {
  suggestionsEl.innerHTML = suggestions
    .map((item) => `<li>${item}</li>`)
    .join("");
};

const renderQualityScore = (quality) => {
  if (!quality) {
    qualityEl.innerHTML = "No quality score available.";
    return;
  }

  const toNumber = (value) => {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  };
  const asPercent = (value) => `${(toNumber(value) * 100).toFixed(1)}%`;

  const score = Math.round(toNumber(quality.score));
  const width = Math.min(Math.max(score, 0), 100);
  const grade =
    score >= 90 ? "A" : score >= 80 ? "B" : score >= 70 ? "C" : score >= 60 ? "D" : "F";
  const health =
    score >= 85 ? "Healthy" : score >= 70 ? "Watch" : score >= 55 ? "At Risk" : "Critical";

  const dims = [
    { key: "missing_rate", label: "Missingness", weightKey: "missing" },
    { key: "duplicate_rate", label: "Duplicates", weightKey: "duplicates" },
    { key: "outlier_rate", label: "Outliers", weightKey: "outliers" },
    { key: "schema_drift_rate", label: "Schema drift", weightKey: "schema_drift" },
  ];

  const hasWeights = Object.keys(quality.weights || {}).length > 0;
  let totalPenalty = 0;
  const breakdownRows = dims
    .map((d) => {
      const rate = toNumber(quality[d.key]);
      const weight = toNumber((quality.weights || {})[d.weightKey]);
      const penalty = rate * weight * 100;
      totalPenalty += penalty;
      return `
        <div class="quality-breakdown-row">
          <span>${d.label}</span>
          <span>${asPercent(rate)}</span>
          <span>${asPercent(weight)}</span>
          <span>${penalty.toFixed(1)}</span>
        </div>
      `;
    })
    .join("");

  const weightingBlock = hasWeights
    ? `
      <div class="quality-weights">
        <h3>Score Formula & Weighting</h3>
        <p class="quality-caption">Score starts at 100 and subtracts weighted penalty points per quality check.</p>
        <div class="quality-breakdown">
          <div class="quality-breakdown-head">
            <span>Check</span>
            <span>Observed</span>
            <span>Weight</span>
            <span>Penalty</span>
          </div>
          ${breakdownRows}
        </div>
        <div class="quality-breakdown-foot">
          <span>Total weighted penalty</span>
          <strong>${totalPenalty.toFixed(1)} points</strong>
        </div>
      </div>
    `
    : "";

  qualityEl.innerHTML = `
    <div class="quality-top">
      <div>
        <div class="quality-score">${score}</div>
        <div class="quality-sub">Overall quality score</div>
      </div>
      <div class="quality-grade-wrap">
        <div class="quality-grade">${grade}</div>
        <div class="quality-pill ${health.toLowerCase().replace(/\s+/g, "-")}">${health}</div>
      </div>
    </div>
    <div class="quality-bar"><span style="width: ${width}%"></span></div>
    <div class="quality-metric-grid">
      <div class="quality-metric"><span>Missing rate</span><strong>${asPercent(quality.missing_rate)}</strong></div>
      <div class="quality-metric"><span>Duplicate rate</span><strong>${asPercent(quality.duplicate_rate)}</strong></div>
      <div class="quality-metric"><span>Outlier rate</span><strong>${asPercent(quality.outlier_rate)}</strong></div>
      <div class="quality-metric"><span>Schema drift rate</span><strong>${asPercent(quality.schema_drift_rate)}</strong></div>
    </div>
    ${weightingBlock}
  `;
};


const renderRootCause = (root) => {
  if (!root) {
    rootCauseEl.innerHTML = "No root cause analysis available.";
    return;
  }

  const severity = (root.risk_severity || "n/a").toLowerCase();
  const severityClass = `severity-${severity.replace(/\s+/g, "-")}`;

  const rows = [
    [
      "Top missing columns",
      (root.top_missing_columns || []).map(([col, count]) => `${col}: ${count}`).join(", ") || "None",
    ],
    ["Outlier columns", (root.outlier_columns || []).join(", ") || "None"],
    ["Impacted KPIs", (root.impacted_kpis || []).join(", ") || "None"],
    ["Root cause hypotheses", (root.root_cause_hypotheses || []).join("; ") || "None"],
  ];

  rootCauseEl.innerHTML = `
    <div class="severity-row">
      <span>Risk severity</span>
      <span class="severity-pill ${severityClass}">${severity}</span>
    </div>
    ${rows
      .map(([label, value]) => `<div class="table-row"><span>${label}</span><span>${value}</span></div>`)
      .join("")}
  `;
};


const renderRemediation = (plan) => {
  if (!plan) {
    remediationEl.innerHTML = "No remediation plan available.";
    return;
  }

  const steps = (plan.steps || [])
    .map((step, idx) => `<li><strong>Step ${idx + 1}:</strong> ${step}</li>`)
    .join("");

  const actions = (plan.one_click_actions || [])
    .map((action) => `<span class="pill">${action}</span>`)
    .join(" ");

  remediationEl.innerHTML = `
    <div class="remediation-wrap">
      <h3>Execution Order</h3>
      <ol class="remediation-list">${steps || "<li>No remediation needed.</li>"}</ol>
      <h3>One-click actions</h3>
      <div class="remediation-actions">${actions || "<span>None</span>"}</div>
    </div>
  `;
};


const renderMissing = (missing) => {
  const entries = Object.entries(missing || {});
  if (entries.length === 0) {
    missingEl.innerHTML = "No missing values detected or not applicable.";
    return;
  }
  missingEl.innerHTML = entries
    .map(
      ([key, value]) =>
        `<div class="table-row"><span>${key}</span><span>${value}</span></div>`
    )
    .join("");
};

const renderDistributions = (distributions) => {
  const entries = Object.entries(distributions || {});
  if (entries.length === 0) {
    distributionsEl.innerHTML = "No numeric distributions found.";
    return;
  }

  distributionsEl.innerHTML = entries
    .map(([col, stats]) => {
      const rows = Object.entries(stats)
        .map(
          ([key, value]) =>
            `<div class="table-row"><span>${key}</span><span>${
              value ?? "-"
            }</span></div>`
        )
        .join("");
      return `<div class="dist-card"><strong>${col}</strong><div class="table">${rows}</div></div>`;
    })
    .join("");
};

const renderPreview = (rows) => {
  if (!rows || rows.length === 0) {
    previewEl.innerHTML = "No preview available.";
    return;
  }
  const columns = Object.keys(rows[0]);
  const gridTemplate = `repeat(${columns.length}, minmax(120px, 1fr))`;
  const header = `
    <div class="preview-row" style="grid-template-columns: ${gridTemplate}">
      ${columns.map((c) => `<strong>${c}</strong>`).join("")}
    </div>
  `;
  const body = rows
    .map(
      (row) => `
      <div class="preview-row" style="grid-template-columns: ${gridTemplate}">
        ${columns.map((c) => `<span>${row[c] ?? ""}</span>`).join("")}
      </div>
    `
    )
    .join("");
  previewEl.innerHTML = `<div class="preview-grid">${header}${body}</div>`;
};

const renderTransformPreview = (rows) => {
  if (!rows || rows.length === 0) {
    transformPreviewEl.innerHTML = "No transformed preview available.";
    return;
  }
  const columns = Object.keys(rows[0]);
  const gridTemplate = `repeat(${columns.length}, minmax(120px, 1fr))`;
  const header = `
    <div class="preview-row" style="grid-template-columns: ${gridTemplate}">
      ${columns.map((c) => `<strong>${c}</strong>`).join("")}
    </div>
  `;
  const body = rows
    .map(
      (row) => `
      <div class="preview-row" style="grid-template-columns: ${gridTemplate}">
        ${columns.map((c) => `<span>${row[c] ?? ""}</span>`).join("")}
      </div>
    `
    )
    .join("");
  transformPreviewEl.innerHTML = `<div class="preview-grid">${header}${body}</div>`;
};

const renderTransformSummary = (summary) => {
  if (!summary) {
    transformSummaryEl.innerHTML = "No transform summary available.";
    return;
  }
  const items = [
    ["Rows before", summary.rows_before],
    ["Rows after", summary.rows_after],
    ["Missing before", summary.missing_before],
    ["Missing after", summary.missing_after],
    ["Duplicates before", summary.duplicates_before],
    ["Duplicates after", summary.duplicates_after],
    ["Duplicates removed", summary.duplicates_removed],
  ];
  transformSummaryEl.innerHTML = items
    .map(
      ([label, value]) =>
        `<div class="table-row"><span>${label}</span><span>${value}</span></div>`
    )
    .join("");
};

const renderEDASummary = (eda) => {
  if (!eda) {
    edaSummaryEl.innerHTML = "No EDA summary available.";
    return;
  }
  const items = [
    ["Rows used", eda.rows],
    ["Columns", eda.columns],
    ["Numeric columns (sample)", (eda.numeric_columns || []).join(", ") || "None"],
  ];
  if (eda.target_balance && Object.keys(eda.target_balance).length > 0) {
    items.push([
      "Target balance",
      Object.entries(eda.target_balance)
        .map(([k, v]) => `${k}: ${v}`)
        .join(", "),
    ]);
  }
  edaSummaryEl.innerHTML = items
    .map(
      ([label, value]) =>
        `<div class="table-row"><span>${label}</span><span>${value}</span></div>`
    )
    .join("");
};

const renderCorrelations = (correlations) => {
  if (!correlations || correlations.length === 0) {
    edaCorrEl.innerHTML = "No correlations computed.";
    return;
  }
  edaCorrEl.innerHTML = correlations
    .map(
      (item) =>
        `<div class="table-row"><span>${item.col1} ↔ ${item.col2}</span><span>${item.corr}</span></div>`
    )
    .join("");
};

const renderModelResults = (models) => {
  if (!models || models.status === "skipped") {
    modelResultsEl.innerHTML = models?.reason || "Models skipped.";
    return;
  }
  const rows = (models.models || []).map((model) => {
    const metrics = Object.entries(model)
      .filter(([key]) => key !== "name")
      .map(([key, value]) => `${key}: ${value}`)
      .join(", ");
    return `<div class="table-row"><span>${model.name}</span><span>${metrics}</span></div>`;
  });
  if (models.xgboost === "not_installed") {
    rows.push(
      `<div class="table-row"><span>XGBoost</span><span>Not installed</span></div>`
    );
  }
  modelResultsEl.innerHTML = rows.join("");
};

const renderFeatureImportance = (models) => {
  if (!models || !models.models || models.models.length === 0) {
    featureImportanceEl.innerHTML = "No feature importance available.";
    return;
  }
  featureImportanceEl.innerHTML = models.models
    .map((model) => {
      const items = model.feature_importance || [];
      if (!items.length) {
        return `<div class="histogram-card"><div class="histogram-title">${model.name}</div><div>No importances computed.</div></div>`;
      }
      const maxVal = Math.max(...items.map((i) => i.importance || 0), 1);
      const bars = items
        .map(
          (item) => `
          <div class="bar-row">
            <span>${item.feature}</span>
            <div class="bar" style="width: ${Math.round(
              (item.importance / maxVal) * 100
            )}%"></div>
            <span>${item.importance}</span>
          </div>
        `
        )
        .join("");
      return `
        <div class="histogram-card">
          <div class="histogram-title">${model.name}</div>
          <div class="histogram-bars">${bars}</div>
        </div>
      `;
    })
    .join("");
};

const renderPCA = (pca) => {
  if (!pca || pca.status !== "ok") {
    pcaResultsEl.innerHTML = pca?.reason || "PCA skipped.";
    return;
  }
  pcaResultsEl.innerHTML = `<div class="table-row"><span>Explained variance</span><span>${(pca.explained_variance_ratio || []).join(
    ", "
  )}</span></div>`;
};

const renderTimeSeries = (ts) => {
  if (!ts || ts.status !== "ok") {
    tsResultsEl.innerHTML = ts?.reason || "Time series skipped.";
    return;
  }
  tsResultsEl.innerHTML = `
    <div class="table-row"><span>Naive MAE</span><span>${ts.naive?.mae}</span></div>
    <div class="table-row"><span>Naive RMSE</span><span>${ts.naive?.rmse}</span></div>
    <div class="table-row"><span>Rolling MAE</span><span>${ts.rolling_mean?.mae}</span></div>
    <div class="table-row"><span>Rolling RMSE</span><span>${ts.rolling_mean?.rmse}</span></div>
  `;
};

const renderHistograms = (histograms, labels = {}, outliers = {}, distributions = {}, correlations = {}) => {
  const entries = Object.entries(histograms || {});
  syncUnifiedChartColumns(
    histograms || {},
    distributions || {},
    outliers || {},
    correlations || {}
  );
  if (entries.length === 0) {
    histogramsEl.innerHTML = "No histograms available.";
    return;
  }

  destroyHistCharts();

  histogramsEl.innerHTML = entries
    .map(([col, bins], idx) => {
      if (!bins || bins.length === 0) {
        return `<div class="histogram-card"><div class="histogram-title">${col}</div><div>No data.</div></div>`;
      }
      const safeId = `hist-${idx}-${col.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}`;
      const minVal = bins[0]?.min ?? "-";
      const maxVal = bins[bins.length - 1]?.max ?? "-";
      const total = bins.reduce((sum, b) => sum + (b.count || 0), 0);
      const distLabel = labels[col] || "unknown";
      const out = outliers[col]
        ? `IQR ${outliers[col].iqr ?? 0}, Z ${outliers[col].zscore ?? 0}, MAD ${outliers[col].mad ?? 0}`
        : "n/a";
      const tableRows = bins
        .map((bin) => {
          const label = `${bin.min ?? "-"} to ${bin.max ?? "-"}`;
          return `
            <div class="bar-row">
              <span>${label}</span>
              <div class="bar" style="width: ${Math.round(
                ((bin.count || 0) / Math.max(...bins.map((b) => b.count || 0), 1)) * 100
              )}%"></div>
              <span>${bin.count ?? 0}</span>
            </div>
          `;
        })
        .join("");
      return `
        <div class="histogram-card" id="${safeId}">
          <div class="histogram-header">
            <div class="histogram-title">${col}</div>
            <div class="chart-toolbar">
              <button class="chart-toggle active" data-target="${safeId}" data-view="bar">Bars</button>
              <button class="chart-toggle" data-target="${safeId}" data-view="density">Density</button>
              <button class="secondary small toggle-bins" data-target="${safeId}">Show bin details</button>
            </div>
          </div>
          <div class="histogram-meta">
            <span class="pill">Range: ${minVal} to ${maxVal}</span>
            <span class="pill">Total: ${total}</span>
            <span class="pill">Distribution: ${distLabel}</span>
            <span class="pill">Outliers: ${out}</span>
          </div>
          <div class="chart-shell">
            <canvas id="${safeId}-canvas" class="chart-canvas"></canvas>
          </div>
          <div class="chart-tooltip">Hover the chart for details.</div>
          <div class="histogram-bars bin-table hidden" id="${safeId}-bins">${tableRows}</div>
        </div>
      `;
    })
    .join("");

  entries.forEach(([col, bins], idx) => {
    if (!bins || bins.length === 0) return;
    const safeId = `hist-${idx}-${col.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}`;
    const canvas = document.getElementById(`${safeId}-canvas`);
    if (!canvas || !window.Chart) return;
    const ctx = canvas.getContext("2d");
    const xLabels = bins.map((bin) => `${bin.min ?? "-"}–${bin.max ?? "-"}`);
    const counts = bins.map((bin) => bin.count || 0);

    const chart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: xLabels,
        datasets: [
          {
            type: "bar",
            label: "Count",
            data: counts,
            backgroundColor: "rgba(26, 111, 255, 0.55)",
            borderRadius: 6,
            borderSkipped: false,
          },
          {
            type: "line",
            label: "Density",
            data: counts,
            borderColor: "#f59f00",
            backgroundColor: "rgba(245, 159, 0, 0.25)",
            tension: 0.35,
            fill: true,
            pointRadius: 0,
            hidden: true,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "rgba(15, 23, 42, 0.9)",
            titleColor: "#fff",
            bodyColor: "#e2e8f0",
          },
        },
        scales: {
          x: {
            ticks: {
              maxRotation: 0,
              autoSkip: true,
              color: "#64748b",
            },
            grid: {
              color: "rgba(148, 163, 184, 0.2)",
            },
          },
          y: {
            ticks: { color: "#64748b" },
            grid: { color: "rgba(148, 163, 184, 0.2)" },
            title: {
              display: true,
              text: "Count",
              color: "#64748b",
            },
          },
        },
      },
    });

    histCharts.set(safeId, chart);
  });

  if (!histEventsBound) {
    histEventsBound = true;
    histogramsEl.addEventListener("click", (event) => {
      const btn = event.target.closest(".toggle-bins");
      if (btn) {
        const target = btn.getAttribute("data-target");
        const table = document.getElementById(`${target}-bins`);
        if (!table) return;
        table.classList.toggle("hidden");
        btn.textContent = table.classList.contains("hidden")
          ? "Show bin details"
          : "Hide bin details";
        return;
      }
      const toggle = event.target.closest(".chart-toggle");
      if (!toggle) return;
      const target = toggle.getAttribute("data-target");
      const view = toggle.getAttribute("data-view");
      const chart = histCharts.get(target);
      if (!chart) return;
      const toggles = toggle.closest(".chart-toolbar")?.querySelectorAll(".chart-toggle");
      if (toggles) {
        toggles.forEach((t) => t.classList.remove("active"));
      }
      toggle.classList.add("active");
      chart.setDatasetVisibility(0, view === "bar");
      chart.setDatasetVisibility(1, view === "density");
      chart.update();
    });
  }
};

const renderAnomalies = (profiles) => {
  const entries = Object.entries(profiles || {});
  if (entries.length === 0) {
    anomaliesEl.innerHTML = "No anomaly data available.";
    return;
  }
  anomaliesEl.innerHTML = entries
    .map(([col, info]) => {
      const issues = (info.anomalies || []).join(", ") || "None";
      return `<div class="table-row"><span>${col} (${info.type})</span><span>${issues}</span></div>`;
    })
    .join("");
};

const renderOutliers = (outliers) => {
  const entries = Object.entries(outliers || {});
  if (entries.length === 0) {
    outliersEl.innerHTML = "No outlier summary available.";
    return;
  }
  outliersEl.innerHTML = entries
    .map(([col, stats]) => {
      const label = `IQR: ${stats.iqr ?? 0}, Z: ${stats.zscore ?? 0}, MAD: ${
        stats.mad ?? 0
      }`;
      return `<div class="table-row"><span>${col}</span><span>${label}</span></div>`;
    })
    .join("");
};

const renderDistributionLabels = (labels) => {
  const entries = Object.entries(labels || {});
  if (entries.length === 0) {
    distLabelsEl.innerHTML = "No distribution labels available.";
    return;
  }
  distLabelsEl.innerHTML = entries
    .map(
      ([col, label]) =>
        `<div class="table-row"><span>${col}</span><span>${label}</span></div>`
    )
    .join("");
};

const renderLabelDistribution = (labels) => {
  const entries = Object.entries(labels || {});
  if (entries.length === 0) {
    labelDistEl.innerHTML = "No label distribution provided.";
    return;
  }
  labelDistEl.innerHTML = entries
    .map(
      ([label, count]) =>
        `<div class="table-row"><span>${label}</span><span>${count}</span></div>`
    )
    .join("");
};

const renderModels = (models) => {
  modelsEl.innerHTML = (models || [])
    .map((item) => `<li>${item}</li>`)
    .join("");
};

const renderAi = (ai) => {
  if (!ai) {
    aiEl.innerHTML = "No AI insights available.";
    return;
  }
  if (ai.ai_text) {
    aiEl.innerHTML = `<div class="ai-output">${ai.ai_text}</div>`;
    return;
  }
  aiEl.innerHTML = `
      <div><strong>Status:</strong> ${ai.status}</div>
      <div>${ai.message}</div>
      <div class="table">
        ${(ai.next_steps || [])
          .map((step) => `<div class="table-row"><span>${step}</span></div>`)
          .join("")}
      </div>
    `;
};

const downloadReport = async (type) => {
  if (!lastAnalysis) return;
  const response = await fetch(`/api/report/${type}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ analysis: lastAnalysis }),
  });

  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    setStatus(`Export failed: ${data.error || response.statusText}`);
    return;
  }

  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = type === "pdf" ? "badawy_dqa_report.pdf" : "badawy_dqa_report.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
};

exportCsvBtn.addEventListener("click", () => downloadReport("csv"));
exportPdfBtn.addEventListener("click", () => downloadReport("pdf"));
if (toggleRawBtn) {
  toggleRawBtn.addEventListener("click", toggleRawOutput);
}
if (unifiedChartColumnEl) {
  unifiedChartColumnEl.addEventListener("change", renderUnifiedChart);
}
if (unifiedChartModeEl) {
  unifiedChartModeEl.addEventListener("change", renderUnifiedChart);
}
initTabs();
if (chatFormEl) {
  chatFormEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    const question = (chatInputEl?.value || "").trim();
    if (!question) return;
    if (chatInputEl) chatInputEl.value = "";
    await sendChatQuestion(question);
  });
}
if (chatQuickEl) {
  chatQuickEl.addEventListener("click", async (event) => {
    const btn = event.target.closest("[data-question]");
    if (!btn) return;
    const question = btn.getAttribute("data-question");
    if (!question) return;
    await sendChatQuestion(question);
  });
}
if (chatCollapseEl) {
  chatCollapseEl.addEventListener("click", () => {
    toggleChatPane();
  });
}
if (chatOpenEl) {
  chatOpenEl.addEventListener("click", () => {
    openChatPane();
  });
}
if (chatOverlayEl) {
  chatOverlayEl.addEventListener("click", () => {
    closeChatPane();
  });
}
window.addEventListener("resize", () => {
  if (!isMobileViewport()) {
    document.body.classList.remove("chat-open-mobile");
  }
  syncChatButtonLabel();
});
syncChatButtonLabel();
if (openContactModalEl) {
  openContactModalEl.addEventListener("click", () => {
    openContactModal();
  });
}
if (closeContactModalEl) {
  closeContactModalEl.addEventListener("click", () => {
    closeContactModal();
  });
}
if (contactModalEl) {
  contactModalEl.addEventListener("click", (event) => {
    if (event.target === contactModalEl) {
      closeContactModal();
    }
  });
}
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeContactModal();
    closeChatPane();
  }
});
if (contactFormEl) {
  contactFormEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!contactStatusEl || !contactSubmitEl) return;
    const formData = new FormData(contactFormEl);
    const name = String(formData.get("name") || "").trim();
    const email = String(formData.get("email") || "").trim();
    const message = String(formData.get("message") || "").trim();
    if (!name || !email || !message) {
      contactStatusEl.textContent = "All fields are required.";
      return;
    }
    contactSubmitEl.disabled = true;
    contactStatusEl.textContent = "Sending...";
    try {
      const response = await fetch("/api/contact", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, email, message }),
      });
      const data = await response.json();
      if (!response.ok) {
        const msg = data?.error?.message || "Contact request failed.";
        const code = data?.error?.code ? ` (${data.error.code})` : "";
        contactStatusEl.textContent = `Error: ${msg}${code}`;
        return;
      }
      contactStatusEl.textContent = data?.message || "Message sent.";
      contactFormEl.reset();
      setTimeout(() => {
        closeContactModal();
        if (contactStatusEl) contactStatusEl.textContent = "";
      }, 900);
    } catch (error) {
      contactStatusEl.textContent = `Network error: ${String(error)}`;
    } finally {
      contactSubmitEl.disabled = false;
    }
  });
}

aiBtn.addEventListener("click", async () => {
  if (!lastAnalysis) return;
  aiBtn.disabled = true;
  aiEl.innerHTML = '<div class="skeleton block"></div>';
  try {
    const response = await fetch("/api/ai-insights", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ analysis: lastAnalysis }),
    });
    const data = await response.json();
    const safeData = sanitizePayload(data);
    if (!response.ok) {
      aiEl.innerHTML = `<div class="ai-output">Error: ${formatApiError(safeData, "AI request failed")}</div>`;
      aiBtn.disabled = false;
      return;
    }
    renderAi(safeData);
    aiBtn.disabled = false;
  } catch (error) {
    aiEl.innerHTML = `<div class="ai-output">Network error: ${String(
      error
    )}</div>`;
    aiBtn.disabled = false;
  }
});

mlRunBtn.addEventListener("click", async () => {
  mlStatusEl.textContent = "";
  edaSummaryEl.innerHTML = '<div class="skeleton block"></div>';
  edaCorrEl.innerHTML = '<div class="skeleton block"></div>';
  modelResultsEl.innerHTML = '<div class="skeleton block"></div>';
  featureImportanceEl.innerHTML = '<div class="skeleton block"></div>';
  pcaResultsEl.innerHTML = '<div class="skeleton block"></div>';
  tsResultsEl.innerHTML = '<div class="skeleton block"></div>';

  const fileInput = form.querySelector('input[type="file"]');
  const file = fileInput?.files?.[0] || lastFile;
  if (!file) {
    mlStatusEl.textContent = "Select a file first.";
    edaSummaryEl.innerHTML = "";
    edaCorrEl.innerHTML = "";
    modelResultsEl.innerHTML = "";
    featureImportanceEl.innerHTML = "";
    pcaResultsEl.innerHTML = "";
    tsResultsEl.innerHTML = "";
    return;
  }

  const allowed = [".csv", ".tsv", ".xls", ".xlsx", ".xlxs", ".json", ".xml"];
  const ext = file.name.includes(".")
    ? `.${file.name.split(".").pop().toLowerCase()}`
    : "";
  if (!allowed.includes(ext)) {
    mlStatusEl.textContent = "EDA/ML supports CSV, TSV, XLSX, JSON, or XML.";
    edaSummaryEl.innerHTML = "";
    edaCorrEl.innerHTML = "";
    modelResultsEl.innerHTML = "";
    featureImportanceEl.innerHTML = "";
    pcaResultsEl.innerHTML = "";
    tsResultsEl.innerHTML = "";
    return;
  }

  const formData = new FormData();
  formData.append("file", file);
  formData.append("target_column", mlTargetEl.value || "");
  formData.append("time_column", mlTimeEl.value || "");
  const selected = Array.from(mlModelEls)
    .filter((el) => el.checked)
    .map((el) => el.value);
  formData.append("model_list", selected.join(","));
  formData.append("test_size", mlTestSizeEl.value || "0.2");
  formData.append("cv_folds", mlCvEl.value || "3");

  try {
    const response = await fetch("/api/eda-ml", {
      method: "POST",
      body: formData,
    });
    const data = await response.json();
    const safeData = sanitizePayload(data);
    if (!response.ok) {
      const message = safeData?.error?.message || "EDA/ML failed";
      const code = safeData?.error?.code ? ` (${safeData.error.code})` : "";
      mlStatusEl.textContent = `Error: ${message}${code}`;
      edaSummaryEl.innerHTML = "";
      edaCorrEl.innerHTML = "";
      modelResultsEl.innerHTML = "";
      pcaResultsEl.innerHTML = "";
      tsResultsEl.innerHTML = "";
      return;
    }
    mlStatusEl.textContent = `EDA/ML complete (rows used: ${safeData.rows_used}, test size: ${safeData.models?.test_size}, cv folds: ${safeData.models?.cv_folds}).`;
    renderEDASummary(safeData.eda);
    renderCorrelations(safeData.eda?.correlations || []);
    renderModelResults(safeData.models);
    renderFeatureImportance(safeData.models);
    renderPCA(safeData.pca);
    renderTimeSeries(safeData.time_series);
  } catch (error) {
    mlStatusEl.textContent = `Network error: ${String(error)}`;
    edaSummaryEl.innerHTML = "";
    edaCorrEl.innerHTML = "";
    modelResultsEl.innerHTML = "";
    featureImportanceEl.innerHTML = "";
    pcaResultsEl.innerHTML = "";
    tsResultsEl.innerHTML = "";
  }
});

const applyTransformPreset = (preset) => {
  if (!dropDuplicatesEl || !missingStrategyEl || !fillValueEl || !targetColumnsEl || !normalizeTextEl || !capOutliersEl) return;

  if (preset === "standard") {
    dropDuplicatesEl.checked = true;
    missingStrategyEl.value = "mode";
    fillValueEl.value = "";
    targetColumnsEl.value = "";
    normalizeTextEl.value = "lower";
    capOutliersEl.checked = false;
    transformStatusEl.textContent = "Preset applied: Standard clean.";
    return;
  }
  if (preset === "numeric") {
    dropDuplicatesEl.checked = true;
    missingStrategyEl.value = "median";
    fillValueEl.value = "";
    normalizeTextEl.value = "none";
    capOutliersEl.checked = true;
    transformStatusEl.textContent = "Preset applied: Numeric clean.";
    return;
  }
  if (preset === "strict") {
    dropDuplicatesEl.checked = true;
    missingStrategyEl.value = "drop";
    fillValueEl.value = "";
    normalizeTextEl.value = "lower";
    capOutliersEl.checked = true;
    transformStatusEl.textContent = "Preset applied: Strict clean.";
    return;
  }
  if (preset === "no_change") {
    dropDuplicatesEl.checked = false;
    missingStrategyEl.value = "none";
    fillValueEl.value = "";
    targetColumnsEl.value = "";
    normalizeTextEl.value = "none";
    capOutliersEl.checked = false;
    transformStatusEl.textContent = "Preset applied: No changes.";
    return;
  }

  transformStatusEl.textContent = "Using custom transform settings.";
};

if (transformPresetEl) {
  transformPresetEl.addEventListener("change", () => {
    applyTransformPreset(transformPresetEl.value);
  });
}

const buildTransformFormData = () => {
  const fileInput = form.querySelector('input[type="file"]');
  const file = fileInput?.files?.[0] || lastFile;
  if (!file) {
    transformStatusEl.textContent = "Select a file first.";
    return null;
  }
  const allowed = [".csv", ".tsv", ".xls", ".xlsx", ".xlxs", ".json", ".xml"];
  const ext = file.name.includes(".")
    ? `.${file.name.split(".").pop().toLowerCase()}`
    : "";
  if (!allowed.includes(ext)) {
    transformStatusEl.textContent =
      "Transform only supports CSV, TSV, XLSX, JSON, or XML files.";
    return null;
  }

  const settings = {
    drop_duplicates: dropDuplicatesEl.checked ? "true" : "false",
    missing_strategy: missingStrategyEl.value,
    fill_value: fillValueEl.value || "",
    target_columns: targetColumnsEl.value || "",
    normalize_text: normalizeTextEl?.value || "none",
    cap_outliers: capOutliersEl?.checked ? "true" : "false",
  };
  return { file, settings };
};

transformBtn.addEventListener("click", async () => {
  transformStatusEl.textContent = "";
  transformSummaryEl.innerHTML = '<div class="skeleton block"></div>';
  transformPreviewEl.innerHTML = '<div class="skeleton block"></div>';

  const payload = buildTransformFormData();
  if (!payload) {
    transformSummaryEl.innerHTML = "";
    transformPreviewEl.innerHTML = "";
    return;
  }

  lastTransformForm = payload;
  const formData = new FormData();
  formData.append("file", payload.file);
  Object.entries(payload.settings).forEach(([key, value]) => {
    formData.append(key, value);
  });
  try {
    const response = await fetch("/api/transform", {
      method: "POST",
      body: formData,
    });
    const data = await response.json();
    const safeData = sanitizePayload(data);
    if (!response.ok) {
      transformStatusEl.textContent = `Error: ${formatApiError(safeData, "Transform failed")}`;
      transformSummaryEl.innerHTML = "";
      transformPreviewEl.innerHTML = "";
      return;
    }
    transformStatusEl.textContent = "Transform applied.";
    renderTransformSummary(safeData.transform_summary);
    renderTransformPreview(safeData.preview || []);
    transformDownloadBtn.disabled = false;
  } catch (error) {
    transformStatusEl.textContent = `Network error: ${String(error)}`;
    transformSummaryEl.innerHTML = "";
    transformPreviewEl.innerHTML = "";
  }
});

transformDownloadBtn.addEventListener("click", async () => {
  if (!lastTransformForm) {
    transformStatusEl.textContent = "Apply a transform first.";
    return;
  }
  try {
    const formData = new FormData();
    formData.append("file", lastTransformForm.file);
    Object.entries(lastTransformForm.settings).forEach(([key, value]) => {
      formData.append(key, value);
    });
    const response = await fetch("/api/transform/download", {
      method: "POST",
      body: formData,
    });
    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      transformStatusEl.textContent = `Error: ${formatApiError(data, "Download failed")}`;
      return;
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "transformed.csv";
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  } catch (error) {
    transformStatusEl.textContent = `Network error: ${String(error)}`;
  }
});

const buildFallbackSampleCsv = () => {
  const rows = [
    "record_id,event_date,region,segment,channel,device_type,is_promo_user,tenure_months,order_count,monthly_spend,support_tickets,last_login_days_ago,nps_score,anomaly_sensor,feedback_text,risk_band,churn_flag,revenue_next_30d",
  ];
  for (let i = 0; i < 180; i += 1) {
    const id = `R${10000 + i}`;
    const date = new Date(2024, 0, (i % 28) + 1).toISOString().slice(0, 10);
    const region = ["North", "South", "East", "West"][i % 4];
    const segment = ["Consumer", "SMB", "Enterprise"][i % 3];
    const channel = ["online", "partner", "reseller"][i % 3];
    const device = ["mobile", "desktop", "tablet"][i % 3];
    const promo = i % 5 === 0 ? 1 : 0;
    const tenure = (i % 36) + 1;
    const orders = (i % 9) + 1;
    const spend = (35 + (i % 80) * 2.3 + (i % 7) * 4).toFixed(2);
    const tickets = i % 6;
    const lastLogin = (i % 14) + 1;
    const nps = (35 + (i % 50) + (i % 4) * 2).toFixed(1);
    const sensor = (40 + (i % 25) * 1.5 + (i % 11)).toFixed(3);
    const feedback = ["good value", "stable platform", "too expensive", "missing features"][i % 4];
    const risk = ["low", "medium", "high"][i % 3];
    const churn = i % 7 === 0 ? 1 : 0;
    const revenue = (Number(spend) * (0.8 + (i % 5) * 0.12)).toFixed(2);
    rows.push(
      [
        id,
        date,
        region,
        segment,
        channel,
        device,
        promo,
        tenure,
        orders,
        spend,
        tickets,
        lastLogin,
        nps,
        sensor,
        feedback,
        risk,
        churn,
        revenue,
      ].join(",")
    );
  }
  return rows.join("\n");
};

sampleBtn.addEventListener("click", async () => {
  clearResults();
  showSkeletons();
  try {
    const samplePaths = ["/static/sample_full_ml.csv", "/static/sample.csv"];
    let sampleBlob = null;
    for (const path of samplePaths) {
      const candidate = await fetch(path);
      if (candidate.ok) {
        sampleBlob = await candidate.blob();
        break;
      }
    }

    if (!sampleBlob) {
      const fallbackCsv = buildFallbackSampleCsv();
      sampleBlob = new Blob([fallbackCsv], { type: "text/csv" });
      setStatus("Static sample not found. Using built-in sample dataset.");
    }

    const file = new File([sampleBlob], "sample_full_ml.csv", { type: "text/csv" });
    lastFile = file;
    if (mlTargetEl) mlTargetEl.value = "churn_flag";
    if (mlTimeEl) mlTimeEl.value = "event_date";
    const formData = new FormData();
    formData.append("file", file);
    formData.append("analysis_intent", "Explore churn drivers and segments");
    formData.append("target_column", "churn_flag");

    const result = await fetch("/api/analyze", {
      method: "POST",
      body: formData,
    });
    const data = await result.json();
    const safeData = sanitizePayload(data);
    if (!result.ok) {
      setStatus(`Error: ${formatApiError(safeData, "Unable to analyze sample dataset.")}`);
      setRawOutput(safeData);
      return;
    }
    setStatus("Sample assessment complete.");
    lastAnalysis = safeData;
    exportCsvBtn.disabled = false;
    exportPdfBtn.disabled = false;
    aiBtn.disabled = false;
    renderOverview(safeData);
    renderKpis(safeData);
    renderQualityScore(safeData.data_quality_score);
    renderRootCause(safeData.root_cause_blast_radius);
    renderRemediation(safeData.auto_remediation);
    renderSuggestions(safeData.suggestions || []);
    renderMissing(safeData.missing_values || {});
    renderPreview(safeData.preview || []);
    renderAnomalies(safeData.column_profiles || {});
    renderDistributions(safeData.numeric_distributions || {});
    renderHistograms(
      safeData.numeric_histograms || {},
      safeData.distribution_labels || {},
      safeData.outlier_summary || {},
      safeData.numeric_distributions || {},
      safeData.numeric_correlations || {}
    );
    renderOutliers(safeData.outlier_summary || {});
    renderDistributionLabels(safeData.distribution_labels || {});
    renderLabelDistribution(safeData.label_distribution || {});
    renderModels(safeData.model_suggestions || []);
    renderAi(safeData.ai_insights);
    setRawOutput(safeData);
  } catch (error) {
    setStatus("Network error: could not load sample dataset.");
    setRawOutput({ error: String(error) });
  }
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  clearResults();
  showSkeletons();

  const fileInput = form.querySelector('input[type="file"]');
  const file = fileInput?.files?.[0];
  if (!file) {
    clearResults();
    setStatus("Please select a file to analyze.");
    return;
  }
  const allowed = [".csv", ".tsv", ".xls", ".xlsx", ".xlxs", ".json", ".xml", ".docx"];
  const ext = file.name.includes(".")
    ? `.${file.name.split(".").pop().toLowerCase()}`
    : "";
  if (!allowed.includes(ext)) {
    clearResults();
    setStatus(
      "Unsupported file type. Please upload CSV, TSV, XLS/XLSX, JSON, XML, or DOCX."
    );
    return;
  }
  if (file.size > 10 * 1024 * 1024) {
    clearResults();
    setStatus("File too large. Maximum size is 10MB.");
    return;
  }

  const formData = new FormData(form);
  lastFile = file;

  try {
    const response = await fetch("/api/analyze", {
      method: "POST",
      body: formData,
    });

    const data = await response.json();
    const safeData = sanitizePayload(data);

    if (!response.ok) {
      setStatus(`Error: ${formatApiError(safeData, "Unable to analyze file")}`);
      setRawOutput(safeData);
      return;
    }

    setStatus("Assessment complete.");
    lastAnalysis = safeData;
    exportCsvBtn.disabled = false;
    exportPdfBtn.disabled = false;
    aiBtn.disabled = false;
    renderOverview(safeData);
    renderKpis(safeData);
    renderQualityScore(safeData.data_quality_score);
    renderRootCause(safeData.root_cause_blast_radius);
    renderRemediation(safeData.auto_remediation);
    renderSuggestions(safeData.suggestions || []);
    renderMissing(safeData.missing_values || {});
    renderPreview(safeData.preview || []);
    renderAnomalies(safeData.column_profiles || {});
    renderDistributions(safeData.numeric_distributions || {});
    renderHistograms(
      safeData.numeric_histograms || {},
      safeData.distribution_labels || {},
      safeData.outlier_summary || {},
      safeData.numeric_distributions || {},
      safeData.numeric_correlations || {}
    );
    renderOutliers(safeData.outlier_summary || {});
    renderDistributionLabels(safeData.distribution_labels || {});
    renderLabelDistribution(safeData.label_distribution || {});
    renderModels(safeData.model_suggestions || []);
    renderAi(safeData.ai_insights);
    setRawOutput(safeData);
  } catch (error) {
    setStatus("Network error: could not reach the backend.");
    setRawOutput({ error: String(error) });
  }
});
