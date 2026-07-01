package statuspage

const statusHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PgBouncer Aurora Operator Status</title>
  <script>
    (function() {
      var stored = localStorage.getItem("statusTheme");
      var dark = stored ? stored === "dark" : window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
      document.documentElement.dataset.theme = dark ? "dark" : "light";
    })();
  </script>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f8fb;
      --panel: #ffffff;
      --panel-soft: #f9fafb;
      --text: #172033;
      --muted: #667085;
      --line: #d9e0ea;
      --line-soft: #edf0f5;
      --good: #16815a;
      --good-bg: #e8f6ef;
      --bad: #c9352b;
      --bad-bg: #fdebea;
      --warn: #a86500;
      --warn-bg: #fff3d6;
      --info: #2668b2;
      --info-bg: #eaf2ff;
      --recent: #b45309;
      --recent-bg: #fff7ed;
      --recent-line: #fed7aa;
      --accent: #2f6feb;
      --idle: #6b7280;
      --idle-bg: #eef0f3;
      --shadow: 0 1px 2px rgba(16, 24, 40, 0.06);
    }
    :root[data-theme="dark"] {
      color-scheme: dark;
      --bg: #111827;
      --panel: #182231;
      --panel-soft: #202b3b;
      --text: #e5e7eb;
      --muted: #9aa4b2;
      --line: #334155;
      --line-soft: #253244;
      --good: #6ee7b7;
      --good-bg: #123529;
      --bad: #fca5a5;
      --bad-bg: #3b171c;
      --warn: #fbbf24;
      --warn-bg: #39290f;
      --info: #93c5fd;
      --info-bg: #172f4f;
      --recent: #fdba74;
      --recent-bg: #352515;
      --recent-line: #b45309;
      --accent: #60a5fa;
      --idle: #cbd5e1;
      --idle-bg: #263244;
      --shadow: 0 1px 2px rgba(0, 0, 0, 0.28);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-width: 320px;
      background: var(--bg);
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.45;
    }
    .shell {
      width: min(1440px, 100%);
      margin: 0 auto;
      padding: 24px;
    }
    header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }
    h1 {
      margin: 0;
      font-size: 28px;
      line-height: 1.15;
      font-weight: 760;
      letter-spacing: 0;
    }
    .subhead {
      margin-top: 6px;
      color: var(--muted);
      font-size: 13px;
    }
    .toolbar {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .button,
    select {
      min-height: 32px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--panel);
      color: var(--text);
      font: inherit;
      font-size: 12px;
      font-weight: 650;
      padding: 0 10px;
      box-shadow: var(--shadow);
    }
    .button {
      cursor: pointer;
    }
    .status-pill,
    .tag,
    .condition-pill {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      border-radius: 999px;
      border: 1px solid transparent;
      font-size: 12px;
      font-weight: 650;
      line-height: 1;
      white-space: nowrap;
    }
    .status-pill {
      gap: 8px;
      padding: 6px 10px;
      background: var(--panel);
      border-color: var(--line);
      box-shadow: var(--shadow);
    }
    .dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: currentColor;
      flex: none;
    }
    .ok { color: var(--good); background: var(--good-bg); border-color: #b9e7d1; }
    .error { color: var(--bad); background: var(--bad-bg); border-color: #f6bfbb; }
    .warn { color: var(--warn); background: var(--warn-bg); border-color: #f2d891; }
    .info { color: var(--info); background: var(--info-bg); border-color: #c8dcff; }
    .idle { color: var(--idle); background: var(--idle-bg); border-color: #d6dae1; }
    .recent-badge {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      border-radius: 999px;
      border: 1px solid var(--recent-line);
      padding: 4px 7px;
      color: var(--recent);
      background: var(--recent-bg);
      font-size: 11px;
      font-weight: 720;
      line-height: 1;
      white-space: nowrap;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric {
      position: relative;
      min-height: 74px;
      padding: 13px 14px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .metric.recent {
      border-color: var(--recent-line);
      background: linear-gradient(180deg, var(--recent-bg) 0%, var(--panel) 72%);
      box-shadow: inset 0 3px 0 var(--recent-line), var(--shadow);
    }
    .metric-label {
      color: var(--muted);
      font-size: 12px;
      font-weight: 640;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .metric-value {
      margin-top: 6px;
      font-size: 24px;
      line-height: 1;
      font-weight: 760;
    }
    .metric-note {
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
    }
    .metric-note-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-top: 5px;
    }
    .metric-note-row .metric-note { margin-top: 0; }
    .content {
      display: grid;
      grid-template-columns: minmax(280px, 400px) minmax(0, 1fr);
      gap: 14px;
      align-items: start;
    }
    .list-panel,
    .detail-panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .panel-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-height: 48px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      background: var(--panel-soft);
    }
    .panel-title h2 {
      margin: 0;
      font-size: 14px;
      font-weight: 760;
      letter-spacing: 0;
    }
    .cr-list {
      display: grid;
      gap: 0;
    }
    .cr-row {
      appearance: none;
      width: 100%;
      border: 0;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      align-items: center;
      min-height: 84px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line-soft);
      text-align: left;
      font: inherit;
      color: inherit;
      cursor: pointer;
      background: var(--panel);
    }
    .cr-row:last-child { border-bottom: 0; }
    .cr-row:hover,
    .cr-row.active { background: color-mix(in srgb, var(--accent) 12%, var(--panel)); }
    .cr-row.active { box-shadow: inset 3px 0 0 var(--accent); }
    .cr-row.recent {
      background: var(--recent-bg);
      box-shadow: inset 3px 0 0 var(--recent-line);
    }
    .cr-row.recent.active {
      background: color-mix(in srgb, var(--accent) 12%, var(--panel));
      box-shadow: inset 3px 0 0 var(--accent), inset 6px 0 0 var(--recent-line);
    }
    .cr-name {
      margin: 0 0 5px;
      overflow-wrap: anywhere;
      font-size: 14px;
      font-weight: 760;
      letter-spacing: 0;
    }
    .cr-meta,
    .time-grid,
    .kv {
      color: var(--muted);
      font-size: 12px;
    }
    .cr-meta {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .cr-state-stack {
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 7px;
    }
    .tag {
      padding: 5px 8px;
      background: var(--idle-bg);
      border-color: var(--line);
      color: var(--text);
    }
    .detail-body { padding: 14px; }
    .detail-head {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      align-items: start;
      margin-bottom: 14px;
    }
    .detail-name {
      margin: 0;
      font-size: 20px;
      font-weight: 760;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }
    .hash-line {
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .sections {
      display: grid;
      gap: 14px;
    }
    .section {
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
      background: var(--panel);
    }
    .section h3 {
      margin: 0;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      background: var(--panel-soft);
      font-size: 13px;
      font-weight: 760;
      letter-spacing: 0;
    }
    .time-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      padding: 12px;
    }
    .time-item { min-width: 0; }
    .time-label,
    .table-label {
      display: block;
      color: var(--muted);
      font-size: 11px;
      font-weight: 680;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .time-value {
      display: block;
      margin-top: 4px;
      color: var(--text);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .table-wrap { overflow-x: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 680px;
    }
    th,
    td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line-soft);
      text-align: left;
      vertical-align: top;
      font-size: 12px;
    }
    th {
      color: var(--muted);
      background: var(--panel-soft);
      font-size: 11px;
      font-weight: 760;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    tr:last-child td { border-bottom: 0; }
    .mono {
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
      font-size: 12px;
    }
    .condition-pill { padding: 5px 8px; }
    .condition-list {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      padding: 12px;
    }
    .condition {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: var(--panel);
    }
    .condition.recent {
      border-color: var(--recent-line);
      background: var(--recent-bg);
      box-shadow: inset 0 3px 0 var(--recent-line);
    }
    .condition-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 8px;
    }
    .condition-type {
      min-width: 0;
      overflow-wrap: anywhere;
      font-size: 13px;
      font-weight: 760;
    }
    .condition-reason {
      color: var(--text);
      font-size: 12px;
      font-weight: 680;
      overflow-wrap: anywhere;
    }
    .condition-message {
      margin-top: 4px;
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .service-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      padding: 12px;
    }
    .service {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: var(--panel);
    }
    .service-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 10px;
    }
    .service-name {
      overflow-wrap: anywhere;
      font-weight: 760;
      font-size: 13px;
    }
    .kv-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
    }
    .kv span {
      display: block;
      color: var(--text);
      font-weight: 720;
      font-size: 14px;
    }
    .empty {
      padding: 24px;
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 1100px) {
      .summary-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .content { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      .shell { padding: 14px; }
      header,
      .detail-head {
        grid-template-columns: 1fr;
        display: grid;
      }
      .toolbar { justify-content: flex-start; }
      h1 { font-size: 23px; }
      .summary-grid,
      .time-grid,
      .service-grid,
      .condition-list { grid-template-columns: 1fr; }
      .kv-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header>
      <div>
        <h1>PgBouncer Aurora Operator Status</h1>
        <div class="subhead" id="subhead">Loading status...</div>
      </div>
      <div class="toolbar" aria-label="overall status">
        <select id="recentWindowSelect" aria-label="local highlight window" title="Highlight window for this browser only"></select>
        <select id="refreshSelect" aria-label="refresh interval"></select>
        <button class="button" id="refreshButton" type="button">Refresh</button>
        <button class="button" id="themeButton" type="button">Theme</button>
        <span class="status-pill idle" id="fetchState"><span class="dot"></span>Loading</span>
      </div>
    </header>

    <section class="summary-grid" aria-label="summary" id="summary"></section>

    <section class="content">
      <aside class="list-panel" aria-label="managed custom resources">
        <div class="panel-title">
          <h2>Managed CRs</h2>
          <span class="tag" id="watchName">watch *</span>
        </div>
        <div class="cr-list" id="crList"></div>
      </aside>

      <section class="detail-panel" aria-label="selected custom resource status">
        <div class="panel-title">
          <h2>Current CR Status</h2>
          <span class="tag" id="selectedNamespace">-</span>
        </div>
        <div class="detail-body" id="detail"></div>
      </section>
    </section>
  </main>

  <script>
    var snapshot = null;
    var selected = "";
    var timer = null;
    var refreshSeconds = Number(localStorage.getItem("statusRefreshSeconds") || "10");
    var recentWindowSeconds = Number(localStorage.getItem("statusRecentWindowSeconds") || "0");
    var theme = localStorage.getItem("statusTheme") || document.documentElement.dataset.theme || "light";

    function escapeHtml(value) {
      return String(value == null ? "" : value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#039;");
    }

    function stateClass(state) {
      if (state === "Ready" || state === "True") return "ok";
      if (state === "Progressing" || state === "Unknown") return "warn";
      if (state === "Frozen" || state === "Degraded" || state === "False") return "error";
      return "idle";
    }

    function conditionClass(condition) {
      var negativeIsGood = ["Frozen", "ReaderFallback", "RoleMismatch", "TrafficTransitioning", "ZoneAwareConflict", "Degraded"];
      if (condition.status === "Unknown") return "warn";
      if (negativeIsGood.indexOf(condition.type) >= 0) {
        return condition.status === "True" ? (condition.type === "ReaderFallback" ? "warn" : "error") : "ok";
      }
      return condition.status === "True" ? "ok" : "error";
    }

    function formatTime(value) {
      if (!value) return "-";
      var raw = value;
      if (typeof value === "object" && value.time) raw = value.time;
      var date = new Date(raw);
      if (Number.isNaN(date.getTime())) return String(raw);
      var pad = function (n) { return String(n).padStart(2, "0"); };
      return date.getFullYear() + "-" + pad(date.getMonth() + 1) + "-" + pad(date.getDate()) +
        " " + pad(date.getHours()) + ":" + pad(date.getMinutes()) + ":" + pad(date.getSeconds());
    }

    function parseTime(value) {
      if (!value) return null;
      var raw = value;
      if (typeof value === "object" && value.time) raw = value.time;
      var date = new Date(raw);
      return Number.isNaN(date.getTime()) ? null : date;
    }

    function recentReferenceTime() {
      return parseTime(snapshot && snapshot.generatedAt) || new Date();
    }

    function recentWindowMs() {
      return effectiveRecentWindowSeconds() * 1000;
    }

    function effectiveRecentWindowSeconds() {
      var value = Number(recentWindowSeconds || (snapshot && snapshot.recentWindowSeconds) || 60);
      if (!Number.isFinite(value) || value <= 0) value = 60;
      return Math.min(Math.max(value, 60), 86400);
    }

    function formatDuration(seconds) {
      seconds = Math.max(Number(seconds || 0), 0);
      if (seconds >= 86400 && seconds % 86400 === 0) return (seconds / 86400) + "d";
      if (seconds >= 3600 && seconds % 3600 === 0) return (seconds / 3600) + "h";
      if (seconds >= 60 && seconds % 60 === 0) return (seconds / 60) + "m";
      return seconds + "s";
    }

    function recentAgeLabel(value) {
      var date = parseTime(value);
      if (!date) return "";
      var diffMs = Math.max(0, recentReferenceTime().getTime() - date.getTime());
      var minutes = Math.floor(diffMs / 60000);
      if (minutes <= 0) return "just now";
      if (minutes < 60) return minutes + "m ago";
      var hours = Math.floor(minutes / 60);
      if (hours < 24) return hours + "h ago";
      return Math.floor(hours / 24) + "d ago";
    }

    function isRecentTime(value) {
      var date = parseTime(value);
      if (!date) return false;
      var diffMs = recentReferenceTime().getTime() - date.getTime();
      return diffMs >= 0 && diffMs <= recentWindowMs();
    }

    function latestRecentTime(values) {
      var latest = null;
      values.forEach(function(value) {
        var date = parseTime(value);
        if (date && isRecentTime(date) && (!latest || date > latest)) latest = date;
      });
      return latest;
    }

    function recentConditions(item) {
      return arrayValue(item.conditions).filter(function(condition) {
        return isRecentTime(condition.lastTransitionTime);
      });
    }

    function resourceRecentInfo(item) {
      var conditionTimes = recentConditions(item).map(function(condition) { return condition.lastTransitionTime; });
      var latest = latestRecentTime([item.lastAppliedTime].concat(conditionTimes));
      if (!latest) return { recent: false, count: 0, label: "" };
      return {
        recent: true,
        count: conditionTimes.length + (isRecentTime(item.lastAppliedTime) ? 1 : 0),
        label: recentAgeLabel(latest)
      };
    }

    function recentMetricCount(kind) {
      return arrayValue(snapshot.resources).filter(function(item) {
        var conditions = recentConditions(item);
        if (kind === "clusters") return resourceRecentInfo(item).recent;
        if (kind === "writers") return conditions.some(function(condition) { return condition.type === "WriterReady"; });
        if (kind === "readers") return conditions.some(function(condition) { return condition.type === "ReaderReady" || condition.type === "ReaderFallback"; });
        if (kind === "degraded") return conditions.some(function(condition) { return condition.type === "Degraded" && condition.status === "True"; });
        if (kind === "frozen") return conditions.some(function(condition) { return condition.type === "Frozen" && condition.status === "True"; });
        return false;
      }).length;
    }

    function recentBadge(label) {
      return '<span class="recent-badge">' + escapeHtml(label) + '</span>';
    }

    function tzLabel(value) {
      if (!value) return "";
      var raw = value;
      if (typeof value === "object" && value.time) raw = value.time;
      var date = new Date(raw);
      if (Number.isNaN(date.getTime())) return "";
      try {
        var parts = new Intl.DateTimeFormat(undefined, { timeZoneName: "short" }).formatToParts(date);
        for (var i = 0; i < parts.length; i++) {
          if (parts[i].type === "timeZoneName") return " " + parts[i].value;
        }
      } catch (e) {}
      return "";
    }

    function arrayValue(value) {
      return Array.isArray(value) ? value : [];
    }

    function serviceValue(resource, role) {
      var summary = resource.serviceSummary || {};
      return summary[role] || {};
    }

    function renderFetchState(ok, message) {
      var node = document.getElementById("fetchState");
      node.className = "status-pill " + (ok ? "ok" : "error");
      node.innerHTML = '<span class="dot"></span>' + escapeHtml(message);
    }

    function refreshOptions(minSeconds) {
      minSeconds = Math.max(Number(minSeconds || 5), 5);
      var values = [0, minSeconds, 10, 30].filter(function(value, index, self) {
        return value === 0 || (value >= minSeconds && self.indexOf(value) === index);
      });
      var select = document.getElementById("refreshSelect");
      select.innerHTML = values.map(function(value) {
        var label = value === 0 ? "Refresh off" : "Refresh " + value + "s";
        return '<option value="' + value + '">' + label + '</option>';
      }).join("");
      if (refreshSeconds > 0 && refreshSeconds < minSeconds) refreshSeconds = minSeconds;
      if (values.indexOf(refreshSeconds) < 0) refreshSeconds = minSeconds;
      select.value = String(refreshSeconds);
    }

    function recentWindowOptions() {
      var values = [60, 300, 900, 3600, 21600, 86400];
      var select = document.getElementById("recentWindowSelect");
      var current = effectiveRecentWindowSeconds();
      select.innerHTML = values.map(function(value) {
        return '<option value="' + value + '">Highlight ' + formatDuration(value) + '</option>';
      }).join("");
      if (values.indexOf(current) < 0) current = 60;
      recentWindowSeconds = current;
      select.value = String(current);
    }

    function applyTheme(value) {
      theme = value === "dark" ? "dark" : "light";
      document.documentElement.dataset.theme = theme;
      localStorage.setItem("statusTheme", theme);
      document.getElementById("themeButton").textContent = theme === "dark" ? "Light" : "Dark";
    }

    function configureTimer() {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
      if (refreshSeconds > 0) {
        timer = setInterval(loadStatus, refreshSeconds * 1000);
      }
      localStorage.setItem("statusRefreshSeconds", String(refreshSeconds));
    }

	function statusJSONPath() {
		var path = window.location.pathname || "/status";
		if (path.endsWith("/")) path = path.slice(0, -1);
		if (path.endsWith("/status")) return path.slice(0, -"/status".length) + "/status.json";
		return "/status.json";
	}

	async function loadStatus() {
		try {
			var response = await fetch(statusJSONPath(), { cache: "no-store" });
        if (!response.ok) throw new Error("HTTP " + response.status);
        snapshot = await response.json();
        refreshOptions(snapshot.refreshMinIntervalSeconds);
        recentWindowOptions();
        if (!selected && snapshot.resources && snapshot.resources.length > 0) selected = snapshot.resources[0].name;
        if (snapshot.resources && !snapshot.resources.some(function(item) { return item.name === selected; })) {
          selected = snapshot.resources.length > 0 ? snapshot.resources[0].name : "";
        }
        render();
        renderFetchState(!snapshot.error, snapshot.error ? "Snapshot error" : "Live");
      } catch (err) {
        renderFetchState(false, "Fetch failed");
        document.getElementById("subhead").textContent = "Failed to load status: " + err.message;
      }
    }

    function renderSummary() {
      var summary = snapshot.summary || {};
      var generatedAt = formatTime(snapshot.generatedAt) + tzLabel(snapshot.generatedAt);
      document.getElementById("subhead").textContent =
        snapshot.namespace + " / generated " + generatedAt + " / min refresh " + snapshot.refreshMinIntervalSeconds + "s / local highlights " + formatDuration(effectiveRecentWindowSeconds());
      document.getElementById("watchName").textContent = "watch " + (snapshot.watchName || "*");
      var items = [
        ["Clusters", summary.clusters || 0, "managed CRs", recentMetricCount("clusters")],
        ["Writers", summary.writers || 0, "service members", recentMetricCount("writers")],
        ["Readers", summary.readers || 0, "service members", recentMetricCount("readers")],
        ["Degraded", summary.degraded || 0, "active condition", recentMetricCount("degraded")],
        ["Frozen", summary.frozen || 0, "blocked plans", recentMetricCount("frozen")]
      ];
      document.getElementById("summary").innerHTML = items.map(function(item) {
        var recentCount = item[3] || 0;
        return '<div class="metric ' + (recentCount > 0 ? "recent" : "") + '"><div class="metric-label">' + escapeHtml(item[0]) + '</div><div class="metric-value">' + escapeHtml(item[1]) + '</div>' +
          '<div class="metric-note-row"><div class="metric-note">' + escapeHtml(item[2]) + '</div>' + (recentCount > 0 ? recentBadge(recentCount + " changed") : "") + '</div></div>';
      }).join("");
    }

    function renderList() {
      var list = document.getElementById("crList");
      var resources = arrayValue(snapshot.resources);
      if (resources.length === 0) {
        list.innerHTML = '<div class="empty">No managed custom resources.</div>';
        return;
      }
      list.innerHTML = resources.map(function(item) {
        var writer = arrayValue((item.lastAppliedMembership || {}).writer).length;
        var reader = arrayValue((item.lastAppliedMembership || {}).reader).length;
        var recent = resourceRecentInfo(item);
        return '<button class="cr-row ' + (item.name === selected ? "active " : "") + (recent.recent ? "recent" : "") + '" type="button" data-name="' + escapeHtml(item.name) + '">' +
          '<span><p class="cr-name">' + escapeHtml(item.name) + '</p><span class="cr-meta">' +
          '<span>' + escapeHtml(item.namespace) + '</span><span>gen ' + item.generation + '/' + item.observedGeneration + '</span><span>writer ' + writer + '</span><span>reader ' + reader + '</span>' +
          '</span></span><span class="cr-state-stack"><span class="condition-pill ' + stateClass(item.state) + '">' + escapeHtml(item.state) + '</span>' +
          (recent.recent ? recentBadge("changed " + recent.label) : "") + '</span></button>';
      }).join("");
      list.querySelectorAll(".cr-row").forEach(function(button) {
        button.addEventListener("click", function() {
          selected = button.dataset.name;
          render();
        });
      });
    }

    function renderService(role, service) {
      var name = service.serviceName || "-";
      var fallback = Boolean(service.fallbackFromWriter);
      return '<div class="service"><div class="service-title"><div class="service-name">' + escapeHtml(name) + '</div>' +
        '<span class="condition-pill ' + (fallback ? "warn" : "info") + '">' + escapeHtml(role) + '</span></div>' +
        '<div class="kv-grid">' +
        '<div class="kv"><span>' + (service.totalCandidates || 0) + '</span>candidates</div>' +
        '<div class="kv"><span>' + (service.healthy || 0) + '</span>healthy</div>' +
        '<div class="kv"><span>' + (service.members || 0) + '</span>members</div>' +
        '<div class="kv"><span>' + (service.readyMembers || 0) + '</span>ready</div>' +
        '</div></div>';
    }

    function renderDetail() {
      var resources = arrayValue(snapshot.resources);
      var item = resources.find(function(candidate) { return candidate.name === selected; });
      var detail = document.getElementById("detail");
      if (!item) {
        document.getElementById("selectedNamespace").textContent = "-";
        detail.innerHTML = '<div class="empty">Select a custom resource.</div>';
        return;
      }
      document.getElementById("selectedNamespace").textContent = item.namespace;
      var membership = item.lastAppliedMembership || {};
      var instances = arrayValue(item.instances);
      var conditions = arrayValue(item.conditions);
      detail.innerHTML =
        '<div class="detail-head"><div><h2 class="detail-name">' + escapeHtml(item.name) + '</h2>' +
        '<div class="hash-line">topology ' + escapeHtml(item.topologyHash || "-") + ' / membership ' + escapeHtml(item.membershipHash || "-") + '</div></div>' +
        '<span class="status-pill ' + stateClass(item.state) + '"><span class="dot"></span>' + escapeHtml(item.state) + '</span></div>' +
        '<div class="sections">' +
        '<section class="section"><h3>Lifecycle</h3><div class="time-grid">' +
        '<div class="time-item"><span class="time-label">Discovery</span><span class="time-value">' + escapeHtml(formatTime(item.lastDiscoveryTime)) + '</span></div>' +
        '<div class="time-item"><span class="time-label">Monitor</span><span class="time-value">' + escapeHtml(formatTime(item.lastMonitorTime)) + '</span></div>' +
        '<div class="time-item"><span class="time-label">Applied</span><span class="time-value">' + escapeHtml(formatTime(item.lastAppliedTime)) + '</span></div>' +
        '<div class="time-item"><span class="time-label">Discovery failures</span><span class="time-value">' + (item.consecutiveDiscoveryFailures || 0) + '</span></div>' +
        '<div class="time-item"><span class="time-label">Writer members</span><span class="time-value">' + escapeHtml(arrayValue(membership.writer).join(", ") || "-") + '</span></div>' +
        '<div class="time-item"><span class="time-label">Reader members</span><span class="time-value">' + escapeHtml(arrayValue(membership.reader).join(", ") || "-") + '</span></div>' +
        '</div></section>' +
        '<section class="section"><h3>Services</h3><div class="service-grid">' + renderService("Writer", serviceValue(item, "writer")) + renderService("Reader", serviceValue(item, "reader")) + '</div></section>' +
        '<section class="section"><h3>Instances</h3><div class="table-wrap"><table><thead><tr><th>Instance</th><th>Role</th><th>Health</th><th>Ready</th><th>AZ</th><th>DBI</th><th>Endpoint</th></tr></thead><tbody>' +
        instances.map(function(instance) {
          var ready = (instance.readyReplicas || 0) + '/' + (instance.desiredReplicas || 0);
          var healthClass = instance.disabled ? "idle" : (instance.healthy ? "ok" : "error");
          var healthLabel = instance.disabled ? "disabled" : (instance.reason || (instance.healthy ? "healthy" : "unhealthy"));
          return '<tr><td class="mono">' + escapeHtml(instance.instanceName) + '</td><td>' + escapeHtml(instance.role) + '</td>' +
            '<td><span class="condition-pill ' + healthClass + '">' + escapeHtml(healthLabel) + '</span></td>' +
            '<td>' + ready + '</td><td>' + escapeHtml(instance.availabilityZone || "-") + '</td><td class="mono">' + escapeHtml(instance.dbiResourceId || "-") + '</td>' +
            '<td class="mono">' + escapeHtml(instance.endpoint || "-") + ':' + (instance.port || "-") + '</td></tr>';
        }).join("") +
        '</tbody></table></div></section>' +
        '<section class="section"><h3>Conditions</h3><div class="condition-list">' +
        conditions.map(function(condition) {
          var recent = isRecentTime(condition.lastTransitionTime);
          return '<div class="condition ' + (recent ? "recent" : "") + '"><div class="condition-top"><div class="condition-type">' + escapeHtml(condition.type) + '</div>' +
            '<span class="condition-pill ' + conditionClass(condition) + '">' + escapeHtml(condition.status) + '</span></div>' +
            '<div class="condition-reason">' + escapeHtml(condition.reason || "-") + '</div>' +
            '<div class="condition-message">' + escapeHtml(condition.message || formatTime(condition.lastTransitionTime)) + '</div>' +
            (recent ? '<div class="condition-message">' + recentBadge("changed " + recentAgeLabel(condition.lastTransitionTime)) + '</div>' : "") + '</div>';
        }).join("") +
        '</div></section></div>';
    }

    function render() {
      if (!snapshot) return;
      renderSummary();
      renderList();
      renderDetail();
    }

    document.getElementById("refreshButton").addEventListener("click", loadStatus);
    document.getElementById("refreshSelect").addEventListener("change", function(event) {
      refreshSeconds = Number(event.target.value);
      configureTimer();
    });
    document.getElementById("recentWindowSelect").addEventListener("change", function(event) {
      recentWindowSeconds = Number(event.target.value);
      localStorage.setItem("statusRecentWindowSeconds", String(recentWindowSeconds));
      render();
    });
    document.getElementById("themeButton").addEventListener("click", function() {
      applyTheme(theme === "dark" ? "light" : "dark");
    });

    applyTheme(theme);
    loadStatus().then(configureTimer);
  </script>
</body>
</html>
`
