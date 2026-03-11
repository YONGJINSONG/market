(() => {
  const NEWS_PROXY_PATHS = new Set([
    "/api/bbc-business-rss",
    "/api/ft-home-rss",
    "/api/reuters-markets",
  ]);
  const YAHOO_CHART_PREFIX = "/api/yahoo-finance/v8/finance/chart/";
  const RRG_PROXY_PATHS = new Set(["/api/rrg", "/api/rrg.php"]);
  const VKOSPI_PROXY_PATH = "/api/krx-vkospi";
  const FRED_PROXY_PATH = "/api/fred-graph/graph/fredgraph.csv";
  const BREADTH_HISTORY_PATH =
    "/api/tradermonty-breadth/market-breadth-analysis/market_breadth_data.csv";
  const BREADTH_SUMMARY_PATH =
    "/api/tradermonty-breadth/market-breadth-analysis/market_breadth_summary.csv";
  const CNN_FEAR_GREED_TARGET = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata";
  const currentScript = document.currentScript;
  const siteBasePath = (() => {
    const scriptUrl = new URL(currentScript?.src || window.location.href, window.location.href);
    const parts = scriptUrl.pathname.split("/");
    const jsIndex = parts.lastIndexOf("js");
    if (jsIndex > 0) {
      return `${parts.slice(0, jsIndex).join("/")}/`;
    }
    return "/";
  })();

  const originalFetch = window.fetch.bind(window);
  let marketPromise = null;
  let newsPromise = null;
  let yahooChartsPromise = null;
  let rrgPromise = null;
  let vkospiPromise = null;
  let cnnFearGreedPromise = null;
  let breadthHistoryPromise = null;
  let breadthSummaryPromise = null;
  const fredPromiseBySeries = new Map();
  let marketSyncPending = false;

  function toUrl(input) {
    if (input instanceof Request) {
      return new URL(input.url, window.location.href);
    }
    return new URL(String(input), window.location.href);
  }

  function sitePath(path) {
    const normalized = path.startsWith("/") ? path.slice(1) : path;
    return `${siteBasePath}${normalized}`;
  }

  function jsonResponse(payload, status = 200) {
    return new Response(JSON.stringify(payload), {
      status,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
      },
    });
  }

  function textResponse(content, status = 200, contentType = "text/plain; charset=utf-8") {
    return new Response(content, {
      status,
      headers: {
        "Content-Type": contentType,
      },
    });
  }

  function escapeXml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&apos;");
  }

  async function loadJson(path) {
    const response = await originalFetch(path, {
      cache: "no-store",
      headers: { Accept: "application/json" },
    });

    if (!response.ok) {
      throw new Error(`${path} request failed (${response.status})`);
    }

    return response.json();
  }

  async function loadText(path, accept = "text/plain, text/csv, */*") {
    const response = await originalFetch(path, {
      cache: "no-store",
      headers: { Accept: accept },
    });

    if (!response.ok) {
      throw new Error(`${path} request failed (${response.status})`);
    }

    return response.text();
  }

  async function loadMarketItems() {
    if (!marketPromise) {
      marketPromise = loadJson(sitePath("data/market.json")).then((payload) => {
        const items = Array.isArray(payload.items) ? payload.items : [];
        return new Map(items.map((item) => [item.symbol, item]));
      });
    }

    return marketPromise;
  }

  async function loadNewsItems() {
    if (!newsPromise) {
      newsPromise = loadJson(sitePath("data/news.json")).then((payload) =>
        Array.isArray(payload.items) ? payload.items : []
      );
    }

    return newsPromise;
  }

  async function loadYahooCharts() {
    if (!yahooChartsPromise) {
      yahooChartsPromise = loadJson(sitePath("data/yahoo-charts.json")).then((payload) => payload.charts || {});
    }

    return yahooChartsPromise;
  }

  function sliceObjectArrays(source, indices) {
    if (!source || typeof source !== "object") {
      return source;
    }

    const next = Array.isArray(source) ? [] : {};
    Object.entries(source).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        next[key] = indices.map((index) => value[index] ?? null);
      } else {
        next[key] = value;
      }
    });
    return next;
  }

  function getRangeDays(range) {
    switch (range) {
      case "5d":
        return 5;
      case "1mo":
        return 31;
      case "3mo":
        return 93;
      case "6mo":
        return 186;
      case "1y":
        return 366;
      case "2y":
        return 732;
      default:
        return null;
    }
  }

  function getWeekKey(unixSeconds) {
    const date = new Date(unixSeconds * 1000);
    const year = date.getUTCFullYear();
    const start = Date.UTC(year, 0, 1);
    const dayOfYear = Math.floor((date.getTime() - start) / (1000 * 60 * 60 * 24));
    const week = Math.floor(dayOfYear / 7);
    return `${year}-${week}`;
  }

  function buildYahooChartPayload(basePayload, url) {
    const chart = basePayload?.chart;
    const result = chart?.result?.[0];
    if (!result || !Array.isArray(result.timestamp)) {
      return basePayload;
    }

    const timestamps = result.timestamp;
    if (!timestamps.length) {
      return basePayload;
    }

    const range = url.searchParams.get("range") || result.meta?.range || "1y";
    const interval = url.searchParams.get("interval") || result.meta?.dataGranularity || "1d";
    const rangeDays = getRangeDays(range);
    const latestTimestamp = timestamps[timestamps.length - 1];

    let indices = timestamps.map((_, index) => index);
    if (rangeDays != null) {
      const cutoff = latestTimestamp - rangeDays * 24 * 60 * 60;
      indices = indices.filter((index) => timestamps[index] >= cutoff);
    }

    if (interval === "1wk" && indices.length > 1) {
      const grouped = new Map();
      indices.forEach((index) => {
        grouped.set(getWeekKey(timestamps[index]), index);
      });
      indices = Array.from(grouped.values());
    }

    if (!indices.length) {
      return basePayload;
    }

    const selectedTimestamps = indices.map((index) => timestamps[index]);
    const quote = ((result.indicators || {}).quote || [])[0] || {};
    const closeValues = Array.isArray(quote.close) ? indices.map((index) => quote.close[index] ?? null) : [];
    const lastClose = [...closeValues].reverse().find((value) => typeof value === "number") ?? result.meta?.regularMarketPrice ?? null;
    const previousClose =
      closeValues.length > 1
        ? [...closeValues.slice(0, -1)].reverse().find((value) => typeof value === "number") ?? result.meta?.previousClose ?? null
        : result.meta?.previousClose ?? null;

    const nextResult = {
      ...result,
      meta: {
        ...result.meta,
        range,
        dataGranularity: interval,
        regularMarketPrice: lastClose,
        previousClose,
        chartPreviousClose: previousClose,
      },
      timestamp: selectedTimestamps,
      indicators: {
        ...result.indicators,
        quote: ((result.indicators || {}).quote || []).map((entry) => sliceObjectArrays(entry, indices)),
        adjclose: ((result.indicators || {}).adjclose || []).map((entry) => sliceObjectArrays(entry, indices)),
      },
    };

    return {
      chart: {
        ...chart,
        result: [nextResult],
      },
    };
  }

  async function loadRrgPayload() {
    if (!rrgPromise) {
      rrgPromise = loadJson(sitePath("data/rrg.json"));
    }

    return rrgPromise;
  }

  async function loadVkospiPayload() {
    if (!vkospiPromise) {
      vkospiPromise = loadJson(sitePath("data/vkospi.json"));
    }

    return vkospiPromise;
  }

  async function loadCnnFearGreed() {
    if (!cnnFearGreedPromise) {
      cnnFearGreedPromise = loadJson(sitePath("data/cnn-fear-greed.json"));
    }

    return cnnFearGreedPromise;
  }

  async function loadFredCsv(seriesId) {
    if (!fredPromiseBySeries.has(seriesId)) {
      fredPromiseBySeries.set(
        seriesId,
        loadText(sitePath(`data/fred/${seriesId}.csv`), "text/csv, text/plain, */*")
      );
    }

    return fredPromiseBySeries.get(seriesId);
  }

  async function loadBreadthHistoryCsv() {
    if (!breadthHistoryPromise) {
      breadthHistoryPromise = loadText(
        sitePath("data/breadth/market_breadth_data.csv"),
        "text/csv, text/plain, */*"
      );
    }

    return breadthHistoryPromise;
  }

  async function loadBreadthSummaryCsv() {
    if (!breadthSummaryPromise) {
      breadthSummaryPromise = loadText(
        sitePath("data/breadth/market_breadth_summary.csv"),
        "text/csv, text/plain, */*"
      );
    }

    return breadthSummaryPromise;
  }

  function buildSyntheticNewsRss(items) {
    const body = items
      .slice(0, 15)
      .map((item) => {
        const title = escapeXml(item.title || "Market update");
        const link = escapeXml(item.url || window.location.href);
        const pubDate = escapeXml(item.published_at || new Date().toUTCString());
        return `<item><title>${title}</title><link>${link}</link><pubDate>${pubDate}</pubDate></item>`;
      })
      .join("");

    return `<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Static Market News</title>${body}</channel></rss>`;
  }

  function formatNumber(value, options = {}) {
    if (typeof value !== "number" || Number.isNaN(value)) {
      return "--";
    }

    return new Intl.NumberFormat("en-US", {
      minimumFractionDigits: options.minimumFractionDigits ?? 0,
      maximumFractionDigits: options.maximumFractionDigits ?? 2,
    }).format(value);
  }

  function formatPrice(item) {
    if (typeof item.price !== "number" || Number.isNaN(item.price)) {
      return "--";
    }

    if (item.symbol === "BTC") {
      return `$${formatNumber(item.price, { maximumFractionDigits: 0 })}`;
    }

    if (item.symbol === "GOLD") {
      return `$${formatNumber(item.price, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    }

    if (item.symbol === "DXY") {
      return formatNumber(item.price, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    return formatNumber(item.price, { maximumFractionDigits: 2 });
  }

  function formatChange(item) {
    if (typeof item.change !== "number" || Number.isNaN(item.change)) {
      return "--";
    }

    const prefix = item.change >= 0 ? "+" : "-";
    const absolute = Math.abs(item.change);
    const digits =
      item.symbol === "GOLD" || item.symbol === "DXY"
        ? { minimumFractionDigits: 2, maximumFractionDigits: 2 }
        : { maximumFractionDigits: 2 };

    return `${prefix}${formatNumber(absolute, digits)}`;
  }

  function formatChangePercent(item) {
    if (typeof item.change_percent !== "number" || Number.isNaN(item.change_percent)) {
      return "--";
    }

    const prefix = item.change_percent >= 0 ? "+" : "-";
    return `${prefix}${Math.abs(item.change_percent).toFixed(2)}%`;
  }

  function applyMarketData(items) {
    const cards = document.querySelectorAll(".indicator-card");

    cards.forEach((card) => {
      const symbolElement = card.querySelector(".text-xs.text-muted-foreground.font-medium");
      const priceElement = card.querySelector(".data-value.text-2xl.font-bold.mt-1");
      const nameElement = card.querySelector(".text-sm.text-muted-foreground");
      const changeElement = card.querySelector(".data-value.text-sm.mt-1");
      const badgeElement = card.querySelector(".text-xs.data-value");

      if (!symbolElement || !priceElement || !nameElement || !changeElement || !badgeElement) {
        return;
      }

      const item = items.get(symbolElement.textContent.trim());
      if (!item) {
        return;
      }

      const isUp = item.up !== false;

      card.classList.toggle("glow-green", isUp);
      card.classList.toggle("glow-red", !isUp);
      badgeElement.classList.toggle("bg-up/15", isUp);
      badgeElement.classList.toggle("text-up", isUp);
      badgeElement.classList.toggle("bg-down/15", !isUp);
      badgeElement.classList.toggle("text-down", !isUp);
      changeElement.classList.toggle("text-up", isUp);
      changeElement.classList.toggle("text-down", !isUp);

      nameElement.textContent = item.name || symbolElement.textContent.trim();
      priceElement.textContent = formatPrice(item);
      changeElement.textContent = formatChange(item);
      badgeElement.textContent = `${isUp ? "▲" : "▼"} ${formatChangePercent(item)}`;
    });
  }

  async function syncMarketCards() {
    try {
      const items = await loadMarketItems();
      applyMarketData(items);
    } catch (error) {
      console.error("static-data-bridge market sync failed", error);
    }
  }

  function queueMarketSync() {
    if (marketSyncPending) {
      return;
    }

    marketSyncPending = true;
    window.requestAnimationFrame(() => {
      marketSyncPending = false;
      void syncMarketCards();
    });
  }

  function isCnnFearGreedRequest(url) {
    if (
      url.hostname === "production.dataviz.cnn.io" &&
      url.pathname === "/index/fearandgreed/graphdata"
    ) {
      return true;
    }

    if (url.hostname !== "corsproxy.io") {
      return false;
    }

    const target = decodeURIComponent(url.search.startsWith("?") ? url.search.slice(1) : "");
    return target.startsWith(CNN_FEAR_GREED_TARGET);
  }

  function filterVkospiOutput(payload, range) {
    const allRows = Array.isArray(payload.output) ? payload.output : [];
    if (!allRows.length) {
      return { ...payload, output: [] };
    }

    const maxDaysByRange = {
      "1mo": 31,
      "3mo": 93,
      "6mo": 186,
      "1y": 366,
    };
    const maxDays = maxDaysByRange[range] || maxDaysByRange["1y"];
    const latestDateText = allRows[allRows.length - 1]?.trd_dd?.replaceAll("/", "-");
    const latestDate = latestDateText ? new Date(`${latestDateText}T00:00:00Z`) : null;
    if (!latestDate || Number.isNaN(latestDate.getTime())) {
      return payload;
    }

    const filteredRows = allRows.filter((row) => {
      const rowDateText = row?.trd_dd?.replaceAll("/", "-");
      const rowDate = rowDateText ? new Date(`${rowDateText}T00:00:00Z`) : null;
      if (!rowDate || Number.isNaN(rowDate.getTime())) {
        return false;
      }
      const dayDiff = (latestDate.getTime() - rowDate.getTime()) / (1000 * 60 * 60 * 24);
      return dayDiff <= maxDays;
    });

    return { ...payload, output: filteredRows.length ? filteredRows : allRows };
  }

  window.fetch = async (input, init) => {
    const url = toUrl(input);

    if (NEWS_PROXY_PATHS.has(url.pathname)) {
      const items = await loadNewsItems();
      return textResponse(buildSyntheticNewsRss(items), 200, "application/rss+xml; charset=utf-8");
    }

    if (isCnnFearGreedRequest(url)) {
      const payload = await loadCnnFearGreed();
      return jsonResponse(payload);
    }

    if (url.pathname.startsWith(YAHOO_CHART_PREFIX)) {
      const symbol = decodeURIComponent(url.pathname.slice(YAHOO_CHART_PREFIX.length));
      const charts = await loadYahooCharts();
      const payload = charts[symbol];
      if (payload) {
        return jsonResponse(buildYahooChartPayload(payload, url));
      }
      return jsonResponse(
        { chart: { result: null, error: { code: "Not Found", description: `No static chart for ${symbol}` } } },
        404
      );
    }

    if (RRG_PROXY_PATHS.has(url.pathname)) {
      const payload = await loadRrgPayload();
      return jsonResponse(payload);
    }

    if (url.pathname === VKOSPI_PROXY_PATH) {
      const payload = await loadVkospiPayload();
      return jsonResponse(filterVkospiOutput(payload, url.searchParams.get("range") || "1y"));
    }

    if (url.pathname === FRED_PROXY_PATH) {
      const seriesId = (url.searchParams.get("id") || "").trim();
      if (!seriesId) {
        return textResponse("DATE,VALUE\n", 404, "text/csv; charset=utf-8");
      }
      try {
        const csv = await loadFredCsv(seriesId);
        return textResponse(csv, 200, "text/csv; charset=utf-8");
      } catch (error) {
        return textResponse(`DATE,VALUE\n`, 404, "text/csv; charset=utf-8");
      }
    }

    if (url.pathname === BREADTH_HISTORY_PATH) {
      const csv = await loadBreadthHistoryCsv();
      return textResponse(csv, 200, "text/csv; charset=utf-8");
    }

    if (url.pathname === BREADTH_SUMMARY_PATH) {
      const csv = await loadBreadthSummaryCsv();
      return textResponse(csv, 200, "text/csv; charset=utf-8");
    }

    return originalFetch(input, init);
  };

  function startMarketObserver() {
    const root = document.getElementById("root");
    if (!root) {
      return;
    }

    queueMarketSync();

    const observer = new MutationObserver(() => {
      queueMarketSync();
    });

    observer.observe(root, { childList: true, subtree: true });
    window.setTimeout(queueMarketSync, 1200);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", startMarketObserver, { once: true });
  } else {
    startMarketObserver();
  }
})();
