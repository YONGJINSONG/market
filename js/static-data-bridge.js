(() => {
  const NEWS_PROXY_PATHS = new Set([
    "/api/bbc-business-rss",
    "/api/ft-home-rss",
    "/api/reuters-markets",
  ]);
  const YAHOO_CHART_PREFIX = "/api/yahoo-finance/v8/finance/chart/";
  const RRG_PROXY_PATHS = new Set(["/api/rrg", "/api/rrg.php"]);

  const originalFetch = window.fetch.bind(window);
  let marketPromise = null;
  let newsPromise = null;
  let yahooChartsPromise = null;
  let rrgPromise = null;
  let marketSyncPending = false;

  function toUrl(input) {
    if (input instanceof Request) {
      return new URL(input.url, window.location.href);
    }
    return new URL(String(input), window.location.href);
  }

  function jsonResponse(payload, status = 200) {
    return new Response(JSON.stringify(payload), {
      status,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
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

  async function loadMarketItems() {
    if (!marketPromise) {
      marketPromise = loadJson("/data/market.json").then((payload) => {
        const items = Array.isArray(payload.items) ? payload.items : [];
        return new Map(items.map((item) => [item.symbol, item]));
      });
    }

    return marketPromise;
  }

  async function loadNewsItems() {
    if (!newsPromise) {
      newsPromise = loadJson("/data/news.json").then((payload) =>
        Array.isArray(payload.items) ? payload.items : []
      );
    }

    return newsPromise;
  }

  async function loadYahooCharts() {
    if (!yahooChartsPromise) {
      yahooChartsPromise = loadJson("/data/yahoo-charts.json").then((payload) => payload.charts || {});
    }

    return yahooChartsPromise;
  }

  async function loadRrgPayload() {
    if (!rrgPromise) {
      rrgPromise = loadJson("/data/rrg.json");
    }

    return rrgPromise;
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

  window.fetch = async (input, init) => {
    const url = toUrl(input);

    if (NEWS_PROXY_PATHS.has(url.pathname)) {
      const items = await loadNewsItems();
      return new Response(buildSyntheticNewsRss(items), {
        status: 200,
        headers: {
          "Content-Type": "application/rss+xml; charset=utf-8",
        },
      });
    }

    if (url.pathname.startsWith(YAHOO_CHART_PREFIX)) {
      const symbol = decodeURIComponent(url.pathname.slice(YAHOO_CHART_PREFIX.length));
      const charts = await loadYahooCharts();
      const payload = charts[symbol];
      if (payload) {
        return jsonResponse(payload);
      }
      return jsonResponse({ chart: { result: null, error: { code: "Not Found", description: `No static chart for ${symbol}` } } }, 404);
    }

    if (RRG_PROXY_PATHS.has(url.pathname)) {
      const payload = await loadRrgPayload();
      return jsonResponse(payload);
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
