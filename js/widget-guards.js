(() => {
  const BLOCKED_SCRIPT_PATTERNS = ["embed-widget-advanced-chart.js"];
  const originalAppendChild = Element.prototype.appendChild;

  function matchesBlockedScript(node) {
    if (!(node instanceof HTMLScriptElement)) {
      return false;
    }

    const src = node.src || node.getAttribute("src") || "";
    return BLOCKED_SCRIPT_PATTERNS.some((pattern) => src.includes(pattern));
  }

  function appendNode(parent, child) {
    originalAppendChild.call(parent, child);
  }

  function parseWidgetConfig(node) {
    const raw = (node.textContent || node.innerHTML || "").trim();
    if (!raw) {
      return {};
    }

    try {
      return JSON.parse(raw);
    } catch {
      return {};
    }
  }

  function createFallbackPanel(container, minHeight) {
    container.innerHTML = "";
    container.classList.add("tradingview-widget-container");

    const panel = document.createElement("div");
    panel.style.height = "100%";
    panel.style.minHeight = minHeight;
    panel.style.display = "flex";
    panel.style.flexDirection = "column";
    panel.style.justifyContent = "center";
    panel.style.alignItems = "flex-start";
    panel.style.gap = "12px";
    panel.style.padding = "20px";
    panel.style.border = "1px solid hsl(220 13% 22%)";
    panel.style.borderRadius = "12px";
    panel.style.background = "rgba(255, 255, 255, 0.02)";
    panel.style.color = "hsl(210 20% 92%)";

    appendNode(container, panel);
    return panel;
  }

  function createTextNode(tagName, text, styles) {
    const node = document.createElement(tagName);
    node.textContent = text;
    Object.assign(node.style, styles);
    return node;
  }

  function createLink(href, text) {
    const link = document.createElement("a");
    link.href = href;
    link.target = "_blank";
    link.rel = "noreferrer";
    link.textContent = text;
    link.style.display = "inline-flex";
    link.style.alignItems = "center";
    link.style.justifyContent = "center";
    link.style.padding = "10px 14px";
    link.style.borderRadius = "10px";
    link.style.border = "1px solid hsl(210 70% 56%)";
    link.style.color = "hsl(210 90% 70%)";
    link.style.textDecoration = "none";
    link.style.fontSize = "13px";
    link.style.fontWeight = "600";
    return link;
  }

  function buildTradingViewSymbolUrl(symbol) {
    if (!symbol) {
      return "https://www.tradingview.com/markets/";
    }

    return `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(symbol)}`;
  }

  function renderTradingViewAdvancedChartFallback(container, node) {
    if (!(container instanceof HTMLElement) || container.dataset.widgetGuardApplied === "true") {
      return;
    }

    container.dataset.widgetGuardApplied = "true";
    const config = parseWidgetConfig(node);
    const symbol = typeof config.symbol === "string" ? config.symbol : "";
    const panel = createFallbackPanel(container, "175px");
    const title = createTextNode(
      "strong",
      symbol
        ? `TradingView chart for ${symbol} was disabled to avoid browser policy warnings.`
        : "TradingView chart was disabled to avoid browser policy warnings.",
      {
        fontSize: "14px",
        lineHeight: "1.5",
      }
    );
    const body = createTextNode(
      "p",
      "The embedded advanced chart widget is the source of the remaining unload warnings in the console, so this page now blocks it and offers a direct chart link.",
      {
        margin: "0",
        fontSize: "13px",
        lineHeight: "1.6",
        color: "hsl(215 20% 72%)",
      }
    );
    const link = createLink(
      buildTradingViewSymbolUrl(symbol),
      symbol ? `Open ${symbol} on TradingView` : "Open TradingView chart"
    );

    appendNode(panel, title);
    appendNode(panel, body);
    appendNode(panel, link);
  }

  Element.prototype.appendChild = function appendChildPatched(node) {
    if (matchesBlockedScript(node)) {
      renderTradingViewAdvancedChartFallback(this, node);
      return node;
    }

    return originalAppendChild.call(this, node);
  };
})();
