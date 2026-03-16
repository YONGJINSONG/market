(() => {
  const BLOCKED_SCRIPT_PATTERNS = ["embed-widget-events.js"];
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

  function renderTradingViewEventsFallback(container) {
    if (!(container instanceof HTMLElement) || container.dataset.widgetGuardApplied === "true") {
      return;
    }

    container.dataset.widgetGuardApplied = "true";
    container.innerHTML = "";
    container.classList.add("tradingview-widget-container");

    const panel = document.createElement("div");
    panel.style.height = "100%";
    panel.style.minHeight = "400px";
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

    const title = document.createElement("strong");
    title.textContent = "경제 캘린더 위젯을 브라우저 정책 경고 때문에 비활성화했습니다.";
    title.style.fontSize = "14px";
    title.style.lineHeight = "1.5";

    const body = document.createElement("p");
    body.textContent =
      "이 영역의 TradingView events 위젯이 unload 및 추적 스크립트 경고를 반복해서 발생시켜, 페이지에서는 로딩을 막고 원본 캘린더 링크만 제공합니다.";
    body.style.margin = "0";
    body.style.fontSize = "13px";
    body.style.lineHeight = "1.6";
    body.style.color = "hsl(215 20% 72%)";

    const link = document.createElement("a");
    link.href = "https://www.tradingview.com/economic-calendar/";
    link.target = "_blank";
    link.rel = "noreferrer";
    link.textContent = "TradingView 경제 캘린더 열기";
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

    appendNode(panel, title);
    appendNode(panel, body);
    appendNode(panel, link);
    appendNode(container, panel);
  }

  Element.prototype.appendChild = function appendChildPatched(node) {
    if (matchesBlockedScript(node)) {
      renderTradingViewEventsFallback(this);
      return node;
    }

    return originalAppendChild.call(this, node);
  };
})();
