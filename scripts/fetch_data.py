#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET


DEFAULT_TIMEOUT = 20
DEFAULT_NEWS_LIMIT = 10
YAHOO_CHART_ENDPOINT = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
DEFAULT_NEWS_QUERY = "stock market OR economy OR inflation OR federal reserve"
GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

MARKET_SYMBOLS = [
    {
        "symbol": "^GSPC",
        "display_symbol": "SPX",
        "name": "S&P 500",
        "source_url": "https://finance.yahoo.com/quote/%5EGSPC/",
    },
    {
        "symbol": "^NDX",
        "display_symbol": "NDX",
        "name": "NASDAQ 100",
        "source_url": "https://finance.yahoo.com/quote/%5ENDX/",
    },
    {
        "symbol": "^DJI",
        "display_symbol": "DJI",
        "name": "Dow Jones Industrial Average",
        "source_url": "https://finance.yahoo.com/quote/%5EDJI/",
    },
    {
        "symbol": "BTC-USD",
        "display_symbol": "BTC",
        "name": "Bitcoin",
        "source_url": "https://finance.yahoo.com/quote/BTC-USD/",
    },
    {
        "symbol": "GC=F",
        "display_symbol": "GOLD",
        "name": "Gold Futures",
        "source_url": "https://finance.yahoo.com/quote/GC=F/",
    },
    {
        "symbol": "DX-Y.NYB",
        "display_symbol": "DXY",
        "name": "US Dollar Index",
        "source_url": "https://finance.yahoo.com/quote/DX-Y.NYB/",
    },
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(url: str, timeout: int) -> Any:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def read_text(url: str, timeout: int) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def to_iso8601_from_unix(timestamp: Any) -> str | None:
    if timestamp in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).replace(microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )
    except (TypeError, ValueError, OSError):
        return None


def strip_html(text: str | None) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_yahoo_chart(symbol: str, timeout: int) -> dict[str, Any]:
    params = urlencode(
        {
            "interval": "1d",
            "range": "5d",
            "includePrePost": "false",
            "events": "div,splits",
        }
    )
    url = f"{YAHOO_CHART_ENDPOINT.format(symbol=quote(symbol, safe=''))}?{params}"
    payload = read_json(url, timeout=timeout)
    chart = payload.get("chart", {})
    error = chart.get("error")
    if error:
        raise RuntimeError(error.get("description") or error.get("code") or f"Yahoo Finance error for {symbol}")

    result = chart.get("result") or []
    if not result:
        raise RuntimeError(f"Yahoo Finance chart result is empty for {symbol}")
    return result[0]


def last_valid_close(result: dict[str, Any]) -> float | None:
    closes = (((result.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or []
    for value in reversed(closes):
        if value is not None:
            return value
    return None


def build_market_payload(timeout: int) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    warnings: list[str] = []

    for config in MARKET_SYMBOLS:
        try:
            raw = fetch_yahoo_chart(config["symbol"], timeout=timeout)
        except RuntimeError as error:
            warnings.append(str(error))
            continue

        meta = raw.get("meta", {})
        price = meta.get("regularMarketPrice")
        previous_close = meta.get("previousClose") or meta.get("chartPreviousClose")
        if price is None:
            price = last_valid_close(raw)

        change = None
        change_percent = None
        if price is not None and previous_close not in (None, 0):
            change = price - previous_close
            change_percent = change / previous_close * 100
        up = None if change is None else change >= 0

        items.append(
            {
                "symbol": config["display_symbol"],
                "quote_symbol": config["symbol"],
                "name": config["name"],
                "price": price,
                "change": change,
                "change_percent": change_percent,
                "up": up,
                "currency": meta.get("currency"),
                "market_state": meta.get("marketState"),
                "exchange": meta.get("exchangeName") or meta.get("fullExchangeName") or meta.get("exchange"),
                "as_of": to_iso8601_from_unix(meta.get("regularMarketTime")),
                "source": "Yahoo Finance",
                "source_url": config["source_url"],
            }
        )

    if not items:
        raise RuntimeError("No market data returned from Yahoo Finance.")

    payload: dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "source": {
            "provider": "Yahoo Finance",
            "endpoint": YAHOO_CHART_ENDPOINT,
        },
        "items": items,
    }
    if warnings:
        payload["warnings"] = warnings
    return payload


def build_google_news_url(query: str) -> str:
    params = {
        "q": query,
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
    }
    return f"{GOOGLE_NEWS_RSS}?{urlencode(params)}"


def extract_news_source(item: ET.Element) -> str | None:
    source = item.find("source")
    if source is not None and source.text:
        return source.text.strip()
    return None


def normalize_title(title: str) -> str:
    cleaned = strip_html(title)
    return cleaned[:-4].strip() if cleaned.endswith("...") else cleaned


def build_news_payload(timeout: int, limit: int, query: str) -> dict[str, Any]:
    rss_url = build_google_news_url(query)
    rss_text = read_text(rss_url, timeout=timeout)
    root = ET.fromstring(rss_text)
    channel = root.find("channel")
    if channel is None:
        raise RuntimeError("Google News RSS response did not contain a channel.")

    items: list[dict[str, Any]] = []
    seen_titles: set[str] = set()

    for node in channel.findall("item"):
        title = normalize_title(node.findtext("title", default=""))
        if not title or title in seen_titles:
            continue

        seen_titles.add(title)
        items.append(
            {
                "title": title,
                "url": node.findtext("link", default="").strip(),
                "source": extract_news_source(node),
                "published_at": node.findtext("pubDate", default="").strip() or None,
                "summary": strip_html(node.findtext("description", default="")) or None,
                "guid": node.findtext("guid", default="").strip() or None,
            }
        )

        if len(items) >= limit:
            break

    if not items:
        raise RuntimeError("No news items were parsed from Google News RSS.")

    return {
        "generated_at": utc_now_iso(),
        "source": {
            "provider": "Google News RSS",
            "endpoint": rss_url,
            "query": query,
        },
        "items": items,
    }


def persist_payload(builder_name: str, builder: Any, output_path: Path) -> tuple[bool, str | None]:
    try:
        payload = builder()
        write_json(output_path, payload)
        return True, None
    except (HTTPError, URLError, ET.ParseError, json.JSONDecodeError, RuntimeError) as error:
        if output_path.exists():
            return False, f"{builder_name} update failed, existing file kept: {error}"
        return False, f"{builder_name} update failed and no existing file is available: {error}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch market and news data and write JSON files for a static site."
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent.parent / "data"),
        help="Directory where market.json and news.json will be written.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("FETCH_DATA_TIMEOUT", DEFAULT_TIMEOUT)),
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--news-limit",
        type=int,
        default=int(os.getenv("FETCH_DATA_NEWS_LIMIT", DEFAULT_NEWS_LIMIT)),
        help="Maximum number of news items to keep.",
    )
    parser.add_argument(
        "--news-query",
        default=os.getenv("FETCH_DATA_NEWS_QUERY", DEFAULT_NEWS_QUERY),
        help="Google News RSS search query.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    market_path = output_dir / "market.json"
    news_path = output_dir / "news.json"

    successes: list[str] = []
    failures: list[str] = []

    market_ok, market_error = persist_payload(
        "market",
        lambda: build_market_payload(timeout=args.timeout),
        market_path,
    )
    if market_ok:
        successes.append(str(market_path))
    elif market_error:
        failures.append(market_error)

    news_ok, news_error = persist_payload(
        "news",
        lambda: build_news_payload(timeout=args.timeout, limit=args.news_limit, query=args.news_query),
        news_path,
    )
    if news_ok:
        successes.append(str(news_path))
    elif news_error:
        failures.append(news_error)

    for path in successes:
        print(f"updated: {path}")
    for message in failures:
        print(f"warning: {message}", file=sys.stderr)

    return 0 if not failures or all(path.exists() for path in (market_path, news_path)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
