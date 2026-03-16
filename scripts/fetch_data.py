#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import csv
import io
import json
import os
import re
import socket
import sys
import time
from http.cookiejar import CookieJar
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen
import xml.etree.ElementTree as ET


DEFAULT_TIMEOUT = 60
DEFAULT_NEWS_LIMIT = 10
DEFAULT_FRED_RETRY_ATTEMPTS = 3
DEFAULT_FRED_RETRY_DELAY = 2
YAHOO_CHART_ENDPOINT = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
DEFAULT_NEWS_QUERY = "stock market OR economy OR inflation OR federal reserve"
GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
CNN_FEAR_GREED_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
FRED_GRAPH_ENDPOINT = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_SERIES_IDS = ("DGS10", "DGS2")
KRX_VKOSPI_SOURCE_URL = (
    "https://eindex.krx.co.kr/contents/GLB/05/0502/0502030101/GLB0502030101T2.jsp"
    "?upmidCd=0202&idxCd=1300&idxId=O2901P"
)
KRX_OTP_URL = "https://eindex.krx.co.kr/contents/COM/GenerateOTP.jspx"
KRX_DATA_URL = "https://eindex.krx.co.kr/contents/IDXE/99/IDXE99000001.jspx"
KRX_BLD_PATH = "GLB/05/0502/0502030101/glb0502030101T2_02"
TRADERMONTY_BREADTH_HISTORY_URL = (
    "https://tradermonty.github.io/market-breadth-analysis/market_breadth_data.csv"
)
TRADERMONTY_BREADTH_SUMMARY_URL = (
    "https://tradermonty.github.io/market-breadth-analysis/market_breadth_summary.csv"
)
SEOUL_TZ = timezone(timedelta(hours=9))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
KRX_BASE_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Origin": "https://eindex.krx.co.kr",
    "Referer": KRX_VKOSPI_SOURCE_URL,
    "User-Agent": USER_AGENT,
    "X-Requested-With": "XMLHttpRequest",
}

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

YAHOO_SNAPSHOT_SYMBOLS = [
    "^VIX",
    "DX-Y.NYB",
    "ZC=F",
    "ZS=F",
    "ZW=F",
    "HE=F",
    "SPY",
    "RSP",
    "IWM",
    "HYG",
    "LQD",
    "TLT",
    "XLY",
    "XLP",
    "XLK",
    "XLV",
    "XLF",
    "XLE",
    "XLI",
    "XLB",
    "XLU",
    "XLRE",
    "XLC",
]

RRG_BENCHMARK = "SPY"
RRG_RS_PERIOD = 10
RRG_TRAIL_LENGTH = 5
RRG_SECTORS = [
    ("Technology", "XLK"),
    ("Health Care", "XLV"),
    ("Financials", "XLF"),
    ("Energy", "XLE"),
    ("Consumer Discretionary", "XLY"),
    ("Consumer Staples", "XLP"),
    ("Industrials", "XLI"),
    ("Materials", "XLB"),
    ("Utilities", "XLU"),
    ("Real Estate", "XLRE"),
    ("Communication Services", "XLC"),
]

BREADTH_UNIVERSE = [
    "SPY",
    "RSP",
    "IWM",
    "XLY",
    "XLP",
    "XLK",
    "XLV",
    "XLF",
    "XLE",
    "XLI",
    "XLB",
    "XLU",
    "XLRE",
    "XLC",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def open_request(request: Request, timeout: int, *, opener: Any | None = None) -> Any:
    if opener is not None:
        return opener.open(request, timeout=timeout)
    return urlopen(request, timeout=timeout)


def read_json(
    url: str,
    timeout: int,
    *,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    opener: Any | None = None,
) -> Any:
    request_headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if headers:
        request_headers.update(headers)
    request = Request(url, headers=request_headers, data=data)
    with open_request(request, timeout=timeout, opener=opener) as response:
        return json.load(response)


def read_text(
    url: str,
    timeout: int,
    *,
    accept: str = "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    opener: Any | None = None,
) -> str:
    request_headers = {
        "User-Agent": USER_AGENT,
        "Accept": accept,
    }
    if headers:
        request_headers.update(headers)
    request = Request(
        url,
        headers=request_headers,
        data=data,
    )
    with open_request(request, timeout=timeout, opener=opener) as response:
        return response.read().decode("utf-8")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(content)
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


def fetch_yahoo_chart(symbol: str, timeout: int, *, interval: str = "1d", data_range: str = "5d") -> dict[str, Any]:
    params = urlencode(
        {
            "interval": interval,
            "range": data_range,
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


def build_yahoo_chart_response(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "chart": {
            "result": [result],
            "error": None,
        }
    }


def extract_closes(result: dict[str, Any]) -> list[float]:
    closes = (((result.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or []
    values: list[float] = []
    for close in closes:
        if close is None:
            continue
        try:
            values.append(float(close))
        except (TypeError, ValueError):
            continue
    return values


def extract_series_points(result: dict[str, Any]) -> list[dict[str, Any]]:
    timestamps = result.get("timestamp") or []
    closes = (((result.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or []
    points: list[dict[str, Any]] = []

    for timestamp, close in zip(timestamps, closes):
        iso = to_iso8601_from_unix(timestamp)
        if iso is None or close is None:
            continue
        try:
            close_value = float(close)
        except (TypeError, ValueError):
            continue
        points.append({"date": iso[:10], "close": close_value})

    return points


def rolling_average(values: list[float], window: int) -> list[float | None]:
    if window <= 0:
        raise ValueError("window must be positive")

    averages: list[float | None] = []
    running_total = 0.0
    for index, value in enumerate(values):
        running_total += value
        if index >= window:
            running_total -= values[index - window]
        if index >= window - 1:
            averages.append(running_total / window)
        else:
            averages.append(None)
    return averages


def rolling_average_optional(values: list[float | None], window: int) -> list[float | None]:
    averages: list[float | None] = []
    for index in range(len(values)):
        if index < window - 1:
            averages.append(None)
            continue
        chunk = values[index - window + 1 : index + 1]
        if any(value is None for value in chunk):
            averages.append(None)
            continue
        averages.append(sum(value for value in chunk if value is not None) / window)
    return averages


def format_csv_number(value: float | None, digits: int = 6) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def bool_to_csv(value: bool) -> str:
    return "true" if value else "false"


def csv_rows_to_text(headers: list[str], rows: list[list[str]]) -> str:
    lines = [",".join(headers)]
    lines.extend(",".join(row) for row in rows)
    return "\n".join(lines) + "\n"


def fred_series_url(series_id: str) -> str:
    params = {
        "id": series_id,
        "cosd": (datetime.now(timezone.utc) - timedelta(days=400)).date().isoformat(),
    }
    return f"{FRED_GRAPH_ENDPOINT}?{urlencode(params)}"


def is_retryable_fetch_error(error: BaseException) -> bool:
    if isinstance(error, HTTPError):
        return error.code == 429 or 500 <= error.code < 600
    if isinstance(error, URLError):
        reason = getattr(error, "reason", None)
        return isinstance(reason, (TimeoutError, socket.timeout)) or "timed out" in str(reason).lower()
    return isinstance(error, (TimeoutError, socket.timeout))


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def scaled_score(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        raise ValueError("upper must be greater than lower")
    normalized = (value - lower) / (upper - lower)
    return clamp(normalized, 0.0, 1.0) * 100


def subtract_months(date: datetime, months: int) -> datetime:
    year = date.year
    month = date.month - months
    while month <= 0:
        year -= 1
        month += 12
    day = min(date.day, calendar.monthrange(year, month)[1])
    return date.replace(year=year, month=month, day=day)


def format_seoul_date(date: datetime) -> str:
    return date.astimezone(SEOUL_TZ).strftime("%Y%m%d")


def resolve_krx_range_start(data_range: str, end_date: datetime) -> datetime:
    if data_range == "1mo":
        return subtract_months(end_date, 1)
    if data_range == "3mo":
        return subtract_months(end_date, 3)
    if data_range == "6mo":
        return subtract_months(end_date, 6)
    return subtract_months(end_date, 12)


def normalize_csv_text(csv_text: str) -> str:
    lines = [line.rstrip() for line in csv_text.replace("\r\n", "\n").split("\n")]
    non_empty_lines = [line for line in lines if line.strip()]
    return "\n".join(non_empty_lines) + "\n"


def validate_csv_text(csv_text: str, required_headers: list[str], label: str) -> str:
    normalized = normalize_csv_text(csv_text)
    reader = csv.DictReader(io.StringIO(normalized))
    fieldnames = reader.fieldnames or []
    missing_headers = [header for header in required_headers if header not in fieldnames]
    if missing_headers:
        raise RuntimeError(f"{label} CSV is missing columns: {', '.join(missing_headers)}")

    if not any(True for _ in reader):
        raise RuntimeError(f"{label} CSV did not contain any rows.")

    return normalized


def resolve_daily_snapshot(result: dict[str, Any]) -> dict[str, Any]:
    timestamps = result.get("timestamp") or []
    closes = (((result.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or []
    valid_points: list[tuple[int, float]] = []

    for timestamp, close in zip(timestamps, closes):
        if close is None:
            continue
        try:
            valid_points.append((int(timestamp), float(close)))
        except (TypeError, ValueError):
            continue

    latest_timestamp = valid_points[-1][0] if valid_points else None
    latest_close = valid_points[-1][1] if valid_points else None
    previous_close = valid_points[-2][1] if len(valid_points) > 1 else None

    meta = result.get("meta") or {}
    regular_market_price = meta.get("regularMarketPrice")
    if regular_market_price is not None:
        try:
            latest_close = float(regular_market_price)
        except (TypeError, ValueError):
            pass

    meta_previous_close = meta.get("previousClose")
    if meta_previous_close is not None:
        try:
            previous_close = float(meta_previous_close)
        except (TypeError, ValueError):
            pass

    return {
        "latest_price": latest_close,
        "previous_close": previous_close,
        "last_updated": to_iso8601_from_unix(latest_timestamp),
    }


def calculate_rrg_trail(sector_prices: list[float], benchmark_prices: list[float]) -> list[dict[str, float]]:
    length = min(len(sector_prices), len(benchmark_prices))
    if length < (RRG_RS_PERIOD + RRG_TRAIL_LENGTH + 5):
        return []

    rs_values: list[float] = []
    for index in range(length):
        benchmark_close = benchmark_prices[index]
        if benchmark_close == 0:
            continue
        rs_values.append(sector_prices[index] / benchmark_close)

    rs_average: list[float] = []
    for index in range(RRG_RS_PERIOD - 1, len(rs_values)):
        window = rs_values[index - RRG_RS_PERIOD + 1 : index + 1]
        rs_average.append(sum(window) / RRG_RS_PERIOD)

    rs_ratio: list[float] = []
    for index, average in enumerate(rs_average):
        if average == 0:
            continue
        raw_index = index + RRG_RS_PERIOD - 1
        rs_ratio.append((rs_values[raw_index] / average) * 100)

    trail: list[dict[str, float]] = []
    start_index = len(rs_ratio) - RRG_TRAIL_LENGTH
    for index in range(RRG_TRAIL_LENGTH):
        ratio_index = start_index + index
        if ratio_index < 1 or ratio_index >= len(rs_ratio):
            continue
        previous = rs_ratio[ratio_index - 1]
        if previous == 0:
            continue
        current = rs_ratio[ratio_index]
        trail.append(
            {
                "rsRatio": round(current, 2),
                "rsMomentum": round((current / previous) * 100, 2),
            }
        )
    return trail


def build_yahoo_snapshots_payload(timeout: int) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    symbols = sorted({item["symbol"] for item in MARKET_SYMBOLS} | set(YAHOO_SNAPSHOT_SYMBOLS))
    charts: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []

    for symbol in symbols:
        try:
            result = fetch_yahoo_chart(symbol, timeout=timeout, interval="1d", data_range="1y")
            charts[symbol] = build_yahoo_chart_response(result)
        except RuntimeError as error:
            warnings.append(f"{symbol}: {error}")

    if not charts:
        raise RuntimeError("No Yahoo chart snapshots were fetched.")

    payload: dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "source": {
            "provider": "Yahoo Finance",
            "endpoint": YAHOO_CHART_ENDPOINT,
            "interval": "1d",
            "range": "1y",
        },
        "charts": charts,
    }
    if warnings:
        payload["warnings"] = warnings
    return payload, charts


def build_rrg_payload(charts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    benchmark_payload = charts.get(RRG_BENCHMARK, {})
    benchmark_result = ((benchmark_payload.get("chart") or {}).get("result") or [None])[0]
    if not isinstance(benchmark_result, dict):
        raise RuntimeError("Benchmark data for RRG is missing.")

    benchmark_prices = extract_closes(benchmark_result)
    if not benchmark_prices:
        raise RuntimeError("Benchmark closes for RRG are missing.")

    sectors: list[dict[str, Any]] = []
    warnings: list[str] = []

    for name, symbol in RRG_SECTORS:
        sector_payload = charts.get(symbol, {})
        sector_result = ((sector_payload.get("chart") or {}).get("result") or [None])[0]
        if not isinstance(sector_result, dict):
            warnings.append(f"{symbol}: chart is missing")
            continue

        trail = calculate_rrg_trail(extract_closes(sector_result), benchmark_prices)
        if not trail:
            warnings.append(f"{symbol}: insufficient trail data")
            continue

        sectors.append({"name": name, "symbol": symbol, "trail": trail})

    if not sectors:
        raise RuntimeError("No RRG sector data could be calculated from Yahoo snapshots.")

    payload: dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "benchmark": RRG_BENCHMARK,
        "sectors": sectors,
        "lastUpdated": utc_now_iso(),
    }
    if warnings:
        payload["warnings"] = warnings
    return payload


def build_vkospi_payload(timeout: int, *, data_range: str = "1y") -> dict[str, Any]:
    end_date = datetime.now(SEOUL_TZ)
    fromdate = format_seoul_date(resolve_krx_range_start(data_range, end_date))
    todate = format_seoul_date(end_date)
    opener = build_opener(HTTPCookieProcessor(CookieJar()))

    page_request = Request(
        KRX_VKOSPI_SOURCE_URL,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": USER_AGENT,
        },
    )
    with open_request(page_request, timeout=timeout, opener=opener) as response:
        status = getattr(response, "status", response.getcode())
        if status != 200:
            raise RuntimeError(f"KRX page request failed ({status})")
        response.read()

    otp = read_text(
        KRX_OTP_URL,
        timeout,
        accept="text/plain, */*",
        headers={
            **KRX_BASE_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        },
        data=urlencode({"name": "form", "bld": KRX_BLD_PATH}).encode("utf-8"),
        opener=opener,
    ).strip()

    if not otp:
        raise RuntimeError("KRX OTP response was empty.")

    payload = read_json(
        KRX_DATA_URL,
        timeout,
        headers={
            **KRX_BASE_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        },
        data=urlencode(
            {
                "idx_cd": "1300",
                "ind_tp_cd": "1",
                "idx_ind_cd": "300",
                "add_data_yn": "",
                "bz_dd": "",
                "fromdate": fromdate,
                "todate": todate,
                "code": otp,
            }
        ).encode("utf-8"),
        opener=opener,
    )

    output = payload.get("output")
    if not isinstance(output, list) or not output:
        raise RuntimeError("Invalid KRX VKOSPI payload.")

    return {
        "generated_at": utc_now_iso(),
        "sourceUrl": KRX_VKOSPI_SOURCE_URL,
        "fromdate": fromdate,
        "todate": todate,
        "output": output,
    }


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
        snapshot = resolve_daily_snapshot(raw)
        price = snapshot["latest_price"]
        previous_close = snapshot["previous_close"]
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
                "as_of": snapshot["last_updated"] or to_iso8601_from_unix(meta.get("regularMarketTime")),
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


def fear_greed_rating(score: int) -> str:
    if score <= 25:
        return "Extreme Fear"
    if score <= 45:
        return "Fear"
    if score <= 55:
        return "Neutral"
    if score <= 75:
        return "Greed"
    return "Extreme Greed"


def get_chart_result(charts: dict[str, dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    payload = charts.get(symbol, {})
    result = ((payload.get("chart") or {}).get("result") or [None])[0]
    return result if isinstance(result, dict) else None


def simple_return(points: list[dict[str, Any]], lookback: int) -> float | None:
    if len(points) <= lookback:
        return None
    latest_close = points[-1]["close"]
    previous_close = points[-(lookback + 1)]["close"]
    if previous_close == 0:
        return None
    return (latest_close / previous_close - 1) * 100


def build_cnn_fear_greed_fallback(charts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    spy_result = get_chart_result(charts, "SPY")
    rsp_result = get_chart_result(charts, "RSP")
    tlt_result = get_chart_result(charts, "TLT")
    vix_result = get_chart_result(charts, "^VIX")
    if not all((spy_result, rsp_result, tlt_result, vix_result)):
        raise RuntimeError("CNN fallback calculation requires SPY, RSP, TLT, and ^VIX snapshots.")

    spy_points = extract_series_points(spy_result)
    rsp_points = extract_series_points(rsp_result)
    tlt_points = extract_series_points(tlt_result)
    vix_points = extract_series_points(vix_result)
    if min(len(spy_points), len(rsp_points), len(tlt_points), len(vix_points)) < 126:
        raise RuntimeError("CNN fallback calculation needs one year of Yahoo history.")

    spy_closes = [point["close"] for point in spy_points]
    spy_sma_125 = rolling_average(spy_closes, 125)[-1]
    if spy_sma_125 is None or spy_sma_125 == 0:
        raise RuntimeError("CNN fallback calculation could not derive SPY momentum.")

    spy_return_20 = simple_return(spy_points, 20)
    rsp_return_20 = simple_return(rsp_points, 20)
    tlt_return_20 = simple_return(tlt_points, 20)
    vix_level = vix_points[-1]["close"]
    if None in (spy_return_20, rsp_return_20, tlt_return_20):
        raise RuntimeError("CNN fallback calculation could not derive 20-day returns.")

    momentum_score = scaled_score((spy_points[-1]["close"] / spy_sma_125 - 1) * 100, -8, 8)
    breadth_score = scaled_score(rsp_return_20 - spy_return_20, -5, 5)
    safe_haven_score = scaled_score(spy_return_20 - tlt_return_20, -10, 10)
    volatility_score = 100 - scaled_score(vix_level, 12, 35)

    score = round((momentum_score + breadth_score + safe_haven_score + volatility_score) / 4)
    rating = fear_greed_rating(score)
    return {
        "fear_and_greed": {
            "score": score,
            "rating": rating,
        },
        "generated_at": utc_now_iso(),
        "source": {
            "provider": "Yahoo Finance fallback",
            "fallback_for": "CNN Fear & Greed",
        },
        "components": {
            "momentum": round(momentum_score, 2),
            "breadth": round(breadth_score, 2),
            "safe_haven": round(safe_haven_score, 2),
            "volatility": round(volatility_score, 2),
        },
    }


def build_cnn_fear_greed_payload(timeout: int, charts: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    try:
        payload = read_json(CNN_FEAR_GREED_URL, timeout)
        if not isinstance(payload, dict):
            raise RuntimeError("CNN Fear & Greed payload is not a JSON object.")

        fear_and_greed = payload.get("fear_and_greed")
        if not isinstance(fear_and_greed, dict) or fear_and_greed.get("score") is None:
            raise RuntimeError("CNN Fear & Greed payload does not contain a score.")

        payload["generated_at"] = utc_now_iso()
        return payload
    except (HTTPError, URLError, json.JSONDecodeError, RuntimeError):
        if charts is None:
            raise
        return build_cnn_fear_greed_fallback(charts)


def build_fred_csv_payload(
    series_id: str,
    timeout: int,
    *,
    retry_attempts: int = DEFAULT_FRED_RETRY_ATTEMPTS,
    retry_delay: int = DEFAULT_FRED_RETRY_DELAY,
) -> str:
    last_error: BaseException | None = None

    for attempt in range(1, retry_attempts + 1):
        print(
            f"info: fetching FRED series_id={series_id} attempt={attempt}/{retry_attempts}",
            file=sys.stderr,
        )
        try:
            csv_text = read_text(
                fred_series_url(series_id),
                timeout,
                accept="text/csv, text/plain, */*",
            )
            rows: list[list[str]] = []

            for line in csv_text.strip().splitlines()[1:]:
                parts = [part.strip() for part in line.split(",", 1)]
                if len(parts) != 2:
                    continue
                date_text, value_text = parts
                if not date_text or value_text in ("", "."):
                    continue
                try:
                    float(value_text)
                except ValueError:
                    continue
                rows.append([date_text, value_text])

            if not rows:
                raise RuntimeError(f"FRED response for {series_id} did not contain numeric rows.")

            return csv_rows_to_text(["DATE", "VALUE"], rows)
        except (HTTPError, URLError, RuntimeError, TimeoutError, socket.timeout) as error:
            last_error = error
            if attempt >= retry_attempts or not is_retryable_fetch_error(error):
                break
            print(
                f"warning: FRED series_id={series_id} attempt={attempt}/{retry_attempts} failed: {error}",
                file=sys.stderr,
            )
            time.sleep(retry_delay * attempt)

    raise RuntimeError(
        f"FRED fetch failed for {series_id} after {retry_attempts} attempt(s): {last_error}"
    ) from last_error


def build_breadth_payloads(timeout: int) -> tuple[str, str]:
    history_csv = validate_csv_text(
        read_text(
            TRADERMONTY_BREADTH_HISTORY_URL,
            timeout,
            accept="text/csv, text/plain, */*",
        ),
        [
            "Date",
            "Breadth_Index_200MA",
            "Breadth_Index_8MA",
            "Breadth_200MA_Trend",
            "Bearish_Signal",
            "Is_Peak",
            "Is_Trough",
            "Is_Trough_8MA_Below_04",
            "Breadth_50_Index_50MA",
            "Breadth_50_Index_8MA",
            "Breadth_50_MA_Trend",
            "Bearish_Signal_50",
            "Is_Peak_50",
            "Is_Trough_50",
        ],
        "Breadth history",
    )
    summary_csv = validate_csv_text(
        read_text(
            TRADERMONTY_BREADTH_SUMMARY_URL,
            timeout,
            accept="text/csv, text/plain, */*",
        ),
        ["Metric", "Value"],
        "Breadth summary",
    )
    return history_csv, summary_csv


def persist_payload(builder_name: str, builder: Any, output_path: Path) -> tuple[bool, str | None]:
    try:
        payload = builder()
        write_json(output_path, payload)
        return True, None
    except (HTTPError, URLError, ET.ParseError, json.JSONDecodeError, RuntimeError) as error:
        if output_path.exists():
            return False, f"{builder_name} update failed, existing file kept: {error}"
        return False, f"{builder_name} update failed and no existing file is available: {error}"


def persist_text_payload(builder_name: str, builder: Any, output_path: Path) -> tuple[bool, str | None]:
    try:
        content = builder()
        write_text(output_path, content)
        return True, None
    except (HTTPError, URLError, ET.ParseError, json.JSONDecodeError, RuntimeError, ValueError) as error:
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
        "--fred-retry-attempts",
        type=int,
        default=int(os.getenv("FETCH_DATA_FRED_RETRY_ATTEMPTS", DEFAULT_FRED_RETRY_ATTEMPTS)),
        help="Maximum retry attempts for FRED CSV downloads.",
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
    yahoo_charts_path = output_dir / "yahoo-charts.json"
    vkospi_path = output_dir / "vkospi.json"
    rrg_path = output_dir / "rrg.json"
    cnn_fear_greed_path = output_dir / "cnn-fear-greed.json"
    fred_dir = output_dir / "fred"
    breadth_dir = output_dir / "breadth"
    fred_paths = {series_id: fred_dir / f"{series_id}.csv" for series_id in FRED_SERIES_IDS}
    breadth_history_path = breadth_dir / "market_breadth_data.csv"
    breadth_summary_path = breadth_dir / "market_breadth_summary.csv"

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

    for series_id, fred_path in fred_paths.items():
        fred_ok, fred_error = persist_text_payload(
            f"fred-{series_id.lower()}",
            lambda series_id=series_id: build_fred_csv_payload(
                series_id=series_id,
                timeout=args.timeout,
                retry_attempts=args.fred_retry_attempts,
            ),
            fred_path,
        )
        if fred_ok:
            successes.append(str(fred_path))
        elif fred_error:
            failures.append(fred_error)

    try:
        yahoo_snapshots_payload, yahoo_charts = build_yahoo_snapshots_payload(timeout=args.timeout)
        write_json(yahoo_charts_path, yahoo_snapshots_payload)
        successes.append(str(yahoo_charts_path))

        vkospi_payload = build_vkospi_payload(timeout=args.timeout)
        write_json(vkospi_path, vkospi_payload)
        successes.append(str(vkospi_path))

        cnn_payload = build_cnn_fear_greed_payload(timeout=args.timeout, charts=yahoo_charts)
        write_json(cnn_fear_greed_path, cnn_payload)
        successes.append(str(cnn_fear_greed_path))

        rrg_payload = build_rrg_payload(yahoo_charts)
        write_json(rrg_path, rrg_payload)
        successes.append(str(rrg_path))

        breadth_history_csv, breadth_summary_csv = build_breadth_payloads(timeout=args.timeout)
        write_text(breadth_history_path, breadth_history_csv)
        write_text(breadth_summary_path, breadth_summary_csv)
        successes.append(str(breadth_history_path))
        successes.append(str(breadth_summary_path))
    except (HTTPError, URLError, ET.ParseError, json.JSONDecodeError, RuntimeError) as error:
        missing_outputs = [
            path
            for path in (
                yahoo_charts_path,
                vkospi_path,
                cnn_fear_greed_path,
                rrg_path,
                breadth_history_path,
                breadth_summary_path,
            )
            if not path.exists()
        ]
        if missing_outputs:
            failures.append(
                "snapshot update failed and missing files remain unavailable: "
                + ", ".join(str(path) for path in missing_outputs)
                + f" ({error})"
            )
        else:
            failures.append(f"snapshot update failed, existing files kept: {error}")

    for path in successes:
        print(f"updated: {path}")
    for message in failures:
        print(f"warning: {message}", file=sys.stderr)

    required_paths = (
        market_path,
        news_path,
        yahoo_charts_path,
        vkospi_path,
        rrg_path,
        cnn_fear_greed_path,
        breadth_history_path,
        breadth_summary_path,
    )
    fred_available = any(path.exists() for path in fred_paths.values())
    return 0 if not failures or (all(path.exists() for path in required_paths) and fred_available) else 1


if __name__ == "__main__":
    raise SystemExit(main())
