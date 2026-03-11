#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
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
CNN_FEAR_GREED_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
FRED_GRAPH_ENDPOINT = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_SERIES_IDS = ("DGS10", "DGS2")
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


def read_json(url: str, timeout: int) -> Any:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def read_text(
    url: str,
    timeout: int,
    *,
    accept: str = "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": accept,
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


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def scaled_score(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        raise ValueError("upper must be greater than lower")
    normalized = (value - lower) / (upper - lower)
    return clamp(normalized, 0.0, 1.0) * 100


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


def build_fred_csv_payload(series_id: str, timeout: int) -> str:
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


def build_breadth_payloads(charts: dict[str, dict[str, Any]]) -> tuple[str, str]:
    series_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for symbol in BREADTH_UNIVERSE:
        chart_payload = charts.get(symbol, {})
        result = ((chart_payload.get("chart") or {}).get("result") or [None])[0]
        if not isinstance(result, dict):
            continue
        points = extract_series_points(result)
        if len(points) >= 200:
            series_by_symbol[symbol] = points

    if len(series_by_symbol) < 5:
        raise RuntimeError("Breadth universe does not have enough Yahoo history.")

    breadth_by_date: dict[str, dict[str, int]] = {}
    for points in series_by_symbol.values():
        closes = [point["close"] for point in points]
        ma50 = rolling_average(closes, 50)
        ma200 = rolling_average(closes, 200)

        for index, point in enumerate(points):
            bucket = breadth_by_date.setdefault(
                point["date"],
                {"above_50": 0, "count_50": 0, "above_200": 0, "count_200": 0},
            )
            close_value = point["close"]

            if ma50[index] is not None:
                bucket["count_50"] += 1
                if close_value > ma50[index]:
                    bucket["above_50"] += 1

            if ma200[index] is not None:
                bucket["count_200"] += 1
                if close_value > ma200[index]:
                    bucket["above_200"] += 1

    dates = sorted(breadth_by_date)
    breadth_200_values: list[float | None] = []
    breadth_50_values: list[float | None] = []
    for date_text in dates:
        bucket = breadth_by_date[date_text]
        breadth_200_values.append(
            (bucket["above_200"] / bucket["count_200"]) if bucket["count_200"] else None
        )
        breadth_50_values.append(
            (bucket["above_50"] / bucket["count_50"]) if bucket["count_50"] else None
        )

    breadth_8_values = rolling_average_optional(breadth_200_values, 8)
    breadth_50_8_values = rolling_average_optional(breadth_50_values, 8)

    history_rows: list[dict[str, Any]] = []
    for index, date_text in enumerate(dates):
        breadth_200 = breadth_200_values[index]
        breadth_8 = breadth_8_values[index]
        breadth_50 = breadth_50_values[index]
        breadth_50_8 = breadth_50_8_values[index]
        if None in (breadth_200, breadth_8, breadth_50, breadth_50_8):
            continue
        history_rows.append(
            {
                "Date": date_text,
                "Breadth_Index_200MA": breadth_200,
                "Breadth_Index_8MA": breadth_8,
                "Breadth_50_Index_50MA": breadth_50,
                "Breadth_50_Index_8MA": breadth_50_8,
            }
        )

    if len(history_rows) < 10:
        raise RuntimeError("Calculated breadth history is too short.")

    for index, row in enumerate(history_rows):
        previous_row = history_rows[index - 1] if index > 0 else None
        next_row = history_rows[index + 1] if index + 1 < len(history_rows) else None

        row["Bearish_Signal"] = (
            row["Breadth_Index_200MA"] <= 0.40 and row["Breadth_Index_8MA"] <= 0.45
        )
        row["Bearish_Signal_50"] = (
            row["Breadth_50_Index_50MA"] <= 0.40 and row["Breadth_50_Index_8MA"] <= 0.45
        )
        row["Breadth_200MA_Trend"] = (
            1
            if previous_row and row["Breadth_Index_200MA"] > previous_row["Breadth_Index_200MA"]
            else 0
        )
        row["Breadth_50_MA_Trend"] = (
            1
            if previous_row and row["Breadth_50_Index_50MA"] > previous_row["Breadth_50_Index_50MA"]
            else 0
        )
        row["Is_Peak"] = bool(
            previous_row
            and next_row
            and row["Breadth_Index_200MA"] >= previous_row["Breadth_Index_200MA"]
            and row["Breadth_Index_200MA"] > next_row["Breadth_Index_200MA"]
        )
        row["Is_Trough"] = bool(
            previous_row
            and next_row
            and row["Breadth_Index_200MA"] <= previous_row["Breadth_Index_200MA"]
            and row["Breadth_Index_200MA"] < next_row["Breadth_Index_200MA"]
        )
        row["Is_Trough_8MA_Below_04"] = row["Is_Trough"] and row["Breadth_Index_8MA"] < 0.40
        row["Is_Peak_50"] = bool(
            previous_row
            and next_row
            and row["Breadth_50_Index_50MA"] >= previous_row["Breadth_50_Index_50MA"]
            and row["Breadth_50_Index_50MA"] > next_row["Breadth_50_Index_50MA"]
        )
        row["Is_Trough_50"] = bool(
            previous_row
            and next_row
            and row["Breadth_50_Index_50MA"] <= previous_row["Breadth_50_Index_50MA"]
            and row["Breadth_50_Index_50MA"] < next_row["Breadth_50_Index_50MA"]
        )

    peaks_200 = [row["Breadth_Index_200MA"] for row in history_rows if row["Is_Peak"]]
    troughs_200 = [row["Breadth_Index_200MA"] for row in history_rows if row["Is_Trough_8MA_Below_04"]]
    latest_row = history_rows[-1]
    score = round(
        (
            latest_row["Breadth_Index_200MA"]
            + latest_row["Breadth_Index_8MA"]
            + latest_row["Breadth_50_Index_50MA"]
            + latest_row["Breadth_50_Index_8MA"]
        )
        / 4
        * 100
    )

    history_csv_rows = [
        [
            row["Date"],
            format_csv_number(row["Breadth_Index_200MA"]),
            format_csv_number(row["Breadth_Index_8MA"]),
            format_csv_number(row["Breadth_50_Index_50MA"]),
            format_csv_number(row["Breadth_50_Index_8MA"]),
            bool_to_csv(row["Bearish_Signal"]),
            bool_to_csv(row["Bearish_Signal_50"]),
            str(row["Breadth_200MA_Trend"]),
            str(row["Breadth_50_MA_Trend"]),
            bool_to_csv(row["Is_Peak"]),
            bool_to_csv(row["Is_Trough"]),
            bool_to_csv(row["Is_Trough_8MA_Below_04"]),
            bool_to_csv(row["Is_Peak_50"]),
            bool_to_csv(row["Is_Trough_50"]),
        ]
        for row in history_rows
    ]
    history_csv = csv_rows_to_text(
        [
            "Date",
            "Breadth_Index_200MA",
            "Breadth_Index_8MA",
            "Breadth_50_Index_50MA",
            "Breadth_50_Index_8MA",
            "Bearish_Signal",
            "Bearish_Signal_50",
            "Breadth_200MA_Trend",
            "Breadth_50_MA_Trend",
            "Is_Peak",
            "Is_Trough",
            "Is_Trough_8MA_Below_04",
            "Is_Peak_50",
            "Is_Trough_50",
        ],
        history_csv_rows,
    )

    summary_rows = [
        ["Score", str(score)],
        [
            "Average Peaks (200MA)",
            format_csv_number(sum(peaks_200) / len(peaks_200) if peaks_200 else None),
        ],
        [
            "Average Troughs (8MA < 0.4)",
            format_csv_number(sum(troughs_200) / len(troughs_200) if troughs_200 else None),
        ],
        ["Analysis Period Start", history_rows[0]["Date"]],
        ["Analysis Period End", history_rows[-1]["Date"]],
    ]
    summary_csv = csv_rows_to_text(["Metric", "Value"], summary_rows)

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
            lambda series_id=series_id: build_fred_csv_payload(series_id=series_id, timeout=args.timeout),
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

        cnn_payload = build_cnn_fear_greed_payload(timeout=args.timeout, charts=yahoo_charts)
        write_json(cnn_fear_greed_path, cnn_payload)
        successes.append(str(cnn_fear_greed_path))

        rrg_payload = build_rrg_payload(yahoo_charts)
        write_json(rrg_path, rrg_payload)
        successes.append(str(rrg_path))

        breadth_history_csv, breadth_summary_csv = build_breadth_payloads(yahoo_charts)
        write_text(breadth_history_path, breadth_history_csv)
        write_text(breadth_summary_path, breadth_summary_csv)
        successes.append(str(breadth_history_path))
        successes.append(str(breadth_summary_path))
    except (HTTPError, URLError, ET.ParseError, json.JSONDecodeError, RuntimeError) as error:
        missing_outputs = [
            path
            for path in (
                yahoo_charts_path,
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
        rrg_path,
        cnn_fear_greed_path,
        breadth_history_path,
        breadth_summary_path,
        *fred_paths.values(),
    )
    return 0 if not failures or all(path.exists() for path in required_paths) else 1


if __name__ == "__main__":
    raise SystemExit(main())
