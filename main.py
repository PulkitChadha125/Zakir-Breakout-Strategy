from __future__ import annotations

import csv
import datetime as dt
import json
import io
import threading
import time
from pathlib import Path
from typing import Any

import requests
from delta_rest_client import DeltaRestClient, OrderType
from flask import Flask, Response, flash, redirect, render_template, request, session, url_for

BASE_DIR = Path(__file__).resolve().parent
TRADE_SETTINGS_PATH = BASE_DIR / "TradeSettings.csv"
CREDENTIALS_PATH = BASE_DIR / "credentials.csv"
DATA_DIR = BASE_DIR / "data"
ORDER_LOG_PATH = BASE_DIR / "OrderLog.csv"
APP_LOG_PATH = BASE_DIR / "AppLog.csv"
STATE_PATH = BASE_DIR / "strategy_state.json"
DELTA_BASE_URL = "https://api.india.delta.exchange"
SCHEDULER_STATE = {
    "thread": None,
    "stop_event": threading.Event(),
    "lock": threading.Lock(),
}
STATE_LOCK = threading.Lock()
ORDER_LOG_FIELDS = [
    "timestamp",
    "symbol",
    "side",
    "entry_price",
    "stop_loss",
    "target_price",
    "status",
    "exit_price",
    "exit_time",
    "reason",
    "api_request",
    "api_response",
]

CSV_COLUMNS = [
    "Symbol",
    "Quantity",
    "Timeframe",
    "MaxProfit",
    "MaxLoss",
    "EnableTrading",
]


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "delta-ema-sma-local-ui"

    ensure_trade_settings_file()

    @app.route("/")
    def index() -> str:
        active_tab = request.args.get("tab", "symbols")
        if active_tab not in {"symbols", "net-positions", "order-log", "app-log"}:
            active_tab = "symbols"
        symbols = load_trade_settings()
        is_logged_in = session.get("broker_logged_in", False)
        net_positions: list[dict[str, Any]] = []
        if active_tab == "net-positions" and is_logged_in:
            net_positions = format_live_positions_for_ui(fetch_margined_positions_list())
        sort_order = request.args.get("sort", "newest")
        newest_first = sort_order != "oldest"
        raw_order_logs = load_order_logs(limit=300, newest_first=newest_first)
        order_logs = format_order_logs_for_ui(raw_order_logs)
        app_logs = load_app_logs(limit=200, newest_first=True)
        return render_template(
            "index.html",
            symbols=symbols,
            is_logged_in=is_logged_in,
            net_positions=net_positions,
            order_logs=order_logs,
            app_logs=app_logs,
            sort_order=sort_order,
            active_tab=active_tab,
        )

    @app.get("/order-log/download")
    def download_order_log() -> Response:
        ensure_order_log_file()
        with ORDER_LOG_PATH.open(mode="r", encoding="utf-8") as csv_file:
            content = csv_file.read()
        return Response(
            content,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=OrderLog.csv"},
        )

    @app.post("/app-log/clear")
    def clear_app_log() -> Any:
        ensure_app_log_file(reset=True)
        flash("Application logs cleared.", "success")
        return redirect(url_for("index", tab="app-log"))

    @app.post("/positions/close")
    def close_position() -> Any:
        if not session.get("broker_logged_in", False):
            flash("Please login first.", "error")
            return redirect(url_for("index", tab="net-positions"))

        symbol = (request.form.get("symbol") or "").strip().upper()
        if not symbol:
            flash("Symbol is required to close position.", "error")
            return redirect(url_for("index", tab="net-positions"))

        positions_by_symbol = fetch_margined_positions_by_symbol()
        position = positions_by_symbol.get(symbol)
        if not position:
            flash(f"No running position found for {symbol}.", "error")
            return redirect(url_for("index", tab="net-positions"))

        status, api_request, api_response = square_off_position(symbol, position)
        if status == "SQUARE_OFF_SENT":
            broker_log(f"[Broker] Manual square-off sent for {symbol}.")
            flash(f"Close order sent for {symbol}.", "success")
        else:
            broker_log(f"[Broker] Manual square-off failed for {symbol}: {api_response}", level="ERROR")
            flash(f"Failed to close {symbol}.", "error")

        return redirect(url_for("index", tab="net-positions"))

    @app.post("/broker/run")
    def broker_login_and_start() -> Any:
        try:
            broker_log("[Broker] Logging in to Delta API...")
            credentials = load_credentials()
            client = DeltaRestClient(
                base_url=DELTA_BASE_URL,
                api_key=credentials["key"],
                api_secret=credentials["secret"],
            )
            client.get_assets(auth=True)
            session["broker_logged_in"] = True
            flash("Logged in to Delta broker API successfully.", "success")
            broker_log("[Broker] Login successful.")
        except Exception as exc:
            session["broker_logged_in"] = False
            flash(f"Broker login failed: {exc}", "error")
            broker_log(f"[Broker] Login failed: {exc}", level="ERROR")
            return redirect(url_for("index"))

        started = start_scheduler_if_needed()
        if started:
            flash("Login successful. Scheduler started for next candle close.", "success")
        else:
            flash("Login successful. Scheduler is already running.", "success")

        return redirect(url_for("index"))

    @app.post("/symbols/load")
    def load_symbols() -> Any:
        flash("Symbol settings loaded from TradeSettings.csv", "success")
        return redirect(url_for("index"))

    @app.post("/symbols/add")
    def add_symbol() -> Any:
        symbol = parse_symbol_payload(request.form)
        if not symbol["Symbol"]:
            flash("Symbol is required.", "error")
            return redirect(url_for("index"))

        symbols = load_trade_settings()
        symbols.append(symbol)
        save_trade_settings(symbols)
        flash(f"Added {symbol['Symbol']}.", "success")
        return redirect(url_for("index"))

    @app.post("/symbols/update/<int:row_id>")
    def update_symbol(row_id: int) -> Any:
        symbols = load_trade_settings()
        if not is_valid_row(symbols, row_id):
            flash("Invalid row selected.", "error")
            return redirect(url_for("index"))

        symbols[row_id] = parse_symbol_payload(request.form)
        save_trade_settings(symbols)
        flash(f"Updated {symbols[row_id]['Symbol']}.", "success")
        return redirect(url_for("index"))

    @app.post("/symbols/trading/<int:row_id>")
    def toggle_trading(row_id: int) -> Any:
        symbols = load_trade_settings()
        if not is_valid_row(symbols, row_id):
            flash("Invalid row selected.", "error")
            return redirect(url_for("index"))

        is_enabled = symbols[row_id]["EnableTrading"].strip().upper() == "TRUE"
        symbols[row_id]["EnableTrading"] = "FALSE" if is_enabled else "TRUE"
        save_trade_settings(symbols)
        if symbols[row_id]["EnableTrading"] == "TRUE":
            reset_symbol_state(symbols[row_id]["Symbol"])
        state = "enabled" if symbols[row_id]["EnableTrading"] == "TRUE" else "disabled"
        flash(f"Trading {state} for {symbols[row_id]['Symbol']}.", "success")
        return redirect(url_for("index"))

    @app.post("/symbols/delete/<int:row_id>")
    def delete_symbol(row_id: int) -> Any:
        symbols = load_trade_settings()
        if not is_valid_row(symbols, row_id):
            flash("Invalid row selected.", "error")
            return redirect(url_for("index"))

        removed = symbols.pop(row_id)
        save_trade_settings(symbols)
        flash(f"Deleted {removed['Symbol']}.", "success")
        return redirect(url_for("index"))

    return app


def is_valid_row(symbols: list[dict[str, str]], row_id: int) -> bool:
    return 0 <= row_id < len(symbols)


def ensure_trade_settings_file() -> None:
    if TRADE_SETTINGS_PATH.exists():
        return
    save_trade_settings([])


def load_credentials() -> dict[str, str]:
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError("credentials.csv not found")

    credentials: dict[str, str] = {}
    with CREDENTIALS_PATH.open(mode="r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            title = (row.get("Title") or "").strip().lower()
            value = (row.get("Value") or "").strip()
            if title:
                credentials[title] = value

    if not credentials.get("key") or not credentials.get("secret"):
        raise ValueError("credentials.csv must contain key and secret")
    return credentials


def log_app_event(level: str, message: str) -> None:
    ensure_app_log_file()
    with APP_LOG_PATH.open(mode="a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["datetime", "level", "message"])
        writer.writerow(
            {
                "datetime": dt.datetime.now(dt.timezone.utc).isoformat(),
                "level": level.upper(),
                "message": message,
            }
        )


def broker_log(message: str, level: str = "INFO") -> None:
    print(message, flush=True)
    log_app_event(level, message)


def start_scheduler_if_needed() -> bool:
    with SCHEDULER_STATE["lock"]:
        running_thread = SCHEDULER_STATE["thread"]
        if running_thread and running_thread.is_alive():
            return False

        SCHEDULER_STATE["stop_event"].clear()
        new_thread = threading.Thread(
            target=run_symbol_fetch_scheduler,
            name="delta-ohlc-scheduler",
            daemon=True,
        )
        SCHEDULER_STATE["thread"] = new_thread
        new_thread.start()
        broker_log("[Broker] OHLC scheduler thread started.")
        return True


def run_symbol_fetch_scheduler() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    next_run_by_key: dict[str, int] = {}
    stop_event = SCHEDULER_STATE["stop_event"]

    while not stop_event.is_set():
        now_dt = dt.datetime.now(dt.timezone.utc)
        now_ts = int(now_dt.timestamp())
        enabled_symbols = [row for row in load_trade_settings() if row["EnableTrading"] == "TRUE"]

        if not enabled_symbols:
            broker_log("[Broker] Scheduler idle: no enabled symbols.")
            stop_event.wait(3)
            continue

        valid_keys = set()
        earliest_next_run: int | None = None
        positions_by_symbol = fetch_margined_positions_by_symbol()

        for row in enabled_symbols:
            symbol = row["Symbol"]
            try:
                timeframe_minutes = int(float(row["Timeframe"]))
                if timeframe_minutes <= 0:
                    raise ValueError("Timeframe should be a positive number")
                resolution = timeframe_to_resolution(timeframe_minutes)
                key = f"{symbol}:{timeframe_minutes}"
                valid_keys.add(key)

                scheduled_ts = next_run_by_key.get(key)
                if scheduled_ts is None:
                    scheduled_ts = int(next_candle_start(now_dt, timeframe_minutes).timestamp())
                    next_run_by_key[key] = scheduled_ts
                    broker_log(
                        f"[Broker] First run scheduled for {symbol} ({timeframe_minutes}m) at "
                        f"{dt.datetime.fromtimestamp(scheduled_ts, tz=dt.timezone.utc).isoformat()}"
                    )

                if now_ts >= scheduled_ts:
                    start_ts = scheduled_ts - (timeframe_minutes * 60 * 200)
                    end_ts = scheduled_ts
                    broker_log(
                        f"[Broker] Fetching data for symbol={symbol}, timeframe={timeframe_minutes}m, "
                        f"resolution={resolution}, start={start_ts}, end={end_ts}"
                    )
                    candles = fetch_ohlc_candles(
                        symbol=symbol,
                        resolution=resolution,
                        start_ts=start_ts,
                        end_ts=end_ts,
                    )
                    save_symbol_candles(symbol=symbol, candles=candles)
                    broker_log(f"[Broker] Saved {len(candles)} candles to data/{symbol}.csv")
                    process_strategy_for_symbol(row, candles, scheduled_ts, positions_by_symbol.get(symbol))
                    next_run_by_key[key] = scheduled_ts + (timeframe_minutes * 60)
                    scheduled_ts = next_run_by_key[key]
                if earliest_next_run is None or scheduled_ts < earliest_next_run:
                    earliest_next_run = scheduled_ts
            except Exception as exc:
                broker_log(f"[Broker] Scheduler error for {symbol}: {exc}", level="ERROR")

        stale_keys = [key for key in next_run_by_key if key not in valid_keys]
        for key in stale_keys:
            del next_run_by_key[key]

        if earliest_next_run is None:
            stop_event.wait(2)
            continue

        sleep_seconds = max(1, min(30, earliest_next_run - int(dt.datetime.now(dt.timezone.utc).timestamp())))
        stop_event.wait(sleep_seconds)


def timeframe_to_resolution(timeframe_minutes: int) -> str:
    supported = {
        1: "1m",
        3: "3m",
        5: "5m",
        15: "15m",
        30: "30m",
        60: "1h",
        120: "2h",
        240: "4h",
        360: "6h",
        1440: "1d",
        10080: "1w",
    }
    if timeframe_minutes not in supported:
        raise ValueError(f"Unsupported timeframe: {timeframe_minutes} min")
    return supported[timeframe_minutes]


def next_candle_start(now: dt.datetime, timeframe_minutes: int) -> dt.datetime:
    interval_seconds = timeframe_minutes * 60
    now_ts = int(now.timestamp())
    floored_ts = now_ts - (now_ts % interval_seconds)
    return dt.datetime.fromtimestamp(floored_ts + interval_seconds, tz=dt.timezone.utc)


def fetch_ohlc_candles(symbol: str, resolution: str, start_ts: int, end_ts: int) -> list[dict[str, Any]]:
    response = requests.get(
        f"{DELTA_BASE_URL}/v2/history/candles",
        params={
            "resolution": resolution,
            "symbol": symbol,
            "start": start_ts,
            "end": end_ts,
        },
        timeout=(5, 15),
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError(payload.get("error", "Delta returned unsuccessful response"))
    return payload.get("result", [])


def create_delta_client() -> DeltaRestClient:
    credentials = load_credentials()
    return DeltaRestClient(
        base_url=DELTA_BASE_URL,
        api_key=credentials["key"],
        api_secret=credentials["secret"],
    )


def fetch_margined_positions_list() -> list[dict[str, Any]]:
    try:
        client = create_delta_client()
        response = client.request("GET", "/v2/positions/margined", auth=True)
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError(payload.get("error", "positions api unsuccessful"))
        result = payload.get("result", [])
        return result if isinstance(result, list) else []
    except Exception as exc:
        broker_log(f"[Broker] Failed to fetch margined positions: {exc}", level="ERROR")
        return []


def fetch_margined_positions_by_symbol() -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for pos in fetch_margined_positions_list():
        symbol = pos.get("product_symbol")
        if symbol:
            mapped[str(symbol)] = pos
    return mapped


def extract_realized_pnl(position: dict[str, Any] | None) -> float | None:
    if not position:
        return None
    value = position.get("realized_pnl")
    if value is None and isinstance(position.get("raw"), dict):
        value = position["raw"].get("realized_pnl")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_live_positions_for_ui(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    formatted: list[dict[str, Any]] = []
    for pos in positions:
        try:
            size = float(pos.get("size", 0) or 0)
        except (TypeError, ValueError):
            size = 0.0
        if size == 0:
            continue
        side = "LONG" if size > 0 else "SHORT"
        formatted.append(
            {
                "symbol": pos.get("product_symbol", ""),
                "size": abs(size),
                "side": side,
                "entry_price": pos.get("entry_price") or pos.get("avg_price") or "",
                "realized_pnl": extract_realized_pnl(pos),
                "unrealized_pnl": extract_unrealized_pnl(pos),
            }
        )
    return formatted


def extract_unrealized_pnl(position: dict[str, Any] | None) -> float | None:
    if not position:
        return None
    value = position.get("unrealized_pnl")
    if value is None and isinstance(position.get("raw"), dict):
        value = position["raw"].get("unrealized_pnl")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def square_off_position(symbol: str, position: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    product_id = position.get("product_id")
    size_value = position.get("size", 0)
    size = abs(int(float(size_value)))
    request_payload = {
        "product_id": int(product_id) if product_id else None,
        "size": size,
        "side": "sell" if float(size_value) > 0 else "buy",
        "order_type": OrderType.MARKET.value,
        "reduce_only": "true",
    }
    if not product_id or size <= 0:
        return "NO_OPEN_POSITION", request_payload, {"success": False, "error": "No open position size"}

    client = create_delta_client()
    try:
        response = client.request("POST", "/v2/orders", payload=request_payload, auth=True)
        response_payload = response.json()
        return "SQUARE_OFF_SENT", request_payload, response_payload
    except Exception as exc:
        return "SQUARE_OFF_FAILED", request_payload, {"success": False, "error": str(exc)}


def save_symbol_candles(symbol: str, candles: list[dict[str, Any]]) -> None:
    output_path = DATA_DIR / f"{symbol}.csv"
    with output_path.open(mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["time", "time_human", "time_iso8601", "open", "high", "low", "close", "volume"],
        )
        writer.writeheader()
        for candle in candles:
            raw_ts = candle.get("time")
            if raw_ts is not None:
                ts_utc = dt.datetime.fromtimestamp(int(raw_ts), tz=dt.timezone.utc)
                time_human = ts_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
                time_iso8601 = ts_utc.isoformat()
            else:
                time_human = ""
                time_iso8601 = ""
            writer.writerow(
                {
                    "time": raw_ts,
                    "time_human": time_human,
                    "time_iso8601": time_iso8601,
                    "open": candle.get("open"),
                    "high": candle.get("high"),
                    "low": candle.get("low"),
                    "close": candle.get("close"),
                    "volume": candle.get("volume", 0),
                }
            )


def get_current_ltp(symbol: str) -> float:
    response = requests.get(
        f"{DELTA_BASE_URL}/v2/tickers/{symbol}",
        timeout=(5, 10),
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError(payload.get("error", "Unable to fetch ticker"))
    ticker = payload.get("result", {})
    return float(ticker.get("close"))


def process_strategy_for_symbol(
    row: dict[str, str],
    candles: list[dict[str, Any]],
    cycle_ts: int,
    live_position: dict[str, Any] | None,
) -> None:
    symbol = row["Symbol"]
    if len(candles) < 2:
        broker_log(f"[Strategy] Skipping {symbol}: insufficient candles.")
        return

    state = get_symbol_state(symbol)
    timeframe_minutes = int(float(row.get("Timeframe", "1") or 1))
    cooldown_until = int(state.get("cooldown_until", 0) or 0)
    if cycle_ts < cooldown_until:
        broker_log(f"[Strategy] Cooldown active for {symbol} until {cooldown_until}.")
        return

    unrealized = extract_unrealized_pnl(live_position)
    max_profit = float(row.get("MaxProfit", "0") or 0)
    max_loss = float(row.get("MaxLoss", "0") or 0)
    open_trade = state.get("open_trade")

    if open_trade and not live_position:
        broker_log(f"[Strategy] Open trade tracked for {symbol} but no live position found yet.")
        return

    if live_position and open_trade and unrealized is not None:
        profit_hit = max_profit > 0 and unrealized >= max_profit
        loss_hit = max_loss > 0 and unrealized <= (-1 * max_loss)
        if profit_hit or loss_hit:
            reason = "TARGET" if profit_hit else "STOP_LOSS"
            broker_log(
                f"[Strategy] PnL hit for {symbol}. unrealized={unrealized}, "
                f"MaxProfit={max_profit}, MaxLoss={max_loss}. Sending square-off."
            )
            status, api_request, api_response = square_off_position(symbol, live_position)
            broker_log(f"[Strategy] Square-off status for {symbol}: {status}")
            finalize_trade_after_square_off(
                symbol,
                state,
                open_trade,
                reason,
                unrealized,
                cycle_ts,
                timeframe_minutes,
                api_request=api_request,
                api_response=api_response,
            )
        return

    trigger_candle = candles[-2]
    ltp = get_current_ltp(symbol)
    entry_side, trigger_upper, trigger_lower = determine_entry_side(trigger_candle, ltp)
    if entry_side is None:
        broker_log(f"[Strategy] No entry for {symbol}. LTP={ltp}")
        return

    stop_loss = float(trigger_candle["low"]) if entry_side == "BUY" else float(trigger_candle["high"])
    target_price = None
    if max_profit > 0:
        target_price = ltp + max_profit if entry_side == "BUY" else ltp - max_profit

    trade = {
        "symbol": symbol,
        "side": entry_side,
        "entry_price": ltp,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "trigger_time": trigger_candle.get("time"),
        "trigger_upper": trigger_upper,
        "trigger_lower": trigger_lower,
        "entry_time": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    state["open_trade"] = trade
    save_symbol_state(symbol, state)
    append_order_log(
        {
            "timestamp": trade["entry_time"],
            "symbol": symbol,
            "side": entry_side,
            "entry_price": trade["entry_price"],
            "stop_loss": trade["stop_loss"],
            "target_price": trade["target_price"] if trade["target_price"] is not None else "",
            "status": "OPEN",
            "exit_price": "",
            "exit_time": "",
            "reason": "Breakout Entry",
        }
    )
    broker_log(
        f"[Strategy] ENTRY {symbol} {entry_side} at {ltp}. Trigger range=({trigger_lower}, {trigger_upper}), "
        f"SL={stop_loss}, Target={target_price}"
    )


def determine_entry_side(trigger_candle: dict[str, Any], ltp: float) -> tuple[str | None, float, float]:
    open_price = float(trigger_candle["open"])
    close_price = float(trigger_candle["close"])
    upper = max(open_price, close_price)
    lower = min(open_price, close_price)

    if ltp > upper:
        return "BUY", upper, lower
    if ltp < lower:
        return "SELL", upper, lower
    return None, upper, lower


def finalize_trade_after_square_off(
    symbol: str,
    state: dict[str, Any],
    trade: dict[str, Any],
    reason: str,
    exit_proxy_price: float,
    cycle_ts: int,
    timeframe_minutes: int,
    api_request: dict[str, Any] | None = None,
    api_response: dict[str, Any] | None = None,
) -> None:
    state["open_trade"] = None
    state["cooldown_until"] = cycle_ts + (timeframe_minutes * 60)
    if reason == "STOP_LOSS":
        state["sl_count"] = int(state.get("sl_count", 0)) + 1
    else:
        state["sl_count"] = 0
    save_symbol_state(symbol, state)

    append_order_log(
        {
            "timestamp": trade["entry_time"],
            "symbol": symbol,
            "side": trade["side"],
            "entry_price": trade["entry_price"],
            "stop_loss": trade["stop_loss"],
            "target_price": trade["target_price"] if trade["target_price"] is not None else "",
            "status": "CLOSED",
            "exit_price": exit_proxy_price,
            "exit_time": dt.datetime.now(dt.timezone.utc).isoformat(),
            "reason": reason,
            "api_request": json.dumps(api_request or {}),
            "api_response": json.dumps(api_response or {}),
        }
    )
    broker_log(f"[Strategy] EXIT {symbol} at {exit_proxy_price}. Reason={reason}")

    if int(state.get("sl_count", 0)) >= 2:
        disable_symbol_trading(symbol)
        broker_log(f"[Strategy] Disabled {symbol} after 2 consecutive stop losses.", level="WARNING")


def disable_symbol_trading(symbol: str) -> None:
    symbols = load_trade_settings()
    changed = False
    for row in symbols:
        if row["Symbol"] == symbol and row["EnableTrading"] == "TRUE":
            row["EnableTrading"] = "FALSE"
            changed = True
    if changed:
        save_trade_settings(symbols)


def default_symbol_state() -> dict[str, Any]:
    return {"sl_count": 0, "open_trade": None, "cooldown_until": 0}


def load_strategy_state() -> dict[str, Any]:
    with STATE_LOCK:
        if not STATE_PATH.exists():
            return {}
        with STATE_PATH.open(mode="r", encoding="utf-8") as state_file:
            try:
                payload = json.load(state_file)
                if isinstance(payload, dict):
                    return payload
            except json.JSONDecodeError:
                return {}
    return {}


def save_strategy_state(payload: dict[str, Any]) -> None:
    with STATE_LOCK:
        with STATE_PATH.open(mode="w", encoding="utf-8") as state_file:
            json.dump(payload, state_file, indent=2)


def get_symbol_state(symbol: str) -> dict[str, Any]:
    state = load_strategy_state()
    return state.get(symbol, default_symbol_state())


def save_symbol_state(symbol: str, symbol_state: dict[str, Any]) -> None:
    state = load_strategy_state()
    state[symbol] = symbol_state
    save_strategy_state(state)


def reset_symbol_state(symbol: str) -> None:
    save_symbol_state(symbol, default_symbol_state())


def ensure_order_log_file() -> None:
    if ORDER_LOG_PATH.exists():
        with ORDER_LOG_PATH.open(mode="r", newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            existing_fields = reader.fieldnames or []
            existing_rows = list(reader)
        if existing_fields == ORDER_LOG_FIELDS:
            return
        with ORDER_LOG_PATH.open(mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=ORDER_LOG_FIELDS)
            writer.writeheader()
            for row in existing_rows:
                normalized = {field: row.get(field, "") for field in ORDER_LOG_FIELDS}
                writer.writerow(normalized)
        return

    with ORDER_LOG_PATH.open(mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=ORDER_LOG_FIELDS)
        writer.writeheader()


def ensure_app_log_file(reset: bool = False) -> None:
    if APP_LOG_PATH.exists() and not reset:
        return
    with APP_LOG_PATH.open(mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["datetime", "level", "message"])
        writer.writeheader()


def load_app_logs(limit: int = 200, newest_first: bool = True) -> list[dict[str, str]]:
    ensure_app_log_file()
    with APP_LOG_PATH.open(mode="r", newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))
    sliced = rows[-limit:]
    return list(reversed(sliced)) if newest_first else sliced


def append_order_log(row: dict[str, Any]) -> None:
    ensure_order_log_file()
    with ORDER_LOG_PATH.open(mode="a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=ORDER_LOG_FIELDS)
        normalized = {field: row.get(field, "") for field in ORDER_LOG_FIELDS}
        writer.writerow(normalized)


def load_order_logs(limit: int = 30, newest_first: bool = True) -> list[dict[str, str]]:
    ensure_order_log_file()
    with ORDER_LOG_PATH.open(mode="r", newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))
    sliced = rows[-limit:]
    return list(reversed(sliced)) if newest_first else sliced


def format_order_logs_for_ui(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    formatted: list[dict[str, str]] = []
    for row in rows:
        status = (row.get("status") or "").upper()
        side = (row.get("side") or "").upper()
        reason = (row.get("reason") or "").upper()
        action = f"LIVE_{side}" if status == "OPEN" else f"SQUAREOFF_{reason or side}"
        entry_exit = "ENTRY" if status == "OPEN" else "EXIT"
        when = row.get("exit_time") or row.get("timestamp") or ""
        api_details = {
            "symbol": row.get("symbol", ""),
            "side": row.get("side", ""),
            "entry_price": row.get("entry_price", ""),
            "stop_loss": row.get("stop_loss", ""),
            "target_price": row.get("target_price", ""),
            "status": row.get("status", ""),
            "exit_price": row.get("exit_price", ""),
            "exit_time": row.get("exit_time", ""),
            "reason": row.get("reason", ""),
            "broker_request": safe_json_loads(row.get("api_request", "")),
            "broker_response": safe_json_loads(row.get("api_response", "")),
        }
        formatted.append(
            {
                "datetime": when,
                "symbol": row.get("symbol", ""),
                "action": action,
                "entry_exit": entry_exit,
                "api_details": api_details,
            }
        )
    return formatted


def safe_json_loads(raw: str) -> Any:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def normalize_row(row: dict[str, str]) -> dict[str, str]:
    normalized = {col: "" for col in CSV_COLUMNS}
    normalized["Symbol"] = row.get("Symbol", "").strip().upper()
    normalized["Quantity"] = row.get("Quantity", row.get("Qty", "1")).strip() or "1"
    normalized["Timeframe"] = row.get("Timeframe", "1").strip() or "1"
    normalized["MaxProfit"] = row.get("MaxProfit", "0").strip() or "0"
    normalized["MaxLoss"] = row.get("MaxLoss", "0").strip() or "0"
    enabled = row.get("EnableTrading", row.get("Trading", "FALSE")).strip().lower()
    normalized["EnableTrading"] = "TRUE" if enabled in {"1", "true", "yes", "enabled"} else "FALSE"
    return normalized


def load_trade_settings() -> list[dict[str, str]]:
    ensure_trade_settings_file()
    with TRADE_SETTINGS_PATH.open(mode="r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        return [normalize_row(row) for row in reader]


def save_trade_settings(symbols: list[dict[str, str]]) -> None:
    with TRADE_SETTINGS_PATH.open(mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(normalize_row(row) for row in symbols)


def parse_symbol_payload(form_data: Any) -> dict[str, str]:
    return normalize_row(
        {
            "Symbol": form_data.get("symbol", ""),
            "Quantity": form_data.get("quantity", "1"),
            "Timeframe": form_data.get("timeframe", "1"),
            "MaxProfit": form_data.get("max_profit", "0"),
            "MaxLoss": form_data.get("max_loss", "0"),
            "EnableTrading": form_data.get("enable_trading", "FALSE"),
        }
    )


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=3000, debug=True)
