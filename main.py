from __future__ import annotations

import csv
import datetime as dt
import json
import io
import os
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any

import requests
from delta_rest_client import DeltaRestClient, OrderType
from flask import Flask, Response, flash, jsonify, redirect, render_template, request, session, url_for

BASE_DIR = Path(__file__).resolve().parent
TRADE_SETTINGS_PATH = BASE_DIR / "TradeSettings.csv"
CREDENTIALS_PATH = BASE_DIR / "credentials.csv"
ORDER_LOG_PATH = BASE_DIR / "OrderLog.csv"
APP_LOG_PATH = BASE_DIR / "AppLog.csv"
STATE_PATH = BASE_DIR / "strategy_state.json"
DELTA_BASE_URL = "https://api.india.delta.exchange"
SCHEDULER_STATE = {
    "thread": None,
    "stop_event": threading.Event(),
    "lock": threading.Lock(),
}
LTP_LOOP_INTERVAL_SEC = 0.2
IDLE_LOOP_INTERVAL_SEC = 0.5
POSITION_SYNC_INTERVAL_SEC = 1.0
ENTRY_LTP_LOG_INTERVAL_SEC = 2.0
OPEN_TRADE_LTP_LOG_INTERVAL_SEC = 3.0
PENDING_POSITION_LOG_INTERVAL_SEC = 3.0
_LAST_ENTRY_LTP_LOG_TS: dict[str, float] = {}
_LAST_OPEN_TRADE_LTP_LOG_TS: dict[str, float] = {}
_LAST_PENDING_POSITION_LOG_TS: dict[str, float] = {}
_ENTRY_LTP_LOG_LOCK = threading.Lock()
_OPEN_TRADE_LTP_LOG_LOCK = threading.Lock()
_PENDING_POSITION_LOG_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
PRODUCT_ID_CACHE: dict[str, int] = {}
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
    "SLCount",
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
        strategy_running = is_scheduler_running()
        net_positions: list[dict[str, Any]] = []
        if active_tab == "net-positions" and is_logged_in:
            net_positions = format_live_positions_for_ui(fetch_margined_positions_list())
        sort_order = request.args.get("sort", "newest")
        newest_first = sort_order != "oldest"
        raw_order_logs = load_order_logs(limit=300, newest_first=newest_first)
        order_logs = format_order_logs_for_ui(raw_order_logs)
        app_logs = load_app_logs(limit=200, newest_first=True)
        broker_status_text = "Logged In" if is_logged_in else "Logged Out"
        strategy_status_text = "Running" if strategy_running else "Stopped"
        return render_template(
            "index.html",
            symbols=symbols,
            is_logged_in=is_logged_in,
            strategy_running=strategy_running,
            broker_status_text=broker_status_text,
            strategy_status_text=strategy_status_text,
            net_positions=net_positions,
            order_logs=order_logs,
            app_logs=app_logs,
            sort_order=sort_order,
            active_tab=active_tab,
        )

    @app.get("/status")
    def status() -> Any:
        is_logged_in = session.get("broker_logged_in", False)
        strategy_running = is_scheduler_running()
        return jsonify(
            {
                "broker_logged_in": is_logged_in,
                "strategy_running": strategy_running,
                "broker_text": "Logged In" if is_logged_in else "Logged Out",
                "strategy_text": "Running" if strategy_running else "Stopped",
            }
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

    @app.post("/order-log/clear")
    def clear_order_log() -> Any:
        ensure_order_log_file(reset=True)
        flash("Order logs cleared.", "success")
        return redirect(url_for("index", tab="order-log"))

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
            apply_manual_close_cooldown(symbol)
            append_order_log(
                {
                    "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "symbol": symbol,
                    "side": "MANUAL_CLOSE",
                    "entry_price": position.get("entry_price") or position.get("avg_price") or "",
                    "stop_loss": "",
                    "target_price": "",
                    "status": "CLOSED",
                    "exit_price": "",
                    "exit_time": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "reason": "MANUAL_SQUARE_OFF",
                    "api_request": json.dumps(api_request or {}),
                    "api_response": json.dumps(api_response or {}),
                }
            )
            flash(f"Close order sent for {symbol}.", "success")
        else:
            broker_log(f"[Broker] Manual square-off failed for {symbol}: {api_response}", level="ERROR")
            append_order_log(
                {
                    "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "symbol": symbol,
                    "side": "MANUAL_CLOSE",
                    "entry_price": position.get("entry_price") or position.get("avg_price") or "",
                    "stop_loss": "",
                    "target_price": "",
                    "status": "ERROR",
                    "exit_price": "",
                    "exit_time": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "reason": "MANUAL_SQUARE_OFF_FAILED",
                    "api_request": json.dumps(api_request or {}),
                    "api_response": json.dumps(api_response or {}),
                }
            )
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

    @app.post("/broker/stop")
    def broker_stop_strategy() -> Any:
        stopped = stop_scheduler_if_running()
        if stopped:
            flash("Strategy stopped successfully.", "success")
        else:
            flash("Strategy is already stopped.", "success")
        return redirect(url_for("index", tab="symbols"))

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


def is_scheduler_running() -> bool:
    with SCHEDULER_STATE["lock"]:
        running_thread = SCHEDULER_STATE["thread"]
        return bool(running_thread and running_thread.is_alive())


def stop_scheduler_if_running() -> bool:
    with SCHEDULER_STATE["lock"]:
        running_thread = SCHEDULER_STATE["thread"]
        if not running_thread or not running_thread.is_alive():
            return False
        SCHEDULER_STATE["stop_event"].set()

    running_thread.join(timeout=2.5)
    with SCHEDULER_STATE["lock"]:
        if SCHEDULER_STATE["thread"] is running_thread and not running_thread.is_alive():
            SCHEDULER_STATE["thread"] = None
    broker_log("[Broker] Strategy scheduler stopped by user.")
    return True


def run_symbol_fetch_scheduler() -> None:
    next_run_by_key: dict[str, int] = {}
    stop_event = SCHEDULER_STATE["stop_event"]
    positions_by_symbol: dict[str, dict[str, Any]] = {}
    next_position_sync_ts = 0.0

    while not stop_event.is_set():
        now_dt = dt.datetime.now(dt.timezone.utc)
        now_ts = int(now_dt.timestamp())
        now_monotonic = time.time()
        enabled_symbols = [row for row in load_trade_settings() if row["EnableTrading"] == "TRUE"]

        if not enabled_symbols:
            broker_log("[Broker] Scheduler idle: no enabled symbols.")
            stop_event.wait(IDLE_LOOP_INTERVAL_SEC)
            continue

        valid_keys = set()
        any_open_trade = any(
            bool(get_symbol_state(row["Symbol"]).get("open_trade")) for row in enabled_symbols
        )
        position_sync_due = now_monotonic >= next_position_sync_ts
        if position_sync_due or any_open_trade:
            try:
                positions_by_symbol = fetch_margined_positions_by_symbol()
            except Exception as exc:
                broker_log(f"[Broker] Position sync failed: {exc}", level="ERROR")
            interval = LTP_LOOP_INTERVAL_SEC if any_open_trade else POSITION_SYNC_INTERVAL_SEC
            next_position_sync_ts = now_monotonic + interval

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
                    broker_log(f"[Broker] Fetched {len(candles)} candles for {symbol}")
                    update_trigger_from_candles(symbol, candles, scheduled_ts, timeframe_minutes)
                    next_run_by_key[key] = scheduled_ts + (timeframe_minutes * 60)
                    scheduled_ts = next_run_by_key[key]
            except Exception as exc:
                broker_log(f"[Broker] Scheduler error for {symbol}: {exc}", level="ERROR")

            try:
                process_ltp_cycle_for_symbol(row, now_ts, positions_by_symbol.get(norm_symbol(symbol)))
            except Exception as exc:
                broker_log(f"[Strategy] LTP cycle error for {symbol}: {exc}", level="ERROR")

        stale_keys = [key for key in next_run_by_key if key not in valid_keys]
        for key in stale_keys:
            del next_run_by_key[key]

        stop_event.wait(LTP_LOOP_INTERVAL_SEC)

    with SCHEDULER_STATE["lock"]:
        if SCHEDULER_STATE["thread"] is threading.current_thread():
            SCHEDULER_STATE["thread"] = None


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


def post_exit_entry_allowed_after_ts(event_dt: dt.datetime, timeframe_minutes: int) -> int:
    """Next boundary after exit bar, plus one full candle — avoid immediate re-entry on same signal bar."""
    interval_seconds = timeframe_minutes * 60
    after_first_boundary = next_candle_start(event_dt, timeframe_minutes)
    return int(after_first_boundary.timestamp()) + interval_seconds


def snap_cooldown_to_timeframe_grid(cooldown_until: int, timeframe_minutes: int) -> int:
    if cooldown_until <= 0:
        return 0
    interval = timeframe_minutes * 60
    return cooldown_until - (cooldown_until % interval)


def norm_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


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
            mapped[norm_symbol(str(symbol))] = pos
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


def get_symbol_timeframe_minutes(symbol: str, default_minutes: int = 1) -> int:
    for row in load_trade_settings():
        if row.get("Symbol", "").strip().upper() == symbol.strip().upper():
            try:
                minutes = int(float(row.get("Timeframe", default_minutes)))
                return minutes if minutes > 0 else default_minutes
            except (TypeError, ValueError):
                return default_minutes
    return default_minutes


def apply_manual_close_cooldown(symbol: str) -> None:
    timeframe_minutes = get_symbol_timeframe_minutes(symbol, default_minutes=1)
    now = dt.datetime.now(dt.timezone.utc)
    next_cycle_ts = post_exit_entry_allowed_after_ts(now, timeframe_minutes)
    state = get_symbol_state(symbol)
    clear_symbol_trade_context(state, next_cycle_ts)
    save_symbol_state(symbol, state)
    broker_log(
        f"[Strategy] Manual close cooldown set for {symbol} until "
        f"{dt.datetime.fromtimestamp(next_cycle_ts, tz=dt.timezone.utc).isoformat()}."
    )


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


def get_current_ltp(symbol: str) -> float:
    response = requests.get(
        f"{DELTA_BASE_URL}/v2/tickers/{norm_symbol(symbol)}",
        timeout=(5, 10),
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError(payload.get("error", "Unable to fetch ticker"))
    ticker = payload.get("result", {})
    return float(ticker.get("close"))


def update_trigger_from_candles(
    symbol: str,
    candles: list[dict[str, Any]],
    cycle_ts: int,
    timeframe_minutes: int,
) -> None:
    if len(candles) < 2:
        broker_log(f"[Strategy] Skipping {symbol}: insufficient candles.")
        return

    trigger_candle = select_previous_completed_candle(candles, cycle_ts)
    if trigger_candle is None:
        broker_log(f"[Strategy] No completed trigger candle found for {symbol} at cycle {cycle_ts}.")
        return

    open_price = float(trigger_candle["open"])
    close_price = float(trigger_candle["close"])
    high_price = float(trigger_candle["high"])
    low_price = float(trigger_candle["low"])

    if close_price >= open_price:
        candle_color = "GREEN"
        buy_trigger = close_price
        sell_trigger = open_price
    else:
        candle_color = "RED"
        buy_trigger = open_price
        sell_trigger = close_price

    state = get_symbol_state(symbol)
    cooldown_until = int(state.get("cooldown_until", 0) or 0)
    if cooldown_until and cycle_ts >= cooldown_until:
        state["cooldown_until"] = 0
    state["active_trigger"] = {
        "time": trigger_candle.get("time"),
        "time_human": dt.datetime.fromtimestamp(int(trigger_candle.get("time")), tz=IST).strftime(
            "%Y-%m-%d %H:%M:%S IST"
        )
        if trigger_candle.get("time") is not None
        else "",
        "candle_color": candle_color,
        "open": open_price,
        "close": close_price,
        "high": high_price,
        "low": low_price,
        "buy_trigger": buy_trigger,
        "sell_trigger": sell_trigger,
        "cycle_ts": cycle_ts,
        "timeframe_minutes": timeframe_minutes,
    }
    save_symbol_state(symbol, state)
    broker_log(
        f"[Strategy] Trigger set for {symbol} ({candle_color}) -> "
        f"buy_trigger={buy_trigger}, sell_trigger={sell_trigger}, trigger_time={state['active_trigger']['time_human']}"
    )


def _should_log_entry_ltp(symbol: str) -> bool:
    key = norm_symbol(symbol)
    with _ENTRY_LTP_LOG_LOCK:
        now = time.time()
        last = _LAST_ENTRY_LTP_LOG_TS.get(key, 0.0)
        if now - last >= ENTRY_LTP_LOG_INTERVAL_SEC:
            _LAST_ENTRY_LTP_LOG_TS[key] = now
            return True
        return False


def _should_log_open_trade_ltp(symbol: str) -> bool:
    key = norm_symbol(symbol)
    with _OPEN_TRADE_LTP_LOG_LOCK:
        now = time.time()
        last = _LAST_OPEN_TRADE_LTP_LOG_TS.get(key, 0.0)
        if now - last >= OPEN_TRADE_LTP_LOG_INTERVAL_SEC:
            _LAST_OPEN_TRADE_LTP_LOG_TS[key] = now
            return True
        return False


def _should_log_pending_position_wait(symbol: str) -> bool:
    key = norm_symbol(symbol)
    with _PENDING_POSITION_LOG_LOCK:
        now = time.time()
        last = _LAST_PENDING_POSITION_LOG_TS.get(key, 0.0)
        if now - last >= PENDING_POSITION_LOG_INTERVAL_SEC:
            _LAST_PENDING_POSITION_LOG_TS[key] = now
            return True
        return False


def evaluate_stop_target_hits(
    open_trade: dict[str, Any],
    ltp: float,
    max_profit: float,
    unrealized: float | None,
) -> tuple[bool, bool, bool]:
    side = str(open_trade.get("side", "")).upper()
    stop_loss = float(open_trade.get("stop_loss", 0))
    target_price_raw = open_trade.get("target_price")
    target_price = float(target_price_raw) if target_price_raw not in (None, "") else None

    stop_hit = (side == "BUY" and ltp <= stop_loss) or (side == "SELL" and ltp >= stop_loss)
    target_hit = False
    if target_price is not None and max_profit > 0:
        target_hit = (side == "BUY" and ltp >= target_price) or (side == "SELL" and ltp <= target_price)
    pnl_target_hit = bool(max_profit > 0 and unrealized is not None and unrealized >= max_profit)
    return stop_hit, target_hit, pnl_target_hit


def resolve_live_position_for_square_off(symbol: str, max_wait_sec: float = 8.0) -> dict[str, Any] | None:
    deadline = time.time() + max_wait_sec
    key = norm_symbol(symbol)
    while time.time() < deadline:
        pos = fetch_margined_positions_by_symbol().get(key)
        if pos:
            try:
                if abs(float(pos.get("size", 0) or 0)) > 1e-12:
                    return pos
            except (TypeError, ValueError):
                pass
        time.sleep(0.1)
    return None


def start_immediate_post_entry_monitor(
    row: dict[str, str],
    cycle_ts: int,
    symbol: str,
    max_wait_seconds: float = 3.0,
    poll_interval_seconds: float = 0.25,
) -> None:
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        try:
            live_position = fetch_margined_positions_by_symbol().get(norm_symbol(symbol))
            process_ltp_cycle_for_symbol(row, cycle_ts, live_position)
            state_now = get_symbol_state(symbol)
            if not state_now.get("open_trade"):
                return
            if live_position:
                return
        except Exception as exc:
            broker_log(f"[Strategy] Immediate post-entry monitor retry failed for {symbol}: {exc}", level="ERROR")
        time.sleep(poll_interval_seconds)


def process_ltp_cycle_for_symbol(row: dict[str, str], cycle_ts: int, live_position: dict[str, Any] | None) -> None:
    symbol = row["Symbol"]
    state = get_symbol_state(symbol)
    timeframe_minutes = int(float(row.get("Timeframe", "1") or 1))
    cooldown_until = int(state.get("cooldown_until", 0) or 0)
    cooldown_until = snap_cooldown_to_timeframe_grid(cooldown_until, timeframe_minutes)
    in_cooldown = cycle_ts < cooldown_until

    max_profit = float(row.get("MaxProfit", "0") or 0)
    open_trade = state.get("open_trade")
    trigger = state.get("active_trigger")

    if open_trade and not live_position:
        refreshed = fetch_margined_positions_by_symbol().get(norm_symbol(symbol))
        if refreshed:
            live_position = refreshed
        else:
            ltp = get_current_ltp(symbol)
            stop_hit, target_hit, pnl_target_hit = evaluate_stop_target_hits(open_trade, ltp, max_profit, None)
            if _should_log_pending_position_wait(symbol):
                broker_log(
                    f"[Strategy] Open trade {symbol}: SL/Target vs LTP (no position row yet) "
                    f"ltp={ltp}, SL={open_trade.get('stop_loss')}, target={open_trade.get('target_price')}"
                )
            if stop_hit or target_hit or pnl_target_hit:
                reason = "STOP_LOSS" if stop_hit else "TARGET"
                broker_log(
                    f"[Strategy] {reason} (LTP-only) for {symbol}: ltp={ltp}, "
                    f"SL={open_trade.get('stop_loss')}, target={open_trade.get('target_price')}"
                )
                live_for_exit = resolve_live_position_for_square_off(symbol)
                if live_for_exit:
                    status, api_request, api_response = square_off_position(symbol, live_for_exit)
                    broker_log(f"[Strategy] Square-off status for {symbol}: {status}")
                    finalize_trade_after_square_off(
                        symbol,
                        state,
                        open_trade,
                        reason,
                        ltp,
                        cycle_ts,
                        timeframe_minutes,
                        api_request=api_request,
                        api_response=api_response,
                    )
                else:
                    broker_log(
                        f"[Strategy] {reason} fired for {symbol} but margined position still missing after wait.",
                        level="ERROR",
                    )
            return

    if live_position and open_trade:
        ltp = get_current_ltp(symbol)
        unrealized = extract_unrealized_pnl(live_position)
        target_price_raw = open_trade.get("target_price")
        target_price = float(target_price_raw) if target_price_raw not in (None, "") else None
        stop_loss = float(open_trade.get("stop_loss", 0))
        if not state.get("position_ltp_debug_logged"):
            broker_log(
                f"[Strategy] Net position response for {symbol}: {json.dumps(live_position, default=str)}"
            )
            state["position_ltp_debug_logged"] = True
            save_symbol_state(symbol, state)

        stop_hit, target_hit, pnl_target_hit = evaluate_stop_target_hits(
            open_trade, ltp, max_profit, unrealized
        )

        if stop_hit or target_hit or pnl_target_hit:
            reason = "STOP_LOSS" if stop_hit else "TARGET"
            broker_log(
                f"[Strategy] {reason} hit for {symbol}. ltp={ltp}, SL={stop_loss}, target={target_price}, "
                f"unrealized={unrealized}, max_profit={max_profit}"
            )
            status, api_request, api_response = square_off_position(symbol, live_position)
            broker_log(f"[Strategy] Square-off status for {symbol}: {status}")
            finalize_trade_after_square_off(
                symbol,
                state,
                open_trade,
                reason,
                ltp,
                cycle_ts,
                timeframe_minutes,
                api_request=api_request,
                api_response=api_response,
            )
        elif _should_log_open_trade_ltp(symbol):
            broker_log(
                f"[Strategy] Open position {symbol}: ltp={ltp}, SL={stop_loss}, target={target_price}, "
                f"uPnL={unrealized}"
            )
        return

    if not trigger:
        return

    buy_trigger = float(trigger.get("buy_trigger"))
    sell_trigger = float(trigger.get("sell_trigger"))

    if in_cooldown:
        if not _should_log_entry_ltp(symbol):
            return
        ltp = get_current_ltp(symbol)
        broker_log(
            f"[Strategy] LTP check {symbol}: ltp={ltp}, buy_trigger={buy_trigger}, sell_trigger={sell_trigger}, "
            f"trigger_color={trigger.get('candle_color')} (cooldown: entry disabled)"
        )
        return

    ltp = get_current_ltp(symbol)
    if _should_log_entry_ltp(symbol):
        broker_log(
            f"[Strategy] LTP check {symbol}: ltp={ltp}, buy_trigger={buy_trigger}, sell_trigger={sell_trigger}, "
            f"trigger_color={trigger.get('candle_color')}"
        )

    if ltp >= buy_trigger:
        entry_side = "BUY"
    elif ltp <= sell_trigger:
        entry_side = "SELL"
    else:
        return

    trigger_open = float(trigger["open"])
    trigger_close = float(trigger["close"])
    candle_color = str(trigger.get("candle_color", "GREEN")).upper()
    if candle_color == "GREEN":
        stop_loss = trigger_open if entry_side == "BUY" else trigger_close
    else:
        stop_loss = trigger_close if entry_side == "BUY" else trigger_open
    target_price = None
    if max_profit > 0:
        target_price = ltp + max_profit if entry_side == "BUY" else ltp - max_profit

    quantity = max(1, int(float(row.get("Quantity", "1") or 1)))
    entry_status, entry_request, entry_response = place_entry_market_order(symbol, entry_side, quantity)
    if entry_status != "ENTRY_SENT":
        broker_log(f"[Strategy] Entry failed for {symbol}: {entry_response}", level="ERROR")
        append_order_log(
            {
                "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                "symbol": symbol,
                "side": entry_side,
                "entry_price": ltp,
                "stop_loss": stop_loss,
                "target_price": target_price if target_price is not None else "",
                "status": "ERROR",
                "exit_price": "",
                "exit_time": dt.datetime.now(dt.timezone.utc).isoformat(),
                "reason": "ENTRY_FAILED",
                "api_request": json.dumps(entry_request or {}),
                "api_response": json.dumps(entry_response or {}),
            }
        )
        return

    trade = {
        "symbol": symbol,
        "side": entry_side,
        "entry_price": ltp,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "trigger_time": trigger.get("time"),
        "trigger_upper": buy_trigger,
        "trigger_lower": sell_trigger,
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
            "api_request": json.dumps(entry_request or {}),
            "api_response": json.dumps(entry_response or {}),
        }
    )
    broker_log(
        f"[Strategy] ENTRY {symbol} {entry_side} at {ltp}. Trigger range=({sell_trigger}, {buy_trigger}), "
        f"SL={stop_loss}, Target={target_price}"
    )
    start_immediate_post_entry_monitor(row, cycle_ts, symbol)


def place_entry_market_order(symbol: str, side: str, quantity: int) -> tuple[str, dict[str, Any], dict[str, Any]]:
    request_payload = {
        "product_symbol": symbol,
        "size": quantity,
        "side": "buy" if side == "BUY" else "sell",
        "order_type": OrderType.MARKET.value,
        "reduce_only": "false",
    }
    client = create_delta_client()
    try:
        response = client.request("POST", "/v2/orders", payload=request_payload, auth=True)
        response_payload = response.json()
        return "ENTRY_SENT", request_payload, response_payload
    except Exception as exc:
        return "ENTRY_FAILED", request_payload, {"success": False, "error": str(exc)}


def select_previous_completed_candle(candles: list[dict[str, Any]], cycle_ts: int) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for candle in candles:
        raw_time = candle.get("time")
        if raw_time is None:
            continue
        try:
            candle_ts = int(raw_time)
        except (TypeError, ValueError):
            continue
        if candle_ts < cycle_ts:
            candidates.append(candle)
    if not candidates:
        return None
    return max(candidates, key=lambda c: int(c.get("time", 0)))


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
    exit_dt = dt.datetime.fromtimestamp(cycle_ts, tz=dt.timezone.utc)
    next_cycle_ts = post_exit_entry_allowed_after_ts(exit_dt, timeframe_minutes)
    clear_symbol_trade_context(state, next_cycle_ts)
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

    sl_limit = get_symbol_sl_limit(symbol)
    if int(state.get("sl_count", 0)) >= sl_limit:
        disable_symbol_trading(symbol)
        broker_log(
            f"[Strategy] Disabled {symbol} after {sl_limit} consecutive stop losses.",
            level="WARNING",
        )


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
    return {
        "sl_count": 0,
        "open_trade": None,
        "cooldown_until": 0,
        "position_ltp_debug_logged": False,
    }


def clear_symbol_trade_context(state: dict[str, Any], cooldown_until: int) -> None:
    # Remove previous trade-specific runtime data so next entry uses fresh candle trigger only.
    state["open_trade"] = None
    state["cooldown_until"] = cooldown_until
    state["active_trigger"] = None
    state["position_ltp_debug_logged"] = False


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


def ensure_order_log_file(reset: bool = False) -> None:
    if reset:
        with ORDER_LOG_PATH.open(mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=ORDER_LOG_FIELDS)
            writer.writeheader()
        return

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
                "entry_price": row.get("entry_price", ""),
                "stop_loss": row.get("stop_loss", ""),
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
    try:
        sl_count = int(float(row.get("SLCount", "2")))
        normalized["SLCount"] = str(sl_count if sl_count > 0 else 2)
    except (TypeError, ValueError):
        normalized["SLCount"] = "2"
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
            "SLCount": form_data.get("sl_count", "2"),
            "EnableTrading": form_data.get("enable_trading", "FALSE"),
        }
    )


def get_symbol_sl_limit(symbol: str, default_limit: int = 2) -> int:
    for row in load_trade_settings():
        if row.get("Symbol", "").strip().upper() == symbol.strip().upper():
            try:
                value = int(float(row.get("SLCount", default_limit)))
                return value if value > 0 else default_limit
            except (TypeError, ValueError):
                return default_limit
    return default_limit


if __name__ == "__main__":
    app = create_app()
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(1.0, lambda: webbrowser.open("http://127.0.0.1:3000")).start()
    app.run(host="0.0.0.0", port=3000, debug=True)
