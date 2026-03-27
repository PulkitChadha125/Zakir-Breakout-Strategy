from __future__ import annotations

import json
import os
import threading
import time
import webbrowser
from typing import Any

import main as core

try:
    import websocket  # type: ignore
except Exception:  # pragma: no cover - runtime dependency guard
    websocket = None


WS_URL = os.environ.get("DELTA_WS_URL", "wss://socket.india.delta.exchange")
WS_RECONNECT_SECONDS = 2.0
WS_CACHE_STALE_SECONDS = 20.0

_WS_STATE = {
    "lock": threading.Lock(),
    "prices": {},  # symbol -> {"price": float, "ts": float}
    "subscribed_symbols": set(),
    "thread": None,
    "stop_event": threading.Event(),
}


def _norm_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def _set_price(symbol: str, price: float) -> None:
    key = _norm_symbol(symbol)
    with _WS_STATE["lock"]:
        _WS_STATE["prices"][key] = {"price": float(price), "ts": time.time()}


def _extract_symbol(msg: dict[str, Any]) -> str | None:
    candidates = [
        msg.get("symbol"),
        msg.get("product_symbol"),
        msg.get("s"),
        msg.get("instrument_name"),
    ]
    for value in candidates:
        if value:
            return _norm_symbol(str(value))

    data = msg.get("data")
    if isinstance(data, dict):
        for key in ("symbol", "product_symbol", "s", "instrument_name"):
            value = data.get(key)
            if value:
                return _norm_symbol(str(value))
    return None


def _extract_price(msg: dict[str, Any]) -> float | None:
    keys = ("close", "mark_price", "last_price", "price", "c", "p")
    for key in keys:
        value = msg.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                pass

    data = msg.get("data")
    if isinstance(data, dict):
        for key in keys:
            value = data.get(key)
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
    return None


def _try_parse_and_store(payload: str) -> None:
    try:
        msg = json.loads(payload)
    except json.JSONDecodeError:
        return

    if isinstance(msg, list):
        for item in msg:
            if isinstance(item, dict):
                _try_store_dict(item)
        return
    if isinstance(msg, dict):
        _try_store_dict(msg)


def _try_store_dict(msg: dict[str, Any]) -> None:
    symbol = _extract_symbol(msg)
    price = _extract_price(msg)
    if symbol and price is not None:
        _set_price(symbol, price)


def _symbols_to_track() -> list[str]:
    rows = core.load_trade_settings()
    return [_norm_symbol(row["Symbol"]) for row in rows if row.get("EnableTrading") == "TRUE"]


def _send_subscriptions(ws: Any, symbols: list[str]) -> None:
    if not symbols:
        return

    channels = ["v2/ticker", "ticker", "all_tickers"]
    payloads: list[dict[str, Any]] = []
    for channel in channels:
        payloads.extend(
            [
                {"type": "subscribe", "payload": {"channels": [{"name": channel, "symbols": symbols}]}},
                {"type": "subscribe", "channel": channel, "symbols": symbols},
                {"method": "subscribe", "params": {"channel": channel, "symbols": symbols}},
            ]
        )
    for payload in payloads:
        try:
            ws.send(json.dumps(payload))
        except Exception:
            continue


def _on_open(ws: Any) -> None:
    core.broker_log("[WS] Connected to WebSocket feed.")
    with _WS_STATE["lock"]:
        _WS_STATE["subscribed_symbols"] = set()
    _refresh_symbol_subscriptions(ws)


def _on_message(_ws: Any, message: str) -> None:
    _try_parse_and_store(message)


def _on_error(_ws: Any, error: Any) -> None:
    core.broker_log(f"[WS] WebSocket error: {error}", level="ERROR")


def _on_close(_ws: Any, _status_code: Any, _message: Any) -> None:
    core.broker_log("[WS] WebSocket disconnected.")


def _refresh_symbol_subscriptions(ws: Any) -> None:
    desired = set(_symbols_to_track())
    with _WS_STATE["lock"]:
        already = set(_WS_STATE["subscribed_symbols"])
    to_add = sorted(desired - already)
    if not to_add:
        return
    _send_subscriptions(ws, to_add)
    with _WS_STATE["lock"]:
        _WS_STATE["subscribed_symbols"].update(to_add)
    core.broker_log(f"[WS] Subscribed symbols: {', '.join(to_add)}")


def _ws_loop() -> None:
    if websocket is None:
        core.broker_log(
            "[WS] websocket-client package not installed. Install with: pip install websocket-client",
            level="ERROR",
        )
        return

    stop_event = _WS_STATE["stop_event"]
    while not stop_event.is_set():
        ws_app = websocket.WebSocketApp(
            WS_URL,
            on_open=_on_open,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )
        try:
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            core.broker_log(f"[WS] run_forever crashed: {exc}", level="ERROR")
        if not stop_event.is_set():
            stop_event.wait(WS_RECONNECT_SECONDS)


def _subscription_refresh_loop() -> None:
    while not _WS_STATE["stop_event"].is_set():
        _WS_STATE["stop_event"].wait(4.0)
        # Triggering happens in _on_open; this loop exists only to keep logic extendable.
        # We intentionally avoid direct ws object sharing to keep thread handling simple.


def start_ws_if_needed() -> None:
    with _WS_STATE["lock"]:
        thread = _WS_STATE["thread"]
        if thread and thread.is_alive():
            return
        _WS_STATE["stop_event"].clear()
        ws_thread = threading.Thread(target=_ws_loop, name="delta-ws-price-feed", daemon=True)
        _WS_STATE["thread"] = ws_thread
        ws_thread.start()
    refresh_thread = threading.Thread(
        target=_subscription_refresh_loop,
        name="delta-ws-subscription-refresh",
        daemon=True,
    )
    refresh_thread.start()
    core.broker_log("[WS] WebSocket LTP feed thread started.")


def stop_ws_if_running() -> None:
    _WS_STATE["stop_event"].set()


def get_current_ltp_websocket(symbol: str) -> float:
    key = _norm_symbol(symbol)
    with _WS_STATE["lock"]:
        payload = _WS_STATE["prices"].get(key)
    if not payload:
        raise RuntimeError(f"No WebSocket LTP yet for {key}")
    age_sec = time.time() - float(payload.get("ts", 0.0))
    if age_sec > WS_CACHE_STALE_SECONDS:
        raise RuntimeError(f"Stale WebSocket LTP for {key}; age={age_sec:.2f}s")
    return float(payload["price"])


def _patched_start_scheduler_if_needed() -> bool:
    start_ws_if_needed()
    return _ORIGINAL_START_SCHEDULER()


def _patched_stop_scheduler_if_running() -> bool:
    stopped = _ORIGINAL_STOP_SCHEDULER()
    stop_ws_if_running()
    return stopped


_ORIGINAL_START_SCHEDULER = core.start_scheduler_if_needed
_ORIGINAL_STOP_SCHEDULER = core.stop_scheduler_if_running

# Patch only LTP source and scheduler lifecycle.
core.get_current_ltp = get_current_ltp_websocket
core.start_scheduler_if_needed = _patched_start_scheduler_if_needed
core.stop_scheduler_if_running = _patched_stop_scheduler_if_running


if __name__ == "__main__":
    app = core.create_app()
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(1.0, lambda: webbrowser.open("http://127.0.0.1:3000")).start()
    app.run(host="0.0.0.0", port=3000, debug=True)
