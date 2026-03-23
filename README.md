# Delta Breakout Strategy

A Flask-based trading dashboard and strategy runner integrated with Delta Broker API (India base URL).  
This project provides:

- Symbol configuration management from CSV
- Live net positions with manual square-off
- Candle-based breakout strategy scheduler
- LTP-based stop-loss / max-profit exits
- Order log + API request/response viewer
- Application event logs

---

## 1) Features

### UI Tabs

- **Symbol Settings**
  - Load symbols from `TradeSettings.csv`
  - Add/Edit/Delete symbol rows
  - Enable/Disable trading per symbol
  - `Login & Start` to authenticate and run scheduler
- **Net Positions**
  - Fetches live running positions from Delta
  - Shows side, size, entry, realized PnL, unrealized PnL
  - Manual `Close Position` (reduce-only market square-off)
- **Order Log**
  - Shows strategy and square-off operations
  - Newest/Oldest sorting
  - Symbol and Entry/Exit filters
  - Download CSV
  - API Details popup with **Broker Request** and **Broker Response**
- **App Log**
  - Application-level logs (INFO/WARNING/ERROR)
  - Clear logs action

---

## 2) Project Structure

- `main.py` - Flask app, broker integration, scheduler, strategy logic
- `templates/index.html` - Main UI
- `static/styles.css` - UI styles
- `TradeSettings.csv` - Symbol configuration
- `credentials.csv` - API credentials
- `data/<SYMBOL>.csv` - OHLC history files
- `OrderLog.csv` - Trade/order-level logs
- `AppLog.csv` - Application-level logs
- `strategy_state.json` - Per-symbol runtime state (SL counter, open trade, cooldown)
- `requirements.txt` - Python dependencies

---

## 3) Setup

## Prerequisites

- Python 3.10+ recommended
- Delta API key and secret

## Install

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Configure files

### `credentials.csv`

Format:

```csv
Title,Value
key,YOUR_API_KEY
secret,YOUR_API_SECRET
totp,OPTIONAL_VALUE
```

### `TradeSettings.csv`

Format:

```csv
Symbol,Quantity,Timeframe,MaxProfit,MaxLoss,SLCount,EnableTrading
XRPUSD,10,1,0.01,0,2,TRUE
BTCUSD,1,5,0.5,0.2,3,FALSE
```

Notes:

- `Timeframe` is in minutes (`1,3,5,15,30,60,120,240,360,1440,10080`)
- `MaxProfit` can be integer or decimal (e.g. `1` or `0.05`)
- `MaxLoss` can be integer or decimal (positive value in settings)
- `SLCount` is max consecutive stop-loss count allowed before auto-disable
- Only `EnableTrading=TRUE` symbols are scheduled

---

## 4) Run the App

```bash
python main.py
```

Open:

- `http://127.0.0.1:3000`

---

## 5) How Trading Works

## Scheduler timing

Per symbol timeframe:

- Floors current time to candle boundary
- First run is next candle boundary
- Then runs repeatedly every timeframe interval

Example (5m):

- Current time `05:03` -> floor `05:00`
- First run `05:05`
- Next runs `05:10`, `05:15`, ...

## Data fetch

At each run:

- Fetches OHLC from `/v2/history/candles`
- Writes `data/<SYMBOL>.csv` with:
  - `time` (unix)
  - `time_human`
  - `time_iso8601`
  - `open, high, low, close, volume`

## Buy / Sell entry conditions

Trigger candle = previous completed candle.

- Green trigger candle:
  - Buy if `LTP > close`
  - Sell if `LTP < open`
- Red trigger candle:
  - Buy if `LTP > open`
  - Sell if `LTP < close`

Wicks are not used for entry breakout check.

Only one trade is allowed per symbol at a time.  
No new entry is taken until the current trade exits.

## Stop-loss and max-profit conditions (LTP based)

Stop-loss is body-based (depends on trigger candle color + entry side):

- If trigger candle is **green** (`close > open`):
  - BUY trade stop-loss = trigger `open`
  - SELL trade stop-loss = trigger `close`
- If trigger candle is **red** (`close < open`):
  - BUY trade stop-loss = trigger `close`
  - SELL trade stop-loss = trigger `open`

- Target is derived from `MaxProfit` distance from entry price:
  - Buy target = `entry + MaxProfit`
  - Sell target = `entry - MaxProfit`

Exit checks are done on live LTP every second:

- For BUY:
  - Stop-loss hit when `LTP <= stop_loss`
  - Max-profit hit when `LTP >= target`
- For SELL:
  - Stop-loss hit when `LTP >= stop_loss`
  - Max-profit hit when `LTP <= target`

On stop-loss or max-profit hit:

- Sends reduce-only market square-off order to broker
- Stores broker request/response in `OrderLog.csv`

## Post-exit behavior

After square-off (SL/target/manual close), symbol enters cooldown until next candle cycle.  
Strategy re-check starts from next aligned candle, not same candle.

## SL counter rule

Per symbol:

- Stop-loss exit -> counter +1
- Target exit -> counter reset to 0
- If counter reaches `SLCount` in `TradeSettings.csv` -> symbol auto-disabled (`EnableTrading=FALSE`)
- Re-enabling symbol from UI resets state to fresh

---

## 6) Net Position Panel

Shows live non-zero positions:

- Symbol
- Side (LONG/SHORT)
- Size
- Entry Price
- Realized PnL
- Unrealized PnL

Manual close sends reduce-only market square-off for selected symbol.

---

## 7) Logs

## Order Log (`OrderLog.csv`)

Includes:

- Trade status (OPEN/CLOSED)
- Entry/exit details
- Stop-loss/target reason
- `api_request` and `api_response` from broker order placement

## Application Log (`AppLog.csv`)

Includes application events such as:

- Login success/failure
- Scheduler lifecycle
- Data fetch events
- Strategy events and errors

---

## 8) Important Notes

- This implementation uses broker APIs directly; test in safe environment first.
- Keep `credentials.csv` private. Do not commit secrets to public repos.
- If broker API schema changes, endpoints/fields may require updates.

---

## 9) Troubleshooting

- **Broker login failed**
  - verify `credentials.csv` key/secret
  - verify internet and Delta API availability
- **No symbols processing**
  - check `EnableTrading=TRUE`
- **No positions in Net Positions**
  - no open positions or API returned zero size
- **No new candles written**
  - wait until next timeframe boundary (scheduler is candle-aligned)

