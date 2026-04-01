"""
server.py — Market Breadth Backend
Deploy on Railway. Set env var: POLYGON_API_KEY
"""

import os, json, time, threading, math, asyncio
from datetime import datetime, date, timedelta
from collections import defaultdict
from http.server import HTTPServer, BaseHTTPRequestHandler

import websocket
import requests as req

# ── Config ────────────────────────────────────────────
POLYGON_KEY  = os.environ["POLYGON_API_KEY"]
HISTORY_FILE = "data/breadth_history.json"
PORT         = int(os.environ.get("PORT", 8080))

# Market hours ET (UTC offset -4 in summer, -5 winter — we use UTC)
MARKET_OPEN_UTC  = 13   # 9:30 ET = 13:30 UTC
MARKET_CLOSE_UTC = 21   # 4:00 ET = 20:00 UTC (we snapshot at 21:00)

MIN_MARKET_CAP = 1_000_000_000  # $1B

# ── Thresholds for color ──────────────────────────────
THRESHOLDS = {
    "advancing":      {"green": 2000, "yellow": 1200},   # higher = better
    "declining":      {"green": 800,  "yellow": 1800, "invert": True},  # lower = better
    "ad_ratio":       {"green": 2.0,  "yellow": 0.8},
    "above_ema20":    {"green": 2000, "yellow": 1200},
    "above_ema50":    {"green": 1900, "yellow": 1100},
    "below_sma200":   {"green": 1000, "yellow": 1800, "invert": True},
    "new_highs":      {"green": 150,  "yellow": 50},
    "dist_lt_neg10":  {"green": 20,   "yellow": 80,  "invert": True},
    "dist_neg10_neg5":{"green": 80,   "yellow": 250, "invert": True},
    "dist_neg5_0":    {"green": 600,  "yellow": 1200,"invert": True},
    "dist_0_5":       {"green": 1500, "yellow": 800},
    "dist_5_10":      {"green": 300,  "yellow": 100},
    "dist_gt_10":     {"green": 80,   "yellow": 30},
}

# ── In-memory state ───────────────────────────────────
lock = threading.Lock()
state = {
    "universe":    {},   # sym → {market_cap, prev_close, ema20, ema50, sma200, hi52, close}
    "live_prices": {},   # sym → latest price from WebSocket
    "breadth":     None, # latest computed breadth dict
    "history":     [],   # list of daily/weekly rows
    "initialized": False,
    "theme_cache": None, # cached theme data, recomputed every 5 min
}

# ── Helpers ───────────────────────────────────────────
def log(msg): print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

def get_color(key, value):
    if value is None: return "neutral"
    t = THRESHOLDS.get(key, {})
    if not t: return "neutral"
    invert = t.get("invert", False)
    g, y = t["green"], t["yellow"]
    if not invert:
        if value >= g: return "green"
        if value >= y: return "yellow"
        return "red"
    else:
        if value <= g: return "green"
        if value <= y: return "yellow"
        return "red"

def calc_sma(closes, period):
    if len(closes) < period: return None
    return sum(closes[-period:]) / period

def calc_ema(closes, period):
    if len(closes) < period: return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for p in closes[period:]: ema = p * k + ema * (1 - k)
    return ema

def is_market_open():
    now = datetime.utcnow()
    if now.weekday() >= 5: return False
    h = now.hour + now.minute / 60
    return MARKET_OPEN_UTC - 0.5 <= h <= MARKET_CLOSE_UTC

# ── Load / Save history ───────────────────────────────
def load_history():
    os.makedirs("data", exist_ok=True)
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE) as f:
            state["history"] = json.load(f)
    log(f"Loaded {len(state['history'])} history rows")

def save_history():
    os.makedirs("data", exist_ok=True)
    with open(HISTORY_FILE, "w") as f:
        json.dump(state["history"], f, indent=2)

# ── Build stock universe ──────────────────────────────
def build_universe():
    """
    Fast universe builder using pre-loaded 2799 symbol list.
    Steps:
    1. Load symbols from universe_symbols.txt
    2. ONE call: get prev_close via grouped daily bars
    3. Per symbol: get industry from Polygon + fetch 200-day history
    """
    log("Building stock universe from universe_symbols.txt...")

    today = date.today()

    # ── Step 1: Load symbol list ───────────────────────────────────────
    sym_file = os.path.join(os.path.dirname(__file__), "universe_symbols.txt")
    if not os.path.exists(sym_file):
        log("  ERROR: universe_symbols.txt not found in repo!")
        state["initialized"] = True
        return

    with open(sym_file) as f:
        symbols = [line.strip() for line in f if line.strip()]
    sym_set = set(symbols)
    log(f"  Loaded {len(symbols)} symbols")

    # ── Step 2: Get prev_close for all in ONE grouped daily bars call ──
    prev_day = today - timedelta(days=1)
    while prev_day.weekday() >= 5:
        prev_day -= timedelta(days=1)
    prev_str = prev_day.strftime("%Y-%m-%d")

    log(f"  Fetching grouped daily bars for {prev_str}...")
    prev_closes = {}
    try:
        r = req.get(
            f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{prev_str}"
            f"?adjusted=true&include_otc=false&apiKey={POLYGON_KEY}",
            timeout=60)
        for b in r.json().get("results", []):
            sym = b.get("T", "")
            if sym in sym_set and b.get("c", 0) > 0:
                prev_closes[sym] = b["c"]
        log(f"  Got prev_close for {len(prev_closes)} symbols")
    except Exception as e:
        log(f"  Grouped bars error: {e}")

    # ── Step 3: Industry + 200-day history per symbol ──────────────────
    today_str = today.strftime("%Y-%m-%d")
    from_date = (today - timedelta(days=300)).strftime("%Y-%m-%d")
    added = 0

    log(f"  Fetching industry + history for {len(symbols)} symbols...")

    for sym in symbols:
        try:
            # Get industry + name from Polygon
            r1 = req.get(
                f"https://api.polygon.io/v3/reference/tickers/{sym}"
                f"?apiKey={POLYGON_KEY}", timeout=10)
            detail   = r1.json().get("results", {})
            industry = detail.get("sic_description") or "Other"
            name     = detail.get("name", sym)

            # Get 200-day history
            r2 = req.get(
                f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day"
                f"/{from_date}/{today_str}?adjusted=true&sort=asc&limit=300"
                f"&apiKey={POLYGON_KEY}", timeout=15)
            bars = r2.json().get("results", [])
            if len(bars) < 20:
                time.sleep(0.06)
                continue

            closes = [b["c"] for b in bars]
            highs  = [b["h"] for b in bars]
            dates  = [datetime.utcfromtimestamp(b["t"]/1000).strftime("%Y-%m-%d")
                      for b in bars]

            ema20  = calc_ema(closes, 20)
            ema50  = calc_ema(closes, 50)  if len(closes) >= 50  else None
            sma200 = calc_sma(closes, 200) if len(closes) >= 200 else None
            hi52   = max(highs[-252:]) if len(highs) >= 252 else max(highs)

            with lock:
                state["universe"][sym] = {
                    "name":        name,
                    "industry":    industry,
                    "prev_close":  prev_closes.get(sym, closes[-2] if len(closes) >= 2 else closes[-1]),
                    "ema20":       ema20,
                    "ema50":       ema50,
                    "sma200":      sma200,
                    "hi52":        hi52,
                    "close":       closes[-1],
                    "hist_prices": dict(zip(dates, closes)),
                }
            added += 1

            if added % 100 == 0:
                log(f"  Progress: {added}/{len(symbols)} done")

            time.sleep(0.08)

        except Exception:
            time.sleep(0.05)

    log(f"  Universe ready: {added} stocks loaded")

    # Backfill 14 trading days of history
    backfill_history(days=14)

    state["initialized"] = True

# ── Backfill 14 days of history ──────────────────────
def backfill_history(days=14):
    """
    On first startup, compute breadth for each of the past N trading days
    using Polygon grouped daily bars, and insert into history.
    Skips dates already in history file.
    """
    log(f"Backfilling {days} trading days of history...")

    existing_dates = {r["date"] for r in state["history"]}

    # Collect past trading days (skip weekends)
    trading_days = []
    d = date.today() - timedelta(days=1)
    while len(trading_days) < days:
        if d.weekday() < 5:  # Mon-Fri only
            trading_days.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)

    trading_days.reverse()  # oldest first

    universe = state["universe"]
    if not universe:
        log("  Universe empty, skipping backfill")
        return

    for day_str in trading_days:
        if day_str in existing_dates:
            log(f"  Skipping {day_str} (already in history)")
            continue

        log(f"  Computing breadth for {day_str}...")
        try:
            # Fetch grouped daily bars for this date
            url = (f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{day_str}"
                   f"?adjusted=true&apiKey={POLYGON_KEY}")
            r = req.get(url, timeout=30)
            bars = r.json().get("results", [])
            if not bars:
                log(f"  No data for {day_str}, skipping")
                continue

            # Build price lookup for this day
            day_prices = {b["T"]: b["c"] for b in bars}

            # Also need previous day prices for A/D and change%
            # Use the universe prev_close as approximation for the oldest day,
            # then chain forward
            prev_day_url = None
            # Find prev trading day
            pd = date.fromisoformat(day_str) - timedelta(days=1)
            while pd.weekday() >= 5:
                pd -= timedelta(days=1)
            pd_str = pd.strftime("%Y-%m-%d")
            r2 = req.get(
                f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{pd_str}"
                f"?adjusted=true&apiKey={POLYGON_KEY}", timeout=30)
            prev_bars = r2.json().get("results", [])
            prev_prices = {b["T"]: b["c"] for b in prev_bars}

            # Compute metrics for this day
            advancing = declining = 0
            above_ema20 = above_ema50 = below_sma200 = new_highs = 0
            dist = defaultdict(int)
            total = 0

            for sym, info in universe.items():
                close = day_prices.get(sym)
                if not close or close <= 0:
                    continue

                prev  = prev_prices.get(sym, info.get("prev_close", 0))
                ema20  = info.get("ema20")
                ema50  = info.get("ema50")
                sma200 = info.get("sma200")
                hi52   = info.get("hi52")

                total += 1

                if prev and prev > 0:
                    if close > prev: advancing += 1
                    elif close < prev: declining += 1

                if ema20  and close > ema20:  above_ema20  += 1
                if ema50  and close > ema50:  above_ema50  += 1
                if sma200 and close < sma200: below_sma200 += 1
                if hi52   and close >= hi52:  new_highs    += 1

                if prev and prev > 0:
                    chg = (close - prev) / prev * 100
                    if   chg < -10: dist["lt_neg10"]   += 1
                    elif chg <  -5: dist["neg10_neg5"] += 1
                    elif chg <   0: dist["neg5_0"]     += 1
                    elif chg <   5: dist["pos0_5"]     += 1
                    elif chg <  10: dist["pos5_10"]    += 1
                    else:           dist["gt_10"]      += 1

            ad_ratio = round(advancing / declining, 2) if declining > 0 else None

            row = {
                "date":            day_str,
                "type":            "daily",
                "advancing":       advancing,
                "declining":       declining,
                "ad_ratio":        ad_ratio,
                "above_ema20":     above_ema20,
                "above_ema50":     above_ema50,
                "below_sma200":    below_sma200,
                "new_highs":       new_highs,
                "dist_lt_neg10":   dist["lt_neg10"],
                "dist_neg10_neg5": dist["neg10_neg5"],
                "dist_neg5_0":     dist["neg5_0"],
                "dist_0_5":        dist["pos0_5"],
                "dist_5_10":       dist["pos5_10"],
                "dist_gt_10":      dist["gt_10"],
                "total_stocks":    total,
            }

            with lock:
                state["history"] = [x for x in state["history"] if x.get("date") != day_str]
                state["history"].append(row)
                state["history"].sort(key=lambda x: x["date"], reverse=True)

            log(f"  ✓ {day_str}: adv={advancing} dec={declining} ema20={above_ema20} nh={new_highs}")
            time.sleep(0.3)  # be gentle with API rate limits

        except Exception as e:
            log(f"  Error backfilling {day_str}: {e}")

    # Add weekly summaries for any Fridays in the backfilled range
    _add_weekly_summaries()

    save_history()
    log(f"Backfill complete — {len(state['history'])} total rows")


def _add_weekly_summaries():
    """Insert weekly average rows for any complete weeks in history."""
    from collections import defaultdict as dd
    daily_rows = [r for r in state["history"] if r.get("type") == "daily"]
    daily_rows.sort(key=lambda r: r["date"])

    # Group by ISO week
    weeks = dd(list)
    for row in daily_rows:
        d = date.fromisoformat(row["date"])
        week_key = d.isocalendar()[:2]  # (year, week)
        weeks[week_key].append(row)

    keys = ["advancing","declining","ad_ratio","above_ema20","above_ema50",
            "below_sma200","new_highs","dist_lt_neg10","dist_neg10_neg5",
            "dist_neg5_0","dist_0_5","dist_5_10","dist_gt_10","total_stocks"]

    for wk, rows in weeks.items():
        if len(rows) < 3:  # skip incomplete weeks
            continue
        week_start = rows[0]["date"]
        week_label = f"Week of {week_start}"

        def avg(k):
            vals = [r[k] for r in rows if r.get(k) is not None]
            return round(sum(vals) / len(vals), 2) if vals else None

        wrow = {"date": week_label, "type": "weekly"}
        for k in keys:
            wrow[k] = avg(k)

        state["history"] = [r for r in state["history"] if r.get("date") != week_label]
        state["history"].append(wrow)

    state["history"].sort(key=lambda r: r["date"], reverse=True)


# ── Compute breadth ───────────────────────────────────
def compute_breadth():
    with lock:
        universe   = dict(state["universe"])
        live       = dict(state["live_prices"])

    advancing = declining = 0
    above_ema20 = above_ema50 = below_sma200 = new_highs = 0
    dist = defaultdict(int)
    total = 0

    for sym, d in universe.items():
        # Use live price if available, else last known close
        close = live.get(sym, d.get("close"))
        if not close or close <= 0:
            continue

        prev   = d.get("prev_close", 0)
        ema20  = d.get("ema20")
        ema50  = d.get("ema50")
        sma200 = d.get("sma200")
        hi52   = d.get("hi52")

        total += 1

        # Advance / Decline
        if prev and prev > 0:
            if close > prev: advancing += 1
            elif close < prev: declining += 1

        # Moving averages
        if ema20  and close > ema20:  above_ema20  += 1
        if ema50  and close > ema50:  above_ema50  += 1
        if sma200 and close < sma200: below_sma200 += 1

        # New 52-week high
        if hi52 and close >= hi52:    new_highs    += 1

        # Change % distribution
        if prev and prev > 0:
            chg = (close - prev) / prev * 100
            if   chg < -10: dist["lt_neg10"]    += 1
            elif chg <  -5: dist["neg10_neg5"]  += 1
            elif chg <   0: dist["neg5_0"]      += 1
            elif chg <   5: dist["pos0_5"]      += 1
            elif chg <  10: dist["pos5_10"]     += 1
            else:           dist["gt_10"]       += 1

    ad_ratio = round(advancing / declining, 2) if declining > 0 else None

    metrics = {
        "advancing":       advancing,
        "declining":       declining,
        "ad_ratio":        ad_ratio,
        "above_ema20":     above_ema20,
        "above_ema50":     above_ema50,
        "below_sma200":    below_sma200,
        "new_highs":       new_highs,
        "dist_lt_neg10":   dist["lt_neg10"],
        "dist_neg10_neg5": dist["neg10_neg5"],
        "dist_neg5_0":     dist["neg5_0"],
        "dist_0_5":        dist["pos0_5"],
        "dist_5_10":       dist["pos5_10"],
        "dist_gt_10":      dist["gt_10"],
        "total_stocks":    total,
    }

    # Attach color for each metric
    colors = {k: get_color(k, v) for k, v in metrics.items()}

    result = {
        "timestamp":    datetime.utcnow().isoformat(),
        "market_open":  is_market_open(),
        "date":         date.today().strftime("%Y-%m-%d"),
        "metrics":      metrics,
        "colors":       colors,
        "history":      state["history"],
    }
    with lock:
        state["breadth"] = result
    return result

# ── WebSocket ─────────────────────────────────────────
def on_message(ws_app, message):
    try:
        events = json.loads(message)
        for ev in events:
            et = ev.get("ev")
            if et == "connected":
                ws_app.send(json.dumps({"action": "auth", "params": POLYGON_KEY}))
            elif et == "auth_success":
                log("Polygon WebSocket authenticated — subscribing A.*")
                ws_app.send(json.dumps({"action": "subscribe", "params": "A.*"}))
            elif et == "A":
                sym   = ev.get("sym")
                close = ev.get("c")
                if sym and close and sym in state["universe"]:
                    state["live_prices"][sym] = close
    except Exception as e:
        pass

def on_error(ws_app, error):
    log(f"WS error: {error}")

def on_close(ws_app, *args):
    log("WS closed — reconnecting in 5s")
    time.sleep(5)
    start_ws()

def start_ws():
    ws_app = websocket.WebSocketApp(
        "wss://socket.polygon.io/stocks",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws_app.run_forever(ping_interval=30)

# ── Background threads ────────────────────────────────
def compute_loop():
    """Recompute breadth every 60 seconds (EMA updated with live price)."""
    while True:
        time.sleep(60)
        if state["initialized"]:
            try:
                compute_breadth()
                log(f"Breadth recomputed")
            except Exception as e:
                log(f"Compute error: {e}")

def theme_cache_loop():
    """Recompute and cache theme data every 5 minutes."""
    # Wait for universe to be ready first
    while not state["initialized"]:
        time.sleep(10)
    time.sleep(30)  # Extra buffer after init
    while True:
        try:
            log("Recomputing theme cache...")
            themes = compute_themes()
            # Build summary (without stocks list for lighter payload)
            summary = {k: {x: v[x] for x in v if x != "stocks"}
                       for k, v in themes.items()}
            with lock:
                state["theme_cache"] = {
                    "themes":    summary,
                    "full":      themes,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            log(f"Theme cache updated: {len(summary)} industries")
        except Exception as e:
            log(f"Theme cache error: {e}")
        time.sleep(300)  # Every 5 minutes

def eod_snapshot_loop():
    """Save EOD row at 9pm UTC (4pm ET + buffer)."""
    while True:
        now = datetime.utcnow()
        target = now.replace(hour=21, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        time.sleep((target - now).total_seconds())

        if not state["initialized"]:
            continue

        log("Saving EOD snapshot...")
        compute_breadth()
        b = state["breadth"]
        if not b:
            continue

        today_str = date.today().strftime("%Y-%m-%d")
        row = {"date": today_str, "type": "daily", **b["metrics"]}

        with lock:
            state["history"] = [r for r in state["history"] if r.get("date") != today_str]
            state["history"].append(row)
            # Sort newest first
            state["history"].sort(key=lambda r: r["date"], reverse=True)
            # Weekly summary on Friday
            if date.today().weekday() == 4:
                daily = [r for r in state["history"] if r.get("type") == "daily"]
                last5 = daily[:5]
                if last5:
                    def avg(k):
                        vals = [r[k] for r in last5 if r.get(k) is not None]
                        return round(sum(vals)/len(vals), 2) if vals else None
                    keys = ["advancing","declining","ad_ratio","above_ema20","above_ema50",
                            "below_sma200","new_highs","dist_lt_neg10","dist_neg10_neg5",
                            "dist_neg5_0","dist_0_5","dist_5_10","dist_gt_10","total_stocks"]
                    wrow = {"date": f"Week of {last5[-1]['date']}", "type": "weekly"}
                    for k in keys: wrow[k] = avg(k)
                    state["history"] = [r for r in state["history"] if r.get("date") != wrow["date"]]
                    state["history"].append(wrow)
                    state["history"].sort(key=lambda r: r["date"], reverse=True)

        save_history()
        log(f"EOD snapshot saved — {len(state['history'])} total rows")

# ── Theme / Industry Performance ─────────────────────
def compute_themes():
    """
    Group universe stocks by industry, compute performance
    for Today, 1W, 1M, 3M, YTD across each group.
    Also store per-stock detail for drill-down.
    Returns dict: { industry_name: { periods, stocks } }
    """
    with lock:
        universe   = dict(state["universe"])
        live       = dict(state["live_prices"])

    today     = date.today()
    year_start = date(today.year, 1, 1).strftime("%Y-%m-%d")

    # Date targets for each period (trading days back)
    period_days = {"1W": 7, "1M": 30, "3M": 91}

    # Group stocks by industry
    from collections import defaultdict as dd
    groups = dd(list)
    for sym, info in universe.items():
        industry = info.get("industry") or info.get("sector") or "Other"
        close = live.get(sym, info.get("close"))
        if not close or close <= 0:
            continue
        groups[industry].append({
            "sym":     sym,
            "name":    info.get("name", sym),
            "close":   close,
            "prev":    info.get("prev_close", close),
            "mktcap":  info.get("market_cap", 0),
            "ema20":   info.get("ema20"),
            "ema50":   info.get("ema50"),
            "sma200":  info.get("sma200"),
            "hist":    info.get("hist_prices", {}),  # {date_str: close}
        })

    result = {}
    for industry, stocks in groups.items():
        if len(stocks) < 2:
            continue

        # Per-stock detail
        stock_rows = []
        for s in sorted(stocks, key=lambda x: x["mktcap"] or 0, reverse=True):
            today_chg = ((s["close"] - s["prev"]) / s["prev"] * 100) if s["prev"] else None

            # Historical period returns
            def period_return(days):
                h = s["hist"]
                if not h: return None
                target = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
                # Find closest available date
                dates = sorted(h.keys())
                past = [d for d in dates if d <= target]
                if not past: return None
                past_close = h[past[-1]]
                if not past_close: return None
                return round((s["close"] - past_close) / past_close * 100, 2)

            def ytd_return():
                h = s["hist"]
                if not h: return None
                dates = sorted(h.keys())
                past = [d for d in dates if d <= year_start]
                if not past: return None
                past_close = h[past[-1]]
                if not past_close: return None
                return round((s["close"] - past_close) / past_close * 100, 2)

            # MA status
            def ma_status(price, ma):
                if not ma: return "—"
                return "▲" if price > ma else "▼"

            stock_rows.append({
                "sym":     s["sym"],
                "name":    s["name"],
                "close":   round(s["close"], 2),
                "mktcap":  s["mktcap"],
                "today":   round(today_chg, 2) if today_chg is not None else None,
                "1w":      period_return(7),
                "1m":      period_return(30),
                "3m":      period_return(91),
                "ytd":     ytd_return(),
                "ema20":   ma_status(s["close"], s["ema20"]),
                "ema50":   ma_status(s["close"], s["ema50"]),
                "sma200":  ma_status(s["close"], s["sma200"]),
            })

        # Group-level weighted average returns (weighted by mktcap)
        def group_return(key):
            weighted_sum = total_w = 0
            for r in stock_rows:
                v = r.get(key)
                w = r.get("mktcap") or 1
                if v is not None:
                    weighted_sum += v * w
                    total_w += w
            return round(weighted_sum / total_w, 2) if total_w else None

        result[industry] = {
            "industry": industry,
            "count":    len(stock_rows),
            "today":    group_return("today"),
            "1w":       group_return("1w"),
            "1m":       group_return("1m"),
            "3m":       group_return("3m"),
            "ytd":      group_return("ytd"),
            "stocks":   stock_rows,
        }

    # Sort by 1M return descending
    sorted_result = dict(sorted(
        result.items(),
        key=lambda x: x[1].get("1m") or -999,
        reverse=True
    ))
    return sorted_result


# ── Enrich universe with industry + historical prices ─
def enrich_universe_history():
    """
    After universe is built, fetch industry label + historical
    prices for 1W/1M/3M/YTD period calculations.
    Stores hist_prices = {date_str: close} per symbol.
    """
    log("Enriching universe with industry + historical prices...")
    today     = date.today().strftime("%Y-%m-%d")
    from_date = (date.today() - timedelta(days=380)).strftime("%Y-%m-%d")

    with lock:
        syms = list(state["universe"].keys())

    enriched = 0
    for sym in syms:
        try:
            info = state["universe"].get(sym, {})

            # Fetch ticker detail for industry/name if missing
            if not info.get("industry"):
                r = req.get(f"https://api.polygon.io/v3/reference/tickers/{sym}"
                            f"?apiKey={POLYGON_KEY}", timeout=15)
                detail = r.json().get("results", {})
                with lock:
                    state["universe"][sym]["industry"] = (
                        detail.get("sic_description") or
                        detail.get("standard_industrial_classification", {}).get("industry", "Other")
                        if isinstance(detail.get("standard_industrial_classification"), dict)
                        else detail.get("sic_description", "Other")
                    )
                    state["universe"][sym]["name"] = detail.get("name", sym)
                time.sleep(0.05)

            # Fetch 380-day history for period returns
            r2 = req.get(f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day"
                         f"/{from_date}/{today}?adjusted=true&sort=asc&limit=400"
                         f"&apiKey={POLYGON_KEY}", timeout=15)
            bars = r2.json().get("results", [])
            hist = {}
            for b in bars:
                d = datetime.utcfromtimestamp(b["t"] / 1000).strftime("%Y-%m-%d")
                hist[d] = b["c"]

            with lock:
                state["universe"][sym]["hist_prices"] = hist

            enriched += 1
            time.sleep(0.05)
        except Exception:
            time.sleep(0.05)

    log(f"Enrichment complete: {enriched} stocks")


# ── HTTP Server ───────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass  # suppress access logs

    def send_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_cors()
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        elif self.path == "/api/breadth":
            b = state.get("breadth")
            if not b:
                # Return empty if not ready yet
                b = {"timestamp": datetime.utcnow().isoformat(),
                     "market_open": False, "initialized": False,
                     "metrics": {}, "colors": {}, "history": state["history"]}
            payload = json.dumps(b).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(payload))
            self.send_cors()
            self.end_headers()
            self.wfile.write(payload)

        elif self.path == "/api/themes":
            # Serve from cache — recomputed every 5 min in background
            cache = state.get("theme_cache")
            if cache:
                payload = json.dumps({"themes": cache["themes"]}).encode()
                self.send_response(200)
            else:
                # Cache not ready yet
                payload = json.dumps({"themes": {}, "status": "loading"}).encode()
                self.send_response(202)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(payload))
            self.send_cors()
            self.end_headers()
            self.wfile.write(payload)

        elif self.path.startswith("/api/themes/"):
            from urllib.parse import unquote
            industry = unquote(self.path[len("/api/themes/"):])
            cache    = state.get("theme_cache")
            data     = (cache or {}).get("full", {}).get(industry)
            if data:
                payload = json.dumps(data).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", len(payload))
                self.send_cors()
                self.end_headers()
                self.wfile.write(payload)
            else:
                self.send_response(404)
                self.end_headers()

        elif self.path == "/api/stream":
            # Server-Sent Events — push every 5 seconds
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_cors()
            self.end_headers()
            try:
                while True:
                    b = state.get("breadth") or {}
                    payload = json.dumps(b)
                    msg = f"data: {payload}\n\n".encode()
                    self.wfile.write(msg)
                    self.wfile.flush()
                    time.sleep(5)
            except Exception:
                pass
        else:
            self.send_response(404)
            self.end_headers()

# ── Main ──────────────────────────────────────────────
if __name__ == "__main__":
    load_history()

    # Build universe in background (takes a few minutes)
    t_universe = threading.Thread(target=build_universe, daemon=True)
    t_universe.start()

    # WebSocket in background
    t_ws = threading.Thread(target=start_ws, daemon=True)
    t_ws.start()

    # Compute loop
    t_compute = threading.Thread(target=compute_loop, daemon=True)
    t_compute.start()

    # Theme cache loop
    t_theme = threading.Thread(target=theme_cache_loop, daemon=True)
    t_theme.start()

    # EOD snapshot loop
    t_eod = threading.Thread(target=eod_snapshot_loop, daemon=True)
    t_eod.start()

    log(f"Server starting on port {PORT}")
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()
