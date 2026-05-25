"""Daily broker summary scraper → CSV → Supabase.

Sources tried per ticker per date (first hit wins):
  1. IPOT  (login-gated, full broker list, server-side date aggregation)
  2. IQPlus (public HTML, fallback)

Usage:
    python scrape.py scrape                              # scrape yesterday
    python scrape.py scrape --date 2026-04-23            # specific date
    python scrape.py scrape --backfill 2025-01-01 2026-04-24  # date range
    python scrape.py scrape --symbol BBCA.JK --limit 5   # filter tickers
    python scrape.py upload                              # upload all CSVs
    python scrape.py upload --date 2026-04-23            # upload one date
"""

import argparse
import csv
import logging
import os
import random
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

from sources import CSV_COLUMNS, IPOTSource, IQPlusSource

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
RETRY_DELAYS = [5, 15, 45]


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def write_csv(date_str: str, rows: list[dict]) -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    path = DATA_DIR / f"{date_str}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            for col in ("bfreq", "blot", "bval", "sfreq", "slot", "sval", "nlot", "nval"):
                r[col] = int(r[col]) if r[col] else None
            for col in ("bavg_per_share", "savg_per_share", "navg_per_share"):
                r[col] = float(r[col]) if r[col] else None
            rows.append(r)
        return rows


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def last_trading_day(today: date) -> date:
    """Most recent IDX trading day strictly BEFORE today (T-1 mode).

    Used by the legacy IQPlus path (T+1 publish — yesterday's data lands
    next morning). Kept for backfill scenarios.
    """
    d = today - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def latest_trading_day(today: date) -> date:
    """Most recent IDX trading day on or before today (T-0 mode).

    Used when the cron runs same-day evening after market close (~18:00 WIB).
    Returns today on weekdays, rolls back to Friday on weekends.
    """
    d = today
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def trading_days_in_range(start: date, end: date) -> list[date]:
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Supabase upload
# ---------------------------------------------------------------------------

def get_supabase():
    """Sectors Supabase — for uploading broker data."""
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


def _is_transient_5xx(err: Exception) -> bool:
    s = str(err)
    return any(code in s for code in ("502", "503", "504", "Bad gateway", "Bad Gateway"))


def batch_upsert(client, rows: list[dict], batch_size: int = 500) -> int:
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        for attempt in range(3):
            try:
                client.table("idx_broker_summary_daily").upsert(
                    batch, on_conflict="symbol,date,broker_code"
                ).execute()
                break
            except Exception as e:
                if attempt < 2 and _is_transient_5xx(e):
                    delay = RETRY_DELAYS[attempt]
                    log.warning(f"Upsert attempt {attempt + 1} hit 5xx, retrying in {delay}s: {e}")
                    time.sleep(delay)
                else:
                    raise
        total += len(batch)
        log.info(f"Upserted {total}/{len(rows)} rows")
    return total


# ---------------------------------------------------------------------------
# Source orchestration
# ---------------------------------------------------------------------------

def _build_sources():
    """Return (primary, fallback). IPOT is primary if ipot_state.json exists."""
    iqplus = IQPlusSource()
    try:
        ipot = IPOTSource()  # uses default state file at broksum/ipot_state.json
        return ipot, iqplus
    except Exception as e:
        log.info(f"IPOT unavailable, IQPlus-only mode: {e}")
        return iqplus, None


def fetch_with_fallback(primary, fallback, ticker, date_str):
    """Try primary → fallback. Returns (rows, source_name)."""
    try:
        rows = primary.fetch(ticker, date_str)
        if rows:
            return rows, primary.name
        log.warning(f"  {ticker}: {primary.name} returned 0 rows")
    except NotImplementedError:
        # IPOT phase-1 skeleton — silently fall through
        pass
    except Exception as e:
        log.warning(f"  {ticker}: {primary.name} failed: {e}")
    if fallback is not None:
        try:
            rows = fallback.fetch(ticker, date_str)
            return rows, fallback.name
        except Exception as e:
            log.warning(f"  {ticker}: {fallback.name} also failed: {e}")
    return [], None


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_scrape(args):
    client = get_supabase()

    # Determine symbols (from Sectors idx_company_profile)
    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        resp = client.table("idx_company_profile").select("symbol").execute()
        symbols = sorted(r["symbol"] for r in resp.data)
    if args.limit:
        symbols = symbols[: args.limit]

    # Determine dates
    if args.backfill:
        start = date.fromisoformat(args.backfill[0])
        end = date.fromisoformat(args.backfill[1])
        dates = trading_days_in_range(start, end)
    elif args.date:
        dates = [date.fromisoformat(args.date)]
    else:
        # Same-day mode (cron runs ~18:00 WIB after market close, IPOT
        # publishes within ~2h of close). For pre-IPOT IQPlus-only mode use
        # last_trading_day() to get yesterday's data.
        dates = [latest_trading_day(date.today())]

    log.info(f"{len(symbols)} symbols, {len(dates)} trading day(s): {dates[0]} → {dates[-1]}")

    DATA_DIR.mkdir(exist_ok=True)
    primary, fallback = _build_sources()
    log.info(f"Primary source: {primary.name}" + (f", fallback: {fallback.name}" if fallback else " (no fallback)"))

    try:
        for di, target_date in enumerate(dates, 1):
            date_str = target_date.isoformat()
            csv_path = DATA_DIR / f"{date_str}.csv"

            if csv_path.exists():
                log.info(f"[{di}/{len(dates)}] {date_str} — CSV exists, skipping")
                continue

            log.info(f"[{di}/{len(dates)}] Scraping {date_str} ...")

            # Holiday detection: check 5 liquid tickers first. If all empty, skip.
            # Always probe via IQPlus — stateless and fast, doesn't burn IPOT auth.
            iqplus_for_probe = fallback if isinstance(fallback, IQPlusSource) else (primary if isinstance(primary, IQPlusSource) else IQPlusSource())
            probe_tickers = {"BBCA", "BBRI", "TLKM", "ASII", "BMRI"}
            probe_rows = []
            for pt in probe_tickers:
                try:
                    probe = iqplus_for_probe.fetch(pt, date_str)
                    probe_rows.extend(probe)
                except Exception:
                    pass
                time.sleep(args.delay + random.uniform(0, args.jitter))
            if not probe_rows:
                log.info(f"  {date_str} — no data from probe tickers, likely holiday, skipping")
                continue

            # Probe rows came from IQPlus — keep them. Fetch the rest via primary.
            all_rows = list(probe_rows)
            failed = 0
            source_counts = {primary.name: 0, "IQPlus": len(probe_rows)}

            for i, symbol in enumerate(symbols, 1):
                ticker = symbol.replace(".JK", "").upper()
                if ticker in probe_tickers:
                    continue

                rows, src = fetch_with_fallback(primary, fallback, ticker, date_str)
                if rows:
                    all_rows.extend(rows)
                    source_counts[src] = source_counts.get(src, 0) + len(rows)
                else:
                    failed += 1

                if i % 50 == 0:
                    log.info(f"  [{i}/{len(symbols)}] {len(all_rows)} rows so far, {failed} failed")

                time.sleep(args.delay + random.uniform(0, args.jitter))

            if all_rows:
                path = write_csv(date_str, all_rows)
                src_summary = ", ".join(f"{k}={v}" for k, v in source_counts.items() if v)
                log.info(f"  {date_str} done — {len(all_rows)} rows → {path} ({failed} failed; sources: {src_summary})")
            else:
                log.warning(f"  {date_str} — no rows scraped ({failed} failed)")
    finally:
        primary.close()
        if fallback is not None:
            fallback.close()


def cmd_upload(args):
    client = get_supabase()
    DATA_DIR.mkdir(exist_ok=True)

    if args.date:
        files = [DATA_DIR / f"{args.date}.csv"]
    else:
        files = sorted(DATA_DIR.glob("*.csv"))

    if not files:
        log.info("No CSV files to upload")
        return

    total = 0
    for path in files:
        if not path.exists():
            log.warning(f"{path} not found, skipping")
            continue
        rows = read_csv(path)
        if not rows:
            log.info(f"{path.name}: empty, skipping")
            continue
        log.info(f"Uploading {path.name} ({len(rows)} rows) ...")
        n = batch_upsert(client, rows)
        total += n

    log.info(f"Upload complete — {total} rows across {len(files)} file(s)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Daily broker summary: IPOT/IQPlus → CSV → Supabase")
    sub = parser.add_subparsers(dest="command")

    sp = sub.add_parser("scrape", help="Scrape and save to CSV")
    sp.add_argument("--date", help="YYYY-MM-DD (default: last trading day)")
    sp.add_argument("--backfill", nargs=2, metavar=("START", "END"), help="Date range for backfill")
    sp.add_argument("--symbol", help="Single symbol e.g. BBCA.JK")
    sp.add_argument("--limit", type=int, help="Max tickers to process")
    sp.add_argument("--delay", type=float, default=1.0, help="Base delay between requests (s)")
    sp.add_argument("--jitter", type=float, default=0.5, help="Random jitter added to delay (s)")

    up = sub.add_parser("upload", help="Upload CSVs to Supabase")
    up.add_argument("--date", help="Upload only this date's CSV")

    args = parser.parse_args()
    if args.command == "scrape":
        cmd_scrape(args)
    elif args.command == "upload":
        cmd_upload(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
