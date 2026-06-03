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

    empty_dates: list[str] = []  # dates that produced 0 rows (probe skip or main-loop empty)

    try:
        for di, target_date in enumerate(dates, 1):
            date_str = target_date.isoformat()
            csv_path = DATA_DIR / f"{date_str}.csv"

            if csv_path.exists():
                log.info(f"[{di}/{len(dates)}] {date_str} — CSV exists, skipping")
                continue

            log.info(f"[{di}/{len(dates)}] Scraping {date_str} ...")

            # Holiday detection: check 5 liquid tickers via the primary source.
            # MUST be primary (IPOT, same-day publish), NOT IQPlus (T+1 publish):
            # the cron fires at 18:00 WIB same-day, before IQPlus has published
            # today's data. Using IQPlus here false-flags every trading day as
            # a holiday and the script silently skips. (Verified May 26-28 2026.)
            probe_source = primary
            probe_tickers = {"BBCA", "BBRI", "TLKM", "ASII", "BMRI"}
            probe_rows = []
            probe_exceptions = []  # log each failure so empty probe is debuggable
            for pt in probe_tickers:
                try:
                    probe = probe_source.fetch(pt, date_str)
                    probe_rows.extend(probe)
                except Exception as e:
                    probe_exceptions.append((pt, type(e).__name__, str(e)[:120]))
                time.sleep(args.delay + random.uniform(0, args.jitter))

            # If all 5 probes raised, it's almost certainly an auth/network issue,
            # not a holiday. Surface the exceptions so the GH log isn't silent.
            if probe_exceptions:
                for pt, etype, msg in probe_exceptions:
                    log.warning(f"  probe {pt} {etype}: {msg}")
            if not probe_rows:
                # Log as both a human-readable warning and a GitHub Actions
                # `::error::` annotation so the run shows a red callout +
                # exits non-zero at end (notifications fire per repo settings).
                # Common causes: real IDX holiday, IPOT auth expired, or
                # IPOT publish delayed past 18:00 WIB. Verify on idx.co.id.
                log.error(
                    f"::error::no data from probe tickers for {date_str}. "
                    f"Could be a real holiday OR an IPOT outage / auth issue. "
                    f"Verify on idx.co.id before dismissing."
                )
                empty_dates.append(date_str)
                continue

            # Probe rows came from the primary source — keep them. Fetch the rest
            # via fetch_with_fallback so any per-ticker IPOT failure falls back to IQPlus.
            all_rows = list(probe_rows)
            failed = 0
            source_counts = {primary.name: len(probe_rows)}

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
                log.error(
                    f"::error::no rows scraped for {date_str} ({failed} ticker failures). "
                    f"Source orchestrator returned empty across the board — IPOT + IQPlus both down?"
                )
                empty_dates.append(date_str)
    finally:
        primary.close()
        if fallback is not None:
            fallback.close()

    # Fail the run if any date produced 0 rows. This breaks GitHub Actions
    # green-checkmark silence on false-skip / outage / auth-expired scenarios.
    # `--allow-empty` overrides (use for explicit backfills of real holidays).
    if empty_dates and not getattr(args, "allow_empty", False):
        log.error(
            f"::error::scrape produced 0 rows for {len(empty_dates)} date(s): {empty_dates}. "
            f"Exit 1 so GH Actions marks the run as failed. "
            f"Re-run with --allow-empty if these are confirmed holidays."
        )
        raise SystemExit(1)


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
# Registry (one-off scrape of IDX exchange-members profile pages)
# ---------------------------------------------------------------------------

REGISTRY_CSV_COLUMNS = [
    "broker_code", "broker_name", "is_foreign", "license_type",
    "member_status", "source_url",
]


def cmd_registry(args):
    """Scrape IDX per-broker profile pages, write data/broker_registry.csv."""
    from sources.idx_members import scrape_members

    client = get_supabase()
    # Distinct broker codes from one recent trading day in our own broksum data
    # — every active IDX member touches a liquid stock on a normal trading day,
    # so a single day's rows give us the full set without paginating the whole
    # table (PostgREST has no native DISTINCT). Latest day from `latest_date_in_table`.
    log.info("Finding latest available date in idx_broker_summary_daily ...")
    latest = (
        client.table("idx_broker_summary_daily")
        .select("date")
        .order("date", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if not latest:
        log.error("No data in idx_broker_summary_daily — can't seed registry")
        return
    seed_date = latest[0]["date"]
    log.info(f"  Pulling distinct broker codes from {seed_date} ...")

    codes: set[str] = set()
    PAGE = 1000
    from_ = 0
    while True:
        data = (
            client.table("idx_broker_summary_daily")
            .select("broker_code")
            .eq("date", seed_date)
            .range(from_, from_ + PAGE - 1)
            .execute()
            .data
        )
        if not data:
            break
        codes.update(r["broker_code"] for r in data if r.get("broker_code"))
        if len(data) < PAGE:
            break
        from_ += PAGE
    sorted_codes = sorted(codes)
    log.info(f"  {len(sorted_codes)} distinct broker codes found")

    if args.limit:
        sorted_codes = sorted_codes[: args.limit]
        log.info(f"  --limit {args.limit} applied → scraping {len(sorted_codes)} codes")

    log.info(f"Launching Playwright to scrape IDX profile pages ...")
    results = scrape_members(sorted_codes)

    DATA_DIR.mkdir(exist_ok=True)
    out_path = DATA_DIR / "broker_registry.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REGISTRY_CSV_COLUMNS)
        writer.writeheader()
        for r in results:
            writer.writerow({
                "broker_code": r["code"],
                "broker_name": r["name"] or "",
                "is_foreign": "true" if r["is_foreign"] else "false",
                "license_type": r["license_type"] or "",
                "member_status": "active" if r["ok"] else "unknown",
                "source_url": r["source_url"],
            })

    n_ok = sum(1 for r in results if r["ok"])
    log.info(f"Wrote {out_path} — {n_ok}/{len(results)} brokers with names")


def cmd_refresh_cohort(args):
    """Refresh `cohort` column in idx_broker_registry from broksum behavior.

    Classification (last 30 days of activity from idx_broker_summary_daily):
      - foreign broker → always 'institutional'
      - domestic with insufficient activity (<1000 trades / 30d) → 'unknown'
      - domestic with avg lots-per-trade < 100 → 'retail'
      - domestic with 100 <= avg lots-per-trade < 250 → 'mixed'
      - domestic with avg lots-per-trade >= 250 → 'institutional'

    The 'mixed' bucket captures the reality that most domestic brokers (Mandiri,
    BCA, BNI, BRI, Bahana, Panin, Trimegah etc.) operate BOTH retail platforms
    and institutional desks. Pure 'institutional' is reserved for foreign
    brokers + specialized domestic players that only handle large-ticket orders.
    """
    from datetime import date, timedelta

    client = get_supabase()

    log.info("Pulling broker registry ...")
    reg_data = (
        client.table("idx_broker_registry")
        .select("broker_code,is_foreign")
        .execute()
        .data
    )
    if not reg_data:
        log.error("idx_broker_registry is empty — run `python scrape.py registry` first")
        return
    is_foreign_map = {r["broker_code"]: bool(r["is_foreign"]) for r in reg_data}
    log.info(f"  {len(is_foreign_map)} brokers in registry")

    end = date.today()
    start = end - timedelta(days=30)
    log.info(f"Pulling broker activity {start} -> {end} ...")

    per_broker: dict[str, dict[str, int]] = {}
    PAGE = 1000
    from_ = 0
    while True:
        data = (
            client.table("idx_broker_summary_daily")
            .select("broker_code,blot,slot,bfreq,sfreq")
            .gte("date", start.isoformat())
            .range(from_, from_ + PAGE - 1)
            .execute()
            .data
        )
        if not data:
            break
        for r in data:
            bc = r.get("broker_code")
            if not bc:
                continue
            agg = per_broker.setdefault(bc, {"lots": 0, "freq": 0})
            agg["lots"] += (r.get("blot") or 0) + (r.get("slot") or 0)
            agg["freq"] += (r.get("bfreq") or 0) + (r.get("sfreq") or 0)
        if len(data) < PAGE:
            break
        from_ += PAGE
    log.info(f"  aggregated {len(per_broker)} brokers from 30-day activity")

    def classify(bc: str, is_foreign: bool) -> tuple[str, float | None]:
        if is_foreign:
            return "Institutional", None
        agg = per_broker.get(bc, {"lots": 0, "freq": 0})
        freq = agg["freq"]
        lots = agg["lots"]
        if freq < 1000:
            return "Unknown", None
        avg = lots / freq
        if avg < 100:
            return "Retail", avg
        if avg < 250:
            return "Mixed", avg
        return "Institutional", avg

    updates = []
    counts = {"Retail": 0, "Mixed": 0, "Institutional": 0, "Unknown": 0}
    for bc, is_foreign in is_foreign_map.items():
        cohort, avg = classify(bc, is_foreign)
        counts[cohort] += 1
        updates.append((bc, cohort, avg))

    log.info(f"  classification: {counts}")

    log.info("Updating registry ...")
    for bc, cohort, _ in updates:
        client.table("idx_broker_registry").update(
            {"cohort": cohort}
        ).eq("broker_code", bc).execute()
    log.info(f"Refreshed cohort for {len(updates)} brokers")

    # Print the borderline cases so user can sanity-check
    domestics = [(bc, c, a) for bc, c, a in updates if a is not None]
    domestics.sort(key=lambda x: x[2] or 0)
    log.info("Domestic brokers ranked by avg lots/trade (lowest first):")
    for bc, cohort, avg in domestics:
        log.info(f"  {bc:>3}  avg={avg:>7.1f} lots/trade  → {cohort}")


def cmd_upload_registry(args):
    """Upsert data/broker_registry.csv into the Supabase idx_broker_registry table."""
    client = get_supabase()
    path = DATA_DIR / "broker_registry.csv"
    if not path.exists():
        log.error(f"{path} not found — run `python scrape.py registry` first")
        return

    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if not r["broker_name"]:
                log.info(f"  skipping {r['broker_code']} — no name scraped")
                continue
            rows.append({
                "broker_code": r["broker_code"],
                "broker_name": r["broker_name"],
                "is_foreign": r["is_foreign"].lower() == "true",
                "license_type": r["license_type"] or None,
                "member_status": r["member_status"],
                "source_url": r["source_url"],
            })

    if not rows:
        log.info("No usable rows to upload")
        return

    log.info(f"Upserting {len(rows)} broker_registry rows to Supabase ...")
    for i in range(0, len(rows), 500):
        batch = rows[i : i + 500]
        client.table("idx_broker_registry").upsert(
            batch, on_conflict="broker_code"
        ).execute()
    log.info(f"Upload complete — {len(rows)} rows in idx_broker_registry")


def cmd_normalize_registry(args):
    """Re-normalize broker_name, member_status, cohort, and license_type for
    all rows already in idx_broker_registry. Idempotent — safe to re-run.

    Used when the normalization rules change without needing a full re-scrape.
    Applies the same _normalize_name / _clean_license helpers as the scraper.
    """
    from sources.idx_members import _normalize_name, _clean_license

    client = get_supabase()
    rows = (
        client.table("idx_broker_registry")
        .select("broker_code, broker_name, member_status, cohort, license_type")
        .execute()
        .data
        or []
    )
    log.info(f"Fetched {len(rows)} rows from idx_broker_registry")

    cohort_title = {
        "retail": "Retail",
        "mixed": "Mixed",
        "institutional": "Institutional",
        "unknown": "Unknown",
    }
    updates = 0
    for r in rows:
        update: dict = {}
        if r.get("broker_name"):
            new_name = _normalize_name(r["broker_name"])
            if new_name != r["broker_name"]:
                update["broker_name"] = new_name
        new_status = (r.get("member_status") or "Unknown").strip().capitalize()
        if new_status != r.get("member_status"):
            update["member_status"] = new_status
        cohort_raw = (r.get("cohort") or "").strip().lower()
        if cohort_raw in cohort_title and cohort_title[cohort_raw] != r.get("cohort"):
            update["cohort"] = cohort_title[cohort_raw]
        new_lic = _clean_license(r.get("license_type"))
        if new_lic != r.get("license_type"):
            update["license_type"] = new_lic
        if not update:
            continue
        client.table("idx_broker_registry").update(update).eq(
            "broker_code", r["broker_code"]
        ).execute()
        updates += 1
        log.info(f"  {r['broker_code']}: {update}")
    log.info(f"Normalized {updates}/{len(rows)} rows")


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
    sp.add_argument(
        "--allow-empty",
        action="store_true",
        help="Don't exit 1 when a date produces 0 rows (use for backfilling confirmed holidays)",
    )

    up = sub.add_parser("upload", help="Upload CSVs to Supabase")
    up.add_argument("--date", help="Upload only this date's CSV")

    reg = sub.add_parser(
        "registry",
        help="Scrape IDX exchange-members profiles → data/broker_registry.csv",
    )
    reg.add_argument("--limit", type=int, help="Max broker codes to scrape (for smoke testing)")

    upreg = sub.add_parser(
        "upload-registry",
        help="Upload data/broker_registry.csv to Supabase idx_broker_registry",
    )

    rcoh = sub.add_parser(
        "refresh-cohort",
        help="Recompute idx_broker_registry.cohort from broksum 30-day activity",
    )

    sub.add_parser(
        "normalize-registry",
        help="Re-normalize broker_name / member_status / cohort / license_type for existing idx_broker_registry rows",
    )

    args = parser.parse_args()
    if args.command == "scrape":
        cmd_scrape(args)
    elif args.command == "upload":
        cmd_upload(args)
    elif args.command == "registry":
        cmd_registry(args)
    elif args.command == "upload-registry":
        cmd_upload_registry(args)
    elif args.command == "refresh-cohort":
        cmd_refresh_cohort(args)
    elif args.command == "normalize-registry":
        cmd_normalize_registry(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
