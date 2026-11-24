"""IQPlus source — public HTML scrape, no auth.

Broker summary lives at `net_buy_sell.php` (nav: BROKSUM). Two query shapes use
the same six columns (Broker/Code | B.Lot | B.Avg | S.Lot | S.Avg | N.Lot) and
carry exact floats in each cell's `data-sort`, so values are rebuilt as
lot*100*avg to the rupiah (verified against IPOT rows already in Supabase:
152/152 rows identical on blot/bval/slot/sval/nlot/nval, and 82/82 on the
average columns).

  scode=<TICKER>&tab=broker  -> that ticker's top brokers by net lot
  scode=<BROKER>&tab=stock   -> that broker's top tickers by net lot

Each list is capped upstream at 10 rows with no request param lifting it, so
`fetch()` alone yields 10 brokers per ticker. `fetch_broker_pass()` runs the
reverse query for every broker seen and refills the pairs the per-ticker pass
missed (measured up to +110% rows on a liquid ticker, +0 on some others).

ponytail: exchange-listed trade frequency is gone from the page, so bfreq/sfreq
are NULL. Broker-level bf/sf still exists at broker_flow.php?api=1&p=1D if that
ever matters more than per-ticker values.
"""

import logging
import re
import time

import requests
from bs4 import BeautifulSoup

from . import derive_avg, winning_avg

log = logging.getLogger(__name__)

# IDX tickers are exactly four letters; the reverse table also lists warrants
# and rights (`BBCANG`, `BBCA-W`) that must not become DB symbols.
_TICKER_RE = re.compile(r"^[A-Z]{4}$")
# Broker codes are exactly two letters. Guards against a placeholder row (the
# old scraper wrote `broker_code='--'` into 2931 rows from 2026-05-29 on).
_BROKER_RE = re.compile(r"^[A-Z]{2}$")

DATA_URL = "https://www.iqplus.info/net_buy_sell.php"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.iqplus.info/",
}


def _num(td):
    """Exact value from the cell's data-sort attribute."""
    raw = ((td.get("data-sort") if td is not None else None) or "").strip()
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_table(html, table_id):
    """Yield (code, blot, bavg, slot, savg) from one IQPlus broker table.

    Raises when the table is absent so a layout change reads as a source
    failure instead of "no trades" (an empty table still renders its header).
    """
    table = BeautifulSoup(html, "html.parser").find("table", id=table_id)
    if table is None:
        raise RuntimeError(f"IQPlus: {table_id} missing (page layout changed)")
    tbody = table.find("tbody")
    if tbody is None:
        return
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
        code = (tds[0].get("data-sort") or "").strip()
        blot, bavg, slot, savg = (_num(td) for td in tds[1:5])
        if not code or blot is None or slot is None:
            continue
        yield code, blot, bavg, slot, savg


def _in_universe(ticker, only_symbols):
    """A reverse-table ticker is usable when it is a plain 4-letter symbol and,
    if a universe is given, one we track."""
    return bool(_TICKER_RE.match(ticker)) and (
        only_symbols is None or f"{ticker}.JK" in only_symbols
    )


def _row(symbol, date_str, broker_code, blot, bavg, slot, savg):
    bval = round(blot * 100 * bavg) if bavg is not None else None
    sval = round(slot * 100 * savg) if savg is not None else None
    bavg_per_share = derive_avg(bval, blot)
    savg_per_share = derive_avg(sval, slot)
    nval = bval - sval if bval is not None and sval is not None else None
    nlot = int(round(blot - slot))
    return {
        "symbol": symbol,
        "date": date_str,
        "broker_code": broker_code,
        "bfreq": None,
        "blot": int(round(blot)),
        "bval": bval,
        "bavg_per_share": bavg_per_share,
        "sfreq": None,
        "slot": int(round(slot)),
        "sval": sval,
        "savg_per_share": savg_per_share,
        "nlot": nlot,
        "nval": nval,
        "navg_per_share": winning_avg(nval, bavg_per_share, savg_per_share),
    }


class IQPlusSource:
    name = "IQPlus"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, scode, tab, date_str):
        resp = self.session.get(
            DATA_URL,
            params={
                "scode": scode.upper(),
                "tab": tab,
                "date_start": date_str,
                "date_end": date_str,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.text

    def fetch(self, ticker, date_str):
        """All broker rows for one ticker on one date (top 10 by net lot)."""
        html = self._get(ticker, "broker", date_str)
        symbol = f"{ticker.upper()}.JK"
        return [
            _row(symbol, date_str, code, blot, bavg, slot, savg)
            for code, blot, bavg, slot, savg in _parse_table(html, "table-broker")
            if _BROKER_RE.match(code)
        ]

    def fetch_broker_pass(self, date_str, broker_codes, delay=0.0, only_symbols=None):
        """Reverse query: each broker's top tickers on one date.

        Fills (ticker, broker) pairs the per-ticker top-10 ranks miss. Costs one
        request per broker (~88/day) regardless of how many tickers exist.

        only_symbols (set of "BBCA.JK") keeps the result inside the tracked
        universe — the reverse table also lists warrants/rights like BBCANG.
        """
        rows = []
        for i, code in enumerate(sorted(broker_codes), 1):
            try:
                html = self._get(code, "stock", date_str)
                parsed = list(_parse_table(html, "table-stock"))
            except Exception as e:
                log.warning(f"  broker pass {code}: {e}")
                continue
            for ticker, blot, bavg, slot, savg in parsed:
                if not _in_universe(ticker, only_symbols):
                    continue
                rows.append(_row(f"{ticker}.JK", date_str, code, blot, bavg, slot, savg))
            if delay and i < len(broker_codes):
                time.sleep(delay)
        return rows

    def close(self):
        self.session.close()


if __name__ == "__main__":
    # Offline self-check: IQPlus's own EXACT numbers in -> same rupiah values out.
    fx = """<table id="table-broker"><tbody><tr>
      <td data-sort="KZ">KZ<span>CLSA</span></td>
      <td data-sort="298587">298,59K</td><td data-sort="5661.6274318708">5.661,63</td>
      <td data-sort="150393">150,39K</td><td data-sort="5594.3069158804">5.594,31</td>
      <td data-sort="148194">+148,19K</td></tr></tbody></table>"""
    (row,) = [
        _row("BBCA.JK", "2026-11-23", code, blot, bavg, slot, savg)
        for code, blot, bavg, slot, savg in _parse_table(fx, "table-broker")
    ]
    assert row["broker_code"] == "KZ"
    assert row["bval"] == 169048835000, row["bval"]
    assert row["sval"] == 84134460000, row["sval"]
    assert row["nval"] == 84914375000, row["nval"]
    assert row["nlot"] == 148194
    assert row["navg_per_share"] == 5661.6274, row["navg_per_share"]
    assert _in_universe("BBCA", None) and _in_universe("BBCA", {"BBCA.JK"})
    assert not _in_universe("BBCANG", None)          # warrant, 6 chars
    assert not _in_universe("BBCA-W", {"BBCA.JK"})   # suffix form
    assert not _in_universe("GOTO", {"BBCA.JK"})     # outside universe
    assert _BROKER_RE.match("KZ") and not _BROKER_RE.match("--")
    try:
        list(_parse_table("<html/>", "table-broker"))
    except RuntimeError:
        print("iqplus self-check ok")
    else:
        raise AssertionError("missing table must raise")
