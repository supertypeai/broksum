"""Per-vendor broker-summary fetchers.

Each source returns row dicts matching CSV_COLUMNS so the orchestrator
in scrape.py can hand them straight to write_csv / batch_upsert.
"""

CSV_COLUMNS = [
    "symbol", "date", "broker_code",
    "bfreq", "blot", "bval", "bavg_per_share",
    "sfreq", "slot", "sval", "savg_per_share",
    "nlot", "nval", "navg_per_share",
]


def derive_avg(val, lot):
    """Avg price per share = val / (lot * 100). Lots are 100-share units."""
    if val is None or lot is None or lot == 0:
        return None
    return round(val / (lot * 100), 4)


def winning_avg(nval, bavg, savg):
    """Securities-display convention for net avg price per share: show the
    winning side's avg. Net buyer -> buy avg. Net seller -> sell avg.

    Matches IPOT / Stockbit / RTI display. The natural-looking
    `|nval| / (|nlot| * 100)` blows up when buy and sell sides are close in
    volume but differ in price (tiny residual / non-tiny residual value
    produces absurd per-share numbers, e.g. 6391 for a stock trading at 3247).
    """
    if nval is None or nval == 0:
        return None
    return bavg if nval > 0 else savg


from .iqplus import IQPlusSource
from .ipot import IPOTSource

__all__ = ["IQPlusSource", "IPOTSource", "CSV_COLUMNS", "derive_avg", "winning_avg"]
