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


from .iqplus import IQPlusSource
from .ipot import IPOTSource

__all__ = ["IQPlusSource", "IPOTSource", "CSV_COLUMNS", "derive_avg"]
