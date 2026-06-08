"""IQPlus source — public HTML scrape, no auth."""

import logging

import requests
from bs4 import BeautifulSoup

from . import derive_avg, winning_avg

log = logging.getLogger(__name__)

FORM_URL = "https://www.iqplus.info/market_summary/historical/net_by_sell_by_date/"
DATA_URL = "https://www.iqplus.info/box_net_buy_sell_bydate_act.php"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": FORM_URL,
}


def _parse_number(text):
    """IQPlus uses '.' as thousands separator: '9.180.000' -> 9180000."""
    s = (text or "").strip()
    if not s:
        return None
    neg = s.startswith("-")
    if neg:
        s = s[1:]
    s = s.replace(".", "")
    if not s.isdigit():
        return None
    return -int(s) if neg else int(s)


class IQPlusSource:
    name = "IQPlus"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        try:
            self.session.get(FORM_URL, timeout=30)
        except Exception as e:
            log.warning(f"IQPlus session warm-up failed: {e}")

    def fetch(self, ticker, date_str):
        """Fetch all broker rows for one ticker on one date."""
        files = {
            "code": (None, ticker.upper()),
            "start_date": (None, date_str),
            "end_date": (None, date_str),
        }
        resp = self.session.post(DATA_URL, files=files, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="greytable")
        if table is None:
            return []
        tbody = table.find("tbody")
        if tbody is None:
            return []

        rows = []
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) != 11:
                continue
            broker_code = tds[0].get_text(strip=True)
            if not broker_code:
                continue

            bfreq = _parse_number(tds[1].get_text(strip=True))
            blot = _parse_number(tds[2].get_text(strip=True))
            bval = _parse_number(tds[3].get_text(strip=True))
            sfreq = _parse_number(tds[5].get_text(strip=True))
            slot = _parse_number(tds[6].get_text(strip=True))
            sval = _parse_number(tds[7].get_text(strip=True))
            nlot = _parse_number(tds[8].get_text(strip=True))
            nval = _parse_number(tds[9].get_text(strip=True))

            bavg = derive_avg(bval, blot)
            savg = derive_avg(sval, slot)
            rows.append({
                "symbol": f"{ticker.upper()}.JK",
                "date": date_str,
                "broker_code": broker_code,
                "bfreq": bfreq,
                "blot": blot,
                "bval": bval,
                "bavg_per_share": bavg,
                "sfreq": sfreq,
                "slot": slot,
                "sval": sval,
                "savg_per_share": savg,
                "nlot": nlot,
                "nval": nval,
                "navg_per_share": winning_avg(nval, bavg, savg),
            })
        return rows

    def close(self):
        self.session.close()
