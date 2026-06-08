"""IPOT (IndoPremier) source — curl_cffi WS hybrid.

One-time setup (human, ~1 min):
    python auth_bootstrap.py        # opens visible Chrome, you scan QR
    -> writes ipot_creds.json with autologintoken + cookies + appsession

Daily cron (automated, no browser):
    python scrape.py scrape ...     # reads ipot_creds.json, runs over WSS
    -> rotates autologintoken on each successful auth, persists back

The curl_cffi (Chrome TLS impersonation) bypasses IPOT's TLS-fingerprint
bot detection that blocks Playwright. Token replay across TLS contexts
verified Phase B 2026-05-07.

Endpoint contract: see sources/_socketcluster.py docstring.
RPC: service='midata', cmd='query', index='en_qu_top_bs',
     args=['b', ticker, '', 'RG', '%', d1, d2] (dates YYYY-M-DD)
Response: pipe-delimited "code|bval|bvol|bfreq|sval|svol|sfreq|nval|nvol|tval|tlot|??"
Volumes are in shares; we /100 to lots to match our DB schema.
"""

import json
import logging
from pathlib import Path

from . import derive_avg, winning_avg
from ._socketcluster import IPOTAuthError, IPOTConnection

log = logging.getLogger(__name__)

DEFAULT_CREDS_PATH = Path(__file__).parent.parent / "ipot_creds.json"


class IPOTSource:
    name = "IPOT"

    def __init__(self, creds_path=None):
        self.creds_path = Path(creds_path) if creds_path else DEFAULT_CREDS_PATH
        if not self.creds_path.exists():
            raise IPOTAuthError(
                f"No IPOT creds at {self.creds_path}. "
                "Run `python auth_bootstrap.py` to create one."
            )
        self._conn: IPOTConnection | None = None

    def _ensure_authenticated(self) -> None:
        if self._conn is not None:
            return
        creds = json.loads(self.creds_path.read_text())
        token = creds.get("autologintoken")
        if not token:
            raise IPOTAuthError(f"{self.creds_path} has no autologintoken")
        self._conn = IPOTConnection(
            autologintoken=token,
            appsession=creds.get("appsession"),
            cookies=creds.get("cookies") or [],
        )
        try:
            self._conn.connect()
            self._conn.authenticate()
        except Exception:
            self._conn = None
            raise
        # Persist rotated token immediately
        creds["autologintoken"] = self._conn.autologintoken
        self.creds_path.write_text(json.dumps(creds, indent=2, default=str))

    def login(self) -> None:
        """Compatibility no-op; auth is lazy via _ensure_authenticated."""
        self._ensure_authenticated()

    def fetch(self, ticker: str, date_str: str) -> list[dict]:
        """Fetch all broker rows for ticker on date.

        date_str is 'YYYY-MM-DD'. Reformatted to IPOT's 'YYYY-M-DD' (no zero-pad).
        """
        self._ensure_authenticated()
        y, m, d = date_str.split("-")
        ipot_date = f"{int(y)}-{int(m)}-{int(d)}"

        records = self._conn.send_request(
            service="midata",
            cmd="query",
            param={
                "source": "datafeed",
                "index": "en_qu_top_bs",
                "args": ["b", ticker.upper(), "", "RG", "%", ipot_date, ipot_date],
            },
        )
        return self._parse_rows(ticker, date_str, records)

    @staticmethod
    def _parse_rows(ticker: str, date_str: str, raw: list) -> list[dict]:
        out = []
        for entry in raw or []:
            s = entry.get("en_qu_top_bs") if isinstance(entry, dict) else None
            if not s:
                continue
            parts = s.split("|")
            if len(parts) < 11:
                continue
            try:
                broker_code = parts[0]
                bval = int(parts[1]) if parts[1] else 0
                bvol = int(parts[2]) if parts[2] else 0
                bfreq = int(parts[3]) if parts[3] else 0
                sval = int(parts[4]) if parts[4] else 0
                svol = int(parts[5]) if parts[5] else 0
                sfreq = int(parts[6]) if parts[6] else 0
                nval = int(parts[7]) if parts[7] else 0
                nvol = int(parts[8]) if parts[8] else 0
                # parts[9] = tval, parts[10] = tlot — derivable, not stored
            except ValueError:
                continue
            if not broker_code:
                continue

            # Volumes in shares → schema stores lots (1 lot = 100 shares).
            blot = bvol // 100 if bvol else 0
            slot = svol // 100 if svol else 0
            nlot = nvol // 100 if nvol else 0

            bavg = derive_avg(bval, blot)
            savg = derive_avg(sval, slot)
            out.append({
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
        return out

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
