"""IDX exchange-members profile scraper.

Hits each broker's per-code profile page at
    https://www.idx.co.id/en/members-and-participants/exchange-members-profiles/<CODE>
and extracts the broker name, license types, company-ownership flag (IDX's
own Local/Foreign classification), and operational status.

The page sits behind a Cloudflare managed challenge so plain HTTP fails; we
use Playwright in persistent-context mode (same `ipot_profile/` directory that
auth_bootstrap.py uses) so the Cloudflare cookie persists across runs.

Page structure (verified against 2026 layout):
- `.company-profile-name` -> broker name
- visible body has tab-separated key:value rows like:
    Member Name      : SUKADANA PRIMA SEKURITAS
    Company Ownership: Local       <-- authoritative IDX foreign/domestic flag
    Operational Status: Active
    License          : Online, ..., Penjamin Emisi Efek, ...

Low-frequency scraper. Member roster changes ~quarterly. Run via
`python scrape.py registry`; no cron.
"""

import logging
import re
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

PROFILE = Path(__file__).parent.parent / "ipot_profile"
IDX_URL_TEMPLATE = (
    "https://www.idx.co.id/en/members-and-participants/exchange-members-profiles/{code}"
)
PAGE_LOAD_TIMEOUT_MS = 45_000
POST_LOAD_DELAY_S = 2.5  # let challenge clear + JS render
INTER_REQUEST_DELAY_S = 2.0  # courtesy gap between requests


# Foreign-parent brokers (their PT entity is Indonesian, but the parent company
# is non-Indonesian). Updated against the IDX 2026-05 member roster. Maintain
# manually — IDX's own "Company Ownership" field is always "Local" for PT-
# incorporated members so it can't be used to derive this.
FOREIGN_BROKER_CODES: frozenset[str] = frozenset({
    "AG",  # KIWOOM SEKURITAS INDONESIA (Korean)
    "AH",  # SHINHAN SEKURITAS INDONESIA (Korean)
    "AI",  # KAY HIAN SEKURITAS (Singapore — UOB Kay Hian)
    "AK",  # UBS SEKURITAS INDONESIA (Swiss)
    "BB",  # VERDHANA SEKURITAS INDONESIA (Japanese — Nomura affiliate)
    "BK",  # J.P. MORGAN SEKURITAS INDONESIA (US)
    "BQ",  # KOREA INVESTMENT AND SEKURITAS INDONESIA (Korean)
    "CP",  # KB VALBURY SEKURITAS (Korean — KB Financial)
    "DP",  # DBS VICKERS SEKURITAS INDONESIA (Singapore)
    "DR",  # RHB SEKURITAS INDONESIA (Malaysian)
    "FS",  # YUANTA SEKURITAS INDONESIA (Taiwanese)
    "GI",  # WEBULL SEKURITAS INDONESIA (US/Chinese)
    "HD",  # KGI SEKURITAS INDONESIA (Taiwanese)
    "KK",  # PHILLIP SEKURITAS INDONESIA (Singapore)
    "KZ",  # CLSA SEKURITAS INDONESIA (Chinese — CITIC)
    "RX",  # MACQUARIE SEKURITAS INDONESIA (Australian)
    "TP",  # OCBC SEKURITAS INDONESIA (Singapore)
    "XA",  # NH KORINDO SEKURITAS INDONESIA (Korean — NH Investment)
    "YP",  # MIRAE ASSET SEKURITAS INDONESIA (Korean)
    "YU",  # CGS INTERNATIONAL SEKURITAS INDONESIA (Malaysian — was CIMB CGS)
    "ZP",  # MAYBANK SEKURITAS INDONESIA (Malaysian)
})


def _extract_name(page) -> Optional[str]:
    """Broker name lives in `.company-profile-name` on the IDX page."""
    for sel in (".company-profile-name", ".company-profile-header"):
        try:
            el = page.query_selector(sel)
            if not el:
                continue
            t = (el.inner_text() or "").strip()
            if t:
                return t
        except Exception:
            continue
    return None


# Parse "Field Name : value" rows out of the visible page text. IDX uses tab or
# colon separators inconsistently; we accept either.
_FIELD_RE = re.compile(r"^([A-Za-z][A-Za-z0-9 ()\-/&]+?)\s*[:\t]\s*(.+?)\s*$")


def _parse_profile_fields(body_text: str) -> dict[str, str]:
    """Extract key:value lines from the visible profile body."""
    out: dict[str, str] = {}
    for raw_line in body_text.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        m = _FIELD_RE.match(line)
        if not m:
            continue
        key = m.group(1).strip().lower()
        val = m.group(2).strip()
        if not val or val == ":":
            continue
        out[key] = val
    return out


def scrape_members(codes: list[str]) -> list[dict]:
    """Scrape IDX member profiles for the given broker codes.

    Returns a list of dicts with the following fields:
      code, name, license_type, is_foreign (IDX-authoritative),
      member_status, source_url, ok
    `ok=False` means the page didn't return a usable broker name (likely
    delisted, 404, or Cloudflare-blocked).
    """
    from playwright.sync_api import sync_playwright

    PROFILE.mkdir(exist_ok=True)
    results: list[dict] = []

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE),
            headless=False,  # Cloudflare bot detection trips on headless
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            viewport={"width": 1366, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
            ),
            locale="id-ID",
            timezone_id="Asia/Jakarta",
        )
        ctx.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        n = len(codes)

        for i, code in enumerate(codes, 1):
            code = code.upper()
            url = IDX_URL_TEMPLATE.format(code=code)
            entry = {
                "code": code,
                "name": None,
                "license_type": None,
                "is_foreign": False,
                "member_status": "unknown",
                "source_url": url,
                "ok": False,
            }
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT_MS)
                time.sleep(POST_LOAD_DELAY_S)

                title = page.title().lower()
                if "just a moment" in title or "checking" in title:
                    log.warning(f"  [{i}/{n}] {code}: Cloudflare challenge — waiting 15s")
                    time.sleep(15)

                name = _extract_name(page)
                if name:
                    entry["name"] = name
                    body = page.inner_text("body")
                    fields = _parse_profile_fields(body)

                    # is_foreign comes from our curated set above (IDX's own
                    # "Company Ownership: Local|Foreign" field is always Local
                    # for PT-incorporated members — can't be used here).
                    entry["is_foreign"] = code in FOREIGN_BROKER_CODES

                    entry["license_type"] = fields.get("license") or None
                    op_status = fields.get("operational status", "").lower()
                    if op_status:
                        entry["member_status"] = op_status

                    entry["ok"] = True
                    log.info(
                        f"  [{i}/{n}] {code}: {name} "
                        f"({'FOREIGN' if entry['is_foreign'] else 'LOCAL'}, "
                        f"{entry['member_status']})"
                    )
                else:
                    log.warning(
                        f"  [{i}/{n}] {code}: name not found (likely delisted or page-shape changed)"
                    )
            except Exception as e:
                log.warning(f"  [{i}/{n}] {code}: failed: {e}")

            results.append(entry)
            time.sleep(INTER_REQUEST_DELAY_S)

        ctx.close()

    return results
