"""One-time IPOT auth bootstrap.

Opens a visible Chrome (Playwright) at the IPOT app, polls until the user
finishes a QR login, captures the autologintoken + appsession + cookies
to ipot_creds.json. Subsequent scrape runs use that file via the
curl_cffi-based IPOTSource — no Playwright needed in the cron.

Run this:
    - on first setup
    - whenever the cron logs `IPOT auth failed` (token expired)
"""

import json
import time
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright


PROFILE = Path(__file__).parent / "ipot_profile"
CREDS = Path(__file__).parent / "ipot_creds.json"
IPOT_URL = "https://www.indopremier.com/#ipot/app"
LOGIN_TIMEOUT_S = 540  # 9 minutes for the human


def main() -> int:
    PROFILE.mkdir(exist_ok=True)
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE),
            headless=False,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            viewport={"width": 1440, "height": 900},
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

        all_pages = []
        ctx.on("page", lambda p: all_pages.append(p))
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        all_pages.append(page)
        page.goto(IPOT_URL, wait_until="domcontentloaded")

        print()
        print("=" * 70)
        print(" >>> A Chrome window opened on your screen.")
        print(" >>> Click the profile icon -> Login -> scan QR with your mobile.")
        print(" >>> This script auto-detects login + saves ipot_creds.json.")
        print("=" * 70)
        print()

        check_js = """() => {
            const has_token = !!(window.localStorage && window.localStorage.getItem('autologintoken'));
            const is_authed = !!(window.sc && typeof window.sc.IsAuthenticated === 'function' && window.sc.IsAuthenticated());
            const lid = (window.sc && window.sc.cust && window.sc.cust.lid) || null;
            return { has_token, is_authed, lid };
        }"""

        deadline = time.time() + LOGIN_TIMEOUT_S
        last_status = ""
        authed_page = None
        while time.time() < deadline:
            for p in list(all_pages):
                try:
                    if p.is_closed():
                        continue
                    s = p.evaluate(check_js)
                    status = f"tok={s['has_token']} auth={s['is_authed']} lid={s['lid']}"
                    if status != last_status:
                        print(f"  status: {status}")
                        last_status = status
                    if s.get("has_token") and s.get("is_authed"):
                        authed_page = p
                        break
                except Exception:
                    pass
            if authed_page:
                break
            time.sleep(2)

        if not authed_page:
            print(f"\n  [FAIL] Timed out after {LOGIN_TIMEOUT_S}s without login")
            ctx.close()
            return 1

        print("  detected — settling 5s for token to finalize...")
        time.sleep(5)
        creds = authed_page.evaluate(
            """() => ({
                autologintoken: window.localStorage.getItem('autologintoken'),
                appsession: typeof appsession !== 'undefined' ? appsession : null,
                lid: window.sc && window.sc.cust && window.sc.cust.lid || null,
            })"""
        )
        cookies = ctx.cookies()
        out = {
            "autologintoken": creds["autologintoken"],
            "appsession": creds["appsession"],
            "lid": creds["lid"],
            "cookies": cookies,
        }
        CREDS.write_text(json.dumps(out, indent=2))
        print()
        print(f"  [OK] Authenticated as {creds.get('lid') or '(authed)'}")
        print(f"  [OK] Saved {CREDS}")
        print(f"       autologintoken length: {len(out['autologintoken'] or '')}")
        print(f"       cookies: {len(cookies)} entries")
        ctx.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
