# IPOT pipeline

Scrapes IDX per-broker daily buy/sell/net per ticker into Supabase
`idx_broker_summary_daily`. Two sources, **first hit wins**:

1. **IPOT** (IndoPremier) — primary. Login-gated, full broker list, server-side date aggregation.
2. **IQPlus** — public HTML fallback (no auth). Used automatically when IPOT fails. Lower coverage; it's a safety net, not a replacement.

Everything below is about **IPOT**, since that's the only part that needs a human.

---

## How IPOT auth works

IPOT needs a logged-in session. The credential is a **rotating `autologintoken`** stored in `ipot_creds.json` (git-ignored; in CI it lives in a GitHub secret — see below).

- **One-time / when it breaks:** a human runs `auth_bootstrap.py`, scans a QR, and it writes `ipot_creds.json`.
- **Daily cron:** reads `ipot_creds.json`, connects over WSS (via `curl_cffi`, which impersonates Chrome's TLS so IPOT's bot detection doesn't block it — Playwright gets blocked, which is why the cron doesn't use a browser). On each successful auth **IPOT rotates the token**, and the scraper immediately writes the new one back to `ipot_creds.json`.

## Re-bootstrap — the #1 operational task

When the cron logs **`IPOT auth failed` / `NEEDLOGIN`** and starts falling back to IQPlus, the token has expired. Fix it:

```bash
python auth_bootstrap.py
```
A visible Chrome opens at the IPOT app → click the profile icon → **Login → scan the QR with your phone**. The script auto-detects login and saves `ipot_creds.json` (~1 min).

Then push it to CI so the next run picks it up:
```bash
gh secret set IPOT_CREDS_JSON --body "$(cat ipot_creds.json)"
```
(or paste the file contents into the `IPOT_CREDS_JSON` secret in the GitHub UI).

## In CI (`.github/workflows/daily-scrape.yml`)

- No browser in CI, so `ipot_creds.json` comes from the GH secret **`IPOT_CREDS_JSON`**.
- The workflow: restore creds → scrape → upload → **push the rotated token back to the secret** (needs a PAT with `secrets: write` as **`GH_PAT`**; the rotation step runs `if: always()` so the token still persists even on an empty/holiday day).
- Missing secret or dead token → auto-falls back to IQPlus and logs it.
- Trigger is `workflow_dispatch`, fired by **cron-job.org** (not GH Actions schedule — that drifts hours).

## Common commands

```bash
python scrape.py scrape                          # latest trading day
python scrape.py scrape --date 2026-06-23        # one date
python scrape.py scrape --backfill 2026-01-01 2026-06-23
python scrape.py upload --date 2026-06-23        # push a CSV to Supabase
python scrape.py backfill-iqplus                 # T+1 IQPlus gap-fill (no auth)
```
Env vars: `SUPABASE_URL`, `SUPABASE_KEY` (upload). `GH_PAT` (CI secret rotation).

## Gotchas

- **Don't run two scrapes off the same `ipot_creds.json` in parallel** — the token rotates, so the second run gets a stale token and fails. CI is serial; fine.
- IPOT dates are `YYYY-M-DD` (no zero-pad) — the code handles the conversion.
- Volumes come in shares; stored as lots (÷100) to match the DB schema.
