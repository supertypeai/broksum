# IPOT pipeline

Scrapes IDX per-broker daily buy/sell/net per ticker into Supabase
`idx_broker_summary_daily`. Two sources, **first hit wins**:

1. **IPOT** (IndoPremier) — primary. Login-gated, full broker list, server-side date aggregation.
2. **IQPlus** — public HTML fallback (no auth). Used automatically when IPOT fails. Lower coverage; it's a safety net, not a replacement.
   Source page: `net_buy_sell.php` (nav BROKSUM), two query shapes over the same six columns
   (`Broker/Code | B.Lot | B.Avg | S.Lot | S.Avg | N.Lot`):
   `scode=<TICKER>&tab=broker` (that ticker's top brokers) and `scode=<BROKER>&tab=stock` (that broker's top tickers).
   - Hard cap of **10 rows per list**, no request param lifts it. Per-ticker pass alone ⇒ 10 brokers/ticker;
     `--backfill-iqplus` then runs a per-broker pass (~88 requests/day) that refills missed pairs
     (measured +40–110% rows on some tickers, +0 on others, plus tickers nobody asked for).
   - **No trade frequency** any more: `bfreq`/`sfreq` land as NULL (the column is nullable).
   - Values are rebuilt as `lot*100*avg` from the page's exact `data-sort` floats. Verified against
     IPOT rows already in Supabase: 152/152 rows identical on blot/bval/slot/sval/nlot/nval and the
     three avg columns.
   - The table is ordered by **net descending**, so it is net buyers only: zero seller rows for a
     liquid name. Every seller/counterparty/frequency view on the site (`heatmap.php?api=broker`,
     `ajax_flow=1`, `FLOW_INIT_SELLERS`, `broker_flow.php?api=1`) is pinned to the latest session —
     date params are ignored or fall back to a preset window — so none of them can fill a gap date.
   - `--backfill-iqplus` writes the CSVs, and the workflow uploads them with
     **`upload --insert-only`** (`Prefer: resolution=ignore-duplicates`): missing
     (symbol,date,broker_code) rows are inserted, existing rows are never modified. A fallback day
     therefore cannot blank an IPOT row's `bfreq`/`sfreq` or replace real numbers with a 10-broker slice.
     The per-broker pass is also restricted to `idx_company_profile` symbols (the reverse table lists
     warrants such as `BBCANG`, which would otherwise become fake `*.JK` symbols).
   - History is a **rolling ~365 days** (2025-11-24 onwards on 2026-11-24); older dates return empty.
   - The page 403s on `HeadlessChrome` user agents — plain requests/curl are fine, only headless
     browsers get blocked.

   IDX's own `primary/TradingSummary/GetBrokerSummary` is **not** a substitute: it is per-broker
   aggregate only (`Volume`, `Value`, `Frequency`, 88 brokers, no per-stock dimension). Reachable
   with `curl_cffi` impersonating `firefox133` when needed, but it cannot fill this table.

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
