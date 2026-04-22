# Scheduled task report — `otb-refresh-from-bi`

- **Run date (KST):** 2026-04-22
- **Target stay months:** 2026-04, 2026-05, 2026-06
- **Booking / cancel source date (예약일자 / 취소일자):** 2026-04-21 (yesterday, KST)
- **Status:** ⚠️ Not executed — required files not reachable from this session

## What the task asked for

1. Query Power BI dataset `8ee000d9-5efb-403f-83ad-9a8e3d3b80eb`
   (cluster `wabi-korea-central-a-primary-redirect.analysis.windows.net`)
   using helpers in `scripts/collect_powerbi.py`.
   - `data_raw`  →  SUM(RNS) grouped by 월 ∈ {4,5,6}, 투숙년도=2026, 예약일자=yesterday
   - `data_cxl_2mthraw`  →  SUM(RNS) for same stay months, 취소일자=yesterday
2. Compute `net_otb = booking_rns − cancel_rns` per month.
3. Update `data/weekly_report.json` at `months[0..2]`.
4. `git add / commit / pull --rebase / push` to `origin/main` using `~/.ssh/id_ed25519`.

## Why the run could not proceed

| Requirement | Status in this session |
|---|---|
| `~/Desktop/gs_daily_trend_news_public_temp/scripts/collect_powerbi.py` | ❌ Not mounted. Sandbox `$HOME` is `/sessions/adoring-friendly-planck`; the only mounted user folder is `sono-competitor-crawler`. |
| `~/Desktop/gs_daily_trend_news_public_temp/data/weekly_report.json` | ❌ Not mounted — cannot read current values, cannot write updates. |
| `~/.ssh/id_ed25519` (git push key) | ❌ Not accessible from sandbox. |
| Power BI auth / session context | ❌ Only the public identifiers (RESOURCE_KEY, TENANT_ID, MODEL_ID, DATASET_ID, REPORT_ID) are in the task file. The `_headers` / session token used by `execute_query` lives in the unreachable helper module. |
| Typing into Terminal via computer-use fallback | ❌ Terminal is tier-"click" in this environment — typing is blocked, so the shell pipeline (`git pull --rebase`, `ssh-add`, `git push`) cannot be driven from computer-use. |

No Power BI API calls were made, `weekly_report.json` was not touched, and no git operations were run against `gs_daily_trend_news_public_temp`.

## To make this task runnable going forward

Any one of these unblocks the scheduled run:

1. **Mount the repo into Cowork** — add `~/Desktop/gs_daily_trend_news_public_temp` as the selected Cowork folder (or alongside the current one) so the sandbox can read/write the repo and `data/weekly_report.json`.
2. **Bring `collect_powerbi.py` into the sandbox** — either commit a self-contained collector (including auth handling) into the mounted folder, or paste the helper functions into the `SKILL.md` task file so the scheduled runner has everything inline.
3. **Provide the Power BI access token / cookie** — e.g. via a secret the helper reads, so the sandbox can call the REST endpoint directly without depending on an externally-authenticated session.
4. **Grant a writable git remote path inside the sandbox** — mount the repo AND make the SSH key (or a deploy-key PAT over HTTPS) available so `git push origin main` works from the sandbox. Today neither is present.

Once the repo and credentials are reachable, the implementation is straightforward:

- DAX `EVALUATE SUMMARIZECOLUMNS('data_raw'[월], FILTER(...), "rns", SUM('data_raw'[RNS]))` with a date-range filter on `예약일자` (UTC ms: yesterday-00:00 KST → today-00:00 KST, i.e. subtract 9h for UTC).
- Same shape against `data_cxl_2mthraw`, swapping the table and `취소일자` filter. Per the SKILL notes, probe the column list first because `취소일자` may be a Date (not DateTime) and the month column name may differ.
- Patch `months[0..2]` of `weekly_report.json` with `{booking_rns, cancel_rns, net_otb}` while preserving the rest of the file.
- Standard git flow from the task file; the `eval "$(ssh-agent -s)"` + `ssh-add` step assumes `~/.ssh/id_ed25519` is on the executing host.

## Recommended follow-up

Re-run this scheduled task after the `gs_daily_trend_news_public_temp` folder is added to Cowork, or port the collector so it can run entirely against the currently-mounted workspace. Until then this task will continue to report "not executed" on every tick.
