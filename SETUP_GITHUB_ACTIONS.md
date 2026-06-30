# MK Top-Down → GitHub Actions (laptop-free, true 1-minute cadence)

This runs your worker on GitHub's free runners. Your laptop stays off.
The Google Sheet is your live dashboard, updated every minute. View it
on your phone.

## How it works

- Two scheduled workflows: morning (09:15–12:30 IST) and afternoon
  (12:30–15:30 IST). Split because GitHub caps one job at 6 hours.
- Each job runs the worker in a continuous loop — `fetch → write → sleep 60s`
  — so you get true 1-minute updates. Cron only STARTS the job; your code
  controls the cadence.
- The worker does its own Fyers TOTP login inside the job. No token pasting.
- State (signal history, performers) is saved to the hidden `_state_cache`
  sheet so the afternoon job picks up where the morning left off.

## One-time setup (~15 minutes)

### 1. Create the repo

- On GitHub: New repository → name it e.g. `mk-topdown` → **Public** → Create.
- (Public = free unlimited Actions minutes. Your secrets are NOT in the code —
  they go in encrypted GitHub Secrets, step 4. Your tuning params can stay
  private in your Sheet — see "Keeping logic private" below.)

### 2. Add the files

Put these in the repo (use GitHub's web UI "Add file → Upload files", or git):

```
mk-topdown/
├── mk_topdown_worker.py            ← the worker (already CI-ready)
├── fyers_auth.py                   ← if your worker imports it separately;
│                                      otherwise auth is inside the worker
├── requirements.txt
└── .github/
    └── workflows/
        ├── mk_morning.yml
        └── mk_afternoon.yml
```

IMPORTANT: the two `.yml` files MUST go inside `.github/workflows/`.
GitHub only recognizes workflows in that exact path.

### 3. Get your Google service-account JSON ready

You already have `service_account.json` locally (the worker uses it).
Open it in a text editor and copy the ENTIRE contents — it's one JSON object
starting with `{` and ending with `}`. You'll paste it as a Secret next.

### 4. Add the Secrets

In your repo: Settings → Secrets and variables → Actions → New repository secret.
Add these eight, one at a time (name on the left, value on the right):

| Secret name                   | Value (from your local config.env / files) |
|-------------------------------|---------------------------------------------|
| FYERS_CLIENT_ID               | your Fyers client id                        |
| FYERS_SECRET_KEY              | your Fyers secret key                       |
| FYERS_REDIRECT_URI           | your Fyers redirect uri                     |
| FYERS_ID                     | your Fyers login id                         |
| FYERS_TOTP_SECRET            | your TOTP secret                            |
| FYERS_PIN                    | your 4-digit PIN                            |
| SHEET_ID                     | your Google Sheet id (from its URL)         |
| GOOGLE_SERVICE_ACCOUNT_JSON  | the ENTIRE service_account.json contents    |

Secrets are encrypted and never visible in logs or to anyone viewing the repo.

### 5. Test it manually before trusting the schedule

- Repo → Actions tab → "MK Top-Down — Morning Session" → "Run workflow" button.
- Watch the live log. You want to see:
  - `Cache restored from sheet` (or "starting fresh" on first ever run)
  - TOTP login success
  - `Refresh in X.Xs` lines appearing about once a minute
- Open your Google Sheet — the dashboard should update within ~1 minute.
- On a weekend it will log "Weekend — nothing to do" and exit. That's correct;
  test on a weekday during/near market hours for a real run.

### 6. Let the schedule take over

Once the manual run works, do nothing. The cron triggers fire automatically:
- Morning job: ~09:10 IST Mon–Fri
- Afternoon job: ~12:25 IST Mon–Fri

Your laptop is now irrelevant. Open the Sheet on your phone any time.

## Keeping your logic private (the option you chose)

The repo is public, so the code is visible — but your *edge* doesn't have to be.
Two things stay private automatically:

1. **All credentials** — in encrypted Secrets, never in code.
2. **Your index weights** — already read from the "Index Weights" tab in your
   private Sheet at runtime, not hardcoded in a way that matters.

If you want to also hide tuning params (QUALITY_WEIGHTS, grade thresholds),
move them into a cell/tab in your private Sheet and have the worker read them
at startup — the public code becomes a harness with no secret numbers. Tell
Claude and it'll wire that up.

## Costs

- GitHub Actions on a public repo: **free, unlimited minutes.**
- Fyers API: free (your existing plan).
- Google Sheets API: free tier, well within limits.
- Total: ₹0/month.

## Risks & things to watch

1. **GitHub cron can be delayed** under platform load (occasionally 5–15 min).
   The worker waits for 09:15 internally, so a late START is harmless. But if
   GitHub is badly delayed past ~09:30, you lose the early-session minutes that
   day. Rare, but it happens. The manual "Run workflow" button is your override.
2. **6-hour job cap** is why there are two jobs. If NSE extends hours (muhurat
   sessions, etc.), adjust the cron + SESSION_END.
3. **Secrets are as sensitive as your account.** Anyone with repo admin access
   can trigger runs (not read secrets, but trigger). Don't add collaborators.
4. **TOTP needs accurate time** — GitHub runners are NTP-synced, so this is a
   non-issue in CI (unlike a drifting laptop clock).
5. **Public repo = public code.** Accept that your signal *logic* is visible.
   Your credentials and weights are not. If the logic itself is sensitive, use
   the private-params approach above.
6. **Mid-session Fyers token revocation** is rare; if a job starts logging auth
   errors, re-run the workflow manually — it does a fresh TOTP login each start.
