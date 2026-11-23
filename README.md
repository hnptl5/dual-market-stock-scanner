# PulseScan — Dual-Market React Stock Scanner (US & India, Auto-Refresh on Vercel)

A $0/month, deploy-once React stock scanner for **US (NYSE/NASDAQ)** and **Indian (NSE)** markets powered by a **Vercel Python Serverless API (`/api/scan`)** + **`yfinance`**.

- **Deploy once, zero GitHub Actions:** Push to GitHub and connect to Vercel once. No bot commits and no daily rebuilds.
- **Automatic Daily 10:00 AM IST Cron:** `vercel.json` automatically triggers `/api/scan?market=ALL` every weekday morning (`30 4 * * 1-5` UTC).
- **Live Auto-Refresh & Refresh Now:** Toggle **Auto-Refresh** (`1m`, `5m`, or `15m`) or click **Refresh Now** anytime to pull live US & NSE quotes, RVOL, RS vs. SPY/NIFTY 50, Multi-Timeframe RSI(14), and Multi-Timeframe EMA/SMA directly from `/api/scan`.

## Deploy Once in 3 Minutes (GitHub + Vercel)

1. **Push this folder to a GitHub repository:**
   ```bash
   git init
   git add .
   git commit -m "Initial PulseScan Vercel auto-refresh scanner"
   git branch -M main
   git remote add origin https://github.com/<your-username>/dual-market-stock-scanner.git
   git push -u origin main
   ```
2. **Import into Vercel (Free Hobby Plan):**
   - Go to [vercel.com/new](https://vercel.com/new) and sign in with GitHub.
   - Select your `dual-market-stock-scanner` repository and click **Deploy**.
   - Vercel automatically detects Vite (React frontend), installs `requirements.txt` for `api/scan.py`, and registers the daily 10:00 AM IST cron from `vercel.json`.
3. **Open your live URL:** Your scanner loads cached US & NSE data immediately, auto-refreshes at your chosen interval, and lets you click **Refresh Now** anytime.

## Expanding Your Stock Universe
Edit `US_UNIVERSE` and `INDIA_UNIVERSE` inside `api/scan.py` (append `.NS` for NSE tickers) and push to `main`—Vercel updates automatically.

## Optional Free Speed Boost: Upstash Redis Baseline Cache (Recommended)
To make **Refresh Now** and **Auto-Refresh** finish in just a few seconds (even after cold starts) at **$0/month**:
1. Open your project in the **Vercel Dashboard → Storage → Create Database → Upstash Redis** (Free tier).
2. Click **Connect Project** (Vercel automatically adds `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN`).
3. Redeploy once. The first scan each day caches the compressed 2-year historical baseline in Redis, and every refresh after that only downloads a tiny 5-day incremental update.
