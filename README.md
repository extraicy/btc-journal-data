# Hyperliquid public data collector

Two GitHub Actions jobs save **public** market data into `data/` as CSV:
- `scan.yml` every **30 minutes**: the top 1,500 Hyperliquid accounts + every account holding >= $1M of BTC / ETH + every account that
  just traded >= $250k on each run; 4,000 more per run: first those that traded >= $10k since their last reading, then the longest-unread
  ones (an account holding a position is re-read 3x as often as an empty one). Pool = leaderboard accounts worth >= $10k + addresses
  seen trading in the last 14 days + every account holding a position.
- `feed.yml` around the clock (5 h 40 min per run, restarted every 6 h): every address that trades BTC / ETH on Hyperliquid, and every
  BTC / ETH liquidation on Bybit (public `allLiquidation` stream). Every 30 min it pushes who just traded, so the scanner re-reads them.
No keys, no trading - it only reads free public endpoints.

**Coverage (fixed 25 Nov 2026):** Hyperliquid's `openInterest` counts longs **and** shorts (tested on DOT + TRX: every trade and fill
vs the reported change, ratio 1.01), so one side = openInterest / 2. Shares in `summary_all.csv` / `status.json` are now % of one side
(older rows corrected: they showed half). The scanner sees ~91-93 % of all BTC / ETH positions.

**Time rule for backtests:** a run's data exists only after it is saved: use `runs.csv` `finished_utc` (or `known_utc` in
`intelligence.csv`), never `time_utc` (the start).

| Path | What |
|---|---|
| `data/intelligence.csv` | **one row per coin per run** - see the column list below. Back-filled from 22 Nov 2026 22:22 (no order book before 25 Nov) |
| `data/changes/<date>/<HHMM>.csv.gz` | every position change >= $10k (+ every liquidation) seen between two readings of an account - see below |
| `data/fills/<date>/<HHMM>.csv.gz` | "whale fills": all BTC / ETH fills of the 100 biggest trader positions since the last run (since 25 Nov 2026 also of the previous run's 100, so a whale that closed its whole position is followed one more run), per account + coin + direction: fills, size, average price, first / last fill time (seconds), $, closed P&L, fee, taker %, liquidation fills (`liquidation_fills` = fills of any liquidation, also another account's that this one traded against; `own_liquidation_fills` = this account itself was liquidated, since 25 Nov 2026) |
| `data/positions/<date>/<HHMM>.csv.gz` | BTC / ETH positions of the accounts read in that run: size, entry, leverage, liquidation price, value, P&L, market-maker flag, top-100 flag. Size 0 = the account closed that position since its last reading |
| `data/summary.csv` | one row per coin per run, top 1,500 accounts only: accounts long / short, $ long / short |
| `data/summary_all.csv` | one row per coin per run, every known account (latest reading of each): $ long / short, Hyperliquid's open interest (`total_oi_usd`, both sides) + one side, coverage % of one side, % read within 1 h / 6 h |
| `data/levels/<date>.csv` | every run: $ of liquidation prices per price bin (BTC $100, ETH $5, within 20 % of the price) - heatmap history |
| `data/levels_core/<date>.csv` | the same, but only for the FIXED group read every run (top 1,500 + big accounts) - comparable over time while coverage grows |
| `data/runs.csv` | one row per scanner run: start, positions done, **finished** |
| branch `live` | the latest picture only (replaced every run): `positions.csv` (every known position), `orders.csv`, `liquidations.csv` (3 days), `status.json`; for the website's intelligence tabs (since 25 Nov 2026): `intel_recent.csv` (last 7 days of `intelligence.csv`), `changes_recent.csv` (24 h of `changes/`: changes >= $100k, market makers >= $500k, liquidations >= $50k; fewer columns), `fills_recent.csv` (24 h of `fills/`), `big_trades_recent.csv` (24 h of `big_trades/`) |
| `data/feed/<date>/<HHMM>.csv.gz` | addresses that traded BTC / ETH during one listener run (trades, $ traded, first / last seen) |
| `data/feed_recent/<date>/<HHMM>.csv.gz` | every 30 min: who traded >= $10k of BTC / ETH in those 30 minutes (trades, $, last trade) |
| `data/big_trades/<date>/<HHMM>.csv.gz` | every 30 min: every taker order >= $100k (all fills of one order added up): time, coin, side (B = taker bought), taker, $, size, average price, fills, makers |
| `data/bybit_liq/<date>/<HHMM>.csv.gz` | every Bybit BTC / ETH liquidation during one listener run: time, symbol, long_liquidated (1 = a long was liquidated), size, bankruptcy price, $ value |
| `data/orders/<date>/<HHMM>.csv.gz` | stop-loss / take-profit / reduce-only BTC / ETH orders |
| `data/liquidations.csv` | BTC / ETH liquidations seen in those accounts' fills (deduplicated) |
| `data/accounts/<date>.csv.gz` | the public leaderboard rows used (account value, P&L + volume per window) |
| `data/collector/deribit/...` + `data/collector/deribit_summary.csv` | Deribit options snapshot every run |
| `data/collector/<name>_<YYYY-MM>.csv` | Hyperliquid market snapshot every run; Binance open interest / long-short / taker 1H + Hyperliquid 1H candles once a day |
| `data/archive_log.csv` | what the monthly archive moved where |

**Monthly archive (since 25 Nov 2026):** from the 2nd of each month, the raw per-run / per-day folders of earlier months (positions,
orders, changes, fills, feed, feed_recent, big_trades, bybit_liq, levels, levels_core, accounts, collector/deribit) are packed into one
`.tar.gz` per folder and attached to the release **`archive-YYYY-MM`** (Releases, right side of the repo page), then removed here.
Unpack a pack in the repository root to get `data/<folder>/...` back. The one-file-per-month / cumulative files stay here.
`Reset history (manual)` (Actions) restarts the git history when the repository gets big (> ~2 GB) - every file is kept.

### `changes/` columns
`time_utc` (this reading) · `since_utc` (the account's previous reading; empty = unknown) · `address` · `coin` ·
`event` OPENED / INCREASED / REDUCED / CLOSED / FLIPPED · `liquidation` `confirmed` (a liquidation fill of this account between the
readings) / `probable` (no fill seen, but the price crossed the old liquidation price between the readings) / empty ·
`size_before`, `size_after`, `size_change` (coins; + = bought) · `usd_change` (size_change x price) · `mid_px` · `entry_before/after` ·
`liq_before/after` · `value_after` · flags `whale` (>= $1M), `top_trader` (top 100 by all-time profit, as of that reading), `mm_like`,
`every_run` (read every run, so `since_utc` is the previous run) · `account_value` · `source` = `observed`: the difference between two
readings - the real trades happened somewhere in between (exact times + prices of the biggest traders: `fills/`).
The first reading of a newly found account is not a change.

### `intelligence.csv` columns (one row per coin per run; $ = position value in USD; + flow = more long / less short)
- **time:** `time_utc` (run start) · `known_utc` (when the row was written - use this in backtests)
- **market:** `mid_px` · `mark_px` · `funding_1h` (Hyperliquid's hourly funding rate) · `oi_one_side_coins` / `oi_one_side_usd` ·
  `oi_change_coins` (since the previous run) · `px_change_pct`
- **quality:** `accounts_known` · `positions_known` · `coverage_long/short_pct` (% of one side seen) · `fresh1h_*` / `fresh6h_*` (% of one
  side read within 1 h / 6 h)
- **who is long / short** (every known position, latest reading, five groups that add up to everything seen): `mm_*` market-maker-like ·
  `top_*` top-100 traders · `whale_*` other positions >= $1M · `mid_*` $100k - $1M · `small_*` < $100k (each `_long_usd` / `_short_usd`) ·
  `unseen_long/short_usd` = one side of open interest minus what we see
- **fixed group vs everyone else** (exact this run): `fixed_long/short_usd` = the accounts read every run (top 1,500 + big) ·
  `rest_long/short_usd` = open interest minus them
- **concentration:** `top10_share_pct` / `top20_share_pct` (the 10 / 20 largest positions as % of all longs + shorts) · `top20_net_usd` ·
  `top20_flow_usd` (what the previous run's 20 largest positions changed since)
- **entry zones:** `long/short_avg_entry` (value-weighted) · `long_underwater_usd` (longs whose entry is above the price = losing) ·
  `short_underwater_usd` (shorts whose entry is below the price) · `long_uw_zone_px/usd` (BTC $500 / ETH $25 bin above the price with
  the most losing longs - they often sell when the price gets back there) · `short_uw_zone_px/usd` (below the price)
- **leverage setting:** `lev10_*` (>= 10x) · `lev25_*` (>= 25x)
- **liquidation distance:** `liq2_*` / `liq5_*` / `liq10_*` = $ whose liquidation price is within 2 / 5 / 10 % of the price (longs below,
  shorts above) · `liq_cluster_below_px/usd` (bin with the most long liquidations within 10 % below) · `liq_cluster_above_px/usd`
  (Hyperliquid's own liquidation price at reading time - for cross-margin accounts it moves with their other positions)
- **order book** (from 25 Nov 2026): `book_bid1/ask1_usd`, `book_bid2/ask2_usd` = resting $ within 1 % / 2 % of the price
- **flows since the previous run** (changes of accounts read in both runs, Δsize x price): `mm_flow_usd` · `top_flow_usd` ·
  `whale_flow_usd` · `other_flow_usd` · `rest_flow_usd` = minus their sum = what everyone else did (every buy has a seller) ·
  `late_flow_usd` (changes found now in accounts last read longer ago - timing unknown) · `opened_usd` / `closed_usd` ·
  `liq_fill_long/short_usd` (liquidation fills in the interval) · `liq_probable_long/short_usd` · `changes_precise` / `changes_late`

Rebuild `changes/` + `intelligence.csv` + `summary_all.csv` from the positions files: `python3 -I intel_backfill.py` (replays every run
with the same code as the live scanner, `intel.py`).

Times are UTC. Sources: Hyperliquid info API + public leaderboard + trades websocket, Binance futures data API, Deribit public API, Bybit public websocket.
