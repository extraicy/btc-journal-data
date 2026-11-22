# Hyperliquid public data collector

Two GitHub Actions jobs save **public** market data into `data/` as CSV:
- `scan.yml` every **30 minutes**: the top 1,500 Hyperliquid accounts + every account holding >= $1M of BTC / ETH on each run, all other known
  accounts (leaderboard accounts worth >= $10k + addresses seen trading) in rolling slices.
- `feed.yml` around the clock (5 h 40 min per run, restarted every 6 h): every address that trades BTC / ETH on Hyperliquid, and every
  BTC / ETH liquidation on Bybit (public `allLiquidation` stream).
No keys, no trading - it only reads free public endpoints.

| Path | What |
|---|---|
| `data/positions/<date>/<HHMM>.csv.gz` | BTC / ETH positions of the accounts read in that run: size, entry, leverage, liquidation price, value, P&L, market-maker flag. Size 0 = the account closed that position since its last reading |
| `data/summary.csv` | one row per coin per run, top 1,500 accounts only: accounts long / short, $ long / short |
| `data/summary_all.csv` | one row per coin per run, every known account (latest reading of each): $ long / short vs total open interest (coverage %) |
| `data/levels/<date>.csv` | every run: $ of liquidation prices per price bin (BTC $100, ETH $5, within 20 % of the price) - heatmap history |
| `data/levels_core/<date>.csv` | the same, but only for the FIXED group read every run (top 1,500 + big accounts) - comparable over time while coverage grows |
| `data/runs.csv` | one row per scanner run: start, positions done, **finished** - use `finished_utc` as the time the data was known (backtests) |
| branch `live` | the latest picture only (replaced every run): `positions.csv` (every known position), `orders.csv`, `liquidations.csv` (3 days), `status.json` |
| `data/bybit_liq/<date>/<HHMM>.csv.gz` | every Bybit BTC / ETH liquidation during one listener run: time, symbol, long_liquidated (1 = a long was liquidated), size, bankruptcy price, $ value |
| `data/feed/<date>/<HHMM>.csv.gz` | addresses that traded BTC / ETH during one listener run (trades, $ traded, first / last seen) |
| `data/orders/<date>/<HHMM>.csv.gz` | stop-loss / take-profit / reduce-only BTC / ETH orders |
| `data/liquidations.csv` | BTC / ETH liquidations seen in those accounts' fills (deduplicated) |
| `data/accounts/<date>.csv.gz` | the public leaderboard rows used (account value, P&L + volume per window) |
| `data/collector/deribit/...` + `data/collector/deribit_summary.csv` | Deribit options snapshot every run |
| `data/collector/<name>_<YYYY-MM>.csv` | Hyperliquid market snapshot every run; Binance open interest / long-short / taker 1H + Hyperliquid 1H candles once a day |

Times are UTC. Sources: Hyperliquid info API + public leaderboard, Binance futures data API, Deribit public API.
