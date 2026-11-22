# Hyperliquid public data collector

A GitHub Actions job (`.github/workflows/scan.yml`) runs every 2 hours and saves **public** market data into `data/` as CSV.
No keys, no trading - it only reads free public endpoints.

| Path | What |
|---|---|
| `data/positions/<date>/<HHMM>.csv.gz` | BTC / ETH positions of large Hyperliquid accounts: size, entry, leverage, liquidation price, value, P&L, market-maker flag |
| `data/summary.csv` | one row per coin per run: accounts long / short, $ long / short |
| `data/orders/<date>/<HHMM>.csv.gz` | stop-loss / take-profit / reduce-only BTC / ETH orders |
| `data/liquidations.csv` | BTC / ETH liquidations seen in those accounts' fills (deduplicated) |
| `data/accounts/<date>.csv.gz` | the public leaderboard rows used (account value, P&L + volume per window) |
| `data/collector/deribit/...` + `data/collector/deribit_summary.csv` | Deribit options snapshot every run |
| `data/collector/<name>_<YYYY-MM>.csv` | Hyperliquid market snapshot every run; Binance open interest / long-short / taker 1H + Hyperliquid 1H candles once a day |

Times are UTC. Sources: Hyperliquid info API + public leaderboard, Binance futures data API, Deribit public API.
