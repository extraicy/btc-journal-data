"""COLLECTOR - records positioning data that has no free long history. Missed hours are fine:
the Binance sets keep 30 days, so a gap of up to 30 days is filled on the next run.

Records (CSV):
  binance_oi_1h_<C>        open interest (coins + USD value), 1H        (Binance keeps only 30 days)
  binance_ls_1h_<C>        long / short account ratio, 1H               (30 days)
  binance_topls_1h_<C>     top traders' long / short position ratio, 1H (30 days)
  binance_taker_1h_<C>     taker buy / sell volume, 1H                  (30 days)
  hl_candles_1h_<C>        Hyperliquid 1H candles                       (Hyperliquid keeps only ~5000 candles)
  hl_ctx_<C>               Hyperliquid snapshot NOW: funding, open interest, premium, mark / oracle price, day volume
                           (no history exists at all - only the hours this ran)
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pos_common as P

COINS = ['BTC', 'ETH', 'BNB', 'SOL']
FD = 'https://fapi.binance.com/futures/data/'


def binance_30d(c):
    out = []
    sets = [
        ('binance_oi_1h_', 'openInterestHist', ['open_interest', 'oi_value_usd'], lambda x: [x['sumOpenInterest'], x['sumOpenInterestValue']]),
        ('binance_ls_1h_', 'globalLongShortAccountRatio', ['long_account', 'short_account', 'ratio'], lambda x: [x['longAccount'], x['shortAccount'], x['longShortRatio']]),
        ('binance_topls_1h_', 'topLongShortPositionRatio', ['long_position', 'short_position', 'ratio'], lambda x: [x['longAccount'], x['shortAccount'], x['longShortRatio']]),
        ('binance_taker_1h_', 'takerlongshortRatio', ['buy_vol', 'sell_vol', 'buy_sell_ratio'], lambda x: [x['buyVol'], x['sellVol'], x['buySellRatio']]),
    ]
    for prefix, ep, cols, row in sets:
        name = prefix + c
        # always re-read the whole 29-day window in 2 pieces (Binance returns the NEWEST 500 rows unless endTime is given);
        # merge() drops duplicates, so this also fills any hole left while the job did not run
        start = P.now_ms() - 29 * 86400_000
        n = 0
        while start < P.now_ms():
            end = start + 499 * 3600_000
            r = P.get(f'{FD}{ep}?symbol={c}USDT&period=1h&limit=500&startTime={start}&endTime={end}')
            if r:
                added, _ = P.merge(name, cols, [(x['timestamp'], row(x)) for x in r]); n += added
            start = end + 1
            time.sleep(0.3)
        out.append(f'{name} +{n}')
    return out


def hl_candles(c):
    name = f'hl_candles_1h_{c}'
    start = P.now_ms() - 72 * 3600_000 if P.MONTHLY else ((P.last_time(name) or 0) or P.now_ms() - 5000 * 3600_000)
    r = P.get('https://api.hyperliquid.xyz/info', {'type': 'candleSnapshot', 'req': {'coin': c, 'interval': '1h', 'startTime': start, 'endTime': P.now_ms()}})
    done = [x for x in r if int(x['T']) < P.now_ms()]
    P.merge(name, ['open', 'high', 'low', 'close', 'volume', 'trades'], [(x['t'], [x['o'], x['h'], x['l'], x['c'], x['v'], x['n']]) for x in done])
    return f'{name} +{len(done)}'


def hl_snapshot(coins):
    meta, ctxs = P.get('https://api.hyperliquid.xyz/info', {'type': 'metaAndAssetCtxs'})
    t = P.now_ms()
    out = []
    for u, x in zip(meta['universe'], ctxs):
        if u['name'] in coins:
            P.merge(f"hl_ctx_{u['name']}", ['funding', 'open_interest', 'premium', 'mark_price', 'oracle_price', 'day_volume_usd'],
                    [(t, [x.get('funding'), x.get('openInterest'), x.get('premium'), x.get('markPx'), x.get('oraclePx'), x.get('dayNtlVlm')])])
            out.append(f"hl_ctx_{u['name']} +1")
    return out


def deribit_options():
    """Deribit options snapshot (BTC + ETH) - see deribit.py."""
    import deribit
    return deribit.run()


def run(coins=COINS, snapshot=True):
    lines = [f'run {P.iso(P.now_ms())} UTC']
    jobs = [lambda: hl_snapshot(coins), deribit_options] if snapshot else []
    for c in coins:
        jobs += [lambda c=c: binance_30d(c), lambda c=c: [hl_candles(c)]]
    for j in jobs:
        try:
            lines += j()
        except Exception as e:  # noqa: BLE001 - one source failing must not stop the others
            lines.append(f'FAILED {e}')
    return lines


def full_due(hours=22):
    """Cloud mode: the Binance 30-day sets + Hyperliquid candles only need one run a day (Binance keeps 30 days)."""
    try: return time.time() - float(open(os.path.join(P.OUT, 'last_full.txt')).read()) >= hours * 3600
    except Exception: return True


if __name__ == '__main__':
    if '--cloud' in sys.argv:
        # GitHub job (every 2 h with the scanner): Hyperliquid snapshot every run, the rest once a day
        if full_due():
            lines = run()
            with open(os.path.join(P.OUT, 'last_full.txt'), 'w') as f: f.write(str(time.time()))
        else:
            lines = [f'run {P.iso(P.now_ms())} UTC (snapshot only)'] + hl_snapshot(COINS)
            try: lines += deribit_options()
            except Exception as e: lines.append(f'deribit FAILED {e}')
        with open(os.path.join(P.OUT, 'collect_log.txt'), 'a', encoding='utf-8') as f:
            f.write(' | '.join(lines) + '\n')
        print('\n'.join(lines))
        sys.exit(0)
    lines = run()
    with open(os.path.join(P.HERE, 'collect.log'), 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    print('\n'.join(lines))
