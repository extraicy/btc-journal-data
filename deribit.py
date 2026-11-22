"""Deribit options recorder. Public API, no key, one request per coin.
Deribit is the main BTC / ETH options exchange. Free API = the state NOW only (no history) -> we record it every run.

Per run and coin (BTC, ETH):
  deribit/<date>/<HHMM>_<COIN>.csv(.gz)  every listed option: expiry, strike, call / put, open interest (coins), volume, mark IV,
                                          mark price, bid / ask, underlying price  (~800-1000 rows)
  deribit_summary.csv  (one row per coin per run): index price, total call / put open interest, put/call ratio, the 3 biggest
                        call "walls" and put "walls" (strikes with most open interest, all expiries), max pain of the nearest
                        big expiry (strike where option buyers lose the most), its open interest

    python -I deribit.py                                  (local: writes into ../data-csv/positioning/)
    POS_OUT=data/collector HL_GZIP=1 python3 -I deribit.py (cloud job)
"""
import csv, gzip, json, os, sys, time, urllib.request, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.environ.get('POS_OUT') or os.path.normpath(os.path.join(HERE, '..', 'data-csv', 'positioning'))
GZ = os.environ.get('HL_GZIP') == '1'
API = 'https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency={}&kind=option'
MONTHS = {m: i + 1 for i, m in enumerate(['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'])}


def get(coin):
    for k in range(4):
        try:
            return json.loads(urllib.request.urlopen(urllib.request.Request(API.format(coin), headers={'User-Agent': 'public-data-client'}), timeout=30).read())['result']
        except Exception:
            if k == 3: raise
            time.sleep(3 * (k + 1))


def parse(name):
    """BTC-25JUN27-88000-C -> (expiry date, strike, 'C')"""
    _, exp, strike, kind = name.split('-')
    d = dt.date(2000 + int(exp[-2:]), MONTHS[exp[-5:-2]], int(exp[:-5]))
    return d, float(strike), kind


def summary(rows, now):
    idx = next((r['underlying_price'] for r in rows if r.get('underlying_price')), None)
    call, put, by_exp = {}, {}, {}
    for r in rows:
        d, k, kind = parse(r['instrument_name']); oi = r.get('open_interest') or 0.0
        (call if kind == 'C' else put)[k] = (call if kind == 'C' else put).get(k, 0.0) + oi
        by_exp.setdefault(d, []).append((k, kind, oi))
    walls = lambda m: sorted(m.items(), key=lambda x: -x[1])[:3]
    # nearest expiry that holds at least 5 % of all open interest (skips tiny dailies)
    tot = sum(call.values()) + sum(put.values())
    exps = sorted(d for d in by_exp if d >= now.date())
    big = next((d for d in exps if sum(x[2] for x in by_exp[d]) >= 0.05 * tot), exps[0] if exps else None)
    pain = None; big_oi = 0.0
    if big:
        opts = by_exp[big]; big_oi = sum(x[2] for x in opts)
        strikes = sorted({x[0] for x in opts})
        def payout(s):   # what option buyers would receive if price settled at s
            return sum(oi * (max(0, s - k) if kind == 'C' else max(0, k - s)) for k, kind, oi in opts)
        pain = min(strikes, key=payout) if strikes else None
    c, p = sum(call.values()), sum(put.values())
    flat = lambda ws: ' '.join(f'{int(k)}:{round(v)}' for k, v in ws)
    return [round(idx, 2) if idx else '', round(c, 1), round(p, 1), round(p / c, 3) if c else '', flat(walls(call)), flat(walls(put)),
            big.isoformat() if big else '', int(pain) if pain else '', round(big_oi, 1)]


def run(coins=('BTC', 'ETH')):
    now = dt.datetime.now(dt.timezone.utc); stamp = now.strftime('%Y-%m-%d %H:%M'); lines = []
    for coin in coins:
        try:
            rows = get(coin)
        except Exception as e:
            lines.append(f'deribit {coin}: FAILED {e}'); continue
        folder = os.path.join(OUT, 'deribit', now.strftime('%Y-%m-%d')); os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"{now.strftime('%H%M')}_{coin}.csv" + ('.gz' if GZ else ''))
        with (gzip.open(path, 'wt', newline='', encoding='utf-8') if GZ else open(path, 'w', newline='', encoding='utf-8')) as fh:
            w = csv.writer(fh)
            w.writerow(['time_utc', 'instrument', 'expiry', 'strike', 'type', 'open_interest', 'volume', 'mark_iv', 'mark_price', 'bid', 'ask', 'underlying_price'])
            for r in rows:
                d, k, kind = parse(r['instrument_name'])
                w.writerow([stamp, r['instrument_name'], d.isoformat(), k, kind, r.get('open_interest'), r.get('volume'), r.get('mark_iv'),
                            r.get('mark_price'), r.get('bid_price'), r.get('ask_price'), r.get('underlying_price')])
        sp = os.path.join(OUT, 'deribit_summary.csv'); new = not os.path.exists(sp)
        with open(sp, 'a', newline='', encoding='utf-8') as fh:
            w = csv.writer(fh)
            if new: w.writerow(['time_utc', 'coin', 'index_price', 'call_oi', 'put_oi', 'put_call_ratio', 'call_walls', 'put_walls',
                                'main_expiry', 'max_pain', 'main_expiry_oi'])
            w.writerow([stamp, coin] + summary(rows, now))
        lines.append(f'deribit {coin}: {len(rows)} options')
    return lines


if __name__ == '__main__':
    print('\n'.join(run()))
