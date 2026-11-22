"""Hyperliquid position scanner. Public data only, no key.

Hyperliquid runs on its own blockchain, so every account's positions and orders are public. Each run:
  positions  (every run) top N accounts by account value (public leaderboard) -> their BTC / ETH positions:
             size, entry, leverage, LIQUIDATION PRICE, value, unrealised P&L  -> positions/<date>/<HHMM>.csv
             + one summary row per coin (accounts long / short, notional, 'trader' accounts only, top-100 by all-time P&L) -> summary.csv
  orders     (every HL_SLOW_EVERY_H hours, or --orders) open BTC / ETH orders of up to HL_SEL_MAX (200) 'trader'
             accounts that hold BTC / ETH:
             trigger orders = stop-loss / take-profit, plus reduce-only limits -> orders/<date>/<HHMM>.csv
  fills      (same times, or --fills) liquidation fills seen in those accounts' recent fills (the field names the liquidated
             account - also other people's liquidations they traded against) -> liquidations.csv (deduplicated)
  accounts   (once a day) the top-N leaderboard rows (value, P&L + volume by window) -> accounts/<date>.csv
'trader' = not market-maker-like: last-month volume < 30 x account value (market makers trade their account many times a month).

Plan mode (env HL_PLAN=1, used by the cloud job every 30 min):
  every run  = the top N accounts + every account last seen holding >= $1M of BTC / ETH ("big")
  rolling    = all other leaderboard accounts worth >= $10k + every address the trade-feed listener saw in the last 14 days
               (data/feed/), HL_SLICE (4000) per run, so each one is re-read every few runs
  state/     = the latest known position of every account (kept in the job's cache, not in git) -> summary_all.csv:
               all known BTC / ETH positions vs Hyperliquid's total open interest (coverage %)
  summary.csv stays "top N only" so its history remains comparable.
  levels/<date>.csv = $ of liquidation prices per price bin (BTC $100, ETH $5, +/- 20 % of the price) every run (heatmap history)
  levels_core/<date>.csv = the same for the FIXED group read every run (top N + big) - comparable over time while coverage grows
  positions files: an account that CLOSED a position since its last reading gets a row with size 0 (so history can be rebuilt)
  runs.csv = one row per run: start (= the time_utc of all its rows), positions done, finished - backtests must use 'finished'
  (the data only exists after the run is saved, ~15 min after the start) - never the start time
  live/ = positions.csv (every known position), orders.csv, liquidations.csv (3 days), status.json - read by the website

Usage:  python -I hl_scanner.py [--n 1500] [--out <folder>] [--orders] [--fills]
        default out: ../data-csv/positioning/hyperliquid  (env HL_OUT overrides)
Rate limit: Hyperliquid allows ~1200 'weight' per minute per IP (account state = 2, orders / fills = 20). We stay at <= ~1100.
"""
import csv, gzip, json, os, sys, time, threading, urllib.request, hashlib, glob, datetime as dt
from concurrent.futures import ThreadPoolExecutor

API = 'https://api.hyperliquid.xyz/info'
LEADERBOARD = 'https://stats-data.hyperliquid.xyz/Mainnet/leaderboard'
COINS = ('BTC', 'ETH')
WEIGHT_PER_MIN = 1100
HERE = os.path.dirname(os.path.abspath(__file__))

args = sys.argv[1:]
N = int(args[args.index('--n') + 1]) if '--n' in args else 1500
OUT = args[args.index('--out') + 1] if '--out' in args else os.environ.get('HL_OUT') or os.path.join(HERE, '..', 'data-csv', 'positioning', 'hyperliquid')
NOW = dt.datetime.now(dt.timezone.utc)
SLOW_EVERY_H = float(os.environ.get('HL_SLOW_EVERY_H', '12'))   # orders + fills: every N hours
SEL_MAX = int(os.environ.get('HL_SEL_MAX', '200'))               # trader accounts checked for orders + fills


def _slow_due():
    try: return time.time() - float(open(os.path.join(OUT, 'last_slow.txt')).read()) >= SLOW_EVERY_H * 3600 - 600
    except Exception: return True


DO_SLOW = '--orders' in args or '--fills' in args or _slow_due()
DO_ORDERS = '--orders' in args or (DO_SLOW and '--fills' not in args)
DO_FILLS = '--fills' in args or (DO_SLOW and '--orders' not in args)
GZ = os.environ.get('HL_GZIP') == '1'          # the cloud job writes .csv.gz (smaller git history)
PLAN = os.environ.get('HL_PLAN') == '1'
SLICE = int(os.environ.get('HL_SLICE', '4000'))
BIG_USD = 1_000_000
STATE = os.environ.get('HL_STATE') or os.path.join(HERE, 'state')
FEED_DAYS = 14
LIVE = os.environ.get('HL_LIVE') or os.path.join(HERE, 'live')   # latest picture for the website (pushed to the 'live' branch, no history)
STEP = {'BTC': 100.0, 'ETH': 5.0}                                 # price bins of the heatmap levels files



class Limiter:
    def __init__(self, per_min):
        self.per_sec = per_min / 60.0; self.t = time.time(); self.lock = threading.Lock()
    def take(self, w):
        with self.lock:
            self.t = max(self.t, time.time()) + w / self.per_sec
            wait = self.t - time.time() - w / self.per_sec
        if wait > 0: time.sleep(wait)

LIM = Limiter(WEIGHT_PER_MIN)


def post(body, weight):
    for k in range(5):
        LIM.take(weight)
        try:
            req = urllib.request.Request(API, data=json.dumps(body).encode(), headers={'Content-Type': 'application/json', 'User-Agent': 'public-data-client'})
            return json.loads(urllib.request.urlopen(req, timeout=30).read())
        except urllib.error.HTTPError as e:
            if e.code == 429: time.sleep(10 * (k + 1)); continue
            if k == 4: raise
        except Exception:
            if k == 4: raise
        time.sleep(2 * (k + 1))


def write(path, header, rows, append=False):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if GZ and not append:
        path += '.gz'; fh = gzip.open(path, 'wt', newline='', encoding='utf-8')
    else:
        new = not os.path.exists(path)
        fh = open(path, 'a' if append else 'w', newline='', encoding='utf-8')
        if append and not new: header = None
    with fh:
        w = csv.writer(fh)
        if header: w.writerow(header)
        w.writerows(rows)
    return path


def win(row, name):
    for k, v in row['windowPerformances']:
        if k == name: return float(v['pnl']), float(v['vlm'])
    return 0.0, 0.0


def feed_addresses(out):
    """Addresses the trade-feed listener saw in the last FEED_DAYS days (data/feed/<date>/<HHMM>.csv.gz)."""
    got = set(); cut = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=FEED_DAYS)).strftime('%Y-%m-%d')
    for f in glob.glob(os.path.join(out, 'feed', '*', '*.csv*')):
        if os.path.basename(os.path.dirname(f)) < cut: continue
        try:
            with (gzip.open(f, 'rt', encoding='utf-8') if f.endswith('.gz') else open(f, encoding='utf-8')) as fh:
                rd = csv.reader(fh); next(rd, None); got |= {r[0].lower() for r in rd if r}
        except Exception: pass
    return got


def load_state():
    """address -> list of [coin, size, entry_px, value, liq_px, seen] (the latest known BTC / ETH positions)."""
    st = {}
    p = os.path.join(STATE, 'latest.csv')
    if os.path.exists(p):
        with open(p, newline='', encoding='utf-8') as fh:
            rd = csv.reader(fh); next(rd, None)
            for r in rd:
                st.setdefault(r[0], [])
                if r[1]: st[r[0]].append(r[1:])
    return st


def update_state(states, rows, stamp, mids=None, core_set=None):
    """Merge this run into state/latest.csv and append the whole-market estimate to summary_all.csv (coverage vs total OI)."""
    st = load_state()
    for addr, s_ in states:
        if s_ is not None: st[addr] = []                     # re-read: forget the old positions of this account
    for r in rows:                                           # rows: time, address, coin, size, entry, liq, value, ...
        if not float(r[3] or 0): continue                     # size-0 = closed marker
        st.setdefault(r[1], []).append([r[2], r[3], r[4], r[6], r[5], stamp, r[10], r[12], r[9]])
    os.makedirs(STATE, exist_ok=True)
    with open(os.path.join(STATE, 'latest.csv'), 'w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh); w.writerow(['address', 'coin', 'size', 'entry_px', 'value_usd', 'liq_px', 'seen_utc', 'mm_like', 'account_value', 'leverage'])
        for a, ps in st.items():
            if ps:
                for x in ps: w.writerow([a] + x)
            else: w.writerow([a, '', '', '', '', '', stamp, '', '', ''])   # known account, no BTC / ETH position
    meta, ctx = post({'type': 'metaAndAssetCtxs'}, 20)
    oi = {m['name']: float(c['openInterest']) * float(c['markPx']) for m, c in zip(meta['universe'], ctx) if m['name'] in COINS}
    now = dt.datetime.strptime(stamp, '%Y-%m-%d %H:%M')
    out = []
    for c in COINS:
        ps = [x for v in st.values() for x in v if x[0] == c]
        L = sum(float(x[3]) for x in ps if float(x[1]) > 0); S = sum(float(x[3]) for x in ps if float(x[1]) < 0)
        age = max([(now - dt.datetime.strptime(x[5], '%Y-%m-%d %H:%M')).total_seconds() / 3600 for x in ps] or [0])
        out.append([stamp, c, len(st), len(ps), round(L), round(S), round(oi.get(c, 0)),
                    round(100 * L / oi[c], 1) if oi.get(c) else '', round(100 * S / oi[c], 1) if oi.get(c) else '', round(age, 1)])
    write(os.path.join(OUT, 'summary_all.csv'), ['time_utc', 'coin', 'accounts_known', 'positions', 'long_usd', 'short_usd', 'total_oi_usd',
          'long_share_pct', 'short_share_pct', 'oldest_reading_h'], out, append=True)
    try: write_levels_and_live(st, stamp, mids or {}, oi, out, core_set or set())
    except Exception as e: print('live / levels failed:', e, flush=True)
    return 'coverage: ' + ' · '.join(f'{r[1]} {r[7]} % long / {r[8]} % short of ${r[6] / 1e9:.2f}B' for r in out) + f' ({len(st)} accounts known)'


def _plain(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh); w.writerow(header); w.writerows(rows)


def _levels(st, stamp, mids, group, fname):
    """$ of liquidation prices per price bin (BTC $100, ETH $5, within 20 % of the price); group = None (everyone) or a set of addresses."""
    lv = []
    for c in COINS:
        mid = float(mids.get(c) or 0)
        if not mid: continue
        step = STEP[c]; bins = {}
        for a, v in st.items():
            if group is not None and a not in group: continue
            for x in v:
                if x[0] != c or not x[4]: continue
                lp = float(x[4])
                if abs(lp / mid - 1) > 0.2: continue
                b = round(lp / step) * step; long_ = float(x[1]) > 0; trader = (x[6] if len(x) > 6 else '0') in ('0', 0, '')
                e = bins.setdefault(b, [0.0, 0.0, 0.0, 0.0])
                e[0 if long_ else 1] += float(x[3])
                if trader: e[2 if long_ else 3] += float(x[3])
        lv += [[stamp, c, mid, b, round(e[0]), round(e[1]), round(e[2]), round(e[3])] for b, e in sorted(bins.items())]
    if lv:
        write(os.path.join(OUT, fname, stamp[:10] + '.csv'),
              ['time_utc', 'coin', 'mid_px', 'price', 'long_liq_usd', 'short_liq_usd', 'long_liq_usd_traders', 'short_liq_usd_traders'], lv, append=True)


def write_levels_and_live(st, stamp, mids, oi, cover, core_set=frozenset()):
    """levels/<date>.csv (heatmap history, appended) + live/positions.csv + live/status.json (overwritten)."""
    for group, fname in ((None, 'levels'), (core_set, 'levels_core')):
        _levels(st, stamp, mids, group, fname)
    pos = [[a] + x[:9] for a, v in st.items() for x in v]
    _plain(os.path.join(LIVE, 'positions.csv'), ['address', 'coin', 'size', 'entry_px', 'value_usd', 'liq_px', 'seen_utc', 'mm_like', 'account_value', 'leverage'],
           [r + [''] * (10 - len(r)) for r in pos])
    status = dict(updated_utc=stamp, accounts_known=len(st), coins={r[1]: dict(mid=float(mids.get(r[1]) or 0), positions=r[3], long_usd=r[4], short_usd=r[5],
                  total_oi_usd=r[6], long_share_pct=r[7], short_share_pct=r[8], oldest_reading_h=r[9]) for r in cover})
    os.makedirs(LIVE, exist_ok=True)
    with open(os.path.join(LIVE, 'status.json'), 'w', encoding='utf-8') as fh: json.dump(status, fh)


def write_live_extras(orows):
    """live/orders.csv (this run's stop / take-profit orders) + live/liquidations.csv (last 3 days, from data/liquidations.csv)."""
    if orows:
        _plain(os.path.join(LIVE, 'orders.csv'), ['time_utc', 'address', 'coin', 'side', 'order_type', 'is_trigger', 'trigger_px', 'trigger_condition',
               'limit_px', 'size', 'reduce_only', 'position_tpsl', 'placed_ms'], orows)
    lp = os.path.join(OUT, 'liquidations.csv')
    if os.path.exists(lp):
        cut = (time.time() - 3 * 86400) * 1000
        with open(lp, newline='', encoding='utf-8') as fh:
            rd = csv.reader(fh); head = next(rd, None); keep = [r for r in rd if r and r[1].isdigit() and float(r[1]) >= cut]
        if head: _plain(os.path.join(LIVE, 'liquidations.csv'), head, keep)


def main():
    t0 = time.time(); stamp = NOW.strftime('%Y-%m-%d %H:%M'); day = NOW.strftime('%Y-%m-%d'); hhmm = NOW.strftime('%H%M')
    for k in range(4):   # the leaderboard is one big download - retry on a network hiccup
        try:
            lb = json.loads(urllib.request.urlopen(urllib.request.Request(LEADERBOARD, headers={'User-Agent': 'public-data-client'}), timeout=90).read())['leaderboardRows']
            break
        except Exception:
            if k == 3: raise
            time.sleep(10 * (k + 1))
    top = sorted(lb, key=lambda r: -float(r['accountValue']))[:N]
    info = {}
    for r in top:
        av = float(r['accountValue']); pnl_m, vlm_m = win(r, 'month'); pnl_all, _ = win(r, 'allTime')
        info[r['ethAddress']] = dict(av=av, mm=vlm_m > 30 * max(av, 1.0), pnl_all=pnl_all, pnl_m=pnl_m, vlm_m=vlm_m)
    smart = set(a for a, _ in sorted(((a, i['pnl_all']) for a, i in info.items() if not i['mm']), key=lambda x: -x[1])[:100])
    core = set(info)
    plan_note = ''; big_now = set()
    if PLAN:
        latest = load_state()
        lbm = {r['ethAddress']: r for r in lb}
        big = {a for a, rows_ in latest.items() for r in rows_ if abs(float(r[3] or 0)) >= BIG_USD}
        pool = {a for a, r in lbm.items() if float(r['accountValue']) >= 1e4} | feed_addresses(OUT)
        rest = sorted(pool - core - big, key=lambda a: hashlib.md5(a.encode()).hexdigest())
        nsl = max(1, -(-len(rest) // SLICE))
        try: k = int(open(os.path.join(STATE, 'slice.txt')).read()) % nsl
        except Exception: k = 0
        part = rest[k * SLICE:(k + 1) * SLICE]
        os.makedirs(STATE, exist_ok=True); open(os.path.join(STATE, 'slice.txt'), 'w').write(str(k + 1))
        for a in list(big) + part:
            if a in info: continue
            r = lbm.get(a)
            if r:
                av = float(r['accountValue']); pnl_m, vlm_m = win(r, 'month'); pnl_all, _ = win(r, 'allTime')
                info[a] = dict(av=av, mm=vlm_m > 30 * max(av, 1.0), pnl_all=pnl_all, pnl_m=pnl_m, vlm_m=vlm_m)
            else:
                info[a] = dict(av=0.0, mm=False, pnl_all=0.0, pnl_m=0.0, vlm_m=0.0)
        big_now = set(big)
        plan_note = f' + {len(big - core)} big + slice {k + 1}/{nsl} ({len(part)} of {len(rest)} rolling)'

    acc_path = os.path.join(OUT, 'accounts', day + '.csv')
    if not os.path.exists(acc_path) and not os.path.exists(acc_path + '.gz'):
        write(acc_path, ['address', 'account_value', 'mm_like', 'pnl_day', 'pnl_week', 'pnl_month', 'pnl_all', 'vlm_day', 'vlm_week', 'vlm_month', 'vlm_all', 'display_name'],
              [[r['ethAddress'], r['accountValue'], int(info[r['ethAddress']]['mm'])] + [win(r, w)[0] for w in ('day', 'week', 'month', 'allTime')]
               + [win(r, w)[1] for w in ('day', 'week', 'month', 'allTime')] + [r.get('displayName') or ''] for r in top])

    def state(addr):
        try: return addr, post({'type': 'clearinghouseState', 'user': addr}, 2)
        except Exception: return addr, None
    with ThreadPoolExecutor(6) as ex: states = list(ex.map(state, list(info)))
    failed = sum(1 for _, s in states if s is None)

    rows, holders = [], []
    agg = {c: dict(nl=0, ns=0, l=0.0, s=0.0, tl=0.0, ts=0.0, sl=0.0, ss=0.0) for c in COINS}
    for addr, st in states:
        if not st: continue
        if not info[addr]['av']:
            try: info[addr]['av'] = float(st.get('marginSummary', {}).get('accountValue') or 0)
            except Exception: pass
        for ap in st.get('assetPositions', []):
            p = ap.get('position', {}); c = p.get('coin')
            if c not in COINS: continue
            sz = float(p['szi']); val = float(p.get('positionValue') or 0); lev = p.get('leverage', {})
            rows.append([stamp, addr, c, sz, p.get('entryPx'), p.get('liquidationPx') or '', val, p.get('unrealizedPnl'),
                         lev.get('type'), lev.get('value'), int(info[addr]['mm']), int(addr in smart), round(info[addr]['av'])])
            if not info[addr]['mm']: holders.append((val, addr))
            if addr not in core: continue          # summary.csv = top N only (comparable history)
            a = agg[c]; side = 'l' if sz > 0 else 's'
            a['n' + side] += 1; a[side] += val
            if not info[addr]['mm']: a['t' + side] += val
            if addr in smart: a['s' + side] += val
    t_pos = time.time()
    if PLAN:   # 1) an account read now that had a position last time but not now -> size-0 row ("closed")
        old = load_state(); have = {(r[1], r[2]) for r in rows}
        for addr, st in states:
            if st is None: continue
            for x in old.get(addr, []):
                if x and x[0] in COINS and (addr, x[0]) not in have:
                    rows.append([stamp, addr, x[0], 0, '', '', 0, '', '', '', int(info[addr]['mm']), int(addr in smart), round(info[addr]['av'])])
    write(os.path.join(OUT, 'positions', day, hhmm + '.csv'),
          ['time_utc', 'address', 'coin', 'size', 'entry_px', 'liquidation_px', 'position_value_usd', 'unrealized_pnl', 'margin', 'leverage',
           'mm_like', 'top100_pnl', 'account_value'], rows)
    try:   # the real price at the scan (20 Nov 2026 - for the dashboard / page markers)
        mids = post({'type': 'allMids'}, 2)
    except Exception:
        mids = {}
    cols = ['time_utc', 'coin', 'accounts_scanned', 'accounts_long', 'accounts_short', 'long_usd', 'short_usd', 'trader_long_usd', 'trader_short_usd',
            'top100_long_usd', 'top100_short_usd', 'mid_px']
    sp = os.path.join(OUT, 'summary.csv')
    if os.path.exists(sp):   # older file without the mid_px column -> add an empty column once
        with open(sp, newline='', encoding='utf-8') as fh:
            old = list(csv.reader(fh))
        if old and old[0] != cols:
            with open(sp, 'w', newline='', encoding='utf-8') as fh:
                w = csv.writer(fh); w.writerow(cols)
                w.writerows([r + [''] * (len(cols) - len(r)) for r in old[1:] if r])
    write(sp, cols,
          [[stamp, c, len(core) - sum(1 for x, s_ in states if s_ is None and x in core), a['nl'], a['ns'], round(a['l']), round(a['s']), round(a['tl']), round(a['ts']), round(a['sl']), round(a['ss']), mids.get(c, '')]
           for c, a in agg.items()],
          append=True)
    log = [f'{stamp} UTC positions: {len(info)} accounts (top {N}{plan_note}; {failed} failed), {len(rows)} BTC / ETH positions, {time.time() - t0:.0f} s']
    if PLAN:
        try: log.append(update_state(states, rows, stamp, mids, core | big_now))
        except Exception as e: log.append(f'state / coverage failed: {e}')

    sel = list(dict.fromkeys(a for _, a in sorted(holders, key=lambda x: -x[0])))[:SEL_MAX]   # biggest trader positions first
    orows_live = []
    if DO_ORDERS and sel:
        def orders(addr):
            try: return addr, post({'type': 'frontendOpenOrders', 'user': addr}, 20)
            except Exception: return addr, None
        with ThreadPoolExecutor(3) as ex: res = list(ex.map(orders, sel))
        orows_live[:] = orows = [[stamp, a, o.get('coin'), o.get('side'), o.get('orderType'), o.get('isTrigger'), o.get('triggerPx') or '', o.get('triggerCondition') or '',
                  o.get('limitPx'), o.get('sz'), o.get('reduceOnly'), o.get('isPositionTpsl'), o.get('timestamp')]
                 for a, oo in res if oo for o in oo if o.get('coin') in COINS and (o.get('isTrigger') or o.get('reduceOnly'))]
        write(os.path.join(OUT, 'orders', day, hhmm + '.csv'),
              ['time_utc', 'address', 'coin', 'side', 'order_type', 'is_trigger', 'trigger_px', 'trigger_condition', 'limit_px', 'size', 'reduce_only', 'position_tpsl', 'placed_ms'], orows)
        log.append(f'orders: {len(sel)} trader accounts -> {len(orows)} BTC / ETH stop / take-profit / reduce-only orders, {time.time() - t0:.0f} s')
    if DO_FILLS and sel:
        def fills(addr):
            try: return addr, post({'type': 'userFills', 'user': addr}, 20)
            except Exception: return addr, None
        with ThreadPoolExecutor(3) as ex: res = list(ex.map(fills, sel))
        lp = os.path.join(OUT, 'liquidations.csv'); seen = set()
        if os.path.exists(lp):
            with open(lp, encoding='utf-8') as fh: seen = {r[0] for r in csv.reader(fh)}
        new = []
        for addr, ff in res:
            for f in ff or []:
                q = f.get('liquidation'); key = f"{f.get('hash')}-{f.get('tid')}"
                if q and f.get('coin') in COINS and key not in seen:
                    seen.add(key)
                    # the liquidated account SELLS a long (side A); the account on the other side BUYS (side B)
                    own = (q.get('liquidatedUser') or '').lower() == addr.lower()
                    long_liq = int((f.get('side') == 'A') if own else (f.get('side') == 'B'))
                    new.append([key, f['time'], dt.datetime.fromtimestamp(f['time'] / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), f['coin'],
                                q.get('liquidatedUser'), q.get('markPx'), q.get('method'), f.get('px'), f.get('sz'), f.get('side'), f.get('dir'), long_liq])
        write(lp, ['key', 'time_ms', 'time_utc', 'coin', 'liquidated_user', 'mark_px', 'method', 'fill_px', 'fill_size', 'fill_side', 'fill_dir', 'long_liquidated'], new, append=True)
        log.append(f'fills: {len(sel)} accounts -> {len(new)} new BTC / ETH liquidation fills, {time.time() - t0:.0f} s')
    if DO_SLOW and sel:
        with open(os.path.join(OUT, 'last_slow.txt'), 'w') as fh: fh.write(str(time.time()))
    if PLAN:
        try: write_live_extras(orows_live)
        except Exception as e: log.append(f'live extras failed: {e}')
    print('\n'.join(log), flush=True)
    write(os.path.join(OUT, 'scanner_log.csv'), ['line'], [[l] for l in log], append=True)
    if PLAN:   # 2) when this run's data became known (backtests use 'finished_utc', never the start)
        f = lambda x: dt.datetime.fromtimestamp(x, dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        write(os.path.join(OUT, 'runs.csv'), ['time_utc', 'start_utc', 'positions_done_utc', 'finished_utc', 'accounts', 'positions_rows'],
              [[stamp, f(t0), f(t_pos), f(time.time()), len(info), len(rows)]], append=True)
    try:   # local mode only (not gzip): optional merge + dashboard
        if not GZ:
            sys.path.insert(0, HERE)
            try:
                import import_cloud
                print('\n'.join(import_cloud.run()), flush=True)
            except Exception as e:
                print('cloud import skipped:', e, flush=True)
            import hl_dashboard
            hl_dashboard.build(OUT)
    except Exception as e:
        print('dashboard not rebuilt:', e, flush=True)


if __name__ == '__main__':
    main()
