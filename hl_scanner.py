"""Hyperliquid position scanner. Public data only, no key.

Hyperliquid runs on its own blockchain, so every account's positions and orders are public. Each run:
  positions  (every run) top N accounts by account value (public leaderboard) -> their BTC / ETH positions:
             size, entry, leverage, LIQUIDATION PRICE, value, unrealised P&L  -> positions/<date>/<HHMM>.csv
             + one summary row per coin (accounts long / short, notional, 'trader' accounts only, top-100 by all-time P&L) -> summary.csv
  orders     (every HL_SLOW_EVERY_H hours, or --orders) open BTC / ETH orders of up to HL_SEL_MAX (200) 'trader'
             accounts that hold BTC / ETH:
             trigger orders = stop-loss / take-profit, plus reduce-only limits -> orders/<date>/<HHMM>.csv
  fills      (same times, or --fills) the recent fills of those accounts:
             liquidation fills (the field names the liquidated account - also other people's liquidations they traded against)
             -> liquidations.csv (deduplicated); plan mode also keeps ALL their BTC / ETH fills, grouped per account + coin + direction
             -> fills/<date>/<HHMM>.csv ("whale fills": exact times and prices of what the biggest traders did)
  accounts   (once a day) the top-N leaderboard rows (value, P&L + volume by window) -> accounts/<date>.csv
'trader' = not market-maker-like: last-month volume < 30 x account value (market makers trade their account many times a month).

Plan mode (env HL_PLAN=1, used by the cloud job every 30 min; rotation rebuilt 25 Nov 2026):
  every run  = the top N accounts + every account last seen holding >= $1M of BTC / ETH ("big")
             + "hot": accounts that traded >= $250k of BTC / ETH since their last reading (trade-feed slices, data/feed_recent/)
  rolling    = HL_SLICE (4000) more accounts per run: first those that traded >= $10k since their last reading (biggest first, up to
               half the slice), then the longest-unread ones (an account holding a position counts 3x as overdue as an empty one).
               Pool = leaderboard accounts worth >= $10k + addresses seen trading in the last 14 days + every account holding a position
               (so a position can never be left un-read). Each account's first / last reading: state/accounts.csv.
  state/     = the latest known position of every account (kept in the job's cache, not in git)
  summary_all.csv = all known BTC / ETH positions vs ONE side of Hyperliquid's open interest (openInterest counts longs + shorts,
               tested 25 Nov 2026) = coverage %, plus "fresh" coverage (positions read within 1 h / 6 h)
  summary.csv stays "top N only" so its history remains comparable.
  levels/<date>.csv = $ of liquidation prices per price bin (BTC $100, ETH $5, +/- 20 % of the price) every run (heatmap history)
  levels_core/<date>.csv = the same for the FIXED group read every run (top N + big) - comparable over time while coverage grows
  positions files: an account that CLOSED a position since its last reading gets a row with size 0 (so history can be rebuilt)
  changes/<date>/<HHMM>.csv = every position change >= $10k since the account's previous reading (opened / increased / reduced /
               closed / flipped; liquidation 'confirmed' by a fill or 'probable' when the price crossed the old liquidation price)
  intelligence.csv = one row per coin per run (positioning by group, concentration, entry zones, leverage, liquidation distance,
               order book depth, flows, open interest, funding) - see intel.py + README.md
  runs.csv   = one row per run: start (= the time_utc of all its rows), positions done, finished - backtests must use 'finished'
  (the data only exists after the run is saved, ~15 min after the start) - never the start time
  live/ = positions.csv (every known position), orders.csv, liquidations.csv (3 days), status.json - read by the website

Usage:  python -I hl_scanner.py [--n 1500] [--out <folder>] [--orders] [--fills]
        default out: ../data-csv/positioning/hyperliquid  (env HL_OUT overrides)
Rate limit: Hyperliquid allows ~1200 'weight' per minute per IP (account state = 2, orders / fills = 20). We stay at <= ~1100.
"""
import csv, gzip, json, os, sys, time, threading, urllib.request, hashlib, glob, datetime as dt
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import intel as I

API = 'https://api.hyperliquid.xyz/info'
LEADERBOARD = 'https://stats-data.hyperliquid.xyz/Mainnet/leaderboard'
COINS = I.COINS
WEIGHT_PER_MIN = 1100

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
BIG_USD = I.BIG_USD
STATE = os.environ.get('HL_STATE') or os.path.join(HERE, 'state')
FEED_DAYS = 14
LIVE = os.environ.get('HL_LIVE') or os.path.join(HERE, 'live')   # latest picture for the website (pushed to the 'live' branch, no history)
STEP = {'BTC': 100.0, 'ETH': 5.0}                                 # price bins of the heatmap levels files
WATCH_USD = 10_000      # traded >= this since the last reading -> front of the rolling queue next run
HOT_USD = 250_000       # traded >= this since the last reading -> read next run no matter what ("new whale" check), up to HOT_MAX
HOT_MAX = 1000
HOLDER_WEIGHT = 3       # an account holding a position becomes overdue 3x faster than an empty one
FILL_COLS = ['time_utc', 'address', 'coin', 'dir', 'fills', 'size', 'avg_px', 'first_fill_utc', 'last_fill_utc', 'usd', 'closed_pnl', 'fee',
             'taker_pct', 'liquidation_fills', 'mm_like', 'top100_pnl', 'own_liquidation_fills']
STATE_COLS = ['address', 'coin', 'size', 'entry_px', 'value_usd', 'liq_px', 'seen_utc', 'mm_like', 'account_value', 'leverage', 'top100_pnl']


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


def append_cols(path, cols, rows, fix=None):
    """Append rows to a CSV whose columns may have grown: an older header is rewritten once (columns aligned by name, new ones
    empty; fix(row_dict) can correct old rows)."""
    if os.path.exists(path):
        with open(path, newline='', encoding='utf-8') as fh:
            head = next(csv.reader(fh), None)
        if head and head != cols:
            with open(path, newline='', encoding='utf-8') as fh:
                old = [dict(zip(head, r)) for r in list(csv.reader(fh))[1:] if r]
            if fix: old = [fix(d) for d in old]
            with open(path, 'w', newline='', encoding='utf-8') as fh:
                w = csv.writer(fh); w.writerow(cols); w.writerows([[d.get(c, '') for c in cols] for d in old])
    return write(path, cols, rows, append=True)


def tail_rows(path, n=4):
    """The header + the last n rows of a CSV (reads only the end of the file)."""
    if not os.path.exists(path): return None, []
    with open(path, 'rb') as fh:
        head = fh.readline().decode('utf-8').strip().split(',')
        fh.seek(0, 2); size = fh.tell(); fh.seek(max(0, size - 65536))
        lines = fh.read().decode('utf-8', 'replace').splitlines()
    rows = [r for r in csv.reader(lines[-n:]) if r and r != head]
    return head, rows


def win(row, name):
    for k, v in row['windowPerformances']:
        if k == name: return float(v['pnl']), float(v['vlm'])
    return 0.0, 0.0


def _stamp_of(f):
    d = os.path.basename(os.path.dirname(f)); hm = os.path.basename(f)[:4]
    return f'{d} {hm[:2]}:{hm[2:]}'


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


def feed_recent(out, hours=8):
    """Who traded lately: the listener's ~30-minute slices (data/feed_recent/<date>/<HHMM>.csv.gz: address, trades, usd_traded,
    last_trade_utc) of the last few hours -> address -> [(last_trade_utc, usd_traded)]."""
    got = {}; cut = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M')
    for f in glob.glob(os.path.join(out, 'feed_recent', '*', '*.csv*')):
        if _stamp_of(f) < cut: continue
        try:
            with (gzip.open(f, 'rt', encoding='utf-8') if f.endswith('.gz') else open(f, encoding='utf-8')) as fh:
                rd = csv.reader(fh); next(rd, None)
                for r in rd:
                    if r: got.setdefault(r[0].lower(), []).append((r[3][:16], float(r[2] or 0)))
        except Exception: pass
    return got


def load_state():
    """address -> list of [coin, size, entry_px, value, liq_px, seen, mm_like, account_value, leverage, top100] (latest known BTC / ETH
    positions; an empty list = a known account without one)."""
    st = {}
    p = os.path.join(STATE, 'latest.csv')
    if os.path.exists(p):
        with open(p, newline='', encoding='utf-8') as fh:
            rd = csv.reader(fh); next(rd, None)
            for r in rd:
                st.setdefault(r[0], [])
                if r[1]: st[r[0]].append(r[1:])
    return st


def load_accounts(latest, stamp):
    """state/accounts.csv: address -> (first_read_utc, last_read_utc). Started from latest.csv when missing (25 Nov 2026): an account
    holding a position = the time of that reading; an empty one = spread over the last 7 h (the old rotation's cycle)."""
    p = os.path.join(STATE, 'accounts.csv'); acc = {}
    if os.path.exists(p):
        with open(p, newline='', encoding='utf-8') as fh:
            rd = csv.reader(fh); next(rd, None)
            for r in rd:
                if r: acc[r[0]] = (r[1], r[2])
        return acc
    now = I.ts(stamp)
    for a, rows_ in latest.items():
        seen = max((r[5] for r in rows_ if len(r) > 5 and r[5]), default='')
        if not seen:
            frac = int(hashlib.md5(a.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
            seen = (now - dt.timedelta(hours=7 * frac)).strftime('%Y-%m-%d %H:%M') + '~'     # '~' = estimated, never used as an exact time
        acc[a] = ('', seen)
    return acc


def save_accounts(acc):
    os.makedirs(STATE, exist_ok=True)
    tmp = os.path.join(STATE, 'accounts.csv.tmp')
    with open(tmp, 'w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh); w.writerow(['address', 'first_read_utc', 'last_read_utc'])
        w.writerows([a, f, l] for a, (f, l) in acc.items())
    os.replace(tmp, os.path.join(STATE, 'accounts.csv'))


def save_state(st, acc):
    os.makedirs(STATE, exist_ok=True)
    tmp = os.path.join(STATE, 'latest.csv.tmp')
    with open(tmp, 'w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh); w.writerow(STATE_COLS)
        for a, ps in st.items():
            if ps:
                for p in ps: w.writerow([a] + p.state_row())
            else: w.writerow([a, '', '', '', '', '', acc.get(a, ('', ''))[1], '', '', '', ''])   # known account, no BTC / ETH position
    os.replace(tmp, os.path.join(STATE, 'latest.csv'))


def fix_summary_row(d):
    """Older summary_all.csv rows: shares were of BOTH sides (half the truth) -> % of one side; one-side OI added."""
    if d.get('oi_one_side_usd'): return d
    tot = I.num(d.get('total_oi_usd'))
    if tot:
        d['long_share_pct'] = round(200 * I.num(d.get('long_usd')) / tot, 1); d['short_share_pct'] = round(200 * I.num(d.get('short_usd')) / tot, 1)
        d['oi_one_side_usd'] = round(tot / 2)
    return d


def market_extras():
    """15-minute candles of the last 3 days (probable-liquidation check) + the order book near the price (depth columns)."""
    candles, books = {}, {}
    t1 = int(time.time() * 1000); t0 = t1 - 3 * 86_400_000
    for c in COINS:
        try:
            cd = sorted(post({'type': 'candleSnapshot', 'req': {'coin': c, 'interval': '15m', 'startTime': t0, 'endTime': t1}}, 20) or [], key=lambda x: x['t'])
            candles[c] = ([int(x['t']) for x in cd], [float(x['h']) for x in cd], [float(x['l']) for x in cd])
        except Exception: pass
        try: books[c] = [post({'type': 'l2Book', 'coin': c, 'nSigFigs': n}, 2).get('levels') for n in (4, 3)]   # fine first, then coarse
        except Exception: pass
    return candles, books


def update_state(states, rows, stamp, mids, fixed, acc):
    """Merge this run into the state, then: summary_all.csv (coverage), levels, live/, changes/, intelligence.csv."""
    old_st = {a: [I.Pos.from_state(r) for r in v] for a, v in load_state().items()}
    read_now = {a: [] for a, s_ in states if s_ is not None}       # every account read now (empty = no BTC / ETH position)
    for r in rows:
        if I.num(r[3]) and r[1] in read_now: read_now[r[1]].append(I.Pos.from_row(r))
    st = dict(old_st)
    for a, ps in read_now.items(): st[a] = ps
    since = {a: acc[a][1] for a in read_now if a in acc and not acc[a][1].endswith('~')}   # the previous reading of each account read now
    known = set(acc) | set(old_st)
    for a in read_now: acc[a] = (acc.get(a, ('', ''))[0] or stamp, stamp)
    save_state(st, acc); save_accounts(acc)
    meta, ctx = post({'type': 'metaAndAssetCtxs'}, 20)
    mk = {m['name']: c for m, c in zip(meta['universe'], ctx) if m['name'] in COINS}
    market = {c: dict(mid=I.num(mids.get(c)) or I.num(mk[c].get('markPx')), mark=I.num(mk[c].get('markPx')), funding=mk[c].get('funding', ''),
                      oi_both=I.num(mk[c].get('openInterest'))) for c in COINS if c in mk}
    oi_both_usd = {c: market[c]['oi_both'] * market[c]['mark'] for c in market}     # Hyperliquid's number: longs + shorts
    cov = I.coverage(st, stamp, {c: v / 2 for c, v in oi_both_usd.items()})
    srows = I.summary_rows(stamp, st, cov, oi_both_usd)
    append_cols(os.path.join(OUT, 'summary_all.csv'), I.SUMMARY_COLS, srows, fix_summary_row)
    log = 'coverage (one side of open interest): ' + ' · '.join(f'{r[1]} {r[7]} % of longs / {r[8]} % of shorts of ${r[10] / 1e9:.2f}B each '
                                                                   f'({r[11]} / {r[12]} % read within 1 h)' for r in srows) + f' ({len(st)} accounts known)'
    try: write_levels_and_live(st, stamp, mids or {}, srows, fixed)
    except Exception as e: log += f' | live / levels failed: {e}'
    try:   # changes + intelligence (after the fills step, so this run's liquidation fills are known)
        candles, books = market_extras()
        lp = os.path.join(OUT, 'liquidations.csv'); cut = (time.time() - 4 * 86400) * 1000; liq_rows = []
        if os.path.exists(lp):
            with open(lp, newline='', encoding='utf-8') as fh:
                liq_rows = [r for r in csv.DictReader(fh) if I.num(r.get('time_ms')) >= cut]
        liq_idx, fills = I.liq_index(liq_rows)
        chg = I.find_changes(stamp, old_st, read_now, since, {c: market[c]['mid'] for c in market}, liq_idx, candles, fixed, known)
        keep = [x for x in chg if I.keep_change(x)]
        if keep: write(os.path.join(OUT, 'changes', stamp[:10], stamp[11:13] + stamp[14:16] + '.csv'), I.CHANGE_COLS, keep)
        ip = os.path.join(OUT, 'intelligence.csv')
        head, last = tail_rows(ip, 4)
        prev = [dict(zip(head, r)) for r in last] if head else []
        prev_stamp = max((d['time_utc'] for d in prev if d.get('time_utc', '') < stamp), default='')
        prev_market = {d['coin']: dict(mid=I.num(d.get('mid_px')), oi_both=2 * I.num(d.get('oi_one_side_coins'))) for d in prev if d.get('time_utc') == prev_stamp}
        book = {c: I.book_depth(books[c], market[c]['mid']) for c in books if c in market and books[c] and all(books[c])}
        known_utc = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M')
        irows = I.intel_rows(stamp, known_utc, prev_stamp, st, old_st, cov, chg, market, prev_market, fills, book, fixed)
        append_cols(ip, I.INTEL_COLS, irows)
        n_pre = sum(1 for x in chg if prev_stamp and x[1] == prev_stamp)
        log += (f' | changes: {len(chg)} ({n_pre} since the previous run, {len(keep)} of >= $10k saved),'
                f' {sum(1 for x in chg if x[5] == "confirmed")} confirmed + {sum(1 for x in chg if x[5] == "probable")} probable liquidations'
                f' | book: {"ok" if book else "not read"}')
    except Exception as e:
        log += f' | changes / intelligence failed: {e}'
    return log


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
            for p in v:
                if p.coin != c or not p.liq: continue
                if abs(p.liq / mid - 1) > 0.2: continue
                b = round(p.liq / step) * step; long_ = p.size > 0
                e = bins.setdefault(b, [0.0, 0.0, 0.0, 0.0])
                e[0 if long_ else 1] += p.value
                if not p.mm: e[2 if long_ else 3] += p.value
        lv += [[stamp, c, mid, b, round(e[0]), round(e[1]), round(e[2]), round(e[3])] for b, e in sorted(bins.items())]
    if lv:
        write(os.path.join(OUT, fname, stamp[:10] + '.csv'),
              ['time_utc', 'coin', 'mid_px', 'price', 'long_liq_usd', 'short_liq_usd', 'long_liq_usd_traders', 'short_liq_usd_traders'], lv, append=True)


def write_levels_and_live(st, stamp, mids, srows, core_set=frozenset()):
    """levels/<date>.csv (heatmap history, appended) + live/positions.csv + live/status.json (overwritten)."""
    for group, fname in ((None, 'levels'), (core_set, 'levels_core')):
        _levels(st, stamp, mids, group, fname)
    pos = [[a] + p.state_row()[:9] for a, v in st.items() for p in v]
    _plain(os.path.join(LIVE, 'positions.csv'), STATE_COLS[:10], pos)
    status = dict(updated_utc=stamp, accounts_known=len(st), coins={r[1]: dict(
        mid=float(mids.get(r[1]) or 0), positions=r[3], long_usd=r[4], short_usd=r[5], total_oi_usd=r[6], oi_one_side_usd=r[10],
        long_share_pct=r[7], short_share_pct=r[8], oldest_reading_h=r[9],
        fresh1h_long_pct=r[11], fresh1h_short_pct=r[12], fresh6h_long_pct=r[13], fresh6h_short_pct=r[14]) for r in srows})
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


def _recent_files(sub, hours):
    """Per-run files of data/<sub>/<date>/<HHMM>.csv[.gz] from the last `hours` hours, oldest first."""
    cut = (NOW - dt.timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M')
    return sorted((f for f in glob.glob(os.path.join(OUT, sub, '*', '*.csv*')) if _stamp_of(f) >= cut), key=_stamp_of)


def write_live_recent():
    """Small 'latest' files for the website's intelligence tabs (live branch - replaced every run, so they never grow):
    intel_recent.csv (7 days of intelligence.csv), changes_recent.csv (24 h: changes >= $100k + every liquidation),
    fills_recent.csv (24 h of whale fills), big_trades_recent.csv (24 h of taker orders >= $100k from the listener)."""
    ip = os.path.join(OUT, 'intelligence.csv')
    if os.path.exists(ip):
        cut = (NOW - dt.timedelta(days=7)).strftime('%Y-%m-%d %H:%M')
        with open(ip, 'rb') as fh:                                   # only the end of the file (7 days ~ 0.5 MB)
            head = fh.readline().decode('utf-8').strip().split(',')
            fh.seek(0, 2); size = fh.tell(); fh.seek(max(0, size - 3_000_000))
            lines = fh.read().decode('utf-8', 'replace').splitlines()[1:]
        _plain(os.path.join(LIVE, 'intel_recent.csv'), head, [r for r in csv.reader(lines) if r and r[0][:2] == '20' and r[0] >= cut])
    def big_change(d):   # market makers churn inventory all day -> only their big moves; liquidations from $50k
        usd = abs(I.num(d.get('usd_change')))
        return usd >= (500_000 if d.get('mm_like') == '1' else 100_000) or (bool(d.get('liquidation')) and usd >= 50_000)
    change_cols = ['time_utc', 'since_utc', 'address', 'coin', 'event', 'liquidation', 'size_before', 'size_after', 'usd_change', 'mid_px',
                   'entry_after', 'value_after', 'whale', 'top_trader', 'mm_like', 'every_run']
    for sub, name, keep, cols in (('changes', 'changes_recent.csv', big_change, change_cols), ('fills', 'fills_recent.csv', None, None),
                                  ('big_trades', 'big_trades_recent.csv', None, None)):
        data = []
        for f in _recent_files(sub, 24):
            try:
                with (gzip.open(f, 'rt', newline='', encoding='utf-8') if f.endswith('.gz') else open(f, newline='', encoding='utf-8')) as fh:
                    x = list(csv.reader(fh))
            except Exception: continue
            if len(x) >= 2: data.append(x)
        if not data: continue
        head = cols or data[-1][0]   # the newest file's columns (columns get added over time - older files leave the new ones empty)
        rows = [[d.get(c, '') for c in head] for x in data for d in (dict(zip(x[0], r)) for r in x[1:] if r) if keep is None or keep(d)]
        _plain(os.path.join(LIVE, name), head, rows)


def load_prev_sel():
    """Last run's whale-fill accounts (the 100 biggest trader positions then)."""
    try:
        with open(os.path.join(STATE, 'fill_accounts.txt'), encoding='utf-8') as fh: return [l.strip() for l in fh if l.strip()]
    except OSError: return []


def save_prev_sel(sel):
    os.makedirs(STATE, exist_ok=True)
    with open(os.path.join(STATE, 'fill_accounts.txt'), 'w', encoding='utf-8', newline='\n') as fh: fh.write('\n'.join(sel) + '\n')


def load_fill_keys():
    p = os.path.join(STATE, 'fill_keys.csv'); keys = {}
    if os.path.exists(p):
        with open(p, newline='', encoding='utf-8') as fh:
            for r in csv.reader(fh):
                if len(r) == 2 and r[1].isdigit(): keys[r[0]] = int(r[1])
    return keys


def save_fill_keys(keys):
    cut = (time.time() - 6 * 3600) * 1000
    os.makedirs(STATE, exist_ok=True)
    with open(os.path.join(STATE, 'fill_keys.csv'), 'w', newline='', encoding='utf-8') as fh:
        csv.writer(fh).writerows([k, t] for k, t in keys.items() if t >= cut)


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
    plan_note = ''; big_now = set(); acc = {}
    if PLAN:
        latest = load_state()
        acc = load_accounts(latest, stamp)
        lbm = {r['ethAddress']: r for r in lb}
        big = {a for a, rows_ in latest.items() for r in rows_ if abs(I.num(r[3])) >= BIG_USD}
        holding = {a for a, rows_ in latest.items() if rows_}
        due = {}                                                    # traded since the last reading -> $ traded since then
        for a, sl in feed_recent(OUT).items():
            lr = acc.get(a, ('', ''))[1]
            usd = sum(u for t, u in sl if not lr or t >= lr)
            if usd >= WATCH_USD: due[a] = usd
        by_usd = [a for a, _ in sorted(due.items(), key=lambda x: -x[1])]
        hot = [a for a in by_usd if due[a] >= HOT_USD and a not in core and a not in big][:HOT_MAX]
        pool = {a for a, r in lbm.items() if float(r['accountValue']) >= 1e4} | feed_addresses(OUT) | holding | set(due)
        rest = pool - core - big - set(hot)
        first = [a for a in by_usd if a in rest][:SLICE // 2]
        def overdue(a):
            lr = acc.get(a, ('', ''))[1]
            age = min(I.hours(lr, stamp), 24 * 7) if lr else 48.0
            return age * (HOLDER_WEIGHT if a in holding else 1)
        fs = set(first)
        others = sorted((a for a in rest if a not in fs), key=lambda a: (-overdue(a), a))
        part = first + others[:max(0, SLICE - len(first))]
        for a in list(big) + hot + part:
            if a in info: continue
            r = lbm.get(a)
            if r:
                av = float(r['accountValue']); pnl_m, vlm_m = win(r, 'month'); pnl_all, _ = win(r, 'allTime')
                info[a] = dict(av=av, mm=vlm_m > 30 * max(av, 1.0), pnl_all=pnl_all, pnl_m=pnl_m, vlm_m=vlm_m)
            else:
                info[a] = dict(av=0.0, mm=False, pnl_all=0.0, pnl_m=0.0, vlm_m=0.0)
        big_now = set(big)
        oldest = max((overdue(a) / (HOLDER_WEIGHT if a in holding else 1) for a in others[max(0, SLICE - len(first)):][:1]), default=0)
        plan_note = (f' + {len(big - core)} big + {len(hot)} hot (traded >= $250k) + {len(part)} rolling ({len(first)} traded since their last'
                     f' reading) of {len(rest)}; next in line unread for {oldest:.1f} h')

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

    sel = list(dict.fromkeys(a for _, a in sorted(holders, key=lambda x: -x[0])))[:SEL_MAX]   # biggest trader positions first
    # whale fills also follow last run's list (owner's go 25 Nov 2026): a whale that closed its whole position is no longer among the
    # biggest holders - without this, its closing trades would be missed
    in_sel = set(sel)
    fsel = sel + [a for a in load_prev_sel() if a not in in_sel] if PLAN else sel
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
        since_ms = int((time.time() - 75 * 60) * 1000)              # the last 75 min (runs are 30 min apart; duplicates are dropped below)
        def fills(addr):
            try:
                if not PLAN:
                    ff = post({'type': 'userFills', 'user': addr}, 20); LIM.take(len(ff or []) // 20); return addr, ff
                ff, start = [], since_ms
                for _ in range(4):                                  # at most 2,000 fills per answer -> next page from the last one
                    page = post({'type': 'userFillsByTime', 'user': addr, 'startTime': start}, 20) or []
                    LIM.take(len(page) // 20)                       # Hyperliquid also counts the size of the answer
                    ff += page
                    if len(page) < 2000: break
                    start = max(int(f['time']) for f in page) + 1
                return addr, ff
            except Exception: return addr, None
        with ThreadPoolExecutor(3) as ex: res = list(ex.map(fills, fsel))
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
        log.append(f'fills: {len(fsel)} accounts ({len(fsel) - len(sel)} only on last run\'s list) -> {len(new)} new BTC / ETH liquidation fills, {time.time() - t0:.0f} s')
        if PLAN:   # whale fills: every BTC / ETH fill of these accounts since the last run, grouped per account + coin + direction
            keys = load_fill_keys(); grp = {}
            fmt = lambda t: dt.datetime.fromtimestamp(t / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            for addr, ff in res:
                for f in ff or []:
                    if f.get('coin') not in COINS: continue
                    key = f"{f.get('hash')}-{f.get('tid')}"
                    if key in keys: continue
                    keys[key] = int(f['time'])
                    e = grp.setdefault((addr, f['coin'], f.get('dir') or ''), dict(n=0, sz=0.0, usd=0.0, t0=f['time'], t1=f['time'], pnl=0.0, fee=0.0, tk=0.0, lq=0, olq=0))
                    px, sz = float(f['px']), float(f['sz'])
                    e['n'] += 1; e['sz'] += sz; e['usd'] += px * sz; e['t0'] = min(e['t0'], f['time']); e['t1'] = max(e['t1'], f['time'])
                    e['pnl'] += float(f.get('closedPnl') or 0); e['fee'] += float(f.get('fee') or 0)
                    if f.get('crossed'): e['tk'] += px * sz
                    q = f.get('liquidation')
                    if q:   # lq = any fill of a liquidation (also when this account was the other side), olq = this account itself was liquidated
                        e['lq'] += 1
                        if (q.get('liquidatedUser') or '').lower() == addr.lower(): e['olq'] += 1
            frows = [[stamp, a, c, d, e['n'], round(e['sz'], 6), round(e['usd'] / e['sz'], 2) if e['sz'] else '', fmt(e['t0']), fmt(e['t1']), round(e['usd']),
                      round(e['pnl'], 2), round(e['fee'], 4), round(100 * e['tk'] / e['usd'], 1) if e['usd'] else '', e['lq'], int(info.get(a, {}).get('mm', 0)), int(a in smart), e['olq']]
                     for (a, c, d), e in sorted(grp.items(), key=lambda x: x[1]['t0'])]
            if frows: write(os.path.join(OUT, 'fills', day, hhmm + '.csv'), FILL_COLS, frows)
            save_fill_keys(keys)
            save_prev_sel(sel)
            log.append(f'whale fills: {sum(e["n"] for e in grp.values())} new BTC / ETH fills of {len({a for a, _, _ in grp})} accounts -> {len(frows)} rows')
    if DO_SLOW and sel:
        with open(os.path.join(OUT, 'last_slow.txt'), 'w') as fh: fh.write(str(time.time()))
    if PLAN:
        try: log.append(update_state(states, rows, stamp, mids, core | big_now, acc))
        except Exception as e: log.append(f'state / coverage failed: {e}')
        try: write_live_extras(orows_live)
        except Exception as e: log.append(f'live extras failed: {e}')
        try: write_live_recent()
        except Exception as e: log.append(f'live recent files failed: {e}')
    print('\n'.join(log), flush=True)
    write(os.path.join(OUT, 'scanner_log.csv'), ['line'], [[l] for l in log], append=True)
    if PLAN:   # 2) when this run's data became known (backtests use 'finished_utc', never the start)
        f = lambda x: dt.datetime.fromtimestamp(x, dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        write(os.path.join(OUT, 'runs.csv'), ['time_utc', 'start_utc', 'positions_done_utc', 'finished_utc', 'accounts', 'positions_rows'],
              [[stamp, f(t0), f(t_pos), f(time.time()), len(info), len(rows)]], append=True)
    try:   # local mode only (not gzip): optional merge + dashboard
        if not GZ:
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
