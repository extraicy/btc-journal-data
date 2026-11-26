"""Hyperliquid LIVE job (26 Nov 2026, owner's go) - trade feed + position scanner + 1-minute market recorder in ONE always-on process.
Public data only, no keys. Replaces the 30-minute scanner runs (hl_scanner.py stays as the automatic fallback) and the feed listener job.

Why: a position can only change through a trade, and the trade feed sees every BTC / ETH trade with both addresses (tested 26 Nov 2026:
2,915 of 2,920 liquidated accounts were seen trading). So instead of re-reading thousands of accounts on a timer to FIND changes, this job
re-reads the accounts that just traded within ~1-2 minutes, and spends the rest of Hyperliquid's speed limit (~1,100 of 1,200 weight a
minute, one address) on what matters next:
  1. traded >= $10k since their last reading (feed)            -> right away
  2. liquidation price within 2 % of the price                 -> every 2 min   (2-5 %: every 10 min)
  3. top N accounts (leaderboard) + every position >= $1M      -> every 15 min
  4. traded < $10k since their last reading                    -> within ~10 min
  5. everyone else (rolling, longest unread first; an account holding a position counts 3x as overdue) - with what is left
Fills (whale trades + liquidation fills) of the 100 biggest trader positions: when the feed saw them trade (also quick in-and-out trades);
probable liquidations of ANY account are confirmed by reading its fills. Stop / take-profit orders of those 100: every 5 min for accounts
with orders or that traded, the rest every 30 min. Their plain limit orders >= $100k are kept as "walls" (separate files).

Files - the SAME research files as the 30-minute scanner, written every 30 minutes (:00 / :30) from the latest readings, plus new ones:
  positions/, summary.csv, summary_all.csv, levels/, levels_core/, changes/, intelligence.csv, orders/, fills/, liquidations.csv, runs.csv,
  scanner_log.csv, accounts/ (daily), feed/, feed_recent/ (:24 / :54), big_trades/, bybit_liq/   (formats unchanged; positions files get
  two columns at the end: account_ntl_usd, account_margin_usd = the whole account's exposure / margin across ALL coins)
  NEW flows_1m/<date>.csv   every minute, per coin + group (mm / top / whale / other): bought, sold, net, taker buys / sells - EXACT flows
                            from the trades themselves (each trade has a buyer and a seller, so the groups add up to zero)
  NEW market_1m/<date>.csv  every minute: price, mark, oracle, funding, open interest, premium, day volume, book depth within 0.5 / 1 / 2 %
  NEW walls/<date>/<HHMM>.csv.gz  plain limit orders >= $100k of the 100 biggest trader positions
  live/ (website, pushed to the 'live' branch every 5 min): status.json, positions.csv, orders.csv, liquidations.csv, walls.csv,
       flows_1m.csv + market_1m.csv (last 24 h), intel_recent.csv, changes_recent.csv, fills_recent.csv, big_trades_recent.csv
Data break: rows from this job's start (see runs.csv 'mode' = live) are fresher than the 30-minute runs before - research must not mix
the two without saying so. Changes carry the exact reading minute (time_utc), not the run's start.

Usage:  python3 -I hl_live.py [--out data] [--dur seconds] [--no-push]   (cloud: env GITHUB_TOKEN + GITHUB_REPOSITORY for the pushes)
"""
import base64, bisect, csv, datetime as dt, glob, gzip, json, os, shutil, socket, ssl, struct, subprocess, sys, threading, time
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
args = sys.argv[1:]
OUT = args[args.index('--out') + 1] if '--out' in args else 'data'
DUR = float(args[args.index('--dur') + 1]) if '--dur' in args else 5.6 * 3600   # stops at the last window end before this (max ~5 h 36)
PUSH = '--no-push' not in args
os.environ['HL_OUT'] = OUT; os.environ.setdefault('HL_PLAN', '1'); os.environ.setdefault('HL_GZIP', '1')
sys.path.insert(0, HERE)
import intel as I            # noqa: E402
import hl_scanner as S       # noqa: E402  (shared helpers + the rate limiter; its main() is not run)

E = lambda k, d: float(os.environ.get(k, d))
N_TOP = int(E('HL_N', 1500))
TRADED_USD = E('HL_TRADED_USD', 10_000)
EVERY = dict(core=E('HL_CORE_MIN', 15) * 60, danger2=E('HL_DANGER2_MIN', 2) * 60, danger5=E('HL_DANGER5_MIN', 10) * 60,
             small=E('HL_SMALL_MIN', 10) * 60, orders_fast=300, orders_slow=1800, fills=300, live=300, window=1800)
SEL_MAX = 100
WALL_USD = 100_000
COINS = I.COINS
HOST = 'api.hyperliquid.xyz'
START = time.time()
LOCK = threading.Lock(); FILE_LOCK = threading.RLock()
LOG = []


def log(s):
    line = f'{dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S")} {s}'
    print(line, flush=True); LOG.append(line)


def utc(t=None, fmt='%Y-%m-%d %H:%M'):
    return dt.datetime.fromtimestamp(time.time() if t is None else t, dt.timezone.utc).strftime(fmt)


# ---------------------------------------------------------------- shared state (st / acc / info only touched by the main thread)
st = {a: [I.Pos.from_state(r) for r in v] for a, v in S.load_state().items()}          # address -> [Pos] (latest BTC / ETH positions)
acc = S.load_accounts({a: [p.state_row() for p in v] for a, v in st.items()}, utc())    # address -> (first_read, last_read)
last_read = {a: I.ms(l.rstrip('~')) / 1000 for a, (f, l) in acc.items() if l}           # address -> epoch s (minute precision at start)
info = {}                    # address -> dict(av, mm, pnl_all) (leaderboard + readings)
core, smart, mm_set, whale_set, sel = set(), set(), set(), set(), []
dirty = {}                   # address -> [usd traded since last reading, last trade ms]   (feed thread)
fills_dirty = {}             # address -> last trade ms (only the fills / orders accounts)
mids, market, books, candles = {}, {}, {}, {}
tape = {}                    # (minute, coin, group) -> [buy, sell, taker_buy, taker_sell, trades]
win = dict(rows={}, closed={}, changes=[], fills={}, keys=S.load_fill_keys(), reads=0)
orders_now, walls_now, orders_read, fills_read = {}, {}, {}, {}
confirm_q = {}               # address -> (since epoch s, change rows to update)
liq_seen, liq_idx, liq_fills = set(), {}, {c: [] for c in COINS}
lb_rows = {}


def load_liqs():
    lp = os.path.join(OUT, 'liquidations.csv'); rows = []
    if os.path.exists(lp):
        with open(lp, newline='', encoding='utf-8') as fh:
            for d in csv.DictReader(fh):
                liq_seen.add(d['key'])
                if I.num(d.get('time_ms')) >= (time.time() - 4 * 86400) * 1000: rows.append(d)
    idx, fl = I.liq_index(rows); liq_idx.update(idx); liq_fills.update(fl)


def add_liq_fills(addr, ff):
    """Liquidation fills of / against `addr` -> liquidations.csv (deduplicated) + the in-memory index."""
    new = []
    for f in ff or []:
        q = f.get('liquidation'); key = f"{f.get('hash')}-{f.get('tid')}"
        if not q or f.get('coin') not in COINS or key in liq_seen: continue
        liq_seen.add(key)
        own = (q.get('liquidatedUser') or '').lower() == addr.lower()
        long_liq = int((f.get('side') == 'A') if own else (f.get('side') == 'B'))
        new.append([key, f['time'], utc(f['time'] / 1000, '%Y-%m-%d %H:%M:%S'), f['coin'], q.get('liquidatedUser'), q.get('markPx'), q.get('method'),
                    f.get('px'), f.get('sz'), f.get('side'), f.get('dir'), long_liq])
        u = (q.get('liquidatedUser') or '').lower()
        if u: bisect.insort(liq_idx.setdefault((u, f['coin']), []), int(f['time']))
        bisect.insort(liq_fills[f['coin']], (int(f['time']), bool(long_liq), float(f['px']) * float(f['sz'])))
    if new:
        with FILE_LOCK:
            S.write(os.path.join(OUT, 'liquidations.csv'), ['key', 'time_ms', 'time_utc', 'coin', 'liquidated_user', 'mark_px', 'method', 'fill_px',
                    'fill_size', 'fill_side', 'fill_dir', 'long_liquidated'], new, append=True)
    return new


# ---------------------------------------------------------------- trade feed (websocket) - copied from feed_listen.py
def connect(host=HOST, path='/ws'):
    s = ssl.create_default_context().wrap_socket(socket.create_connection((host, 443), timeout=30), server_hostname=host)
    key = base64.b64encode(os.urandom(16)).decode()
    s.sendall((f'GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
               f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: public-data-client\r\n\r\n').encode())
    h = b''
    while b'\r\n\r\n' not in h:
        c = s.recv(1)
        if not c: raise ConnectionError('closed during handshake')
        h += c
    if b' 101 ' not in h.split(b'\r\n')[0]: raise ConnectionError(h[:120])
    return s


def send(s, obj, op=1):
    d = json.dumps(obj).encode() if obj is not None else b''; m = os.urandom(4); n = len(d)
    hdr = bytes([0x80 | op]) + (bytes([0x80 | n]) if n < 126 else bytes([0x80 | 126]) + struct.pack('>H', n))
    s.sendall(hdr + m + bytes(b ^ m[i % 4] for i, b in enumerate(d)))


def recvn(s, n):
    b = b''
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c: raise ConnectionError('closed')
        b += c
    return b


def frame(s):
    a, b = recvn(s, 2); op = a & 0x0F; n = b & 0x7F
    if n == 126: n = struct.unpack('>H', recvn(s, 2))[0]
    elif n == 127: n = struct.unpack('>Q', recvn(s, 8))[0]
    if b & 0x80: recvn(s, 4)
    return op, recvn(s, n)


feed = dict(seen={}, recent={}, orders={}, trades=0, reconnects=0, liqs=[], bybit='')
FEED_PATH = os.path.join(OUT, 'feed', utc(START, '%Y-%m-%d'), utc(START, '%H%M') + '.csv.gz')
BYBIT_PATH = os.path.join(OUT, 'bybit_liq', utc(START, '%Y-%m-%d'), utc(START, '%H%M') + '.csv.gz')


def group_of(a):
    return 'mm' if a in mm_set else 'top' if a in smart else 'whale' if a in whale_set else 'other'


def on_trade(t):
    usd = float(t['px']) * float(t['sz']); ms = int(t['time']); us = [u.lower() for u in t.get('users', [])]
    with LOCK:
        feed['trades'] += 1
        for u in us:
            e = feed['seen'].setdefault(u, [0, 0.0, ms, ms]); e[0] += 1; e[1] += usd; e[3] = ms
            r = feed['recent'].setdefault(u, [0, 0.0, ms]); r[0] += 1; r[1] += usd; r[2] = max(r[2], ms)
            d = dirty.setdefault(u, [0.0, ms]); d[0] += usd; d[1] = max(d[1], ms)
            if u in sel_set: fills_dirty[u] = ms
        if len(us) == 2:   # users = [buyer, seller]; side B = the taker bought
            buyer, seller = us
            taker, maker = (buyer, seller) if t.get('side') == 'B' else (seller, buyer)
            h = t.get('hash') or ''
            key = (h, t['coin'], taker) if h.replace('0x', '').strip('0') else ('tid', t.get('tid'))
            o = feed['orders'].setdefault(key, [t['coin'], t.get('side'), taker, 0.0, 0.0, ms, ms, 0, set()])
            o[3] += usd; o[4] += float(t['sz']); o[6] = max(o[6], ms); o[7] += 1; o[8].add(maker)
            mnt = ms // 60000 * 60
            for who, i in ((buyer, 0), (seller, 1)):
                x = tape.setdefault((mnt, t['coin'], group_of(who)), [0.0, 0.0, 0.0, 0.0, 0]); x[i] += usd
            x = tape.setdefault((mnt, t['coin'], group_of(taker)), [0.0, 0.0, 0.0, 0.0, 0])
            x[2 if t.get('side') == 'B' else 3] += usd; x[4] += 1


def feed_thread():
    while True:
        try:
            s = connect(); s.settimeout(20)
            for c in COINS: send(s, {'method': 'subscribe', 'subscription': {'type': 'trades', 'coin': c}})
            last_ping = time.time(); buf = b''
            while True:
                if time.time() - last_ping > 45: send(s, {'method': 'ping'}); last_ping = time.time()
                try: op, d = frame(s)
                except socket.timeout: continue
                if op == 8: raise ConnectionError('server close')
                if op == 9: send(s, None, op=10); continue
                if op == 0: buf += d; continue
                if op != 1: continue
                if buf: d = buf + d; buf = b''
                m = json.loads(d)
                if m.get('channel') == 'trades':
                    for t in m['data']: on_trade(t)
        except Exception as e:  # noqa: BLE001
            feed['reconnects'] += 1; print('feed reconnect:', str(e)[:120], flush=True); time.sleep(3)


def bybit_thread():
    refused = 0
    while True:
        try:
            s = connect('stream.bybit.com', '/v5/public/linear'); s.settimeout(25)
            send(s, {'op': 'subscribe', 'args': ['allLiquidation.BTCUSDT', 'allLiquidation.ETHUSDT']})
            feed['bybit'] = feed['bybit'] or 'connected'; last_ping = time.time(); buf = b''
            while True:
                if time.time() - last_ping > 20: send(s, {'op': 'ping'}); last_ping = time.time()
                try: op, d = frame(s)
                except socket.timeout: continue
                if op == 8: raise ConnectionError('server close')
                if op == 9: send(s, None, op=10); continue
                if op == 0: buf += d; continue
                if op != 1: continue
                if buf: d = buf + d; buf = b''
                m = json.loads(d)
                if not str(m.get('topic', '')).startswith('allLiquidation'): continue
                for x in m.get('data', []):
                    t = int(x['T']); v = float(x['v']); p = float(x['p'])
                    feed['liqs'].append([t, utc(t / 1000, '%Y-%m-%d %H:%M:%S'), x['s'], int(x['S'] == 'Buy'), v, p, round(v * p, 2)])
        except Exception as e:  # noqa: BLE001
            msg = str(e)[:160]
            if any(k in msg for k in (' 403 ', ' 451 ', 'Forbidden')):
                refused += 1
                if refused >= 3: feed['bybit'] = 'REFUSED - ' + msg; return
            time.sleep(5)


def gz_write(path, head, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path + '.tmp', 'wt', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(head); w.writerows(rows)
    os.replace(path + '.tmp', path)


def save_feed():
    with LOCK: seen = dict(feed['seen']); liqs = list(feed['liqs'])
    with FILE_LOCK:
        gz_write(FEED_PATH, ['address', 'trades', 'usd_traded', 'first_seen_utc', 'last_seen_utc'],
                 [[a, n, round(u), utc(f0 / 1000), utc(f1 / 1000)] for a, (n, u, f0, f1) in sorted(seen.items(), key=lambda x: -x[1][1])])
        if liqs: gz_write(BYBIT_PATH, ['time_ms', 'time_utc', 'symbol', 'long_liquidated', 'size', 'bankruptcy_px', 'value_usd'], liqs)


def write_slice():
    """feed_recent + big_trades for the slice that just ended (at :24 / :54, as the listener did)."""
    with LOCK:
        rec, ords = feed['recent'], feed['orders']; feed['recent'], feed['orders'] = {}, {}
    d, hm = utc(None, '%Y-%m-%d'), utc(None, '%H%M')
    rows = [[a, n, round(u), utc(l / 1000)] for a, (n, u, l) in sorted(rec.items(), key=lambda x: -x[1][1]) if u >= 10_000]
    big = [[o[5], utc(o[5] / 1000, '%Y-%m-%d %H:%M:%S'), o[0], o[1], o[2], round(o[3]), round(o[4], 6), round(o[3] / o[4], 2) if o[4] else '', o[7], len(o[8])]
           for o in sorted(ords.values(), key=lambda x: x[5]) if o[3] >= 100_000]
    with FILE_LOCK:
        if rows: gz_write(os.path.join(OUT, 'feed_recent', d, hm + '.csv.gz'), ['address', 'trades', 'usd_traded', 'last_trade_utc'], rows)
        if big: gz_write(os.path.join(OUT, 'big_trades', d, hm + '.csv.gz'),
                         ['time_ms', 'time_utc', 'coin', 'side', 'taker', 'usd', 'size', 'avg_px', 'fills', 'makers'], big)
    return len(rows), len(big)


# ---------------------------------------------------------------- 1-minute market recorder
MKT_COLS = ['time_utc', 'coin', 'mid_px', 'mark_px', 'oracle_px', 'funding_1h', 'oi_both_coins', 'oi_one_side_usd', 'premium', 'day_ntl_vlm',
            'best_bid', 'best_ask', 'spread_bps', 'bid05_usd', 'ask05_usd', 'bid1_usd', 'ask1_usd', 'bid2_usd', 'ask2_usd']
FLOW_COLS = ['time_utc', 'coin', 'group', 'bought_usd', 'sold_usd', 'net_usd', 'taker_buy_usd', 'taker_sell_usd', 'taker_trades']


def daily_append(sub, head, rows, t):
    with FILE_LOCK:
        S.write(os.path.join(OUT, sub, utc(t, '%Y-%m-%d') + '.csv'), head, rows, append=True)


def market_tick():
    t = time.time() // 60 * 60
    meta, ctx = S.post({'type': 'metaAndAssetCtxs'}, 20)
    am = S.post({'type': 'allMids'}, 2) or {}
    rows = []
    for m, c in zip(meta['universe'], ctx):
        if m['name'] not in COINS: continue
        k = m['name']; mark = I.num(c.get('markPx')); mid = I.num(am.get(k)) or I.num(c.get('midPx')) or mark
        mids[k] = mid; market[k] = dict(mid=mid, mark=mark, funding=c.get('funding', ''), oi_both=I.num(c.get('openInterest')))
        try:
            bk = [S.post({'type': 'l2Book', 'coin': k, 'nSigFigs': n}, 2).get('levels') for n in (5, 4, 3)]
            books[k] = bk
        except Exception: bk = books.get(k) or []
        dep = I.book_depth(bk, mid, pcts=(0.5, 1, 2)) if bk and all(bk) else {}
        bb = float(bk[0][0][0]['px']) if bk and bk[0] and bk[0][0] else ''; ba = float(bk[0][1][0]['px']) if bk and bk[0] and bk[0][1] else ''
        rows.append([utc(t), k, mid, mark, c.get('oraclePx', ''), c.get('funding', ''), c.get('openInterest', ''),
                     round(I.num(c.get('openInterest')) / 2 * mark), c.get('premium', ''), c.get('dayNtlVlm', ''), bb, ba,
                     round(1e4 * (ba - bb) / mid, 2) if bb and ba and mid else ''] +
                    [round(dep[x]) if dep.get(x) is not None else '' for x in ('bid0.5', 'ask0.5', 'bid1', 'ask1', 'bid2', 'ask2')])
    daily_append('market_1m', MKT_COLS, rows, t)


def flush_tape(upto):
    """Write every finished minute of the trade-flow tally (flows_1m/<date>.csv)."""
    with LOCK:
        done = sorted(k for k in tape if k[0] < upto)
        rows = [(k, tape.pop(k)) for k in done]
    out = {}
    for (m, c, g), (b, s, tb, ts, n) in rows:
        out.setdefault(m, []).append([utc(m), c, g, round(b), round(s), round(b - s), round(tb), round(ts), n])
    for m, rr in sorted(out.items()): daily_append('flows_1m', FLOW_COLS, rr, m)


def refresh_candles():
    t1 = int(time.time() * 1000); t0 = t1 - 3 * 86_400_000
    for c in COINS:
        try:
            cd = sorted(S.post({'type': 'candleSnapshot', 'req': {'coin': c, 'interval': '15m', 'startTime': t0, 'endTime': t1}}, 20) or [], key=lambda x: x['t'])
            candles[c] = ([int(x['t']) for x in cd], [float(x['h']) for x in cd], [float(x['l']) for x in cd])
        except Exception: pass


def market_thread():
    last_c = 0
    while True:
        nxt = (time.time() // 60 + 1) * 60 + 3
        time.sleep(max(0.0, nxt - time.time()))
        try:
            market_tick(); flush_tape(time.time() // 60 * 60)
            if time.time() - last_c > 900: refresh_candles(); last_c = time.time()
        except Exception as e:  # noqa: BLE001
            print('market tick failed:', str(e)[:120], flush=True)


# ---------------------------------------------------------------- fills + orders of the biggest trader positions, liquidation checks
sel_set = set()


def get_fills(addr, since_ms):
    ff, start = [], since_ms
    for _ in range(4):
        page = S.post({'type': 'userFillsByTime', 'user': addr, 'startTime': start}, 20) or []
        S.LIM.take(len(page) // 20); ff += page
        if len(page) < 2000: break
        start = max(int(f['time']) for f in page) + 1
    return ff


def whale_fills(addr, ff):
    with LOCK:
        for f in ff:
            if f.get('coin') not in COINS: continue
            key = f"{f.get('hash')}-{f.get('tid')}"
            if key in win['keys']: continue
            win['keys'][key] = int(f['time'])
            e = win['fills'].setdefault((addr, f['coin'], f.get('dir') or ''), dict(n=0, sz=0.0, usd=0.0, t0=f['time'], t1=f['time'], pnl=0.0, fee=0.0, tk=0.0, lq=0, olq=0))
            px, sz = float(f['px']), float(f['sz'])
            e['n'] += 1; e['sz'] += sz; e['usd'] += px * sz; e['t0'] = min(e['t0'], f['time']); e['t1'] = max(e['t1'], f['time'])
            e['pnl'] += float(f.get('closedPnl') or 0); e['fee'] += float(f.get('fee') or 0)
            if f.get('crossed'): e['tk'] += px * sz
            q = f.get('liquidation')
            if q:
                e['lq'] += 1
                if (q.get('liquidatedUser') or '').lower() == addr.lower(): e['olq'] += 1


def side_thread():
    """Every ~20 s: confirm probable liquidations, fills of top accounts that traded, due stop / take-profit orders."""
    while True:
        time.sleep(20)
        try:
            now = time.time()
            with LOCK: cq = list(confirm_q.items()); confirm_q.clear()
            for addr, (since, rows) in cq:
                try: ff = get_fills(addr, int((since - 120) * 1000))
                except Exception: continue
                add_liq_fills(addr, ff)
                own = [int(f['time']) for f in ff if f.get('liquidation') and (f['liquidation'].get('liquidatedUser') or '').lower() == addr.lower()]
                if own:
                    with LOCK:
                        for r in rows: r[5] = 'confirmed'
            with LOCK:
                due = [a for a, t in fills_dirty.items() if now - fills_read.get(a, 0) >= EVERY['fills']]
            for a in due:
                try: ff = get_fills(a, int(max(fills_read.get(a, now - 4500), now - 4500) * 1000) - 60_000)
                except Exception: continue
                fills_read[a] = now
                with LOCK: fills_dirty.pop(a, None)
                add_liq_fills(a, ff); whale_fills(a, ff)
            for a in list(sel):
                fast = bool(orders_now.get(a)) or orders_read.get(a, 0) < max(fills_read.get(a, 0), 0) or a in due
                if now - orders_read.get(a, 0) < (EVERY['orders_fast'] if fast else EVERY['orders_slow']): continue
                try: oo = S.post({'type': 'frontendOpenOrders', 'user': a}, 20) or []
                except Exception: continue
                orders_read[a] = now; stamp = utc(now)
                o_rows, w_rows = [], []
                for o in oo:
                    if o.get('coin') not in COINS: continue
                    if o.get('isTrigger') or o.get('reduceOnly'):
                        o_rows.append([stamp, a, o.get('coin'), o.get('side'), o.get('orderType'), o.get('isTrigger'), o.get('triggerPx') or '',
                                       o.get('triggerCondition') or '', o.get('limitPx'), o.get('sz'), o.get('reduceOnly'), o.get('isPositionTpsl'), o.get('timestamp')])
                    else:
                        usd = I.num(o.get('limitPx')) * I.num(o.get('sz'))
                        if usd >= WALL_USD:
                            w_rows.append([stamp, a, o.get('coin'), o.get('side'), o.get('limitPx'), o.get('sz'), round(usd), o.get('timestamp'),
                                           int(info.get(a, {}).get('mm', False)), int(a in smart)])
                with LOCK: orders_now[a] = o_rows; walls_now[a] = w_rows
        except Exception as e:  # noqa: BLE001
            print('side thread:', str(e)[:160], flush=True)


# ---------------------------------------------------------------- leaderboard, picking whom to read, reading
def load_leaderboard():
    global core, smart, lb_rows
    for k in range(4):
        try:
            lb = json.loads(S.urllib.request.urlopen(S.urllib.request.Request(S.LEADERBOARD, headers={'User-Agent': 'public-data-client'}), timeout=90).read())['leaderboardRows']
            break
        except Exception:
            if k == 3: raise
            time.sleep(10 * (k + 1))
    lb_rows = {r['ethAddress']: r for r in lb}
    top = sorted(lb, key=lambda r: -float(r['accountValue']))[:N_TOP]
    for r in lb:
        av = float(r['accountValue']); pnl_m, vlm_m = S.win(r, 'month'); pnl_all, _ = S.win(r, 'allTime')
        info[r['ethAddress']] = dict(av=av, mm=vlm_m > 30 * max(av, 1.0), pnl_all=pnl_all)
    core = {r['ethAddress'] for r in top}
    smart = set(a for a, _ in sorted(((a, info[a]['pnl_all']) for a in core if not info[a]['mm']), key=lambda x: -x[1])[:100])
    day = utc(None, '%Y-%m-%d'); ap = os.path.join(OUT, 'accounts', day + '.csv')
    if not os.path.exists(ap) and not os.path.exists(ap + '.gz'):
        with FILE_LOCK:
            S.write(ap, ['address', 'account_value', 'mm_like', 'pnl_day', 'pnl_week', 'pnl_month', 'pnl_all', 'vlm_day', 'vlm_week', 'vlm_month', 'vlm_all', 'display_name'],
                    [[r['ethAddress'], r['accountValue'], int(info[r['ethAddress']]['mm'])] + [S.win(r, w)[0] for w in ('day', 'week', 'month', 'allTime')]
                     + [S.win(r, w)[1] for w in ('day', 'week', 'month', 'allTime')] + [r.get('displayName') or ''] for r in top])


def relabel():
    global mm_set, whale_set, sel, sel_set
    mm_set = {a for a, i in info.items() if i.get('mm')}
    holders = sorted(((p.value, a) for a, v in st.items() for p in v if not (info.get(a) or {}).get('mm') and not p.mm), reverse=True)
    whale_set = {a for v, a in holders if v >= I.BIG_USD and a not in smart}
    new_sel = list(dict.fromkeys(a for _, a in holders))[:SEL_MAX]
    prev = [a for a in S.load_prev_sel() if a not in set(new_sel)]
    sel = new_sel + prev; sel_set = set(sel)
    S.save_prev_sel(new_sel)


cache = dict(pool=[], pool_t=0, danger=[], danger_t=0)


def danger_lists():
    if time.time() - cache['danger_t'] < 30: return cache['danger']
    d2, d5 = [], []
    for a, v in st.items():
        for p in v:
            m = mids.get(p.coin)
            if not (m and p.liq) or p.value < 10_000: continue
            x = ((m - p.liq) if p.size > 0 else (p.liq - m)) / m
            if 0 <= x <= 0.02: d2.append(a)
            elif 0 <= x <= 0.05: d5.append(a)
    cache['danger'], cache['danger_t'] = (d2, d5), time.time()
    return d2, d5


def rolling_pool():
    if time.time() - cache['pool_t'] < 600: return cache['pool']
    holding = {a for a, v in st.items() if v}
    pool = {a for a, r in lb_rows.items() if float(r['accountValue']) >= 1e4} | S.feed_addresses(OUT) | holding
    with LOCK: pool |= set(feed['seen'])
    now = time.time()
    def overdue(a):
        lr = last_read.get(a); age = min((now - lr) / 3600, 24 * 7) if lr else 48.0
        return age * (3 if a in holding else 1)
    cache['pool'] = sorted(pool, key=lambda a: -overdue(a)); cache['pool_t'] = now
    return cache['pool']


def pick(n):
    now = time.time(); out = []; got = set()
    def add(a):
        if a not in got and len(out) < n: got.add(a); out.append(a)
    with LOCK: dd = sorted(((a, e[0], e[1]) for a, e in dirty.items()), key=lambda x: -x[1])
    for a, usd, _ in dd:
        if usd >= TRADED_USD and now - last_read.get(a, 0) > 20: add(a)
    d2, d5 = danger_lists()
    for a in d2:
        if now - last_read.get(a, 0) >= EVERY['danger2']: add(a)
    for a in sorted(core | big_now(), key=lambda a: last_read.get(a, 0)):
        if now - last_read.get(a, 0) >= EVERY['core']: add(a)
        else: break
    for a in d5:
        if now - last_read.get(a, 0) >= EVERY['danger5']: add(a)
    for a, usd, lt in dd:
        if usd < TRADED_USD and now - lt / 1000 >= EVERY['small'] - 60: add(a)
    if len(out) < n:
        pool = rolling_pool()
        i = cache.setdefault('pi', 0)
        while len(out) < n and pool:
            a = pool[i % len(pool)]; i += 1
            if now - last_read.get(a, 0) > 600: add(a)
            if i - cache['pi'] > 5 * n: break
        cache['pi'] = i % max(1, len(pool))
    return out


_big = dict(t=0, s=set())


def big_now():
    if time.time() - _big['t'] > 60:
        _big['s'] = {a for a, v in st.items() for p in v if p.value >= I.BIG_USD}; _big['t'] = time.time()
    return _big['s']


def read(addr):
    try: return addr, time.time(), S.post({'type': 'clearinghouseState', 'user': addr}, 2)
    except Exception: return addr, time.time(), None


known = set(acc) | set(st)


def apply_read(addr, t_req, js):
    if js is None: return
    stamp = utc(t_req)
    i = info.setdefault(addr, dict(av=0.0, mm=False, pnl_all=0.0))
    ms_ = js.get('marginSummary') or {}
    av = I.num(ms_.get('accountValue')); i['av'] = av or i['av']
    ntl, mu = I.num(ms_.get('totalNtlPos')), I.num(ms_.get('totalMarginUsed'))
    new, rows = [], []
    for ap in js.get('assetPositions', []):
        p = ap.get('position', {}); c = p.get('coin')
        if c not in COINS: continue
        sz = float(p['szi']); val = float(p.get('positionValue') or 0); lev = p.get('leverage', {})
        new.append(I.Pos(c, sz, I.num(p.get('entryPx')), val, I.num(p.get('liquidationPx'), None), stamp, bool(i['mm']), round(i['av']),
                         I.num(lev.get('value')), addr in smart))
        rows.append([stamp, addr, c, sz, p.get('entryPx'), p.get('liquidationPx') or '', val, p.get('unrealizedPnl'), lev.get('type'), lev.get('value'),
                     int(i['mm']), int(addr in smart), round(i['av']), round(ntl), round(mu)])
    old = st.get(addr, [])
    sn = acc.get(addr, ('', ''))[1]
    since = {addr: sn} if sn and not sn.endswith('~') else {}
    with LOCK: d = dirty.get(addr)
    traded_ms = d[1] if d else None
    # an account read for the first time is a new find, not a change (as in the 30-minute scanner)
    chg = I.find_changes(stamp, {addr: old} if addr in st else {}, {addr: new}, since, mids, liq_idx, candles, core | big_now(), known)
    for x in chg:
        x.append(traded_ms)                                       # extra field (not written): when the feed last saw this account trade
        if x[4] in ('CLOSED', 'REDUCED', 'FLIPPED') and x[5] != 'confirmed':
            o = next((p for p in old if p.coin == x[3]), None)
            if x[5] == 'probable' or (o and o.liq and mids.get(x[3]) and abs(mids[x[3]] / o.liq - 1) < 0.03):
                with LOCK:
                    q = confirm_q.setdefault(addr, (last_read.get(addr, t_req - 3600), []))
                    q[1].append(x)
    have = {p.coin for p in new}
    closed = [[stamp, addr, p.coin, 0, '', '', 0, '', '', '', int(i['mm']), int(addr in smart), round(i['av']), round(ntl), round(mu)]
              for p in old if p.coin not in have]
    with LOCK:
        win['rows'][addr] = rows + closed; win['changes'] += chg; win['reads'] += 1
        e = dirty.get(addr)
        if e and e[1] <= t_req * 1000 - 1500: dirty.pop(addr, None)
    st[addr] = new; known.add(addr)
    acc[addr] = (acc.get(addr, ('', ''))[0] or stamp, stamp); last_read[addr] = t_req


# ---------------------------------------------------------------- the 30-minute research files + the 5-minute live picture
POS_COLS = ['time_utc', 'address', 'coin', 'size', 'entry_px', 'liquidation_px', 'position_value_usd', 'unrealized_pnl', 'margin', 'leverage',
            'mm_like', 'top100_pnl', 'account_value', 'account_ntl_usd', 'account_margin_usd']
ORD_COLS = ['time_utc', 'address', 'coin', 'side', 'order_type', 'is_trigger', 'trigger_px', 'trigger_condition', 'limit_px', 'size', 'reduce_only', 'position_tpsl', 'placed_ms']
WALL_COLS = ['time_utc', 'address', 'coin', 'side', 'limit_px', 'size', 'usd', 'placed_ms', 'mm_like', 'top100_pnl']
CHANGE_LIVE = ['time_utc', 'since_utc', 'address', 'coin', 'event', 'liquidation', 'size_before', 'size_after', 'usd_change', 'mid_px',
               'entry_after', 'value_after', 'whale', 'top_trader', 'mm_like', 'every_run']


def coverage_now(stamp):
    oi_both_usd = {c: market[c]['oi_both'] * (market[c]['mark'] or market[c]['mid']) for c in market}
    cov = I.coverage(st, stamp, {c: v / 2 for c, v in oi_both_usd.items()})
    return cov, I.summary_rows(stamp, st, cov, oi_both_usd), oi_both_usd


def status_json(stamp, srows):
    return dict(updated_utc=stamp, accounts_known=len(st), mode='live', coins={r[1]: dict(
        mid=float(mids.get(r[1]) or 0), positions=r[3], long_usd=r[4], short_usd=r[5], total_oi_usd=r[6], oi_one_side_usd=r[10],
        long_share_pct=r[7], short_share_pct=r[8], oldest_reading_h=r[9],
        fresh1h_long_pct=r[11], fresh1h_short_pct=r[12], fresh6h_long_pct=r[13], fresh6h_short_pct=r[14]) for r in srows})


def recent_daily(sub, hours=24):
    out, head = [], None
    for d in sorted({utc(time.time() - k * 86400, '%Y-%m-%d') for k in (1, 0)}):
        p = os.path.join(OUT, sub, d + '.csv')
        if not os.path.exists(p): continue
        with open(p, newline='', encoding='utf-8') as fh:
            r = list(csv.reader(fh))
        head = r[0]; cut = utc(time.time() - hours * 3600)
        out += [x for x in r[1:] if x and x[0] >= cut]
    return head, out


def window_files(stamp, final):
    """Write (or re-write) the current window's changes + fills files (named after the window's end, as the old runs)."""
    d, hm = stamp[:10], stamp[11:13] + stamp[14:16]
    with LOCK:
        chg = [x[:len(I.CHANGE_COLS)] for x in win['changes']]
        grp = dict(win['fills'])
    keep = [x for x in chg if I.keep_change(x)]
    with FILE_LOCK:
        if keep: S.write(os.path.join(OUT, 'changes', d, hm + '.csv'), I.CHANGE_COLS, keep)
        frows = [[stamp, a, c, dr, e['n'], round(e['sz'], 6), round(e['usd'] / e['sz'], 2) if e['sz'] else '', utc(e['t0'] / 1000, '%Y-%m-%d %H:%M:%S'),
                  utc(e['t1'] / 1000, '%Y-%m-%d %H:%M:%S'), round(e['usd']), round(e['pnl'], 2), round(e['fee'], 4),
                  round(100 * e['tk'] / e['usd'], 1) if e['usd'] else '', e['lq'], int(info.get(a, {}).get('mm', False)), int(a in smart), e['olq']]
                 for (a, c, dr), e in sorted(grp.items(), key=lambda x: x[1]['t0'])]
        if frows: S.write(os.path.join(OUT, 'fills', d, hm + '.csv'), S.FILL_COLS, frows)


def publish_live(stamp, push=True):
    cov, srows, _ = coverage_now(stamp)
    L = S.LIVE; os.makedirs(L, exist_ok=True)
    S._plain(os.path.join(L, 'positions.csv'), S.STATE_COLS[:10], [[a] + p.state_row()[:9] for a, v in st.items() for p in v])
    with LOCK:
        orows = [r for a in sel for r in orders_now.get(a, [])]; wrows = [r for a in sel for r in walls_now.get(a, [])]
    S.write_live_extras(orows)
    S._plain(os.path.join(L, 'walls.csv'), WALL_COLS, wrows)
    for sub, name in (('flows_1m', 'flows_1m.csv'), ('market_1m', 'market_1m.csv')):
        h, rr = recent_daily(sub)
        if h: S._plain(os.path.join(L, name), h, rr)
    window_files(next_boundary_stamp(), False)
    # Bybit liquidations of the last 24 h (every one, from the public stream; owner's go 26 Nov 2026 18:50 UTC): saved files + this run's
    bl, cut = {}, (time.time() - 86400) * 1000
    for f in glob.glob(os.path.join(OUT, 'bybit_liq', '*', '*.csv.gz')):
        if os.path.basename(os.path.dirname(f)) < utc(time.time() - 2 * 86400, '%Y-%m-%d'): continue
        try:
            with gzip.open(f, 'rt', newline='', encoding='utf-8') as fh:
                for r in list(csv.reader(fh))[1:]:
                    if r and float(r[0]) >= cut: bl[(r[0], r[2], r[4])] = r
        except Exception: pass
    with LOCK: mine = list(feed['liqs'])
    for r in mine:
        if r[0] >= cut: bl[(str(r[0]), r[2], str(r[4]))] = r
    S._plain(os.path.join(L, 'bybit_liq.csv'), ['time_ms', 'time_utc', 'symbol', 'long_liquidated', 'size', 'bankruptcy_px', 'value_usd'],
             sorted(bl.values(), key=lambda r: float(r[0])))
    with LOCK: ords = list(feed['orders'].values())   # the current half hour's big taker orders (big_trades_recent only has finished slices)
    S._plain(os.path.join(L, 'big_trades_now.csv'), ['time_ms', 'time_utc', 'coin', 'side', 'taker', 'usd', 'size', 'avg_px', 'fills', 'makers'],
             [[o[5], utc(o[5] / 1000, '%Y-%m-%d %H:%M:%S'), o[0], o[1], o[2], round(o[3]), round(o[4], 6), round(o[3] / o[4], 2) if o[4] else '', o[7], len(o[8])]
              for o in sorted(ords, key=lambda x: x[5]) if o[3] >= 100_000])
    S.NOW = dt.datetime.now(dt.timezone.utc)
    try: S.write_live_recent()
    except Exception as e: log(f'live recent failed: {e}')
    with open(os.path.join(L, 'status.json'), 'w', encoding='utf-8') as fh: json.dump(status_json(stamp, srows), fh)
    if push and PUSH: threading.Thread(target=push_live, daemon=True).start()


def git(*a, cwd=None):
    return subprocess.run(['git', *a], capture_output=True, text=True, timeout=300, cwd=cwd)


def remote():
    tok, repo = os.environ.get('GITHUB_TOKEN'), os.environ.get('GITHUB_REPOSITORY')
    return f'https://x-access-token:{tok}@github.com/{repo}.git' if tok and repo else None


live_lock = threading.Lock()


def push_live():
    if not remote() or not live_lock.acquire(blocking=False): return
    try:
        tmp = os.path.join(HERE, '.live_push'); shutil.rmtree(tmp, ignore_errors=True)
        shutil.copytree(S.LIVE, tmp)
        git('init', '-q', '-b', 'live', cwd=tmp); git('config', 'user.name', 'hl-live', cwd=tmp); git('config', 'user.email', 'hl-live@users.noreply.github.com', cwd=tmp)
        git('add', '-A', cwd=tmp); git('commit', '-q', '-m', 'live ' + utc(None, '%Y-%m-%d_%H%M'), cwd=tmp)
        r = git('push', '-q', '-f', remote(), 'live', cwd=tmp)
        if r.returncode != 0: log('live push failed: ' + r.stderr[-200:].replace(os.environ.get('GITHUB_TOKEN', '#'), '***'))
    finally:
        live_lock.release()


data_lock = threading.Lock()


def push_data(label):
    if not PUSH or not remote(): return
    with data_lock:
        _push_data(label)


def _push_data(label):
    with FILE_LOCK:
        git('add', OUT)
        if git('diff', '--cached', '--quiet').returncode == 0: return
        git('commit', '-q', '-m', f'live {label}')
    for i in range(5):
        r = git('pull', '--rebase', '-X', 'theirs', '-q')
        if r.returncode != 0: git('rebase', '--abort'); git('pull', '--no-rebase', '-X', 'ours', '-q', '--no-edit')
        if git('push', '-q').returncode == 0: return
        time.sleep(10 * (i + 1))
    log('data push failed - kept for the next window / the final save')


def next_boundary_stamp(t=None):
    t = time.time() if t is None else t
    return utc((t // 1800 + 1) * 1800)


old_win = {}     # the state at the previous window's end (for intelligence: who held what before)


def close_window(stamp, prev_stamp):
    """Everything the 30-minute scanner wrote, for the window that just ended at `stamp`."""
    global old_win
    t_now = time.time(); d, hm = stamp[:10], stamp[11:13] + stamp[14:16]
    with LOCK:
        rows = [r for v in win['rows'].values() for r in v]; reads = win['reads']
        chg = list(win['changes'])
    with FILE_LOCK:
        S.write(os.path.join(OUT, 'positions', d, hm + '.csv'), POS_COLS, rows)
    # summary.csv = the top N accounts only (comparable history)
    agg = {c: dict(nl=0, ns=0, l=0.0, s=0.0, tl=0.0, ts=0.0, sl=0.0, ss=0.0) for c in COINS}
    for a in core:
        for p in st.get(a, []):
            x = agg[p.coin]; side = 'l' if p.size > 0 else 's'
            x['n' + side] += 1; x[side] += p.value
            if not p.mm: x['t' + side] += p.value
            if a in smart: x['s' + side] += p.value
    n_core = sum(1 for a in core if t_now - last_read.get(a, 0) <= 1800)
    with FILE_LOCK:
        S.write(os.path.join(OUT, 'summary.csv'), ['time_utc', 'coin', 'accounts_scanned', 'accounts_long', 'accounts_short', 'long_usd', 'short_usd',
                'trader_long_usd', 'trader_short_usd', 'top100_long_usd', 'top100_short_usd', 'mid_px'],
                [[stamp, c, n_core, a['nl'], a['ns'], round(a['l']), round(a['s']), round(a['tl']), round(a['ts']), round(a['sl']), round(a['ss']), mids.get(c, '')]
                 for c, a in agg.items()], append=True)
    cov, srows, oi_both_usd = coverage_now(stamp)
    fixed = core | big_now()
    with FILE_LOCK:
        S.append_cols(os.path.join(OUT, 'summary_all.csv'), I.SUMMARY_COLS, srows, S.fix_summary_row)
        for group, fname in ((None, 'levels'), (fixed, 'levels_core')): S._levels(st, stamp, mids, group, fname)
    window_files(stamp, True)
    # intelligence: a change is "precise" when it surely happened in this window (the feed saw the trade, or the previous reading was in it)
    t0 = I.ms(prev_stamp) if prev_stamp else 0
    ichg = []
    for x in chg:
        y = list(x[:len(I.CHANGE_COLS)])
        tr = x[len(I.CHANGE_COLS)] if len(x) > len(I.CHANGE_COLS) else None
        if prev_stamp and ((tr and tr > t0 - 300_000) or (x[1] and I.ms(x[1]) >= t0 - 60_000)): y[1] = prev_stamp
        ichg.append(y)
    st_i = {}
    for a, v in st.items():   # the fixed group read in this window counts as "read this run"
        st_i[a] = [I.Pos(p.coin, p.size, p.entry, p.value, p.liq, stamp, p.mm, p.av, p.lev, p.top) for p in v] if (a in fixed and t_now - last_read.get(a, 0) <= 1800) else v
    ip = os.path.join(OUT, 'intelligence.csv')
    head, last = S.tail_rows(ip, 4)
    prev = [dict(zip(head, r)) for r in last] if head else []
    pst = max((x['time_utc'] for x in prev if x.get('time_utc', '') < stamp), default='')
    prev_market = {x['coin']: dict(mid=I.num(x.get('mid_px')), oi_both=2 * I.num(x.get('oi_one_side_coins'))) for x in prev if x.get('time_utc') == pst}
    book = {c: I.book_depth(books[c], mids[c]) for c in books if c in mids and books[c] and all(books[c])}
    irows = I.intel_rows(stamp, utc(), pst, st_i, old_win or st, cov, ichg, market, prev_market, liq_fills, book, fixed)
    with FILE_LOCK:
        S.append_cols(ip, I.INTEL_COLS, irows)
        with LOCK:
            orows = [r for a in sel for r in orders_now.get(a, [])]; wrows = [r for a in sel for r in walls_now.get(a, [])]
        S.write(os.path.join(OUT, 'orders', d, hm + '.csv'), ORD_COLS, orows)
        if wrows: S.write(os.path.join(OUT, 'walls', d, hm + '.csv'), WALL_COLS, wrows)
        S.save_state(st, acc); S.save_accounts(acc); S.save_fill_keys(win['keys'])
    save_feed()
    old_win = dict(st)
    line = (f'{stamp} UTC live window: {reads} readings ({len(rows)} position rows), {len(chg)} changes '
            f'({sum(1 for x in chg if x[5] == "confirmed")} confirmed + {sum(1 for x in chg if x[5] == "probable")} probable liquidations), '
            f'{len(orows)} orders, {len(wrows)} walls, feed {feed["trades"]} trades / {feed["reconnects"]} reconnects, bybit {feed["bybit"] or "-"} | '
            f'coverage ' + ' · '.join(f'{r[1]} {r[7]} / {r[8]} % ({r[11]} / {r[12]} % read within 1 h)' for r in srows))
    with FILE_LOCK:
        S.write(os.path.join(OUT, 'scanner_log.csv'), ['line'], [[line]], append=True)
        S.append_cols(os.path.join(OUT, 'runs.csv'), ['time_utc', 'start_utc', 'positions_done_utc', 'finished_utc', 'accounts', 'positions_rows', 'mode'],
                      [[stamp, utc(I.ms(stamp) / 1000 - 1800, '%Y-%m-%d %H:%M:%S'), utc(I.ms(stamp) / 1000, '%Y-%m-%d %H:%M:%S'),
                        utc(None, '%Y-%m-%d %H:%M:%S'), reads, len(rows), 'live']])
    log(line)
    with LOCK:
        win['rows'], win['changes'], win['fills'], win['reads'] = {}, [], {}, 0


def run_collector():
    if not os.path.exists(os.path.join(HERE, 'collect.py')): return
    env = dict(os.environ, POS_OUT=os.path.join(OUT, 'collector'), POS_MONTHLY='1', HL_GZIP='1')
    try:
        if True:
            r = subprocess.run([sys.executable, '-I', os.path.join(HERE, 'collect.py'), '--cloud'], capture_output=True, text=True, timeout=600, env=env)
        if r.returncode != 0: log('collector failed: ' + (r.stderr or r.stdout)[-200:])
    except Exception as e: log(f'collector failed: {e}')


# ---------------------------------------------------------------- main loop
def main():
    load_liqs(); load_leaderboard(); relabel()
    market_tick(); refresh_candles()                # prices before the first readings
    for f in (feed_thread, bybit_thread, market_thread, side_thread): threading.Thread(target=f, daemon=True).start()
    log(f'start: {len(st)} accounts known, {len(core)} top accounts, {len(sel)} fill / order accounts, push {"on" if PUSH and remote() else "off"}')
    stop_at = START + DUR
    nb = (time.time() // 1800 + 1) * 1800; prev_stamp = utc(nb - 1800)
    nl = (time.time() // EVERY['live'] + 1) * EVERY['live']
    ns = (time.time() // 60) * 60; ns = next(ns + k * 60 for k in range(1, 62) if utc(ns + k * 60, '%M') in ('24', '54'))
    pool = ThreadPoolExecutor(8)
    while True:
        now = time.time()
        if now >= nb:
            stamp = utc(nb)
            try: close_window(stamp, prev_stamp)
            except Exception as e: log(f'window {stamp} failed: {e}')
            prev_stamp = stamp; nb += 1800
            run_collector()
            try: publish_live(stamp, push=True)
            except Exception as e: log(f'live failed: {e}')
            threading.Thread(target=push_data, args=(stamp,), daemon=True).start()
            if nb > stop_at: break                    # the next window would end after the stop time (GitHub jobs: max 6 h)
            try: load_leaderboard(); relabel()
            except Exception as e: log(f'leaderboard failed: {e}')
            continue
        if now >= nl:
            nl += EVERY['live']
            try: publish_live(utc(now))
            except Exception as e: log(f'live failed: {e}')
        if now >= ns:
            ns += 1800
            try: log('slice: %d traders, %d big orders' % write_slice())
            except Exception as e: log(f'slice failed: {e}')
        batch = pick(36)
        if not batch: time.sleep(1); continue
        for addr, t_req, js in pool.map(read, batch):
            try: apply_read(addr, t_req, js)
            except Exception as e: print('apply failed', addr[:10], str(e)[:120], flush=True)
    write_slice(); save_feed()
    with FILE_LOCK: S.save_state(st, acc); S.save_accounts(acc)
    push_data('final ' + utc())
    log(f'end: feed {feed["trades"]} trades, {len(feed["seen"])} addresses, {feed["reconnects"]} reconnects')


if __name__ == '__main__':
    main()
