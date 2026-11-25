"""Hyperliquid trade-feed listener + Bybit liquidation recorder. Public websockets, no keys, standard library only.

Listens to every BTC / ETH trade for DURATION seconds and records each address that traded (both sides of every trade):
  data/feed/<date>/<HHMM>.csv.gz   address, trades, usd_traded, first_seen_utc, last_seen_utc   (HHMM = start time, UTC)
The position scanner (hl_scanner.py, plan mode) reads these files to find accounts that are not on the public leaderboard.
At the same time it records EVERY Bybit BTC / ETH liquidation (public "allLiquidation" stream, batches every 0.5 s):
  data/bybit_liq/<date>/<HHMM>.csv.gz   time_ms, time_utc, symbol, long_liquidated, size, bankruptcy_px, value_usd
  (Bybit side "Buy" = a LONG position was liquidated.) If Bybit refuses the connection, the reason is printed and the run carries on.

Every 30 minutes (at :24 and :54 UTC, just before the scanner's :30 / :00 runs) it also writes two small files (added 25 Nov 2026):
  data/feed_recent/<date>/<HHMM>.csv.gz  address, trades, usd_traded, last_trade_utc - everyone who traded >= $10k of BTC / ETH in
                                         those 30 minutes (99 % of the volume). The scanner re-reads these accounts on its next run.
  data/big_trades/<date>/<HHMM>.csv.gz   every taker order of >= $100k (all fills of one order added up): time, coin, side (B = the
                                         taker bought), taker address, $, size, average price, fills, makers
  (HHMM = when the slice was written.) With FEED_PUSH=1 (the cloud job) each slice is committed + pushed right away.

Usage:  python3 -I feed_listen.py <seconds> [--out data]
"""
import socket, ssl, os, sys, json, base64, struct, time, csv, gzip, threading, subprocess, datetime as dt

HOST = 'api.hyperliquid.xyz'; COINS = ('BTC', 'ETH')
args = sys.argv[1:]
DUR = float(args[0]) if args and not args[0].startswith('--') else 3600
OUT = args[args.index('--out') + 1] if '--out' in args else 'data'
START = dt.datetime.now(dt.timezone.utc)
PATH = os.path.join(OUT, 'feed', START.strftime('%Y-%m-%d'), START.strftime('%H%M') + '.csv.gz')
BPATH = os.path.join(OUT, 'bybit_liq', START.strftime('%Y-%m-%d'), START.strftime('%H%M') + '.csv.gz')


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


seen = {}   # address -> [trades, usd, first_ms, last_ms]
trades = reconnects = 0
liqs = []; bybit_note = []
SLICE_MIN_USD = 10_000      # feed_recent: traded at least this much in the slice
BIG_ORDER_USD = 100_000     # big_trades: taker orders of at least this much
recent = {}                 # this slice: address -> [trades, usd, last_ms]
orders = {}                 # this slice: tx hash -> [coin, side, taker, usd, size, first_ms, last_ms, fills, makers (set)]
pub_lock = threading.Lock(); pub_log = []


def next_slice_time(now):
    """The next :24 or :54 UTC (epoch seconds)."""
    t = dt.datetime.fromtimestamp(now, dt.timezone.utc).replace(second=0, microsecond=0)
    for k in range(1, 62):
        x = t + dt.timedelta(minutes=k)
        if x.minute in (24, 54): return x.timestamp()
    return now + 1800


def git(*a):
    return subprocess.run(['git', *a], capture_output=True, text=True, timeout=180)


def push(paths, label):
    """Commit + push this slice's files (cloud job only). Runs in its own thread, so the feed keeps being read."""
    if os.environ.get('FEED_PUSH') != '1': return
    with pub_lock:
        try:
            git('add', *paths)
            if git('diff', '--cached', '--quiet').returncode == 0: return
            git('commit', '-q', '-m', f'feed slice {label}')
            for i in range(5):
                if git('pull', '--rebase', '-q').returncode != 0: git('rebase', '--abort')
                if git('push', '-q').returncode == 0: pub_log.append(f'{label} pushed'); return
                time.sleep(10 * (i + 1))
            pub_log.append(f'{label} push failed (kept for the final save)')
        except Exception as e:  # noqa: BLE001
            pub_log.append(f'{label} push failed: {e}')


def write_slice():
    """feed_recent + big_trades files for the slice that just ended -> (their paths, a label)."""
    global recent, orders
    rec, ords = recent, orders; recent, orders = {}, {}
    now = dt.datetime.now(dt.timezone.utc); d = now.strftime('%Y-%m-%d'); hm = now.strftime('%H%M')
    fmt = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M')
    fmts = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    rows = [[a, n, round(u), fmt(l)] for a, (n, u, l) in sorted(rec.items(), key=lambda x: -x[1][1]) if u >= SLICE_MIN_USD]
    big = [[o[5], fmts(o[5]), o[0], o[1], o[2], round(o[3]), round(o[4], 6), round(o[3] / o[4], 2) if o[4] else '', o[7], len(o[8])]
           for o in sorted(ords.values(), key=lambda x: x[5]) if o[3] >= BIG_ORDER_USD]
    paths = []
    for sub, head, data in (('feed_recent', ['address', 'trades', 'usd_traded', 'last_trade_utc'], rows),
                            ('big_trades', ['time_ms', 'time_utc', 'coin', 'side', 'taker', 'usd', 'size', 'avg_px', 'fills', 'makers'], big)):
        if not data: continue
        p = os.path.join(OUT, sub, d, hm + '.csv.gz'); os.makedirs(os.path.dirname(p), exist_ok=True)
        with gzip.open(p + '.tmp', 'wt', newline='', encoding='utf-8') as f:
            w = csv.writer(f); w.writerow(head); w.writerows(data)
        os.replace(p + '.tmp', p); paths.append(p)
    return paths, f'{d} {hm} ({len(rows)} traders, {len(big)} big orders)'


def save():
    os.makedirs(os.path.dirname(PATH), exist_ok=True)
    tmp = PATH + '.tmp'
    with gzip.open(tmp, 'wt', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(['address', 'trades', 'usd_traded', 'first_seen_utc', 'last_seen_utc'])
        fmt = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M')
        for a, (n, u, f0, f1) in sorted(seen.items(), key=lambda x: -x[1][1]): w.writerow([a, n, round(u), fmt(f0), fmt(f1)])
    os.replace(tmp, PATH)


def save_bybit():
    if not liqs: return
    os.makedirs(os.path.dirname(BPATH), exist_ok=True)
    with gzip.open(BPATH + '.tmp', 'wt', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(['time_ms', 'time_utc', 'symbol', 'long_liquidated', 'size', 'bankruptcy_px', 'value_usd']); w.writerows(list(liqs))
    os.replace(BPATH + '.tmp', BPATH)


def bybit():
    """Every Bybit BTC / ETH liquidation until the end of the run (reconnects; gives up only if the connection is refused 3 times)."""
    refused = 0
    while time.time() < end:
        try:
            s = connect('stream.bybit.com', '/v5/public/linear'); s.settimeout(25)
            send(s, {'op': 'subscribe', 'args': ['allLiquidation.BTCUSDT', 'allLiquidation.ETHUSDT']})
            if not bybit_note: bybit_note.append('connected')
            last_ping = time.time(); buf = b''
            while time.time() < end:
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
                    liqs.append([t, dt.datetime.fromtimestamp(t / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), x['s'],
                                 int(x['S'] == 'Buy'), v, p, round(v * p, 2)])
        except Exception as e:
            msg = str(e)[:160]
            if any(k in msg for k in (' 403 ', ' 451 ', 'Forbidden')):
                refused += 1
                if refused >= 3: bybit_note.append('REFUSED - ' + msg); return
            print('bybit reconnect:', msg, flush=True); time.sleep(5)


end = time.time() + DUR; last_save = time.time(); next_pub = next_slice_time(time.time())
threading.Thread(target=bybit, daemon=True).start()
while time.time() < end:
    try:
        s = connect(); s.settimeout(20)
        for c in COINS: send(s, {'method': 'subscribe', 'subscription': {'type': 'trades', 'coin': c}})
        last_ping = time.time(); buf = b''
        while time.time() < end:
            if time.time() - last_ping > 45: send(s, {'method': 'ping'}); last_ping = time.time()
            if time.time() - last_save > 1800: save(); save_bybit(); last_save = time.time()
            if time.time() >= next_pub:
                ps, label = write_slice(); next_pub = next_slice_time(time.time() + 60)
                if ps: threading.Thread(target=push, args=(ps, label), daemon=True).start()
            try: op, d = frame(s)
            except socket.timeout: continue
            if op == 8: raise ConnectionError('server close')
            if op == 9: send(s, None, op=10); continue
            if op == 0: buf += d; continue
            if op != 1: continue
            if buf: d = buf + d; buf = b''
            m = json.loads(d)
            if m.get('channel') != 'trades': continue
            for t in m['data']:
                trades += 1; usd = float(t['px']) * float(t['sz']); ms = int(t['time'])
                us = [u.lower() for u in t.get('users', [])]
                for u in us:
                    e = seen.setdefault(u, [0, 0.0, ms, ms]); e[0] += 1; e[1] += usd; e[3] = ms
                    r = recent.setdefault(u, [0, 0.0, ms]); r[0] += 1; r[1] += usd; r[2] = max(r[2], ms)
                if len(us) == 2:   # users = [buyer, seller]; side B = the taker bought, so the taker is the buyer
                    taker, maker = (us[0], us[1]) if t.get('side') == 'B' else (us[1], us[0])
                    h = t.get('hash') or ''   # one taker order = one transaction hash; system trades (liquidations...) have an all-zero hash
                    key = (h, t['coin'], taker) if h.replace('0x', '').strip('0') else ('tid', t.get('tid'))
                    o = orders.setdefault(key, [t['coin'], t.get('side'), taker, 0.0, 0.0, ms, ms, 0, set()])
                    o[3] += usd; o[4] += float(t['sz']); o[6] = max(o[6], ms); o[7] += 1; o[8].add(maker)
    except Exception as e:
        reconnects += 1; print('reconnect:', str(e)[:120], flush=True); time.sleep(3)
save(); save_bybit()
write_slice()          # the last part-slice: saved by the job's final save step
with pub_lock: pass    # let a push that is still running finish
print(f'feed: {trades} trades, {len(seen)} addresses, {reconnects} reconnects -> {PATH}', flush=True)
print('slices: ' + ('; '.join(pub_log[-14:]) or 'none pushed (local run)'), flush=True)
print(f'bybit liquidations: {len(liqs)} ({"; ".join(bybit_note) or "never connected"}) -> {BPATH if liqs else "nothing saved"}', flush=True)
