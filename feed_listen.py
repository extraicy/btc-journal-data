"""Hyperliquid trade-feed listener. Public websocket, no key, standard library only.

Listens to every BTC / ETH trade for DURATION seconds and records each address that traded (both sides of every trade):
  data/feed/<date>/<HHMM>.csv.gz   address, trades, usd_traded, first_seen_utc, last_seen_utc   (HHMM = start time, UTC)
The position scanner (hl_scanner.py, plan mode) reads these files to find accounts that are not on the public leaderboard.

Usage:  python3 -I feed_listen.py <seconds> [--out data]
"""
import socket, ssl, os, sys, json, base64, struct, time, csv, gzip, datetime as dt

HOST = 'api.hyperliquid.xyz'; COINS = ('BTC', 'ETH')
args = sys.argv[1:]
DUR = float(args[0]) if args and not args[0].startswith('--') else 3600
OUT = args[args.index('--out') + 1] if '--out' in args else 'data'
START = dt.datetime.now(dt.timezone.utc)
PATH = os.path.join(OUT, 'feed', START.strftime('%Y-%m-%d'), START.strftime('%H%M') + '.csv.gz')


def connect():
    s = ssl.create_default_context().wrap_socket(socket.create_connection((HOST, 443), timeout=30), server_hostname=HOST)
    key = base64.b64encode(os.urandom(16)).decode()
    s.sendall((f'GET /ws HTTP/1.1\r\nHost: {HOST}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
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


def save():
    os.makedirs(os.path.dirname(PATH), exist_ok=True)
    tmp = PATH + '.tmp'
    with gzip.open(tmp, 'wt', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(['address', 'trades', 'usd_traded', 'first_seen_utc', 'last_seen_utc'])
        fmt = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M')
        for a, (n, u, f0, f1) in sorted(seen.items(), key=lambda x: -x[1][1]): w.writerow([a, n, round(u), fmt(f0), fmt(f1)])
    os.replace(tmp, PATH)


end = time.time() + DUR; last_save = time.time()
while time.time() < end:
    try:
        s = connect(); s.settimeout(20)
        for c in COINS: send(s, {'method': 'subscribe', 'subscription': {'type': 'trades', 'coin': c}})
        last_ping = time.time(); buf = b''
        while time.time() < end:
            if time.time() - last_ping > 45: send(s, {'method': 'ping'}); last_ping = time.time()
            if time.time() - last_save > 1800: save(); last_save = time.time()
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
                for u in t.get('users', []):
                    e = seen.setdefault(u.lower(), [0, 0.0, ms, ms]); e[0] += 1; e[1] += usd; e[3] = ms
    except Exception as e:
        reconnects += 1; print('reconnect:', str(e)[:120], flush=True); time.sleep(3)
save()
print(f'feed: {trades} trades, {len(seen)} addresses, {reconnects} reconnects -> {PATH}', flush=True)
