"""Shared helpers for the positioning data (funding, open interest, long/short ratios, basis, Hyperliquid).
Public, free, no-key endpoints only.

Local mode stores CSV in ../data-csv/positioning/.
Every file: first column time_ms (UTC, milliseconds), second column time_utc (readable), then the values.
Rows are merged by time_ms (re-running never duplicates) and kept sorted.
"""
import csv, json, os, time, urllib.request, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.environ.get('POS_OUT') or os.path.normpath(os.path.join(HERE, '..', 'data-csv', 'positioning'))
os.makedirs(OUT, exist_ok=True)
# POS_MONTHLY=1 writes one file per month (<name>_<YYYY-MM>.csv) so each run only rewrites
# the current month's small file (keeps the git history small). (monthly files can be merged later).
MONTHLY = os.environ.get('POS_MONTHLY') == '1'
UA = {'User-Agent': 'public-data-client', 'Content-Type': 'application/json'}


def iso(ms):
    return dt.datetime.fromtimestamp(int(ms) / 1000, dt.timezone.utc).strftime('%Y-%m-%d %H:%M')


def now_ms():
    return int(time.time() * 1000)


def get(url, body=None, tries=4):
    """GET (or POST json when body is given). Polite retries; returns parsed JSON or raises."""
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, headers=UA)
            return json.loads(urllib.request.urlopen(req, timeout=30).read())
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 + 3 * i)
    raise RuntimeError(f'{url} failed: {last}')


def path(name):
    return os.path.join(OUT, name + '.csv')


def read(name):
    """-> (header, {time_ms: row_values_list})"""
    p = path(name)
    if not os.path.exists(p):
        return None, {}
    with open(p, newline='', encoding='utf-8') as f:
        r = csv.reader(f)
        header = next(r, None)
        return header, {int(row[0]): row[2:] for row in r if row}


def merge(name, columns, rows):
    """rows: iterable of (time_ms, [values...]). Merges into the CSV; newer values win. Returns (added, total)."""
    if MONTHLY:
        by = {}
        for t, vals in rows:
            by.setdefault(iso(t)[:7], []).append((t, vals))
        added = total = 0
        for month, part in by.items():
            a, n = _merge_one(f'{name}_{month}', columns, part); added += a; total += n
        return added, total
    return _merge_one(name, columns, rows)


def _merge_one(name, columns, rows):
    header, have = read(name)
    before = len(have)
    for t, vals in rows:
        have[int(t)] = [str(v) for v in vals]
    with open(path(name), 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['time_ms', 'time_utc'] + list(columns))
        for t in sorted(have):
            w.writerow([t, iso(t)] + have[t])
    return len(have) - before, len(have)


def last_time(name):
    _, have = read(name)
    return max(have) if have else None
