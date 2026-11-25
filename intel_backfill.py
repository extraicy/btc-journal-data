"""Rebuild changes/, intelligence.csv and summary_all.csv from the saved positions files (owner's go, 25 Nov 2026).

Replays every plan-mode run (from 22 Nov 2026 22:22 UTC) in order, exactly like the scanner merges its state, and computes the same
numbers with intel.py. Use it once to back-fill the history, or again whenever intel.py changes (it rewrites the files it makes).

    python3 -I intel_backfill.py [--data data] [--out <folder>] [--check]

--out   where to write (default = the data folder itself)
--check compare the replayed coverage with summary_all.csv (the live scanner's own numbers) and stop - writes nothing
Back-filled rows have no order book (it was not recorded then). known_utc = runs.csv finished_utc (first 3 runs: start + 20 min).
"""
import csv, gzip, glob, json, os, sys, time, urllib.request, datetime as dt
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import intel as I

args = sys.argv[1:]
DATA = args[args.index('--data') + 1] if '--data' in args else os.path.join(HERE, 'data')
OUT = args[args.index('--out') + 1] if '--out' in args else DATA
CHECK = '--check' in args
FIRST = '2026-11-22 22:22'          # first plan-mode run (closed-position markers exist from here on)


def rd(path):
    op = gzip.open(path, 'rt', newline='', encoding='utf-8') if path.endswith('.gz') else open(path, newline='', encoding='utf-8')
    with op as fh: return list(csv.reader(fh))


def dicts(path):
    r = rd(path); return [dict(zip(r[0], x)) for x in r[1:] if x] if r else []


def candles_15m(start_ms, end_ms):
    out = {}
    for c in I.COINS:
        body = {'type': 'candleSnapshot', 'req': {'coin': c, 'interval': '15m', 'startTime': start_ms, 'endTime': end_ms}}
        req = urllib.request.Request('https://api.hyperliquid.xyz/info', data=json.dumps(body).encode(),
                                     headers={'Content-Type': 'application/json', 'User-Agent': 'public-data-client'})
        cd = sorted(json.loads(urllib.request.urlopen(req, timeout=60).read()), key=lambda x: x['t'])
        out[c] = ([int(x['t']) for x in cd], [float(x['h']) for x in cd], [float(x['l']) for x in cd])
        time.sleep(0.5)
    return out


def main():
    files = []
    for f in glob.glob(os.path.join(DATA, 'positions', '*', '*.csv*')):
        d = os.path.basename(os.path.dirname(f)); hm = os.path.basename(f)[:4]
        stamp = f'{d} {hm[:2]}:{hm[2:]}'
        if stamp >= FIRST: files.append((stamp, f))
    files.sort()
    old_sum = {(r['time_utc'], r['coin']): r for r in dicts(os.path.join(DATA, 'summary_all.csv'))}
    mids = {(r['time_utc'], r['coin']): r.get('mid_px', '') for r in dicts(os.path.join(DATA, 'summary.csv'))}
    runs = {r['time_utc']: r for r in dicts(os.path.join(DATA, 'runs.csv'))}
    ctx = {c: sorted(((int(r['time_ms']), r) for f in glob.glob(os.path.join(DATA, 'collector', f'hl_ctx_{c}_*.csv')) for r in dicts(f)),
                     key=lambda x: x[0]) for c in I.COINS}
    core_by_day = {}
    for f in sorted(glob.glob(os.path.join(DATA, 'accounts', '*.csv*'))):
        core_by_day[os.path.basename(f)[:10]] = {r[0] for r in rd(f)[1:] if r}
    liq_rows = dicts(os.path.join(DATA, 'liquidations.csv'))
    liq_idx, fills = I.liq_index(liq_rows)
    t_first = I.ms(files[0][0]) - 2 * 86_400_000
    candles = candles_15m(t_first, int(time.time() * 1000)) if not CHECK else {}
    print(f'{len(files)} plan-mode runs {files[0][0]} -> {files[-1][0]}; {len(liq_rows)} liquidation fills; '
          f'{sum(len(v[0]) for v in candles.values())} 15m candles', flush=True)

    def ctx_at(c, stamp):
        t = I.ms(stamp); best = None
        for tm, r in ctx[c]:
            if abs(tm - t) <= 25 * 60_000 and (best is None or abs(tm - t) < abs(best[0] - t)): best = (tm, r)
        return best[1] if best else None

    st, prev_stamp, prev_market, prev_fixed, seen_before = {}, '', {}, set(), set()
    out_sum, out_int, out_chg = [], [], {}
    worst = 0.0
    for stamp, f in files:
        rows = rd(f)[1:]
        read_now = {}
        for r in rows:
            if not r: continue
            read_now.setdefault(r[1], [])
            if I.num(r[3]): read_now[r[1]].append(I.Pos.from_row(r))
        big = {a for a, v in st.items() for p in v if p.value >= I.BIG_USD}
        day = stamp[:10]
        core = core_by_day.get(day) or core_by_day.get(max([d for d in core_by_day if d <= day] or [''], default=''), set()) or set()
        fixed = core | big
        since = {a: prev_stamp for a in read_now if a in prev_fixed}
        market, oi_both_usd = {}, {}
        for c in I.COINS:
            cx = ctx_at(c, stamp) or {}
            mid = I.num(mids.get((stamp, c))) or I.num(cx.get('mark_price'))
            mark = I.num(cx.get('mark_price')) or mid
            market[c] = dict(mid=mid, mark=mark, funding=cx.get('funding', ''), oi_both=I.num(cx.get('open_interest')))
            s = old_sum.get((stamp, c))
            oi_both_usd[c] = I.num(s['total_oi_usd']) if s else market[c]['oi_both'] * mark
            if s and mark: market[c]['oi_both'] = oi_both_usd[c] / mark     # the same end-of-run number the live scanner used
        old_st = dict(st)
        chg = I.find_changes(stamp, old_st, read_now, since, {c: market[c]['mid'] for c in I.COINS}, liq_idx, candles, fixed,
                             known=seen_before | prev_fixed)
        seen_before |= set(read_now)
        for a, ps in read_now.items(): st[a] = ps
        one = {c: oi_both_usd[c] / 2 for c in I.COINS}
        cov = I.coverage(st, stamp, one)
        srows = I.summary_rows(stamp, st, cov, oi_both_usd)
        for sr in srows:
            o = old_sum.get((stamp, sr[1]))
            if o:   # keep the live scanner's own numbers (its state is the truth - the replay misses one lost run + one cache hiccup)
                for k, i in (('long_usd', 4), ('short_usd', 5)):
                    d = abs(I.num(o[k]) - sr[i]) / max(I.num(o[k]), 1); worst = max(worst, d)
                    if CHECK and d > 0.001: print(f'  {stamp} {sr[1]} {k}: live {o[k]} vs replay {sr[i]} ({100 * d:.2f} %)')
                tot = I.num(o['total_oi_usd'])
                sr[2:11] = [int(o['accounts_known']), int(o['positions']), int(I.num(o['long_usd'])), int(I.num(o['short_usd'])), int(tot),
                            round(200 * I.num(o['long_usd']) / tot, 1) if tot else '', round(200 * I.num(o['short_usd']) / tot, 1) if tot else '',
                            I.num(o['oldest_reading_h']), round(tot / 2)]
        out_sum += srows
        run = runs.get(stamp)
        known = run['finished_utc'][:16] if run else (I.ts(stamp) + dt.timedelta(minutes=20)).strftime('%Y-%m-%d %H:%M')
        # market for the next run's "change since" columns: prefer the same numbers as this row
        out_int += I.intel_rows(stamp, known, prev_stamp, st, old_st, cov, chg, market, prev_market, fills, {}, fixed)
        out_chg[stamp] = [x for x in chg if I.keep_change(x)]
        prev_stamp, prev_market, prev_fixed = stamp, market, fixed
    print(f'replay vs live summary_all: largest difference {100 * worst:.3f} % (long / short $)', flush=True)
    if CHECK: return
    os.makedirs(OUT, exist_ok=True)
    def write(path, cols, rows):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', newline='', encoding='utf-8') as fh:
            w = csv.writer(fh); w.writerow(cols); w.writerows(rows)
    # summary_all: keep rows of runs without a positions file as they were (shares corrected to one side)
    have = {(r[0], r[1]) for r in out_sum}
    extra = []
    for (t, c), o in old_sum.items():
        if (t, c) in have: continue
        tot = I.num(o['total_oi_usd'])
        extra.append([t, c, o['accounts_known'], o['positions'], o['long_usd'], o['short_usd'], o['total_oi_usd'],
                      round(200 * I.num(o['long_usd']) / tot, 1) if tot else '', round(200 * I.num(o['short_usd']) / tot, 1) if tot else '',
                      o['oldest_reading_h'], round(tot / 2), '', '', '', ''])
    write(os.path.join(OUT, 'summary_all.csv'), I.SUMMARY_COLS, sorted(out_sum + extra, key=lambda r: (r[0], r[1])))
    write(os.path.join(OUT, 'intelligence.csv'), I.INTEL_COLS, out_int)
    for stamp, rows in out_chg.items():   # one compressed file per run, like positions/
        p = os.path.join(OUT, 'changes', stamp[:10], stamp[11:13] + stamp[14:16] + '.csv.gz')
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with gzip.open(p, 'wt', newline='', encoding='utf-8') as fh:
            w = csv.writer(fh); w.writerow(I.CHANGE_COLS); w.writerows(rows)
    print(f'wrote summary_all.csv ({len(out_sum) + len(extra)} rows), intelligence.csv ({len(out_int)} rows), '
          f'changes/ ({sum(len(v) for v in out_chg.values())} rows in {len(out_chg)} run files) -> {OUT}', flush=True)


if __name__ == '__main__':
    main()
