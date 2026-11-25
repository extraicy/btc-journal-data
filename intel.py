"""Position intelligence for the Hyperliquid scanner (owner's go, 25 Nov 2026). Public data only.

Shared by hl_scanner.py (every run, plan mode) and intel_backfill.py (rebuilds the same files from the saved positions files),
so live rows and back-filled rows come from the same code:
  changes/<date>/<HHMM>.csv.gz   every BTC / ETH position change of >= $10k (+ every liquidation) seen between two readings of an
                       account (one row per account + coin; the first reading of a new account is not a change)
  intelligence.csv     one row per coin per run - who is long / short, concentration, entry zones, leverage, liquidation distance,
                       order book depth, flows since the previous run, open interest + price change, funding, data quality
  summary_all.csv      coverage of the whole market (one side of open interest), incl. "fresh" coverage
Every column is explained in README.md. $ = position value in USD. A + flow = more long / less short.
Hyperliquid's openInterest counts longs + shorts (both sides - tested 25 Nov 2026), so one side = openInterest / 2.
"""
import bisect, datetime as dt

COINS = ('BTC', 'ETH')
BIG_USD = 1_000_000                    # whale = a position worth >= $1M (not market-maker-like, not a top-100 trader)
MID_USD = 100_000                      # mid-size = $100k - $1M, small = below $100k
ZONE = {'BTC': 500.0, 'ETH': 25.0}     # entry-price bins for the "underwater zone" columns
LIQ_BIN = {'BTC': 100.0, 'ETH': 5.0}   # liquidation-price bins (the same as levels/)
CANDLE_MS = 15 * 60_000                # 15-minute candles for the "probable liquidation" check
CHANGE_MIN_USD = 10_000                # changes/ keeps changes of >= $10k + every liquidation (99 % of the $ moved); intelligence counts all

CHANGE_COLS = ['time_utc', 'since_utc', 'address', 'coin', 'event', 'liquidation', 'size_before', 'size_after', 'size_change',
               'usd_change', 'mid_px', 'entry_before', 'entry_after', 'liq_before', 'liq_after', 'value_after', 'whale', 'top_trader',
               'mm_like', 'account_value', 'every_run', 'source']

INTEL_COLS = [
    'time_utc', 'known_utc', 'coin', 'mid_px', 'mark_px', 'funding_1h', 'oi_one_side_coins', 'oi_one_side_usd', 'oi_change_coins', 'px_change_pct',
    'accounts_known', 'positions_known', 'coverage_long_pct', 'coverage_short_pct',
    'fresh1h_long_pct', 'fresh1h_short_pct', 'fresh6h_long_pct', 'fresh6h_short_pct',
    'mm_long_usd', 'mm_short_usd', 'whale_long_usd', 'whale_short_usd', 'top_long_usd', 'top_short_usd',
    'mid_long_usd', 'mid_short_usd', 'small_long_usd', 'small_short_usd', 'unseen_long_usd', 'unseen_short_usd',
    'fixed_long_usd', 'fixed_short_usd', 'rest_long_usd', 'rest_short_usd',
    'top10_share_pct', 'top20_share_pct', 'top20_net_usd', 'top20_flow_usd',
    'long_avg_entry', 'short_avg_entry', 'long_underwater_usd', 'short_underwater_usd',
    'long_uw_zone_px', 'long_uw_zone_usd', 'short_uw_zone_px', 'short_uw_zone_usd',
    'lev10_long_usd', 'lev10_short_usd', 'lev25_long_usd', 'lev25_short_usd',
    'liq2_long_usd', 'liq2_short_usd', 'liq5_long_usd', 'liq5_short_usd', 'liq10_long_usd', 'liq10_short_usd',
    'liq_cluster_below_px', 'liq_cluster_below_usd', 'liq_cluster_above_px', 'liq_cluster_above_usd',
    'book_bid1_usd', 'book_ask1_usd', 'book_bid2_usd', 'book_ask2_usd',
    'mm_flow_usd', 'whale_flow_usd', 'top_flow_usd', 'other_flow_usd', 'rest_flow_usd', 'late_flow_usd',
    'opened_usd', 'closed_usd', 'liq_fill_long_usd', 'liq_fill_short_usd', 'liq_probable_long_usd', 'liq_probable_short_usd',
    'changes_precise', 'changes_late']

SUMMARY_COLS = ['time_utc', 'coin', 'accounts_known', 'positions', 'long_usd', 'short_usd', 'total_oi_usd', 'long_share_pct', 'short_share_pct',
                'oldest_reading_h', 'oi_one_side_usd', 'fresh1h_long_pct', 'fresh1h_short_pct', 'fresh6h_long_pct', 'fresh6h_short_pct']


def num(x, d=0.0):
    if x is None or x == '': return d
    try: return float(x)
    except (TypeError, ValueError): return d


def flag(x):
    return str(x).strip() in ('1', 'True', 'true')


def ts(s):
    return dt.datetime.strptime(s[:16], '%Y-%m-%d %H:%M')


def ms(s):
    return int(ts(s).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)


def hours(a, b):
    """b - a in hours (two 'YYYY-MM-DD HH:MM' stamps)."""
    return (ts(b) - ts(a)).total_seconds() / 3600


class Pos:
    """One BTC / ETH position as last read. value = Hyperliquid's positionValue (USD, always >= 0); the side is the sign of size."""
    __slots__ = ('coin', 'size', 'entry', 'value', 'liq', 'seen', 'mm', 'av', 'lev', 'top')

    def __init__(self, coin, size, entry, value, liq, seen, mm, av, lev, top):
        self.coin, self.size, self.entry, self.value, self.liq = coin, size, entry, value, liq
        self.seen, self.mm, self.av, self.lev, self.top = seen, mm, av, lev, top

    @classmethod
    def from_state(cls, r):
        """state/latest.csv row without the address: coin, size, entry_px, value_usd, liq_px, seen_utc, mm_like, account_value, leverage, top100_pnl."""
        g = lambda i: r[i] if len(r) > i else ''
        return cls(r[0], num(r[1]), num(g(2)), num(g(3)), num(g(4), None), g(5), flag(g(6)), num(g(7)), num(g(8)), flag(g(9)))

    @classmethod
    def from_row(cls, r):
        """positions-file row: time_utc, address, coin, size, entry_px, liquidation_px, position_value_usd, unrealized_pnl, margin, leverage,
        mm_like, top100_pnl, account_value (the scanner's own rows have the same layout)."""
        g = lambda i: r[i] if len(r) > i else ''
        return cls(r[2], num(r[3]), num(g(4)), num(g(6)), num(g(5), None), str(r[0])[:16], flag(g(10)), num(g(12)), num(g(9)), flag(g(11)))

    def state_row(self):
        f = lambda x: '' if x is None else x
        return [self.coin, self.size, self.entry or '', self.value, f(self.liq), self.seen, int(self.mm), round(self.av), self.lev or '', int(self.top)]


def group(p):
    """The five groups that split every position (each position is in exactly one): market-maker-like, top-100 trader (by all-time
    profit, as of that reading), whale (other positions >= $1M), mid ($100k - $1M), small (< $100k)."""
    if p.mm: return 'mm'
    if p.top: return 'top'
    if p.value >= BIG_USD: return 'whale'
    return 'mid' if p.value >= MID_USD else 'small'


def classify(s0, s1):
    if s0 == 0: return 'OPENED'
    if s1 == 0: return 'CLOSED'
    if (s0 > 0) != (s1 > 0): return 'FLIPPED'
    return 'INCREASED' if abs(s1) > abs(s0) else 'REDUCED'


def liq_index(liq_rows):
    """liquidations.csv rows (dicts) -> {(liquidated address, coin): sorted [time_ms]} and a per-coin list of (time_ms, long_liq, usd)."""
    idx, fills = {}, {c: [] for c in COINS}
    for r in liq_rows:
        c = r.get('coin')
        if c not in COINS: continue
        t = int(num(r.get('time_ms')))
        u = (r.get('liquidated_user') or '').lower()
        if u: idx.setdefault((u, c), []).append(t)
        fills[c].append((t, flag(r.get('long_liquidated')), num(r.get('fill_px')) * num(r.get('fill_size'))))
    for v in idx.values(): v.sort()
    for v in fills.values(): v.sort()
    return idx, fills


def price_range(candles, coin, t0, t1):
    """(lowest low, highest high) of the 15-minute candles overlapping [t0, t1] (ms) or None."""
    cd = candles.get(coin)
    if not cd: return None
    times, highs, lows = cd
    i = bisect.bisect_left(times, t0 - CANDLE_MS + 1); j = bisect.bisect_right(times, t1)
    if i >= j: return None
    return min(lows[i:j]), max(highs[i:j])


def find_changes(stamp, old_st, read_now, since, mids, liq_idx, candles, every_run=frozenset(), known=None):
    """old_st: address -> [Pos] before this run. read_now: address -> [Pos] for every account read now (an empty list = no BTC / ETH
    position now). since: address -> time of that account's previous reading ('' = unknown). known: every address read before this run
    (None = all) - the FIRST reading of an account is not a change (it is a new find). Returns change rows (lists, CHANGE_COLS)."""
    out = []
    t1 = ms(stamp)
    for addr, new_ps in read_now.items():
        if known is not None and addr not in known and addr not in old_st: continue
        old = {p.coin: p for p in old_st.get(addr, [])}
        new = {p.coin: p for p in new_ps}
        for c in COINS:
            o, n = old.get(c), new.get(c)
            s0 = o.size if o else 0.0; s1 = n.size if n else 0.0
            if round(s0, 8) == round(s1, 8): continue
            mid = num(mids.get(c))
            if not mid: continue
            ev = classify(s0, s1)
            ref = n or o
            sn = since.get(addr) or (o.seen if o else '')
            liq = ''
            if ev in ('CLOSED', 'REDUCED', 'FLIPPED'):
                t0 = ms(sn) if sn else t1 - 86_400_000
                hits = liq_idx.get((addr.lower(), c), [])
                k = bisect.bisect_right(hits, t0)
                if k < len(hits) and hits[k] <= t1 + 1_800_000:     # a liquidation fill of this account between the readings (+ the reading time)
                    liq = 'confirmed'
                elif o and o.liq and sn:
                    pr = price_range(candles, c, ms(sn), t1)
                    if pr and ((s0 > 0 and pr[0] <= o.liq) or (s0 < 0 and pr[1] >= o.liq)): liq = 'probable'
            whale = max(o.value if o else 0.0, n.value if n else 0.0) >= BIG_USD
            out.append([stamp, sn, addr, c, ev, liq, s0, s1, round(s1 - s0, 8), round((s1 - s0) * mid), mid,
                        o.entry if o else '', n.entry if n else '', (o.liq if o and o.liq else ''), (n.liq if n and n.liq else ''),
                        round(n.value) if n else 0, int(whale), int(ref.top), int(ref.mm), round(ref.av), int(addr in every_run), 'observed'])
    return out


def _side_sum(ps, cond=lambda p: True):
    L = sum(p.value for p in ps if p.size > 0 and cond(p)); S = sum(p.value for p in ps if p.size < 0 and cond(p))
    return L, S


def coverage(st, stamp, oi_one_usd):
    """-> {coin: dict(positions, long, short, cover_l, cover_s, f1l, f1s, f6l, f6s, oldest_h)} - shares are % of ONE side of open interest."""
    out = {}
    for c in COINS:
        ps = [p for v in st.values() for p in v if p.coin == c]
        L, S = _side_sum(ps)
        f1 = _side_sum(ps, lambda p: p.seen and hours(p.seen, stamp) <= 1.0)
        f6 = _side_sum(ps, lambda p: p.seen and hours(p.seen, stamp) <= 6.0)
        one = oi_one_usd.get(c) or 0
        pct = lambda x: round(100 * x / one, 1) if one else ''
        age = max([hours(p.seen, stamp) for p in ps if p.seen] or [0])
        out[c] = dict(ps=ps, positions=len(ps), long=L, short=S, cover_l=pct(L), cover_s=pct(S), f1l=pct(f1[0]), f1s=pct(f1[1]),
                      f6l=pct(f6[0]), f6s=pct(f6[1]), oldest_h=round(age, 1))
    return out


def summary_rows(stamp, st, cov, oi_both_usd):
    return [[stamp, c, len(st), cov[c]['positions'], round(cov[c]['long']), round(cov[c]['short']), round(oi_both_usd.get(c, 0)),
             cov[c]['cover_l'], cov[c]['cover_s'], cov[c]['oldest_h'], round(oi_both_usd.get(c, 0) / 2),
             cov[c]['f1l'], cov[c]['f1s'], cov[c]['f6l'], cov[c]['f6s']] for c in COINS]


def _argmax_bin(items, step):
    bins = {}
    for px, v in items:
        b = round(px / step) * step; bins[b] = bins.get(b, 0.0) + v
    if not bins: return '', 0
    b = max(bins, key=bins.get)
    return b, round(bins[b])


def intel_rows(stamp, known, prev_stamp, st, old_st, cov, chg, market, prev_market, fills, book, fixed):
    """One intelligence row per coin.
    st / old_st: the state after / before this run (address -> [Pos]); cov: coverage(); chg: this run's change rows (lists, CHANGE_COLS);
    market: coin -> dict(mid, mark, funding, oi_both) (oi_both = Hyperliquid openInterest in coins = longs + shorts);
    prev_market: the same from the previous run ({} = none); fills: per-coin [(time_ms, long_liq, usd)] liquidation fills (all known);
    book: coin -> dict(bid1, ask1, bid2, ask2) ({} = not read); fixed: the addresses read EVERY run (top accounts + big positions)."""
    rows = []
    t0 = ms(prev_stamp) if prev_stamp else None; t1 = ms(stamp)
    for c in COINS:
        m = market.get(c) or {}; pm = prev_market.get(c) or {}
        mid = num(m.get('mid')) or num(m.get('mark')); mark = num(m.get('mark'))
        oi_c = num(m.get('oi_both')) / 2; oi_u = oi_c * (mark or mid)
        ps = cov[c]['ps']
        r = dict(time_utc=stamp, known_utc=known, coin=c, mid_px=mid, mark_px=mark, funding_1h=m.get('funding', ''),
                 oi_one_side_coins=round(oi_c, 4), oi_one_side_usd=round(oi_u),
                 oi_change_coins=round(oi_c - num(pm.get('oi_both')) / 2, 4) if pm.get('oi_both') else '',
                 px_change_pct=round(100 * (mid / num(pm.get('mid')) - 1), 3) if num(pm.get('mid')) and mid else '',
                 accounts_known=len(st), positions_known=len(ps),
                 coverage_long_pct=cov[c]['cover_l'], coverage_short_pct=cov[c]['cover_s'],
                 fresh1h_long_pct=cov[c]['f1l'], fresh1h_short_pct=cov[c]['f1s'], fresh6h_long_pct=cov[c]['f6l'], fresh6h_short_pct=cov[c]['f6s'])
        g = {k: [0.0, 0.0] for k in ('mm', 'whale', 'top', 'mid', 'small')}
        for p in ps: g[group(p)][0 if p.size > 0 else 1] += p.value
        for k, (L, S) in g.items(): r[k + '_long_usd'] = round(L); r[k + '_short_usd'] = round(S)
        r['unseen_long_usd'] = round(oi_u - cov[c]['long']); r['unseen_short_usd'] = round(oi_u - cov[c]['short'])
        fx = [p for a, v in st.items() if a in fixed for p in v if p.coin == c and p.seen == stamp]
        FL, FS = _side_sum(fx)
        r.update(fixed_long_usd=round(FL), fixed_short_usd=round(FS), rest_long_usd=round(oi_u - FL), rest_short_usd=round(oi_u - FS))
        big = sorted(ps, key=lambda p: -p.value)
        both = 2 * oi_u
        r['top10_share_pct'] = round(100 * sum(p.value for p in big[:10]) / both, 1) if both else ''
        r['top20_share_pct'] = round(100 * sum(p.value for p in big[:20]) / both, 1) if both else ''
        r['top20_net_usd'] = round(sum(p.value if p.size > 0 else -p.value for p in big[:20]))
        prev20 = {a for a, p in sorted(((a, p) for a, v in old_st.items() for p in v if p.coin == c), key=lambda x: -x[1].value)[:20]}
        longs = [p for p in ps if p.size > 0 and p.entry]; shorts = [p for p in ps if p.size < 0 and p.entry]
        wavg = lambda xs: round(sum(p.entry * p.value for p in xs) / sum(p.value for p in xs), 2) if sum(p.value for p in xs) else ''
        r['long_avg_entry'] = wavg(longs); r['short_avg_entry'] = wavg(shorts)
        uw_l = [p for p in longs if mid and p.entry > mid]; uw_s = [p for p in shorts if mid and p.entry < mid]
        r['long_underwater_usd'] = round(sum(p.value for p in uw_l)); r['short_underwater_usd'] = round(sum(p.value for p in uw_s))
        r['long_uw_zone_px'], r['long_uw_zone_usd'] = _argmax_bin([(p.entry, p.value) for p in uw_l if p.entry <= mid * 1.2], ZONE[c])
        r['short_uw_zone_px'], r['short_uw_zone_usd'] = _argmax_bin([(p.entry, p.value) for p in uw_s if p.entry >= mid * 0.8], ZONE[c])
        for x in (10, 25):
            L, S = _side_sum(ps, lambda p: p.lev >= x); r[f'lev{x}_long_usd'] = round(L); r[f'lev{x}_short_usd'] = round(S)
        dist = lambda p: ((mid - p.liq) if p.size > 0 else (p.liq - mid)) / mid if (mid and p.liq) else None
        for x in (2, 5, 10):
            L, S = _side_sum(ps, lambda p: dist(p) is not None and 0 <= dist(p) <= x / 100)
            r[f'liq{x}_long_usd'] = round(L); r[f'liq{x}_short_usd'] = round(S)
        r['liq_cluster_below_px'], r['liq_cluster_below_usd'] = _argmax_bin(
            [(p.liq, p.value) for p in ps if p.size > 0 and dist(p) is not None and 0 <= dist(p) <= 0.10], LIQ_BIN[c])
        r['liq_cluster_above_px'], r['liq_cluster_above_usd'] = _argmax_bin(
            [(p.liq, p.value) for p in ps if p.size < 0 and dist(p) is not None and 0 <= dist(p) <= 0.10], LIQ_BIN[c])
        b = book.get(c) or {}
        for k in ('bid1', 'ask1', 'bid2', 'ask2'): r[f'book_{k}_usd'] = round(b[k]) if b.get(k) is not None else ''
        # flows: changes of accounts read in BOTH runs ("precise" - exactly the last half hour); everyone else = minus their sum (zero-sum)
        fl = dict(mm=0.0, whale=0.0, top=0.0, other=0.0); late = opened = closed = prob_l = prob_s = top20 = 0.0; n_pre = n_late = 0
        for x in chg:
            if x[3] != c: continue
            usd = num(x[9]); s0 = num(x[6]); s1 = num(x[7]); px = num(x[10])
            if not (prev_stamp and x[1] == prev_stamp):
                late += usd; n_late += 1; continue
            n_pre += 1
            k = 'mm' if flag(x[18]) else 'top' if flag(x[17]) else 'whale' if flag(x[16]) else 'other'
            fl[k] += usd
            if x[2] in prev20: top20 += usd
            if s0 == 0 or (s1 != 0 and (s0 > 0) == (s1 > 0) and abs(s1) > abs(s0)): opened += abs(s1 - s0) * px
            elif s1 == 0 or (s0 > 0) == (s1 > 0): closed += abs(s1 - s0) * px
            else: closed += abs(s0) * px; opened += abs(s1) * px        # flipped
            if x[5] == 'probable':
                cut = (abs(s0) if (s1 == 0 or (s0 > 0) != (s1 > 0)) else abs(s0) - abs(s1)) * px
                if s0 > 0: prob_l += cut
                else: prob_s += cut
        for k, v in fl.items(): r[k + '_flow_usd'] = round(v) if prev_stamp else ''
        r['rest_flow_usd'] = round(-sum(fl.values())) if prev_stamp else ''
        r['late_flow_usd'] = round(late); r['opened_usd'] = round(opened) if prev_stamp else ''; r['closed_usd'] = round(closed) if prev_stamp else ''
        r['top20_flow_usd'] = round(top20) if prev_stamp else ''
        lf = [x for x in fills.get(c, []) if t0 is not None and t0 < x[0] <= t1]
        r['liq_fill_long_usd'] = round(sum(x[2] for x in lf if x[1])) if t0 is not None else ''
        r['liq_fill_short_usd'] = round(sum(x[2] for x in lf if not x[1])) if t0 is not None else ''
        r['liq_probable_long_usd'] = round(prob_l) if prev_stamp else ''; r['liq_probable_short_usd'] = round(prob_s) if prev_stamp else ''
        r['changes_precise'] = n_pre; r['changes_late'] = n_late
        rows.append([r.get(k, '') for k in INTEL_COLS])
    return rows


def book_depth(levels, mid, pcts=(1, 2)):
    """levels: Hyperliquid l2Book 'levels' ([bids, asks], each [{px, sz, n}]) from one or more books (finest first).
    -> {'bid1': $, 'ask1': $, 'bid2': $, 'ask2': $}: resting $ within x % of the price, from the finest book that reaches that far."""
    out = {}
    for x in pcts:
        lo, hi = mid * (1 - x / 100), mid * (1 + x / 100)
        for side, key, inside, reach in ((0, f'bid{x}', lambda p: p >= lo, lambda lv: float(lv[-1]['px']) <= lo),
                                         (1, f'ask{x}', lambda p: p <= hi, lambda lv: float(lv[-1]['px']) >= hi)):
            val = None
            for bk in levels:
                lv = bk[side] if bk and len(bk) > side else []
                if not lv: continue
                v = sum(float(l['px']) * float(l['sz']) for l in lv if inside(float(l['px'])))
                val = v
                if reach(lv): break                      # this book covers the whole range - use it
            out[key] = val
    return out


def keep_change(x):
    """Should a change row go into changes/ (all rows still count in intelligence.csv)?"""
    return abs(num(x[9])) >= CHANGE_MIN_USD or bool(x[5])
