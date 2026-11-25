"""Monthly archive - keeps this repository small (owner's go, 25 Nov 2026).

From the 2nd of a month, the raw per-run files of every earlier month are packed into one file per folder and uploaded as assets of
the GitHub release 'archive-YYYY-MM' of this repository (public, free). Only after GitHub reports every upload with the right size
are those files removed from the repository - nothing is deleted before its copy is safely in the release.

  archived (raw, per run / per day):  positions, orders, changes, fills, feed, feed_recent, big_trades, bybit_liq, levels, levels_core,
                                      accounts, collector/deribit
  kept in the repo (one file per month or one cumulative file): summary.csv, summary_all.csv, intelligence.csv, runs.csv,
                                      liquidations.csv, scanner_log.csv, collector/<name>_<YYYY-MM>.csv, collector/deribit_summary.csv
  data/archive_log.csv = what was archived where.  Download: https://github.com/<owner>/<repo>/releases/tag/archive-YYYY-MM

Usage:  python3 -I archive.py [--data data] [--dry-run]
Needs the gh command (installed on GitHub's runners) with GH_TOKEN set; the job's own token is enough (contents: write).
"""
import csv, datetime as dt, glob, json, os, re, subprocess, sys, tarfile

args = sys.argv[1:]
DATA = args[args.index('--data') + 1] if '--data' in args else 'data'
DRY = '--dry-run' in args
FOLDERS = ['positions', 'orders', 'changes', 'fills', 'feed', 'feed_recent', 'big_trades', 'bybit_liq', 'levels', 'levels_core',
           'accounts', os.path.join('collector', 'deribit')]
WORK = os.environ.get('RUNNER_TEMP') or os.path.join(os.path.dirname(os.path.abspath(__file__)), 'archive_tmp')


def gh(*a):
    r = subprocess.run(['gh', *a], capture_output=True, text=True, timeout=1800)
    if r.returncode != 0: raise RuntimeError(f'gh {a[0]} {a[1] if len(a) > 1 else ""}: {(r.stderr or r.stdout).strip()[:300]}')
    return r.stdout


def month_of(path, folder):
    """'YYYY-MM' of a file inside a folder: data/<folder>/<YYYY-MM-DD>/... or data/<folder>/<YYYY-MM-DD>.csv(.gz)."""
    rel = os.path.relpath(path, os.path.join(DATA, folder)).replace('\\', '/')
    m = re.match(r'(\d{4}-\d{2})-\d{2}', rel)
    return m.group(1) if m else None


def main():
    today = dt.datetime.now(dt.timezone.utc)
    if os.environ.get('ARCHIVE_TODAY'): today = dt.datetime.fromisoformat(os.environ['ARCHIVE_TODAY']).replace(tzinfo=dt.timezone.utc)   # tests only
    if today.day < 2 and not DRY:
        print('archive: waiting for the 2nd of the month (late listener files of the last day still arrive on the 1st)'); return
    this_month = today.strftime('%Y-%m')
    todo = {}                                                       # (month, folder) -> [files]
    for folder in FOLDERS:
        for f in glob.glob(os.path.join(DATA, folder, '**', '*'), recursive=True):
            if not os.path.isfile(f): continue
            m = month_of(f, folder)
            if m and m < this_month: todo.setdefault((m, folder), []).append(f)
    if not todo:
        print('archive: nothing to archive'); return
    os.makedirs(WORK, exist_ok=True)
    log = []
    for month in sorted({m for m, _ in todo}):
        tag = f'archive-{month}'
        parts = []
        for (m, folder), files in sorted(todo.items()):
            if m != month: continue
            name = f"{folder.replace(os.sep, '-').replace('/', '-')}-{month}.tar.gz"
            path = os.path.join(WORK, name)
            with tarfile.open(path, 'w:gz') as tf:
                for f in sorted(files): tf.add(f, arcname=os.path.relpath(f, DATA).replace('\\', '/'))
            parts.append((folder, files, name, path, os.path.getsize(path)))
        total = sum(p[4] for p in parts)
        print(f'archive {month}: {sum(len(p[1]) for p in parts)} files in {len(parts)} packs, {total / 1e6:.1f} MB', flush=True)
        if DRY:
            for p in parts: print(f'   {p[2]}: {len(p[1])} files, {p[4] / 1e6:.2f} MB')
            continue
        try: gh('release', 'view', tag)
        except RuntimeError:
            gh('release', 'create', tag, '--title', f'Raw data {month}',
               '--notes', f'Raw per-run files of {month} (Hyperliquid scanner + trade feed + Bybit liquidations), one .tar.gz per folder. '
                          'Unpack into the repository root to get data/<folder>/... back. See README.md for the columns.')
        gh('release', 'upload', tag, *[p[3] for p in parts], '--clobber')
        have = {a['name']: a['size'] for a in json.loads(gh('release', 'view', tag, '--json', 'assets'))['assets']}
        bad = [p[2] for p in parts if have.get(p[2]) != p[4]]
        if bad: raise RuntimeError(f'upload check failed for {bad} - nothing removed')
        for folder, files, name, path, size in parts:              # safe in the release -> remove from the repository
            for f in files: os.remove(f)
            for d in sorted({os.path.dirname(f) for f in files}, key=len, reverse=True):
                try: os.rmdir(d)
                except OSError: pass
            log.append([month, folder.replace(os.sep, '/'), len(files), size, f'{tag}/{name}', today.strftime('%Y-%m-%d %H:%M')])
            os.remove(path)
    if log:
        p = os.path.join(DATA, 'archive_log.csv'); new = not os.path.exists(p)
        with open(p, 'a', newline='', encoding='utf-8') as fh:
            w = csv.writer(fh)
            if new: w.writerow(['month', 'folder', 'files', 'bytes', 'release_asset', 'archived_utc'])
            w.writerows(log)
        print(f'archive: {sum(x[2] for x in log)} files moved to the release(s), {sum(x[3] for x in log) / 1e6:.1f} MB', flush=True)


if __name__ == '__main__':
    main()
