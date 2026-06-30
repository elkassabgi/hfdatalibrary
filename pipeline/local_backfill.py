"""
local_backfill.py — Run the pipeline locally for missing dates.
Processes dates sequentially, writes directly to R2.

Usage:
    python local_backfill.py
"""
import os
import sys
import time
import subprocess

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# R2 credentials — read from the ENVIRONMENT, never hardcoded.
# (The keys that used to live here were exposed in git history and have been
#  rotated; set the new pair as env vars / .env before running.)
_missing = [k for k in ('R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_ENDPOINT')
            if not os.environ.get(k)]
if _missing:
    raise SystemExit("Missing R2 env vars: " + ", ".join(_missing) +
                     ". Set R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT first.")
ENV = {
    **os.environ,
    'R2_BUCKET': os.environ.get('R2_BUCKET', 'hfdatalibrary-data'),
}

MISSING_DATES = [
    '2026-03-30',
    '2026-03-31',
    '2026-04-01',
    '2026-04-02',
    '2026-04-06',
    '2026-04-07',
    '2026-04-08',
    '2026-04-09',
]

DAILY_UPDATE = os.path.join(SCRIPT_DIR, 'daily_update.py')


def main():
    total = len(MISSING_DATES)
    succeeded = 0
    failed = 0
    failed_dates = []

    print(f'{"="*50}')
    print(f'LOCAL BACKFILL: {total} dates to process')
    print(f'{"="*50}')
    print(flush=True)

    for i, d in enumerate(MISSING_DATES):
        n = i + 1
        print(f'\n{"="*50}')
        print(f'[{n}/{total}] Processing {d}')
        print(f'{"="*50}', flush=True)

        start = time.time()
        result = subprocess.run(
            [sys.executable, DAILY_UPDATE, d],
            env=ENV,
            cwd=SCRIPT_DIR,
        )
        elapsed = (time.time() - start) / 60

        if result.returncode == 0:
            print(f'[{n}/{total}] {d} — SUCCESS ({elapsed:.1f} min)')
            succeeded += 1
        else:
            print(f'[{n}/{total}] {d} — FAILED ({elapsed:.1f} min, exit code {result.returncode})')
            failed += 1
            failed_dates.append(d)

        print(flush=True)

    print(f'\n{"="*50}')
    print(f'BACKFILL COMPLETE')
    print(f'  Succeeded: {succeeded}/{total}')
    print(f'  Failed:    {failed}/{total}')
    if failed_dates:
        print(f'  Failed dates: {", ".join(failed_dates)}')
    print(f'{"="*50}')


if __name__ == '__main__':
    main()
