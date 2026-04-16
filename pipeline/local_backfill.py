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

# R2 credentials
ENV = {
    **os.environ,
    'R2_ACCESS_KEY_ID': '9be00ae01f4b11dfa9a546cf5663bced',
    'R2_SECRET_ACCESS_KEY': '8e94bb2b310467dee03f710c90f629087c5a2d5f3551278f01d83fe8c245a4f6',
    'R2_ENDPOINT': 'https://ce51d5c7fe3859098751b89bbebeab7a.r2.cloudflarestorage.com',
    'R2_BUCKET': 'hfdatalibrary-data',
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
