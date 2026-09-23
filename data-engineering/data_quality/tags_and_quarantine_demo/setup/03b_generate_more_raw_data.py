"""Second batch of synthetic data. Reuses 03_generate_raw_data.py with a different
seed + filename suffix so Auto Loader sees the files as new.

Run locally after Phase 3:
    uv run python setup/03b_generate_more_raw_data.py
"""

from __future__ import annotations

import sys

# Delegate to the main generator with batch2 settings.
sys.argv = [
    sys.argv[0],
    "--suffix", "_batch2",
    "--n-policies", "600",
    "--n-claims", "4000",
    "--n-files", "1",
]

from importlib import import_module  # noqa: E402

mod = import_module("03_generate_raw_data")
sys.exit(mod.main())
