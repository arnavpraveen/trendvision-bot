"""Load KEY=value lines from the .env next to this file into os.environ (stdlib only)."""
import os
from pathlib import Path

_f = Path(__file__).with_name(".env")
if _f.exists():
    for line in _f.read_text().splitlines():
        k, sep, v = line.partition("=")
        if sep and not k.lstrip().startswith("#"):
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))
