#!/bin/sh
# This script ensures that we can always build a wheel and that
# the wheel includes the SQL queries.
#
# See https://github.com/m-lab/iqb/issues/199.

set -eu

OUTDIR=$(mktemp -d)
trap 'rm -rf "$OUTDIR"' EXIT

# Emit the wheel into an output directory
uv build library/ --out-dir "$OUTDIR"

# Make sure the wheel can be imported and contains SQL files
uv run --no-project --with "$OUTDIR"/*.whl --python 3.13 python3 -c "
from importlib.resources import files
import iqb.queries
sql = [f.name for f in files(iqb.queries).iterdir() if f.name.endswith('.sql')]
assert len(sql) > 0, 'No .sql files found in installed wheel'
for name in sql:
    text = files(iqb.queries).joinpath(name).read_text(encoding='utf-8')
    assert len(text) > 0, f'{name} is empty'
print(f'OK: {len(sql)} SQL query files verified in wheel')
"
