"""Summarize schemas.json: seasons, rows, column counts and the seasons where the column set or types change.

Local only; reads OUT_DIR/schemas.json written by probe_schemas.py.

Usage: python analyze_schemas.py OUT_DIR [label,label,...]
"""
import json, sys, re
from pathlib import Path

res = json.loads((Path(sys.argv[1]) / "schemas.json").read_text())
only = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else None
if only and only - set(res):
    sys.exit(f"unknown label: {', '.join(sorted(only - set(res)))}. Labels in schemas.json: {', '.join(res)}")


def short(xs, n=14):
    xs = list(xs)
    return ", ".join(xs[:n]) + (f" …(+{len(xs) - n})" if len(xs) > n else "")


for label, recs in res.items():
    if only and label not in only:
        continue
    # group variants (e.g. pfr pass/rush/rec/def, ngs passing/rushing/receiving) by file stem without the year
    groups = {}
    for r in recs:
        stem = re.sub(r"_?(19|20)\d\d", "", r["file"]).replace(".parquet", "")
        groups.setdefault(stem, []).append(r)
    for stem, rs in groups.items():
        # probe_schemas.py writes a record with "error" in place of rows and columns when a footer read fails
        failed = sorted(r["file"] for r in rs if "error" in r)
        failed_seasons = {r["season"] for r in rs if "error" in r}
        rs = [r for r in rs if "error" not in r]
        if not rs:
            print(f"\n## {label} :: {stem}  no file was read; failed: {', '.join(failed)}")
            continue
        rs.sort(key=lambda r: (r["season"] or 0))
        seasons = [r["season"] for r in rs if r["season"]]
        rows = [r["rows"] for r in rs]
        print(f"\n## {label} :: {stem}  files={len(rs)}"
              + (f" seasons={seasons[0]}-{seasons[-1]}" if seasons else "")
              + f" rows(min/max/last)={min(rows)}/{max(rows)}/{rows[-1]} cols(first/last)={len(rs[0]['columns'])}/{len(rs[-1]['columns'])}"
              + f" last_update={max(r['updated_at'] for r in rs)[:16]}")
        if failed:
            print("   FAILED to read, left out of this summary:", ", ".join(failed))
        if seasons:
            missing = sorted(set(range(seasons[0], seasons[-1] + 1)) - set(seasons) - failed_seasons)
            if missing:
                print("   MISSING seasons:", missing)
        prev = None
        for r in rs:
            cols = dict(r["columns"])
            if prev is not None:
                add = [c for c in cols if c not in prev]
                rem = [c for c in prev if c not in cols]
                typ = [f"{c}:{prev[c]}->{cols[c]}" for c in cols if c in prev and prev[c] != cols[c]]
                if add or rem or typ:
                    print(f"   {r['season'] or r['file']}: rows={r['rows']}"
                          + (f" | +{len(add)}: {short(add)}" if add else "")
                          + (f" | -{len(rem)}: {short(rem)}" if rem else "")
                          + (f" | type {len(typ)}: {short(typ, 8)}" if typ else ""))
            prev = cols
        last = rs[-1]
        print("   latest columns:", ", ".join(f"{n}:{t}" for n, t in last["columns"]))
        if last.get("kv"):
            print("   file metadata keys:", list(last["kv"].items())[:4])
