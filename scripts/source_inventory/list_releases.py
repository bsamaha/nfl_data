"""List every release (dataset) and asset in nflverse/nflverse-data via the public GitHub API.

Writes OUT_DIR/releases.json, which the other scripts read, and prints a per-release summary.
Uses one unauthenticated API call (the limit is 60 an hour).

Usage: python list_releases.py OUT_DIR
"""
import json, re, sys, urllib.request, collections
from pathlib import Path

out = Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
rels, page = [], 1
while True:
    url = f"https://api.github.com/repos/nflverse/nflverse-data/releases?per_page=100&page={page}"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json", "User-Agent": "source-inventory"})
    with urllib.request.urlopen(req, timeout=60) as r:
        batch = json.load(r)
        remaining = r.headers.get("X-RateLimit-Remaining")
    if not batch:
        break
    rels += batch
    page += 1
    if len(batch) < 100:
        break
print("releases:", len(rels), "| api calls left this hour:", remaining)

slim = []
for r in rels:
    slim.append({"tag": r["tag_name"], "name": r.get("name"), "published_at": r.get("published_at"),
                 "body": (r.get("body") or "")[:400],
                 "assets": [{"name": a["name"], "size": a["size"], "updated_at": a["updated_at"],
                             "url": a["browser_download_url"]} for a in r.get("assets", [])]})
(out / "releases.json").write_text(json.dumps(slim, indent=1))

yr = re.compile(r"(?<!\d)(19\d\d|20\d\d)(?!\d)")
for r in sorted(slim, key=lambda x: x["tag"]):
    a = r["assets"]
    ext = collections.Counter(x["name"].rsplit(".", 1)[-1] for x in a)
    pq = [x for x in a if x["name"].endswith(".parquet")]
    years = sorted({int(m.group(1)) for x in pq for m in [yr.search(x["name"])] if m})
    latest = max((x["updated_at"] for x in a), default="-")
    mb = sum(x["size"] for x in pq) / 1e6
    stems = collections.Counter(yr.sub("{Y}", x["name"]) for x in pq)
    print(f"\n[{r['tag']}] assets={len(a)} ext={dict(ext)} parquet_MB={mb:.1f} latest_update={latest}")
    print(f"   seasons: {years[0]}-{years[-1]} ({len(years)})" if years else "   seasons: (no year in names)")
    print("   parquet patterns:", dict(list(stems.most_common(8))))
