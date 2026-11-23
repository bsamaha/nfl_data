"""List every release (dataset) and asset in nflverse/nflverse-data via the public GitHub API.

Writes OUT_DIR/releases.json, which the other scripts read, and prints a per-release summary.
Uses one unauthenticated API call.

Usage: python list_releases.py OUT_DIR
"""

import collections
import json
import sys
import urllib.request
from pathlib import Path

from nfl_remote import SEASON_YEAR_PATTERN, USER_AGENT_HEADERS, extract_season_year

output_dir = Path(sys.argv[1])
output_dir.mkdir(parents=True, exist_ok=True)
releases, page = [], 1
while True:
    page_url = f"https://api.github.com/repos/nflverse/nflverse-data/releases?per_page=100&page={page}"
    request = urllib.request.Request(page_url, headers={**USER_AGENT_HEADERS, "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        release_page = json.load(response)
        rate_limit_remaining = response.headers.get("X-RateLimit-Remaining")
    releases += release_page
    page += 1
    if len(release_page) < 100:
        break
print("releases:", len(releases), "| api calls left this hour:", rate_limit_remaining)

slim_releases = []
for release in releases:
    slim_releases.append(
        {
            "tag": release["tag_name"],
            "assets": [
                {
                    "name": asset["name"],
                    "size": asset["size"],
                    "updated_at": asset["updated_at"],
                    "url": asset["browser_download_url"],
                }
                for asset in release.get("assets", [])
            ],
        }
    )
(output_dir / "releases.json").write_text(json.dumps(slim_releases, indent=1))

for release in sorted(slim_releases, key=lambda item: item["tag"]):
    release_assets = release["assets"]
    extension_counts = collections.Counter(asset["name"].rsplit(".", 1)[-1] for asset in release_assets)
    parquet_assets = [asset for asset in release_assets if asset["name"].endswith(".parquet")]
    seasons = sorted({extract_season_year(asset["name"]) for asset in parquet_assets} - {None})
    latest_update = max((asset["updated_at"] for asset in release_assets), default="-")
    parquet_megabytes = sum(asset["size"] for asset in parquet_assets) / 1e6
    filename_patterns = collections.Counter(SEASON_YEAR_PATTERN.sub("{Y}", asset["name"]) for asset in parquet_assets)
    print(
        f"\n[{release['tag']}] assets={len(release_assets)} ext={dict(extension_counts)} parquet_MB={parquet_megabytes:.1f} latest_update={latest_update}"
    )
    print(f"   seasons: {seasons[0]}-{seasons[-1]} ({len(seasons)})" if seasons else "   seasons: (no year in names)")
    print("   parquet patterns:", dict(filename_patterns.most_common(8)))
