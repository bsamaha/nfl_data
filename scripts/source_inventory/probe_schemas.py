"""Read the Parquet footer of every file listed in DATASET_PATTERNS and save column lists and row counts.

Only the tail of each file is fetched: the last 64 KB, then the exact footer when it is larger.
Writes OUT_DIR/schemas.json. Run list_releases.py first.

Usage: python probe_schemas.py OUT_DIR
"""
import concurrent.futures as concurrent_futures
import json
import sys
from pathlib import Path

from nfl_remote import (
    UNMATCHED_ASSET_PATTERNS,
    extract_season_year,
    find_release_assets,
    load_releases_by_tag,
    print_bytes_fetched,
    read_parquet_footer,
)

output_dir = Path(sys.argv[1])
releases_by_tag = load_releases_by_tag(output_dir)

# dataset label -> (release tag, file-name regex)
DATASET_PATTERNS = {
    "pbp": ("pbp", r"play_by_play_\d{4}\.parquet"),
    "pbp_participation": ("pbp_participation", r"pbp_participation_\d{4}\.parquet"),
    "stats_player_week": ("stats_player", r"stats_player_week_\d{4}\.parquet"),
    "stats_player_reg": ("stats_player", r"stats_player_reg_\d{4}\.parquet"),
    "stats_player_post": ("stats_player", r"stats_player_post_\d{4}\.parquet"),
    "stats_team_week": ("stats_team", r"stats_team_week_\d{4}\.parquet"),
    # frozen; the last file is read for the rename map in SOURCE_INVENTORY
    "legacy_player_stats": ("player_stats", r"player_stats_2024\.parquet"),
    "schedules": ("schedules", r"games\.parquet"),
    "players": ("players", r"players\.parquet"),
    # roster files before 1990 are not read
    "rosters": ("rosters", r"roster_(199\d|20\d\d)\.parquet"),
    "weekly_rosters": ("weekly_rosters", r"roster_weekly_\d{4}\.parquet"),
    "snap_counts": ("snap_counts", r"snap_counts_\d{4}\.parquet"),
    "injuries": ("injuries", r"injuries_\d{4}\.parquet"),
    "depth_charts": ("depth_charts", r"depth_charts_\d{4}\.parquet"),
    "nextgen_stats": ("nextgen_stats", r"ngs_(passing|rushing|receiving)\.parquet"),
    "ftn_charting": ("ftn_charting", r"ftn_charting_\d{4}\.parquet"),
    "pfr_advstats_week": ("pfr_advstats", r"advstats_week_(pass|rush|rec|def)_\d{4}\.parquet"),
    "pfr_advstats_season": ("pfr_advstats", r"advstats_season_(pass|rush|rec|def)\.parquet"),
    "draft_picks": ("draft_picks", r"draft_picks\.parquet"),
    "combine": ("combine", r"combine\.parquet"),
    "contracts": ("contracts", r"historical_contracts\.parquet"),
    "officials": ("officials", r"officials\.parquet"),
    "trades": ("trades", r"trades\.parquet"),
    "teams": ("teams", r"teams_colors_logos\.parquet"),
    "espn_qbr": ("espn_data", r"qbr_(season|week)_level\.parquet"),
}

probe_jobs = [
    (dataset_label, asset)
    for dataset_label, (release_tag, filename_regex) in DATASET_PATTERNS.items()
    for asset in find_release_assets(releases_by_tag, release_tag, filename_regex)
]
print("files to probe:", len(probe_jobs))


def probe_asset_schema(job):
    dataset_label, asset = job
    try:
        parquet_metadata = read_parquet_footer(asset["url"], asset["size"])
        arrow_schema = parquet_metadata.schema.to_arrow_schema()
        kv_metadata = {
            meta_key.decode(): (meta_value.decode(errors="replace")[:200])
            for meta_key, meta_value in (parquet_metadata.metadata or {}).items()
            if meta_key not in (b"ARROW:schema", b"r")
        }
        return dataset_label, {
            "file": asset["name"],
            "season": extract_season_year(asset["name"]),
            "updated_at": asset["updated_at"],
            "rows": parquet_metadata.num_rows,
            "columns": [[field.name, str(field.type)] for field in arrow_schema],
            "kv": kv_metadata,
        }
    except Exception as error:  # noqa: BLE001 - one unreadable file is reported and the rest are still read
        return dataset_label, {
            "file": asset["name"],
            "season": extract_season_year(asset["name"]),
            "error": f"{type(error).__name__}: {error}"[:200],
        }


schemas_by_dataset: dict[str, list[dict]] = {}
with concurrent_futures.ThreadPoolExecutor(8) as executor:
    for dataset_label, schema_record in executor.map(probe_asset_schema, probe_jobs):
        schemas_by_dataset.setdefault(dataset_label, []).append(schema_record)
(output_dir / "schemas.json").write_text(json.dumps(schemas_by_dataset, indent=1))
failed_probes = [
    (dataset_label, record["file"], record["error"])
    for dataset_label, records in schemas_by_dataset.items()
    for record in records
    if "error" in record
]
print("ok:", sum(len(records) for records in schemas_by_dataset.values()) - len(failed_probes), "errors:", len(failed_probes))
print_bytes_fetched()
for failed_probe in failed_probes:
    print("  ", failed_probe)
if failed_probes or UNMATCHED_ASSET_PATTERNS:
    sys.exit(1)
