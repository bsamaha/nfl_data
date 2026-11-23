"""Test the key of each dataset listed in KEY_CHECKS in every season, match game ids against the schedule where the
dataset has them, and print the seasons where relocation team codes, postseason week numbers and the participation
personnel format change.

Reads the key columns and a few value columns of each season file remotely, and the schedule whole; nothing is saved.
Run list_releases.py first.

Usage: python probe_keys_all_seasons.py OUT_DIR
"""
import concurrent.futures as concurrent_futures
import sys

import polars as pl
from nfl_remote import (
    UNMATCHED_ASSET_PATTERNS,
    count_key_violations,
    extract_season_year,
    find_release_assets,
    load_releases_by_tag,
    print_bytes_fetched,
    read_remote_columns,
)

releases_by_tag = load_releases_by_tag(sys.argv[1])
schedules_frame = read_remote_columns(
    next(asset for asset in releases_by_tag["schedules"]["assets"] if asset["name"] == "games.parquet")
)
schedule_game_ids_by_season = {
    season: set(season_games["game_id"])
    for (season,), season_games in schedules_frame.group_by("season")
}
RELOCATED_TEAM_CODES = {"OAK", "SD", "STL", "LV", "LAC", "LA"}

# dataset_label: (release tag, file regex, key columns, extra columns to read, game-id column or None)
KEY_CHECKS = {
    "pbp": ("pbp", r"play_by_play_\d{4}\.parquet", ["game_id", "play_id"], ["home_team"], "game_id"),
    "stats_player_week": ("stats_player", r"stats_player_week_\d{4}\.parquet", ["player_id", "game_id"], ["week", "season_type"], "game_id"),
    "stats_team_week": ("stats_team", r"stats_team_week_\d{4}\.parquet", ["team", "game_id"], [], "game_id"),
    "snap_counts": ("snap_counts", r"snap_counts_\d{4}\.parquet", ["game_id", "pfr_player_id"], [], "game_id"),
    "injuries": ("injuries", r"injuries_\d{4}\.parquet", ["season", "game_type", "week", "team", "gsis_id"], [], None),
    "weekly_rosters": ("weekly_rosters", r"roster_weekly_\d{4}\.parquet", ["season", "week", "game_type", "team", "gsis_id"], [], None),
    # roster files before 1990 are not read
    "rosters": ("rosters", r"roster_(199\d|20\d\d)\.parquet", ["season", "team", "gsis_id"], [], None),
    # the week-based depth chart layout ended with the 2024 season
    "depth_charts_week_layout": (
        "depth_charts",
        r"depth_charts_(200\d|201\d|202[0-4])\.parquet",
        ["season", "club_code", "week", "game_type", "formation", "depth_position", "depth_team", "gsis_id"],
        [],
        None,
    ),
    "depth_charts_snapshot_layout": (
        "depth_charts",
        r"depth_charts_(202[5-9]|20[3-9]\d)\.parquet",
        ["dt", "team", "pos_id", "pos_rank"],
        [],
        None,
    ),
    "pbp_participation": ("pbp_participation", r"pbp_participation_\d{4}\.parquet", ["nflverse_game_id", "play_id"], ["offense_personnel"], "nflverse_game_id"),
    "ftn_charting": ("ftn_charting", r"ftn_charting_\d{4}\.parquet", ["nflverse_game_id", "nflverse_play_id"], [], "nflverse_game_id"),
    "pfr_week_rec": ("pfr_advstats", r"advstats_week_rec_\d{4}\.parquet", ["game_id", "pfr_player_id"], [], "game_id"),
}


def check_one_asset(job):
    dataset_label, asset, key_columns, extra_columns, game_id_column = job
    season = extract_season_year(asset["name"])
    try:
        frame = read_remote_columns(asset, key_columns + extra_columns)
        null_key_rows, duplicate_key_rows = count_key_violations(frame, key_columns)
        string_id_columns = [col for col in key_columns if col.endswith("_id") and frame[col].dtype == pl.String]
        season_report = {
            "season": season,
            "rows": frame.height,
            "null_rows": null_key_rows,
            "dup": duplicate_key_rows,
            "nulls": {col: frame[col].null_count() for col in key_columns if frame[col].null_count()},
            "empty": {col: int((frame[col] == "").sum()) for col in string_id_columns if (frame[col] == "").sum()},
        }
        if game_id_column:
            dataset_game_ids = set(frame[game_id_column].drop_nulls().unique())
            season_report["not_in_sched"] = len(dataset_game_ids - schedule_game_ids_by_season.get(season, set()))
            season_report["sched_missing"] = len(schedule_game_ids_by_season.get(season, set()) - dataset_game_ids)
        if dataset_label == "pbp":
            season_report["home_codes"] = sorted(set(frame["home_team"].unique()) & RELOCATED_TEAM_CODES)
        if dataset_label == "stats_player_week":
            season_report["post_weeks"] = (
                frame.filter(pl.col("season_type") == "POST")
                .select(pl.col("week").min().alias("min_week"), pl.col("week").max().alias("max_week"))
                .row(0)
            )
        if dataset_label == "pbp_participation":
            season_report["personnel"] = (
                frame["offense_personnel"].drop_nulls().mode().to_list()[:1],
                round(frame["offense_personnel"].is_not_null().mean(), 3),
            )
        return dataset_label, season_report
    except Exception as error:  # noqa: BLE001 - one unreadable file is reported and the rest are still checked
        return dataset_label, {"season": season, "error": f"{type(error).__name__}: {error}"[:160]}


def find_value_changes(season_value_pairs):
    """Keep only the seasons where the value differs from the season before."""
    transitions, previous_value = {}, object()
    for season, value in season_value_pairs:
        if value != previous_value:
            transitions[season] = value
            previous_value = value
    return transitions


check_jobs = [
    (dataset_label, asset, key_columns, extra_columns, game_id_column)
    for dataset_label, (release_tag, filename_regex, key_columns, extra_columns, game_id_column) in KEY_CHECKS.items()
    for asset in find_release_assets(releases_by_tag, release_tag, filename_regex)
]
reports_by_dataset: dict[str, list[dict]] = {}
with concurrent_futures.ThreadPoolExecutor(6) as executor:
    for dataset_label, season_report in executor.map(check_one_asset, check_jobs):
        reports_by_dataset.setdefault(dataset_label, []).append(season_report)

for dataset_label, season_reports in reports_by_dataset.items():
    season_reports.sort(key=lambda report: report["season"])
    valid_reports = [report for report in season_reports if "error" not in report]
    print(
        f"\n## {dataset_label}: files={len(season_reports)} "
        f"seasons {season_reports[0]['season']}-{season_reports[-1]['season']} key={KEY_CHECKS[dataset_label][2]}"
    )
    print("   null-key rows by season:", {report["season"]: report["null_rows"] for report in valid_reports if report["null_rows"]} or "none")
    print("   dup rows by season:", {report["season"]: report["dup"] for report in valid_reports if report["dup"]} or "none")
    print("   null key cells by season:", {report["season"]: report["nulls"] for report in valid_reports if report["nulls"]} or "none")
    print("   empty-string ids by season:", {report["season"]: report["empty"] for report in valid_reports if report["empty"]} or "none")
    print("   empty files:", [report["season"] for report in valid_reports if report["rows"] == 0] or "none")
    if KEY_CHECKS[dataset_label][4]:
        print(
            "   game ids not in schedules:",
            {report["season"]: report["not_in_sched"] for report in valid_reports if report["not_in_sched"]} or "none",
            "| schedule games absent here:",
            {report["season"]: report["sched_missing"] for report in valid_reports if report["sched_missing"]} or "none",
        )
    if dataset_label == "pbp":
        print(
            "   home_team relocation codes, by season where they change:",
            find_value_changes((report["season"], report["home_codes"]) for report in valid_reports),
        )
    if dataset_label == "stats_player_week":
        print(
            "   POST week range, by season where it changes:",
            find_value_changes(
                (report["season"], report["post_weeks"])
                for report in valid_reports
                if report["post_weeks"] != (None, None)
            ),
        )
    if dataset_label == "pbp_participation":
        print(
            "   most common personnel value and non-null share, by season where they change:",
            find_value_changes((report["season"], report["personnel"]) for report in valid_reports),
        )
    for report in season_reports:
        if "error" in report:
            print("   ERROR", report)

print("\n## schedules")
postseason_weeks = (
    schedules_frame.filter(pl.col("game_type") != "REG")
    .group_by("season")
    .agg(pl.col("week").min().alias("min_week"), pl.col("week").max().alias("max_week"))
    .sort("season")
)
print(
    "   POST week range, by season where it changes:",
    find_value_changes((season, (min_week, max_week)) for season, min_week, max_week in postseason_weeks.rows()),
)
home_teams_by_season = schedules_frame.group_by("season").agg(pl.col("home_team").unique().alias("teams")).sort("season")
print(
    "   relocation codes, by season where they change:",
    find_value_changes(
        (season, sorted(set(teams) & RELOCATED_TEAM_CODES)) for season, teams in home_teams_by_season.rows()
    ),
)
print_bytes_fetched()
if UNMATCHED_ASSET_PATTERNS or any("error" in report for reports in reports_by_dataset.values() for report in reports):
    sys.exit(1)
