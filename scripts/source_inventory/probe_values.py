"""Value, ID-coverage and team-code checks on selected seasons of each nflverse dataset.

Reads each sampled file remotely, whole or by column, and prints what it finds; nothing is saved.
The current season is taken from the newest stats_player_week file, so the script follows the calendar.
Run list_releases.py first.

Usage: python probe_values.py OUT_DIR
"""

import json
import re
import sys
import traceback
import urllib.request

import polars as pl
from nfl_remote import (
    USER_AGENT_HEADERS,
    count_key_violations,
    extract_season_year,
    load_releases_by_tag,
    print_bytes_fetched,
    read_remote_columns,
)

releases_by_tag = load_releases_by_tag(sys.argv[1])


def latest_season_for_pattern(release_tag, filename_regex):
    seasons = [
        extract_season_year(asset["name"])
        for asset in releases_by_tag.get(release_tag, {}).get("assets", [])
        if re.fullmatch(filename_regex, asset["name"])
    ]
    if not seasons:
        sys.exit(f"releases.json has no file matching {filename_regex} in release {release_tag}")
    return max(seasons)


CURRENT_SEASON = latest_season_for_pattern(
    "stats_player", r"stats_player_week_\d{4}\.parquet"
)  # in progress or just finished
PREVIOUS_SEASON = CURRENT_SEASON - 1  # last complete season
LATEST_PARTICIPATION_SEASON = latest_season_for_pattern("pbp_participation", r"pbp_participation_\d{4}\.parquet")
PRE_RELOCATION_SEASONS = (2015, 2019)  # seasons before Rams, Chargers and Raiders moves settled, for team-code checks
LAST_OLD_LAYOUT_SEASON = (
    2024  # final season before 2025 layout changes: week-based depth charts, injuries.date_modified
)
MODERN_GSIS_ID_PATTERN = r"^00-00\d{5}$"
CACHED_FRAMES: dict[str, pl.DataFrame] = {}  # frames reused by later cross-dataset checks
TEAM_CODES_BY_DATASET_SEASON: dict[tuple[str, int], set[str]] = {}
print(
    f"current season={CURRENT_SEASON} previous={PREVIOUS_SEASON} "
    f"latest participation season={LATEST_PARTICIPATION_SEASON}"
)


def fetch_dataset_frame(release_tag, filename, columns=None):
    asset = next(
        (item for item in releases_by_tag.get(release_tag, {}).get("assets", []) if item["name"] == filename),
        None,
    )
    if asset is None:
        raise LookupError(f"releases.json has no file {filename} in release {release_tag}")
    return read_remote_columns(asset, columns)


def print_key_summary(frame, key_columns):
    null_key_rows, duplicate_key_rows = count_key_violations(frame, key_columns)
    null_counts_by_col = {col: frame[col].null_count() for col in key_columns if frame[col].null_count()}
    print(
        f"   key {key_columns}: rows={frame.height} null_key_rows={null_key_rows} "
        f"dup_rows={duplicate_key_rows} null_keys={null_counts_by_col or 0}"
    )


def format_value_counts(frame, column_name, limit=12):
    if column_name not in frame.columns:
        return f"{column_name}: (absent)"
    counts_frame = frame[column_name].value_counts(sort=True).head(limit)
    return f"{column_name}: " + ", ".join(f"{row[0]}={row[1]}" for row in counts_frame.iter_rows())


def print_section(title):
    print(f"\n=== {title} ===")


def check_schedules():
    print_section("schedules (games.parquet)")
    frame = fetch_dataset_frame("schedules", "games.parquet")
    print_key_summary(frame, ["game_id"])
    print(
        "   seasons:", frame["season"].min(), "-", frame["season"].max(), "|", format_value_counts(frame, "game_type")
    )
    print(
        "   " + format_value_counts(frame, "weekday"),
        "|",
        format_value_counts(frame, "roof"),
        "|",
        format_value_counts(frame, "location"),
    )
    print(
        "   null gameday:",
        frame["gameday"].null_count(),
        "null gametime:",
        frame["gametime"].null_count(),
        "by season:",
        dict(frame.filter(pl.col("gametime").is_null()).group_by("season").len().sort("season").rows()),
        "sample:",
        frame.select("game_id", "gameday", "gametime").tail(1).rows(),
    )
    coverage_by_season = (
        frame.group_by("season")
        .agg(
            pl.len().alias("games"),
            pl.col("spread_line").is_not_null().sum().alias("spread"),
            pl.col("total_line").is_not_null().sum().alias("total"),
            pl.col("temp").is_not_null().sum().alias("temp"),
            pl.col("result").is_null().sum().alias("unplayed"),
        )
        .sort("season")
    )
    print(
        "   per season (games, spread, total, temp, unplayed) first 2 and last 3:",
        coverage_by_season.head(2).rows(),
        "...",
        coverage_by_season.tail(3).rows(),
    )
    unplayed_games = frame.filter((pl.col("season") == CURRENT_SEASON) & pl.col("result").is_null())
    print(
        f"   {CURRENT_SEASON} unplayed games={unplayed_games.height}, "
        f"with spread_line={unplayed_games['spread_line'].is_not_null().sum()}, "
        f"weeks {unplayed_games['week'].min()}-{unplayed_games['week'].max()}"
    )
    for season in (*PRE_RELOCATION_SEASONS, PREVIOUS_SEASON):
        TEAM_CODES_BY_DATASET_SEASON[("schedules", season)] = set(
            frame.filter(pl.col("season") == season)["home_team"].unique()
        )
    print("   all team codes:", sorted(set(frame["home_team"].unique()) | set(frame["away_team"].unique())))
    print(
        "   id columns non-null share:",
        {
            col: round(frame[col].is_not_null().mean(), 3)
            for col in ("old_game_id", "gsis", "nfl_detail_id", "pfr", "pff", "espn", "ftn", "stadium_id")
        },
    )


def check_stats_player():
    print_section("stats_player_week")
    selected_columns = [
        "player_id",
        "player_display_name",
        "season",
        "week",
        "season_type",
        "game_id",
        "team",
        "position",
        "position_group",
    ]
    sampled_seasons = sorted({1999, *PRE_RELOCATION_SEASONS, PREVIOUS_SEASON - 1, PREVIOUS_SEASON, CURRENT_SEASON})
    for season in sampled_seasons:  # 1999 is the first stats_player_week season
        frame = fetch_dataset_frame("stats_player", f"stats_player_week_{season}.parquet", selected_columns)
        TEAM_CODES_BY_DATASET_SEASON[("stats_player_week", season)] = set(frame["team"].drop_nulls().unique())
        print(
            f" {season}: rows={frame.height} players={frame['player_id'].n_unique()} "
            f"gsis-pattern={frame['player_id'].str.contains(MODERN_GSIS_ID_PATTERN).mean():.4f}"
        )
        print_key_summary(frame, ["player_id", "season", "week"])
        if season in (PREVIOUS_SEASON, CURRENT_SEASON):
            print(
                "   " + format_value_counts(frame, "season_type"),
                "| weeks:",
                frame.group_by("season_type")
                .agg(pl.col("week").min().alias("min_week"), pl.col("week").max().alias("max_week"))
                .rows(),
            )
            print("   " + format_value_counts(frame, "position_group", 10))
            multi_team_players = (
                frame.group_by("player_id")
                .agg(pl.col("team").n_unique().alias("team_count"))
                .filter(pl.col("team_count") > 1)
                .height
            )
            print(
                f"   players with >1 team in season: {multi_team_players}; "
                f"null team={frame['team'].null_count()} null game_id={frame['game_id'].null_count()}"
            )
            print(
                "   null-player rows sample:",
                frame.filter(pl.col("player_id").is_null())
                .select("player_display_name", "position", "team", "week")
                .head(3)
                .rows(),
            )
        if season == PREVIOUS_SEASON:
            CACHED_FRAMES["stats_player_week"] = frame


def check_stats_team():
    print_section(f"stats_team_week {PREVIOUS_SEASON}")
    frame = fetch_dataset_frame(
        "stats_team", f"stats_team_week_{PREVIOUS_SEASON}.parquet", ["season", "week", "team", "season_type"]
    )
    print_key_summary(frame, ["season", "week", "team"])
    print("   " + format_value_counts(frame, "season_type"))


def check_pbp():
    print_section("pbp")
    selected_columns = [
        "game_id",
        "play_id",
        "week",
        "season_type",
        "posteam",
        "home_team",
        "game_date",
        "start_time",
        "time_of_day",
    ]
    for season in (*PRE_RELOCATION_SEASONS, PREVIOUS_SEASON):
        frame = fetch_dataset_frame("pbp", f"play_by_play_{season}.parquet", selected_columns)
        TEAM_CODES_BY_DATASET_SEASON[("pbp", season)] = set(frame["posteam"].drop_nulls().unique()) - {""}
        print(
            f" {season}: rows={frame.height} | play_id whole numbers={(frame['play_id'] == frame['play_id'].floor()).all()} | "
            f"{format_value_counts(frame, 'season_type')}",
            f"| weeks {frame['week'].min()}-{frame['week'].max()} | null posteam={frame['posteam'].null_count()}",
        )
        print(
            "   kickoff fields sample:",
            frame.select("game_id", "home_team", "game_date", "start_time", "time_of_day").head(1).rows(),
            "| time_of_day null share:",
            round(frame["time_of_day"].is_null().mean(), 3),
        )


def check_players():
    print_section("players")
    frame = fetch_dataset_frame("players", "players.parquet")
    CACHED_FRAMES["players"] = frame
    non_null_gsis = frame["gsis_id"].drop_nulls()
    print(
        f"   rows={frame.height} null gsis_id={frame['gsis_id'].null_count()} "
        f"gsis dup={non_null_gsis.len() - non_null_gsis.n_unique()} "
        f"gsis-pattern={frame['gsis_id'].str.contains(MODERN_GSIS_ID_PATTERN).mean():.4f}"
    )
    for id_column in ("pfr_id", "espn_id", "pff_id", "otc_id", "esb_id", "nfl_id", "smart_id"):
        non_null_ids = frame[id_column].drop_nulls()
        print(
            f"   {id_column}: non-null={non_null_ids.len()} dup_values={non_null_ids.len() - non_null_ids.n_unique()}"
        )
    print("   " + format_value_counts(frame, "position_group", 14))
    print("   " + format_value_counts(frame, "status", 10))


def check_rosters():
    print_section("rosters (season level)")
    for season in (*PRE_RELOCATION_SEASONS, PREVIOUS_SEASON, CURRENT_SEASON):
        frame = fetch_dataset_frame("rosters", f"roster_{season}.parquet")
        TEAM_CODES_BY_DATASET_SEASON[("rosters", season)] = set(frame["team"].drop_nulls().unique())
        print(f" {season}: rows={frame.height} null gsis={frame['gsis_id'].null_count()}")
        print_key_summary(frame, ["season", "gsis_id"])
        if season == CURRENT_SEASON:
            print(
                "   " + format_value_counts(frame, "status", 12),
                "|",
                format_value_counts(frame, "game_type"),
                "| weeks:",
                frame["week"].min(),
                frame["week"].max(),
            )
            external_id_columns = (
                "espn_id",
                "sportradar_id",
                "yahoo_id",
                "rotowire_id",
                "pff_id",
                "pfr_id",
                "fantasy_data_id",
                "sleeper_id",
                "esb_id",
                "smart_id",
            )
            print(
                "   other-id non-null share:",
                {col: round(frame[col].is_not_null().mean(), 2) for col in external_id_columns},
            )

    print_section("weekly_rosters")
    weekly_columns = ["season", "team", "week", "game_type", "gsis_id", "status", "status_description_abbr"]
    for season in (*PRE_RELOCATION_SEASONS, PREVIOUS_SEASON, CURRENT_SEASON):
        frame = fetch_dataset_frame("weekly_rosters", f"roster_weekly_{season}.parquet", weekly_columns)
        TEAM_CODES_BY_DATASET_SEASON[("weekly_rosters", season)] = set(frame["team"].drop_nulls().unique())
        print(
            f" {season}: rows={frame.height} null gsis={frame['gsis_id'].null_count()} "
            f"empty gsis={(frame['gsis_id'] == '').sum()} weeks {frame['week'].min()}-{frame['week'].max()}"
        )
        print_key_summary(frame, ["season", "week", "game_type", "gsis_id"])
        if season in (PRE_RELOCATION_SEASONS[0], PREVIOUS_SEASON):
            print("   " + format_value_counts(frame, "status", 12))
            print("   " + format_value_counts(frame, "game_type"))
            print("   " + format_value_counts(frame, "status_description_abbr", 10))
        if season == PREVIOUS_SEASON:
            CACHED_FRAMES["weekly_rosters"] = frame


def check_snap_counts():
    print_section("snap_counts")
    for season in (*PRE_RELOCATION_SEASONS, PREVIOUS_SEASON, CURRENT_SEASON):
        frame = fetch_dataset_frame("snap_counts", f"snap_counts_{season}.parquet")
        TEAM_CODES_BY_DATASET_SEASON[("snap_counts", season)] = set(frame["team"].drop_nulls().unique())
        print(
            f" {season}: rows={frame.height} null pfr_player_id={frame['pfr_player_id'].null_count()} "
            f"weeks {frame['week'].min()}-{frame['week'].max()} | {format_value_counts(frame, 'game_type')}"
        )
        if season == PREVIOUS_SEASON:
            CACHED_FRAMES["snap_counts"] = frame


def check_injuries():
    print_section("injuries")
    sampled_seasons = sorted({*PRE_RELOCATION_SEASONS, LAST_OLD_LAYOUT_SEASON, PREVIOUS_SEASON, CURRENT_SEASON})
    for season in sampled_seasons:
        frame = fetch_dataset_frame("injuries", f"injuries_{season}.parquet")
        TEAM_CODES_BY_DATASET_SEASON[("injuries", season)] = set(frame["team"].drop_nulls().unique())
        print(
            f" {season}: rows={frame.height} null gsis={frame['gsis_id'].null_count()} "
            f"weeks {frame['week'].min()}-{frame['week'].max()} | {format_value_counts(frame, 'game_type')} | "
            f"has date_modified={'date_modified' in frame.columns}"
        )
        if season in (LAST_OLD_LAYOUT_SEASON, CURRENT_SEASON):
            print("   " + format_value_counts(frame, "report_status", 8))
            print("   " + format_value_counts(frame, "practice_status", 8))
        if "date_modified" in frame.columns and season == LAST_OLD_LAYOUT_SEASON:
            date_modified = frame["date_modified"]
            weekday_counts = (
                frame.select(date_modified.dt.weekday().alias("weekday"))["weekday"].value_counts(sort=True).rows()
            )
            print(
                "   date_modified:",
                date_modified.min(),
                "->",
                date_modified.max(),
                "nulls",
                date_modified.null_count(),
                "| ISO weekday counts (5=Friday):",
                weekday_counts,
            )
        if season == PREVIOUS_SEASON:
            CACHED_FRAMES["injuries"] = frame
        if season == CURRENT_SEASON:
            print("   rows by week:", frame.group_by("week").len().sort("week").rows())


def check_depth_charts():
    print_section("depth_charts, week-based layout")
    for season in (*PRE_RELOCATION_SEASONS, LAST_OLD_LAYOUT_SEASON):
        frame = fetch_dataset_frame("depth_charts", f"depth_charts_{season}.parquet")
        TEAM_CODES_BY_DATASET_SEASON[("depth_charts", season)] = set(frame["club_code"].drop_nulls().unique())
        print(
            f" {season}: rows={frame.height} null gsis={frame['gsis_id'].null_count()} "
            f"weeks {frame['week'].min()}-{frame['week'].max()} | {format_value_counts(frame, 'game_type')} | "
            f"{format_value_counts(frame, 'formation')}"
        )
        print_key_summary(
            frame,
            [
                "season",
                "club_code",
                "week",
                "game_type",
                "formation",
                "position",
                "depth_position",
                "depth_team",
                "gsis_id",
            ],
        )

    print_section("depth_charts, snapshot layout")
    for season in (PREVIOUS_SEASON, CURRENT_SEASON):
        frame = fetch_dataset_frame("depth_charts", f"depth_charts_{season}.parquet")
        TEAM_CODES_BY_DATASET_SEASON[("depth_charts", season)] = set(frame["team"].drop_nulls().unique())
        print(
            f" {season}: rows={frame.height} distinct dt={frame['dt'].n_unique()} "
            f"dt range {frame['dt'].min()} -> {frame['dt'].max()}"
        )
        print(
            f"   null gsis={frame['gsis_id'].null_count()} ({frame['gsis_id'].is_null().mean():.3f}) "
            f"null espn={frame['espn_id'].null_count()} | {format_value_counts(frame, 'pos_grp', 8)}"
        )
        print_key_summary(frame, ["dt", "team", "pos_grp_id", "pos_id", "pos_slot", "pos_rank"])
        snapshots_per_team = (
            frame.group_by("team")
            .agg(pl.col("dt").n_unique().alias("snapshot_count"))
            .select(pl.col("snapshot_count").min().alias("min"), pl.col("snapshot_count").max().alias("max"))
            .rows()
        )
        print(
            "   snapshots per team (min, max):",
            snapshots_per_team,
            "| sample:",
            frame.select("dt", "team", "pos_abb", "pos_slot", "pos_rank", "player_name").head(1).rows(),
        )
        CACHED_FRAMES["depth_charts"] = frame


def check_nextgen_stats():
    print_section("nextgen_stats")
    for category in ("passing", "rushing", "receiving"):
        frame = fetch_dataset_frame(
            "nextgen_stats", f"ngs_{category}.parquet", ["season", "season_type", "week", "player_gsis_id"]
        )
        print(
            f" {category}: rows={frame.height} seasons {frame['season'].min()}-{frame['season'].max()} "
            f"week-0 rows={frame.filter(pl.col('week') == 0).height} null gsis={frame['player_gsis_id'].null_count()} "
            f"distinct players={frame['player_gsis_id'].n_unique()} | {format_value_counts(frame, 'season_type')}"
        )
        print_key_summary(frame, ["season", "season_type", "week", "player_gsis_id"])
        if category == "receiving":
            CACHED_FRAMES["ngs_receiving"] = frame
            print(f"   {CURRENT_SEASON} max week:", frame.filter(pl.col("season") == CURRENT_SEASON)["week"].max())


def check_participation_and_charting():
    print_section(f"pbp_participation {LATEST_PARTICIPATION_SEASON}")
    participation_columns = [
        "offense_personnel",
        "offense_formation",
        "defense_personnel",
        "route",
        "defense_man_zone_type",
        "was_pressure",
        "n_offense",
    ]
    participation_frame = fetch_dataset_frame(
        "pbp_participation", f"pbp_participation_{LATEST_PARTICIPATION_SEASON}.parquet", participation_columns
    )
    print(
        "   non-null share:",
        {col: round(participation_frame[col].is_not_null().mean(), 3) for col in participation_frame.columns},
    )
    print("   " + format_value_counts(participation_frame, "offense_personnel", 4))
    print("   " + format_value_counts(participation_frame, "offense_formation", 8))

    print_section(f"ftn_charting {CURRENT_SEASON}")
    charting_frame = fetch_dataset_frame(
        "ftn_charting", f"ftn_charting_{CURRENT_SEASON}.parquet", ["week", "is_play_action", "date_pulled"]
    )
    print(
        "   weeks",
        charting_frame["week"].min(),
        "-",
        charting_frame["week"].max(),
        "| date_pulled max:",
        charting_frame["date_pulled"].max(),
        "| play_action share:",
        round(charting_frame["is_play_action"].mean(), 3),
    )


def check_pfr_receiving():
    print_section(f"pfr_week_rec {PREVIOUS_SEASON}")
    frame = fetch_dataset_frame("pfr_advstats", f"advstats_week_rec_{PREVIOUS_SEASON}.parquet")
    print("   " + format_value_counts(frame, "game_type"))


def check_id_coverage():
    print_section(f"ID coverage against players.parquet ({PREVIOUS_SEASON} unless noted)")
    if "players" not in CACHED_FRAMES:
        print("   skipped: the players frame was not read")
        return
    players_frame = CACHED_FRAMES["players"]
    known_gsis_ids = set(players_frame["gsis_id"].drop_nulls())
    known_pfr_ids = set(players_frame["pfr_id"].drop_nulls())

    def print_id_coverage(label, cache_key, id_column, reference_ids):
        if cache_key not in CACHED_FRAMES:
            print(f"   {label}: skipped, its frame was not read")
            return
        observed_ids = {value for value in CACHED_FRAMES[cache_key][id_column] if value}
        missing_ids = observed_ids - reference_ids
        missing_share = len(missing_ids) / max(len(observed_ids), 1)
        print(
            f"   {label}: distinct={len(observed_ids)} missing={len(missing_ids)} ({missing_share:.3%}) e.g. {sorted(missing_ids)[:3]}"
        )

    print_id_coverage("stats_player_week player_id -> gsis_id", "stats_player_week", "player_id", known_gsis_ids)
    print_id_coverage("weekly_rosters gsis_id -> gsis_id", "weekly_rosters", "gsis_id", known_gsis_ids)
    print_id_coverage("injuries gsis_id -> gsis_id", "injuries", "gsis_id", known_gsis_ids)
    print_id_coverage(
        "ngs receiving (all seasons) player_gsis_id -> gsis_id", "ngs_receiving", "player_gsis_id", known_gsis_ids
    )
    print_id_coverage(f"depth_charts {CURRENT_SEASON} gsis_id -> gsis_id", "depth_charts", "gsis_id", known_gsis_ids)
    print_id_coverage("snap_counts pfr_player_id -> pfr_id", "snap_counts", "pfr_player_id", known_pfr_ids)
    if "snap_counts" in CACHED_FRAMES:
        skill_positions = ["QB", "RB", "WR", "TE"]
        mapped_skill_players = (
            CACHED_FRAMES["snap_counts"]
            .filter(pl.col("position").is_in(skill_positions))
            .select("pfr_player_id")
            .unique()
            .join(players_frame.select("pfr_id", "gsis_id"), left_on="pfr_player_id", right_on="pfr_id", how="left")
        )
        print(
            f"   snap-count QB/RB/WR/TE players: {mapped_skill_players.height}, "
            f"mapped to a gsis_id: {mapped_skill_players['gsis_id'].is_not_null().sum()}"
        )


def check_team_codes():
    print_section("team codes by dataset, compared with the schedule for that season")
    for season in (*PRE_RELOCATION_SEASONS, PREVIOUS_SEASON):
        schedule_teams = TEAM_CODES_BY_DATASET_SEASON.get(("schedules", season), set())
        print(f" {season}: schedule has {len(schedule_teams)} codes")
        for (dataset_name, recorded_season), team_codes in sorted(TEAM_CODES_BY_DATASET_SEASON.items()):
            if recorded_season == season and dataset_name != "schedules":
                print(
                    f"   {dataset_name}: n={len(team_codes)} "
                    f"extra={sorted(team_codes - schedule_teams)} missing={sorted(schedule_teams - team_codes)}"
                )


def check_external_sources():
    print_section("outside nflverse-data: ff_playerids header, ffopportunity releases")
    playerids_request = urllib.request.Request(
        "https://github.com/dynastyprocess/data/raw/master/files/db_playerids.csv",
        headers={**USER_AGENT_HEADERS, "Range": "bytes=0-1500"},
    )
    with urllib.request.urlopen(playerids_request, timeout=60) as response:
        print("   db_playerids.csv columns:", response.read().decode("utf-8", "replace").split("\n")[0])
    ffopportunity_request = urllib.request.Request(
        "https://api.github.com/repos/ffverse/ffopportunity/releases?per_page=10",
        headers={**USER_AGENT_HEADERS, "Accept": "application/vnd.github+json"},
    )
    with urllib.request.urlopen(ffopportunity_request, timeout=60) as response:
        for release in json.load(response):
            parquet_names = sorted(asset["name"] for asset in release["assets"] if asset["name"].endswith(".parquet"))
            latest_update = max((asset["updated_at"] for asset in release["assets"]), default="-")
            print(
                f"   ffopportunity [{release['tag_name']}] parquet files={len(parquet_names)} "
                f"last three={parquet_names[-3:]} latest update={latest_update}"
            )


failed_checks = []
for check_fn in (
    check_schedules,
    check_stats_player,
    check_stats_team,
    check_pbp,
    check_players,
    check_rosters,
    check_snap_counts,
    check_injuries,
    check_depth_charts,
    check_nextgen_stats,
    check_participation_and_charting,
    check_pfr_receiving,
    check_id_coverage,
    check_team_codes,
    check_external_sources,
):
    try:
        check_fn()
    except Exception:  # noqa: BLE001 - a check that fails is reported and the other checks still run
        traceback.print_exc(limit=2)
        failed_checks.append(check_fn.__name__)
print_bytes_fetched()
if failed_checks:
    sys.exit(f"checks that raised an error: {', '.join(failed_checks)}")
