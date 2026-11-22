"""Test the key of each dataset listed in CHECKS in every season, match game ids against the schedule where the
dataset has them, and print the seasons where relocation team codes, postseason week numbers and the participation
personnel format change.

Reads the key columns and a few value columns of each season file remotely, and the schedule whole; nothing is saved.
Run list_releases.py first.

Usage: python probe_keys_all_seasons.py OUT_DIR
"""
import sys, concurrent.futures as cf
import polars as pl
from nfl_remote import NO_FILES, load_listing, assets, season_of, read_columns, count_key_problems, print_fetched

L = load_listing(sys.argv[1])
GAMES = read_columns(next(a for a in L["schedules"]["assets"] if a["name"] == "games.parquet"))
GID = {y: set(g["game_id"]) for (y,), g in GAMES.group_by("season")}
RELOCATED = {"OAK", "SD", "STL", "LV", "LAC", "LA"}

# label: (release tag, file regex, key, extra columns to read, game-id column or None)
CHECKS = {
    "pbp": ("pbp", r"play_by_play_\d{4}\.parquet", ["game_id", "play_id"], ["home_team"], "game_id"),
    "stats_player_week": ("stats_player", r"stats_player_week_\d{4}\.parquet", ["player_id", "game_id"], ["week", "season_type"], "game_id"),
    "stats_team_week": ("stats_team", r"stats_team_week_\d{4}\.parquet", ["team", "game_id"], [], "game_id"),
    "snap_counts": ("snap_counts", r"snap_counts_\d{4}\.parquet", ["game_id", "pfr_player_id"], [], "game_id"),
    "injuries": ("injuries", r"injuries_\d{4}\.parquet", ["season", "game_type", "week", "team", "gsis_id"], [], None),
    "weekly_rosters": ("weekly_rosters", r"roster_weekly_\d{4}\.parquet", ["season", "week", "game_type", "team", "gsis_id"], [], None),
    # roster files before 1990 are not read
    "rosters": ("rosters", r"roster_(199\d|20\d\d)\.parquet", ["season", "team", "gsis_id"], [], None),
    # the week-based depth chart layout ended with the 2024 season
    "depth_charts_week_layout": ("depth_charts", r"depth_charts_(200\d|201\d|202[0-4])\.parquet",
                                 ["season", "club_code", "week", "game_type", "formation", "depth_position", "depth_team", "gsis_id"], [], None),
    "depth_charts_snapshot_layout": ("depth_charts", r"depth_charts_(202[5-9]|20[3-9]\d)\.parquet", ["dt", "team", "pos_id", "pos_rank"], [], None),
    "pbp_participation": ("pbp_participation", r"pbp_participation_\d{4}\.parquet", ["nflverse_game_id", "play_id"], ["offense_personnel"], "nflverse_game_id"),
    "ftn_charting": ("ftn_charting", r"ftn_charting_\d{4}\.parquet", ["nflverse_game_id", "nflverse_play_id"], [], "nflverse_game_id"),
    "pfr_week_rec": ("pfr_advstats", r"advstats_week_rec_\d{4}\.parquet", ["game_id", "pfr_player_id"], [], "game_id"),
}


def one(job):
    label, a, key, extra, gcol = job
    y = season_of(a["name"])
    try:
        df = read_columns(a, key + extra)
        null_rows, dup = count_key_problems(df, key)
        ids = [c for c in key if c.endswith("_id") and df[c].dtype == pl.String]
        rec = {"season": y, "rows": df.height, "null_rows": null_rows, "dup": dup,
               "nulls": {c: df[c].null_count() for c in key if df[c].null_count()},
               "empty": {c: int((df[c] == "").sum()) for c in ids if (df[c] == "").sum()}}
        if gcol:
            g = set(df[gcol].drop_nulls().unique())
            rec["not_in_sched"] = len(g - GID.get(y, set()))
            rec["sched_missing"] = len(GID.get(y, set()) - g)
        if label == "pbp":
            rec["home_codes"] = sorted(set(df["home_team"].unique()) & RELOCATED)
        if label == "stats_player_week":
            rec["post_weeks"] = df.filter(pl.col("season_type") == "POST").select(pl.col("week").min().alias("lo"), pl.col("week").max().alias("hi")).row(0)
        if label == "pbp_participation":
            rec["personnel"] = (df["offense_personnel"].drop_nulls().mode().to_list()[:1], round(df["offense_personnel"].is_not_null().mean(), 3))
        return label, rec
    except Exception as e:
        return label, {"season": y, "error": f"{type(e).__name__}: {e}"[:160]}


def changes(pairs):
    """Keep only the seasons where the value differs from the season before."""
    out, prev = {}, object()
    for season, value in pairs:
        if value != prev:
            out[season] = value
            prev = value
    return out


jobs = [(label, a, key, extra, gcol) for label, (tag, rx, key, extra, gcol) in CHECKS.items() for a in assets(L, tag, rx)]
res = {}
with cf.ThreadPoolExecutor(6) as ex:
    for label, rec in ex.map(one, jobs):
        res.setdefault(label, []).append(rec)

for label, recs in res.items():
    recs.sort(key=lambda r: r["season"])
    ok = [r for r in recs if "error" not in r]
    print(f"\n## {label}: files={len(recs)} seasons {recs[0]['season']}-{recs[-1]['season']} key={CHECKS[label][2]}")
    print("   null-key rows by season:", {r["season"]: r["null_rows"] for r in ok if r["null_rows"]} or "none")
    print("   dup rows by season:", {r["season"]: r["dup"] for r in ok if r["dup"]} or "none")
    print("   null key cells by season:", {r["season"]: r["nulls"] for r in ok if r["nulls"]} or "none")
    print("   empty-string ids by season:", {r["season"]: r["empty"] for r in ok if r["empty"]} or "none")
    print("   empty files:", [r["season"] for r in ok if r["rows"] == 0] or "none")
    if CHECKS[label][4]:
        print("   game ids not in schedules:", {r["season"]: r["not_in_sched"] for r in ok if r["not_in_sched"]} or "none",
              "| schedule games absent here:", {r["season"]: r["sched_missing"] for r in ok if r["sched_missing"]} or "none")
    if label == "pbp":
        print("   home_team relocation codes, by season where they change:", changes((r["season"], r["home_codes"]) for r in ok))
    if label == "stats_player_week":
        print("   POST week range, by season where it changes:", changes((r["season"], r["post_weeks"]) for r in ok if r["post_weeks"] != (None, None)))
    if label == "pbp_participation":
        print("   most common personnel value and non-null share, by season where they change:", changes((r["season"], r["personnel"]) for r in ok))
    for r in recs:
        if "error" in r:
            print("   ERROR", r)

print("\n## schedules")
post = GAMES.filter(pl.col("game_type") != "REG").group_by("season").agg(pl.col("week").min().alias("lo"), pl.col("week").max().alias("hi")).sort("season")
print("   POST week range, by season where it changes:", changes((s, (lo, hi)) for s, lo, hi in post.rows()))
home = GAMES.group_by("season").agg(pl.col("home_team").unique().alias("t")).sort("season")
print("   relocation codes, by season where they change:", changes((s, sorted(set(t) & RELOCATED)) for s, t in home.rows()))
print_fetched()
if NO_FILES or any("error" in r for recs in res.values() for r in recs):
    sys.exit(1)
