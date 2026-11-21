"""Test each dataset's key in every season and match its game ids against the schedule.

Reads only the key columns of each season file remotely; nothing is saved.
A dataset is healthy when the "dup rows", "null key cells" and "game ids not in schedules" lines all say "none".
Run list_releases.py first.

Usage: python probe_keys_all_seasons.py OUT_DIR
"""
import sys, concurrent.futures as cf
import polars as pl
from nfl_remote import load_listing, assets, season_of, read_columns

L = load_listing(sys.argv[1])
GAMES = read_columns(next(a for a in L["schedules"]["assets"] if a["name"] == "games.parquet"))[0]
GID = {y: set(g["game_id"]) for (y,), g in GAMES.group_by("season")}
RELOCATED = {"OAK", "SD", "STL", "LV", "LAC", "LA"}

# label: (release tag, file regex, columns to read, key, game-id column or None)
CHECKS = {
    "pbp": ("pbp", r"play_by_play_\d{4}\.parquet", ["game_id", "play_id", "home_team"], ["game_id", "play_id"], "game_id"),
    "stats_player_week": ("stats_player", r"stats_player_week_\d{4}\.parquet", ["player_id", "game_id", "season", "week", "season_type"], ["player_id", "game_id"], "game_id"),
    "stats_team_week": ("stats_team", r"stats_team_week_\d{4}\.parquet", ["team", "game_id"], ["team", "game_id"], "game_id"),
    "snap_counts": ("snap_counts", r"snap_counts_\d{4}\.parquet", ["game_id", "pfr_player_id"], ["game_id", "pfr_player_id"], "game_id"),
    "injuries": ("injuries", r"injuries_\d{4}\.parquet", ["season", "game_type", "week", "team", "gsis_id"], ["season", "game_type", "week", "team", "gsis_id"], None),
    "weekly_rosters": ("weekly_rosters", r"roster_weekly_\d{4}\.parquet", ["season", "week", "game_type", "team", "gsis_id"], ["season", "week", "game_type", "team", "gsis_id"], None),
    "rosters": ("rosters", r"roster_(199\d|20\d\d)\.parquet", ["season", "team", "gsis_id"], ["season", "team", "gsis_id"], None),
    # the week-based depth chart layout ended with the 2024 season
    "depth_charts_week_layout": ("depth_charts", r"depth_charts_(200\d|201\d|202[0-4])\.parquet",
                                 ["season", "club_code", "week", "game_type", "formation", "depth_position", "depth_team", "gsis_id"],
                                 ["season", "club_code", "week", "game_type", "formation", "depth_position", "depth_team", "gsis_id"], None),
    "depth_charts_snapshot_layout": ("depth_charts", r"depth_charts_(202[5-9]|20[3-9]\d)\.parquet", ["dt", "team", "pos_id", "pos_rank"], ["dt", "team", "pos_id", "pos_rank"], None),
    "pbp_participation": ("pbp_participation", r"pbp_participation_\d{4}\.parquet", ["nflverse_game_id", "play_id", "offense_personnel"], ["nflverse_game_id", "play_id"], "nflverse_game_id"),
    "ftn_charting": ("ftn_charting", r"ftn_charting_\d{4}\.parquet", ["nflverse_game_id", "nflverse_play_id"], ["nflverse_game_id", "nflverse_play_id"], "nflverse_game_id"),
    "pfr_advstats_week_rec": ("pfr_advstats", r"advstats_week_rec_\d{4}\.parquet", ["game_id", "pfr_player_id"], ["game_id", "pfr_player_id"], "game_id"),
}


def one(job):
    label, a, cols, key, gcol = job
    y = season_of(a["name"])
    try:
        df, n = read_columns(a, cols)
        k = [c for c in key if c in df.columns]
        ids = [c for c in k if c.endswith("_id") and df[c].dtype == pl.String]
        rec = {"season": y, "rows": df.height, "dup": df.height - df.select(k).unique().height,
               "nulls": {c: df[c].null_count() for c in k if df[c].null_count()},
               "empty": {c: int((df[c] == "").sum()) for c in ids if (df[c] == "").sum()}, "bytes": n}
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


jobs = [(label, a, cols, key, gcol) for label, (tag, rx, cols, key, gcol) in CHECKS.items() for a in assets(L, tag, rx)]
res = {}
with cf.ThreadPoolExecutor(6) as ex:
    for label, rec in ex.map(one, jobs):
        res.setdefault(label, []).append(rec)

total = 0
for label, recs in res.items():
    recs.sort(key=lambda r: r["season"])
    ok = [r for r in recs if "error" not in r]
    total += sum(r["bytes"] for r in ok)
    print(f"\n## {label}: files={len(recs)} seasons {recs[0]['season']}-{recs[-1]['season']} key={CHECKS[label][3]}")
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
        print("   POST week range, by season where it changes:", changes((r["season"], r["post_weeks"]) for r in ok))
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
print("   null gametime by season:", dict(GAMES.filter(pl.col("gametime").is_null()).group_by("season").len().sort("season").rows()) or "none")
print(f"\nbytes fetched: {total / 1e6:.1f} MB")
