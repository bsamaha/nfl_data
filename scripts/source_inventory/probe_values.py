"""Value, ID-coverage and team-code checks on selected seasons of each nflverse dataset.

Reads each sampled file remotely, whole or by column, and prints what it finds; nothing is saved.
The current season is taken from the newest stats_player_week file, so the script follows the calendar.
Run list_releases.py first.

Usage: python probe_values.py OUT_DIR
"""
import json, re, sys, traceback, urllib.request
import polars as pl
from nfl_remote import load_listing, read_columns, season_of, UA, count_key_problems, print_fetched

L = load_listing(sys.argv[1])


def latest(tag, regex):
    seasons = [season_of(a["name"]) for a in L.get(tag, {}).get("assets", []) if re.fullmatch(regex, a["name"])]
    if not seasons:
        sys.exit(f"releases.json has no file matching {regex} in release {tag}")
    return max(seasons)


CUR = latest("stats_player", r"stats_player_week_\d{4}\.parquet")   # season in progress or just finished
PREV = CUR - 1                                                      # last complete season
PART = latest("pbp_participation", r"pbp_participation_\d{4}\.parquet")
OLD = (2015, 2019)          # seasons before the Rams, Chargers and Raiders moves settled, for team-code checks
LAST_OLD_LAYOUT = 2024      # final season before the 2025 layout changes: week-based depth charts, injuries.date_modified
GS = r"^00-00\d{5}$"        # modern gsis id pattern
KEEP = {}                   # frames reused by later checks
TEAMS = {}                  # (dataset, season) -> set of team codes
print(f"current season={CUR} previous={PREV} latest participation season={PART}")


def get(tag, name, cols=None):
    a = next((x for x in L.get(tag, {}).get("assets", []) if x["name"] == name), None)
    if a is None:
        raise LookupError(f"releases.json has no file {name} in release {tag}")
    return read_columns(a, cols)


def key(df, keys):
    null_rows, dup = count_key_problems(df, keys)
    nulls = {k: df[k].null_count() for k in keys if df[k].null_count()}
    print(f"   key {keys}: rows={df.height} null_key_rows={null_rows} dup_rows={dup} null_keys={nulls or 0}")


def vc(df, col, n=12):
    if col not in df.columns:
        return f"{col}: (absent)"
    d = df[col].value_counts(sort=True).head(n)
    return f"{col}: " + ", ".join(f"{r[0]}={r[1]}" for r in d.iter_rows())


def sec(t):
    print(f"\n=== {t} ===")


def schedules():
    sec("schedules (games.parquet)")
    df = get("schedules", "games.parquet")
    key(df, ["game_id"])
    print("   seasons:", df["season"].min(), "-", df["season"].max(), "|", vc(df, "game_type"))
    print("   " + vc(df, "weekday"), "|", vc(df, "roof"), "|", vc(df, "location"))
    print("   null gameday:", df["gameday"].null_count(), "null gametime:", df["gametime"].null_count(),
          "by season:", dict(df.filter(pl.col("gametime").is_null()).group_by("season").len().sort("season").rows()),
          "sample:", df.select("game_id", "gameday", "gametime").tail(1).rows())
    cov = df.group_by("season").agg(pl.len().alias("games"), pl.col("spread_line").is_not_null().sum().alias("spread"),
                                    pl.col("total_line").is_not_null().sum().alias("total"), pl.col("temp").is_not_null().sum().alias("temp"),
                                    pl.col("result").is_null().sum().alias("unplayed")).sort("season")
    print("   per season (games, spread, total, temp, unplayed) first 2 and last 3:", cov.head(2).rows(), "...", cov.tail(3).rows())
    fut = df.filter((pl.col("season") == CUR) & pl.col("result").is_null())
    print(f"   {CUR} unplayed games={fut.height}, with spread_line={fut['spread_line'].is_not_null().sum()}, weeks {fut['week'].min()}-{fut['week'].max()}")
    for y in (*OLD, PREV):
        TEAMS[("schedules", y)] = set(df.filter(pl.col("season") == y)["home_team"].unique())
    print("   all team codes:", sorted(set(df["home_team"].unique()) | set(df["away_team"].unique())))
    print("   id columns non-null share:",
          {c: round(df[c].is_not_null().mean(), 3) for c in ("old_game_id", "gsis", "nfl_detail_id", "pfr", "pff", "espn", "ftn", "stadium_id")})


def stats_player():
    sec("stats_player_week")
    cols = ["player_id", "player_display_name", "season", "week", "season_type", "game_id", "team", "position", "position_group"]
    for y in sorted({1999, *OLD, PREV - 1, PREV, CUR}):  # 1999 is the first stats_player_week season
        df = get("stats_player", f"stats_player_week_{y}.parquet", cols)
        TEAMS[("stats_player_week", y)] = set(df["team"].drop_nulls().unique())
        print(f" {y}: rows={df.height} players={df['player_id'].n_unique()} gsis-pattern={df['player_id'].str.contains(GS).mean():.4f}")
        key(df, ["player_id", "season", "week"])
        if y in (PREV, CUR):
            print("   " + vc(df, "season_type"), "| weeks:",
                  df.group_by("season_type").agg(pl.col("week").min().alias("lo"), pl.col("week").max().alias("hi")).rows())
            print("   " + vc(df, "position_group", 10))
            multi = df.group_by("player_id").agg(pl.col("team").n_unique().alias("t")).filter(pl.col("t") > 1).height
            print(f"   players with >1 team in season: {multi}; null team={df['team'].null_count()} null game_id={df['game_id'].null_count()}")
            print("   null-player rows sample:",
                  df.filter(pl.col("player_id").is_null()).select("player_display_name", "position", "team", "week").head(3).rows())
        if y == PREV:
            KEEP["sp"] = df


def stats_team():
    sec(f"stats_team_week {PREV}")
    df = get("stats_team", f"stats_team_week_{PREV}.parquet", ["season", "week", "team", "season_type"])
    key(df, ["season", "week", "team"])
    print("   " + vc(df, "season_type"))


def pbp():
    sec("pbp")
    for y in (*OLD, PREV):
        df = get("pbp", f"play_by_play_{y}.parquet",
                 ["game_id", "play_id", "week", "season_type", "posteam", "home_team", "game_date", "start_time", "time_of_day"])
        TEAMS[("pbp", y)] = set(df["posteam"].drop_nulls().unique()) - {""}
        print(f" {y}: rows={df.height} | play_id whole numbers={(df['play_id'] == df['play_id'].floor()).all()} | {vc(df, 'season_type')}",
              f"| weeks {df['week'].min()}-{df['week'].max()} | null posteam={df['posteam'].null_count()}")
        print("   kickoff fields sample:", df.select("game_id", "home_team", "game_date", "start_time", "time_of_day").head(1).rows(),
              "| time_of_day null share:", round(df["time_of_day"].is_null().mean(), 3))


def players():
    sec("players")
    df = get("players", "players.parquet")
    KEEP["players"] = df
    print(f"   rows={df.height} null gsis_id={df['gsis_id'].null_count()} gsis dup={df['gsis_id'].drop_nulls().len() - df['gsis_id'].drop_nulls().n_unique()} gsis-pattern={df['gsis_id'].str.contains(GS).mean():.4f}")
    for c in ("pfr_id", "espn_id", "pff_id", "otc_id", "esb_id", "nfl_id", "smart_id"):
        nn = df[c].drop_nulls()
        print(f"   {c}: non-null={nn.len()} dup_values={nn.len() - nn.n_unique()}")
    print("   " + vc(df, "position_group", 14))
    print("   " + vc(df, "status", 10))


def rosters():
    sec("rosters (season level)")
    for y in (*OLD, PREV, CUR):
        df = get("rosters", f"roster_{y}.parquet")
        TEAMS[("rosters", y)] = set(df["team"].drop_nulls().unique())
        print(f" {y}: rows={df.height} null gsis={df['gsis_id'].null_count()}")
        key(df, ["season", "gsis_id"])
        if y == CUR:
            print("   " + vc(df, "status", 12), "|", vc(df, "game_type"), "| weeks:", df["week"].min(), df["week"].max())
            print("   other-id non-null share:", {c: round(df[c].is_not_null().mean(), 2) for c in (
                "espn_id", "sportradar_id", "yahoo_id", "rotowire_id", "pff_id", "pfr_id", "fantasy_data_id", "sleeper_id", "esb_id", "smart_id")})
    sec("weekly_rosters")
    for y in (*OLD, PREV, CUR):
        df = get("weekly_rosters", f"roster_weekly_{y}.parquet",
                 ["season", "team", "week", "game_type", "gsis_id", "status", "status_description_abbr"])
        TEAMS[("weekly_rosters", y)] = set(df["team"].drop_nulls().unique())
        print(f" {y}: rows={df.height} null gsis={df['gsis_id'].null_count()} empty gsis={(df['gsis_id'] == '').sum()} weeks {df['week'].min()}-{df['week'].max()}")
        key(df, ["season", "week", "game_type", "gsis_id"])
        if y in (OLD[0], PREV):
            print("   " + vc(df, "status", 12))
            print("   " + vc(df, "game_type"))
            print("   " + vc(df, "status_description_abbr", 10))
        if y == PREV:
            KEEP["weekly_rosters"] = df


def snaps():
    sec("snap_counts")
    for y in (*OLD, PREV, CUR):
        df = get("snap_counts", f"snap_counts_{y}.parquet")
        TEAMS[("snap_counts", y)] = set(df["team"].drop_nulls().unique())
        print(f" {y}: rows={df.height} null pfr_player_id={df['pfr_player_id'].null_count()} weeks {df['week'].min()}-{df['week'].max()} | {vc(df, 'game_type')}")
        if y == PREV:
            KEEP["sn"] = df


def injuries():
    sec("injuries")
    for y in sorted({*OLD, LAST_OLD_LAYOUT, PREV, CUR}):
        df = get("injuries", f"injuries_{y}.parquet")
        TEAMS[("injuries", y)] = set(df["team"].drop_nulls().unique())
        print(f" {y}: rows={df.height} null gsis={df['gsis_id'].null_count()} weeks {df['week'].min()}-{df['week'].max()} | {vc(df, 'game_type')} | has date_modified={'date_modified' in df.columns}")
        if y in (LAST_OLD_LAYOUT, CUR):
            print("   " + vc(df, "report_status", 8))
            print("   " + vc(df, "practice_status", 8))
        if "date_modified" in df.columns and y == LAST_OLD_LAYOUT:
            dm = df["date_modified"]
            print("   date_modified:", dm.min(), "->", dm.max(), "nulls", dm.null_count(),
                  "| ISO weekday counts (5=Friday):", df.select(dm.dt.weekday().alias("wd"))["wd"].value_counts(sort=True).rows())
        if y == PREV:
            KEEP["inj"] = df
        if y == CUR:
            print("   rows by week:", df.group_by("week").len().sort("week").rows())


def depth():
    sec("depth_charts, week-based layout")
    for y in (*OLD, LAST_OLD_LAYOUT):
        df = get("depth_charts", f"depth_charts_{y}.parquet")
        TEAMS[("depth_charts", y)] = set(df["club_code"].drop_nulls().unique())
        print(f" {y}: rows={df.height} null gsis={df['gsis_id'].null_count()} weeks {df['week'].min()}-{df['week'].max()} | {vc(df, 'game_type')} | {vc(df, 'formation')}")
        key(df, ["season", "club_code", "week", "game_type", "formation", "position", "depth_position", "depth_team", "gsis_id"])
    sec("depth_charts, snapshot layout")
    for y in (PREV, CUR):
        df = get("depth_charts", f"depth_charts_{y}.parquet")
        TEAMS[("depth_charts", y)] = set(df["team"].drop_nulls().unique())
        print(f" {y}: rows={df.height} distinct dt={df['dt'].n_unique()} dt range {df['dt'].min()} -> {df['dt'].max()}")
        print(f"   null gsis={df['gsis_id'].null_count()} ({df['gsis_id'].is_null().mean():.3f}) null espn={df['espn_id'].null_count()} | {vc(df, 'pos_grp', 8)}")
        key(df, ["dt", "team", "pos_grp_id", "pos_id", "pos_slot", "pos_rank"])
        per = df.group_by("team").agg(pl.col("dt").n_unique().alias("n")).select(pl.col("n").min().alias("min"), pl.col("n").max().alias("max")).rows()
        print("   snapshots per team (min, max):", per, "| sample:", df.select("dt", "team", "pos_abb", "pos_slot", "pos_rank", "player_name").head(1).rows())
        KEEP["dc"] = df


def ngs():
    sec("nextgen_stats")
    for k in ("passing", "rushing", "receiving"):
        df = get("nextgen_stats", f"ngs_{k}.parquet", ["season", "season_type", "week", "player_gsis_id"])
        print(f" {k}: rows={df.height} seasons {df['season'].min()}-{df['season'].max()} week-0 rows={df.filter(pl.col('week') == 0).height} null gsis={df['player_gsis_id'].null_count()} distinct players={df['player_gsis_id'].n_unique()} | {vc(df, 'season_type')}")
        key(df, ["season", "season_type", "week", "player_gsis_id"])
        if k == "receiving":
            KEEP["ngs"] = df
            print(f"   {CUR} max week:", df.filter(pl.col("season") == CUR)["week"].max())


def participation():
    sec(f"pbp_participation {PART}")
    df = get("pbp_participation", f"pbp_participation_{PART}.parquet",
             ["offense_personnel", "offense_formation", "defense_personnel", "route",
              "defense_man_zone_type", "was_pressure", "n_offense"])
    print("   non-null share:", {c: round(df[c].is_not_null().mean(), 3) for c in df.columns})
    print("   " + vc(df, "offense_personnel", 4))
    print("   " + vc(df, "offense_formation", 8))
    sec(f"ftn_charting {CUR}")
    df = get("ftn_charting", f"ftn_charting_{CUR}.parquet", ["week", "is_play_action", "date_pulled"])
    print("   weeks", df["week"].min(), "-", df["week"].max(), "| date_pulled max:", df["date_pulled"].max(),
          "| play_action share:", round(df["is_play_action"].mean(), 3))


def pfr():
    sec(f"pfr_week_rec {PREV}")
    df = get("pfr_advstats", f"advstats_week_rec_{PREV}.parquet")
    print("   " + vc(df, "game_type"))


def coverage():
    sec(f"ID coverage against players.parquet ({PREV} unless noted)")
    if "players" not in KEEP:
        print("   skipped: the players frame was not read")
        return
    P = KEEP["players"]
    g = set(P["gsis_id"].drop_nulls())
    p = set(P["pfr_id"].drop_nulls())

    def cov(name, frame, col, ref):
        if frame not in KEEP:
            print(f"   {name}: skipped, its frame was not read")
            return
        ids = set(x for x in KEEP[frame][col] if x)
        miss = ids - ref
        print(f"   {name}: distinct={len(ids)} missing={len(miss)} ({len(miss) / max(len(ids), 1):.3%}) e.g. {sorted(miss)[:3]}")
    cov("stats_player_week player_id -> gsis_id", "sp", "player_id", g)
    cov("weekly_rosters gsis_id -> gsis_id", "weekly_rosters", "gsis_id", g)
    cov("injuries gsis_id -> gsis_id", "inj", "gsis_id", g)
    cov("ngs receiving (all seasons) player_gsis_id -> gsis_id", "ngs", "player_gsis_id", g)
    cov(f"depth_charts {CUR} gsis_id -> gsis_id", "dc", "gsis_id", g)
    cov("snap_counts pfr_player_id -> pfr_id", "sn", "pfr_player_id", p)
    if "sn" in KEEP:
        skill = ["QB", "RB", "WR", "TE"]
        m = KEEP["sn"].filter(pl.col("position").is_in(skill)).select("pfr_player_id").unique().join(
            P.select("pfr_id", "gsis_id"), left_on="pfr_player_id", right_on="pfr_id", how="left")
        print(f"   snap-count QB/RB/WR/TE players: {m.height}, mapped to a gsis_id: {m['gsis_id'].is_not_null().sum()}")


def teams():
    sec("team codes by dataset, compared with the schedule for that season")
    for y in (*OLD, PREV):
        base = TEAMS.get(("schedules", y), set())
        print(f" {y}: schedule has {len(base)} codes")
        for (ds, yy), s in sorted(TEAMS.items()):
            if yy == y and ds != "schedules":
                print(f"   {ds}: n={len(s)} extra={sorted(s - base)} missing={sorted(base - s)}")


def external():
    sec("outside nflverse-data: ff_playerids header, ffopportunity releases")
    req = urllib.request.Request("https://github.com/dynastyprocess/data/raw/master/files/db_playerids.csv", headers={**UA, "Range": "bytes=0-1500"})
    with urllib.request.urlopen(req, timeout=60) as r:
        print("   db_playerids.csv columns:", r.read().decode("utf-8", "replace").split("\n")[0])
    req = urllib.request.Request("https://api.github.com/repos/ffverse/ffopportunity/releases?per_page=10",
                                 headers={**UA, "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        for rel in json.load(r):
            names = sorted(a["name"] for a in rel["assets"] if a["name"].endswith(".parquet"))
            print(f"   ffopportunity [{rel['tag_name']}] parquet files={len(names)} last three={names[-3:]} latest update={max((a['updated_at'] for a in rel['assets']), default='-')}")


failed = []
for fn in (schedules, stats_player, stats_team, pbp, players, rosters, snaps, injuries, depth, ngs, participation, pfr, coverage, teams, external):
    try:
        fn()
    except Exception:
        traceback.print_exc(limit=2)
        failed.append(fn.__name__)
print_fetched()
if failed:
    sys.exit(f"checks that raised an error: {', '.join(failed)}")
