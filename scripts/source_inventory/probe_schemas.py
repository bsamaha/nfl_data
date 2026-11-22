"""Read the Parquet footer of every file listed in SETS and save column lists and row counts.

Only the tail of each file is fetched: the last 64 KB, then the exact footer when it is larger.
Writes OUT_DIR/schemas.json. Run list_releases.py first.

Usage: python probe_schemas.py OUT_DIR
"""
import json, sys, concurrent.futures as cf
from pathlib import Path
from nfl_remote import NO_FILES, load_listing, assets, season_of, read_footer, print_fetched

out = Path(sys.argv[1])
L = load_listing(out)

# dataset label -> (release tag, file-name regex)
SETS = {
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

jobs = [(label, a) for label, (tag, rx) in SETS.items() for a in assets(L, tag, rx)]
print("files to probe:", len(jobs))


def probe(job):
    label, a = job
    try:
        md = read_footer(a["url"], a["size"])
        sch = md.schema.to_arrow_schema()
        kv = {k.decode(): (v.decode(errors="replace")[:200]) for k, v in (md.metadata or {}).items()
              if k not in (b"ARROW:schema", b"r")}
        return label, {"file": a["name"], "season": season_of(a["name"]),
                       "updated_at": a["updated_at"], "rows": md.num_rows,
                       "columns": [[f.name, str(f.type)] for f in sch], "kv": kv}
    except Exception as e:
        return label, {"file": a["name"], "season": season_of(a["name"]), "error": f"{type(e).__name__}: {e}"[:200]}


res = {}
with cf.ThreadPoolExecutor(8) as ex:
    for label, rec in ex.map(probe, jobs):
        res.setdefault(label, []).append(rec)
(out / "schemas.json").write_text(json.dumps(res, indent=1))
errs = [(l, r["file"], r["error"]) for l, rs in res.items() for r in rs if "error" in r]
print("ok:", sum(len(v) for v in res.values()) - len(errs), "errors:", len(errs))
print_fetched()
for e in errs:
    print("  ", e)
if errs or NO_FILES:
    sys.exit(1)
