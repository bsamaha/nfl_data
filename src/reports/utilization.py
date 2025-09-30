from __future__ import annotations

import subprocess
from pathlib import Path

try:
    import structlog  # type: ignore
    logger = structlog.get_logger(__name__)
except Exception:  # pragma: no cover
    class _DummyLogger:
        def info(self, *args, **kwargs):
            pass

        def warning(self, *args, **kwargs):
            pass

    logger = _DummyLogger()


def _run_sql(path: Path, season: int, season_type: str = "REG") -> None:
    cmd = ["bash", "scripts/run_query.sh", "-f", str(path), "-s", str(season), "-t", season_type]
    logger.info("run_sql", file=str(path), season=season, season_type=season_type)
    subprocess.run(cmd, check=True)


def materialize_team_week_context(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/utilization/materialize_team_week_context.sql"), season, season_type)


def materialize_player_week(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/utilization/materialize_player_week.sql"), season, season_type)


def smoke_validate_receiving_events(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/utilization/receiving_events_by_player_week.sql"), season, season_type)

def backfill_weekly_from_pbp(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/utilization/backfill/write_weekly_from_pbp.sql"), season, season_type)



def materialize_player_week_stats(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/reports/materialize_player_week_stats.sql"), season, season_type)


def materialize_player_week_utilization(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/reports/materialize_player_week_utilization.sql"), season, season_type)


def materialize_player_week_utilization_receiving(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/reports/materialize_player_week_utilization_receiving.sql"), season, season_type)


def materialize_player_week_utilization_rushing(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/reports/materialize_player_week_utilization_rushing.sql"), season, season_type)


def materialize_player_week_utilization_wr(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/reports/materialize_player_week_utilization_wr.sql"), season, season_type)


def materialize_player_week_utilization_te(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/reports/materialize_player_week_utilization_te.sql"), season, season_type)


def materialize_player_week_utilization_rb(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/reports/materialize_player_week_utilization_rb.sql"), season, season_type)



def materialize_defense_position_points_allowed(season: int, season_type: str = "REG") -> None:
    _run_sql(Path("queries/reports/materialize_defense_position_points_allowed.sql"), season, season_type)

