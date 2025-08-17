# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

from typing import Optional
import pandas as pd

from ..config import DatasetConfig
from .nflverse import (
    fetch_pbp,
    fetch_schedules,
    fetch_weekly,
    fetch_rosters,
    fetch_injuries,
    fetch_depth_charts,
    fetch_snap_counts,
    fetch_officials,
    fetch_win_totals,
    fetch_scoring_lines,
    fetch_draft_picks,
    fetch_combine,
)


def fetch_dataset_bootstrap(cfg: DatasetConfig, years: str) -> pd.DataFrame:
    if cfg.importer == "pbp":
        return fetch_pbp(years=years, options=cfg.options)
    if cfg.importer == "schedules":
        return fetch_schedules(years=years, options=cfg.options)
    if cfg.importer == "weekly":
        return fetch_weekly(years=years, options=cfg.options)
    if cfg.importer == "rosters":
        return fetch_rosters(years=years, options=cfg.options)
    if cfg.importer == "injuries":
        return fetch_injuries(years=years, options=cfg.options)
    if cfg.importer == "depth_charts":
        return fetch_depth_charts(years=years, options=cfg.options)
    if cfg.importer == "snap_counts":
        return fetch_snap_counts(years=years, options=cfg.options)
    if cfg.importer == "officials":
        return fetch_officials(years=years, options=cfg.options)
    if cfg.importer == "win_totals":
        return fetch_win_totals(years=years, options=cfg.options)
    if cfg.importer == "scoring_lines":
        return fetch_scoring_lines(years=years, options=cfg.options)
    if cfg.importer == "draft_picks":
        return fetch_draft_picks(years=years, options=cfg.options)
    if cfg.importer == "combine":
        return fetch_combine(years=years, options=cfg.options)
    raise NotImplementedError(f"Importer not implemented: {cfg.importer}")


def fetch_dataset_update(
    cfg: DatasetConfig, season: int, since: Optional[str]
) -> pd.DataFrame:
    if cfg.importer == "pbp":
        return fetch_pbp(years=str(season), options=cfg.options)
    if cfg.importer == "schedules":
        return fetch_schedules(years=str(season), options=cfg.options)
    if cfg.importer == "weekly":
        return fetch_weekly(years=str(season), options=cfg.options)
    if cfg.importer == "rosters":
        return fetch_rosters(years=str(season), options=cfg.options)
    if cfg.importer == "injuries":
        return fetch_injuries(years=str(season), options=cfg.options)
    if cfg.importer == "depth_charts":
        return fetch_depth_charts(years=str(season), options=cfg.options)
    if cfg.importer == "snap_counts":
        return fetch_snap_counts(years=str(season), options=cfg.options)
    if cfg.importer == "officials":
        return fetch_officials(years=str(season), options=cfg.options)
    if cfg.importer == "win_totals":
        return fetch_win_totals(years=str(season), options=cfg.options)
    if cfg.importer == "scoring_lines":
        return fetch_scoring_lines(years=str(season), options=cfg.options)
    if cfg.importer == "draft_picks":
        return fetch_draft_picks(years=str(season), options=cfg.options)
    if cfg.importer == "combine":
        return fetch_combine(years=str(season), options=cfg.options)
    raise NotImplementedError(f"Importer not implemented: {cfg.importer}")

