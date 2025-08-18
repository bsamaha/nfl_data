# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import json
import pandas as pd
import yaml


def _ensure_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_yaml_to_rows(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    # Scoring rules
    scoring = doc.get("scoring", {}) or {}
    for metric, cfg in scoring.items():
        if not isinstance(cfg, dict):
            cfg = {"points": cfg}
        row: Dict[str, Any] = {
            "section": "scoring",
            "id": str(metric),
            "metric": str(metric),
            "points": cfg.get("points"),
            "per_yard": cfg.get("per_yard"),
            "per_units": cfg.get("per_units"),
            "units": cfg.get("units"),
            "threshold": cfg.get("threshold"),
            "bonus_points": cfg.get("bonus_points"),
            "modes": json.dumps(_ensure_list(cfg.get("modes"))),
            "types": json.dumps(_ensure_list(cfg.get("types"))),
            "notes": cfg.get("notes"),
        }
        rows.append(row)

    # Lineup slots
    lineup = doc.get("lineup", {}) or {}
    weekly_slots = lineup.get("weekly_slots", []) or []
    for slot in weekly_slots:
        row = {
            "section": "lineup",
            "id": f"slot_{slot.get('slot')}",
            "slot": slot.get("slot"),
            "count": slot.get("count"),
            "eligible_positions": json.dumps(_ensure_list(slot.get("eligible_positions"))),
        }
        rows.append(row)

    # Roster constraints
    roster = doc.get("roster", {}) or {}
    if roster:
        # Flat constraints
        for k in ("total_slots", "bench_slots", "min_teams", "max_qb", "max_te"):
            if k in roster:
                rows.append({
                    "section": "roster",
                    "id": k,
                    "constraint": k,
                    "value": roster.get(k),
                })
        # Auto-draft caps per position
        caps = roster.get("position_caps_during_auto_draft") or {}
        for pos, cap in (caps.items() if isinstance(caps, dict) else []):
            rows.append({
                "section": "roster",
                "id": f"auto_cap_{pos}",
                "constraint": "auto_draft_cap",
                "position": pos,
                "value": cap,
            })

    # Tournament rounds
    tournaments = doc.get("tournaments", {}) or {}
    rounds = tournaments.get("rounds", []) or []
    for rnd in rounds:
        row = {
            "section": "tournaments_rounds",
            "id": f"round_{rnd.get('round')}",
            "round": rnd.get("round"),
            "weeks": json.dumps(_ensure_list(rnd.get("weeks"))),
        }
        rows.append(row)
    if "tie_breakers" in tournaments:
        rows.append({
            "section": "tournaments",
            "id": "tie_breakers",
            "description": tournaments.get("tie_breakers"),
        })

    # Scoring period
    scoring_period = doc.get("scoring_period", {}) or {}
    for k, v in scoring_period.items():
        rows.append({
            "section": "scoring_period",
            "id": k,
            "property": k,
            "value": v,
        })

    # Draft timing rules (optional)
    draft = doc.get("draft", {}) or {}
    for k, v in draft.items():
        if k == "schedules":
            for sched in draft.get("schedules", []) or []:
                rows.append({
                    "section": "draft",
                    "id": f"schedule_{sched.get('label')}",
                    "label": sched.get("label"),
                    "start": sched.get("start"),
                    "fast_seconds_per_pick": sched.get("fast_seconds_per_pick"),
                    "slow_default_hours_per_pick": sched.get("slow_default_hours_per_pick"),
                    "overnight": json.dumps(_ensure_list(sched.get("overnight"))),
                    "notes": sched.get("notes"),
                })
        else:
            rows.append({
                "section": "draft",
                "id": k,
                "property": k,
                "value": v,
            })

    return rows


def fetch_dk_bestball(years: Optional[str] = None, options: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    # Read static YAML (path can be overridden by options)
    opts = options or {}
    yaml_path = Path(opts.get("path") or "catalog/draftkings/bestball.yml")
    doc = yaml.safe_load(yaml_path.read_text())
    rows = _normalize_yaml_to_rows(doc)
    df = pd.DataFrame(rows)
    # Stamp source for lineage/write_bronze to avoid defaulting to nflverse
    if "source" not in df.columns:
        df["source"] = "draftkings"
    # Ensure key/partition columns are strings
    for col in ("section", "id"):
        if col in df.columns:
            df[col] = df[col].astype(str)
    # Avoid mixed-type failures by making generic 'value' textual
    if "value" in df.columns:
        df["value"] = df["value"].astype("string")
    return df


