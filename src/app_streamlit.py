from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional

import duckdb
import polars as pl
import pandas as pd
import streamlit as st
import altair as alt


QUERIES_DIR = Path("queries")
RESEARCH_DIR = Path("research")


def list_sql_files() -> List[Path]:
    if not QUERIES_DIR.exists():
        return []
    return sorted([p for p in QUERIES_DIR.glob("**/*.sql")])


def run_sql(path: Path, params: dict[str, str] | None = None) -> pl.DataFrame:
    sql = path.read_text()
    # Simple parameter substitution for :name placeholders
    params = params or {}
    for k, v in params.items():
        # If value looks numeric, keep as-is; else quote
        if re.fullmatch(r"-?\d+(\.\d+)?", str(v)):
            repl = str(v)
        else:
            repl = f"'{v}'"
        sql = re.sub(fr"\:{re.escape(k)}\b", repl, sql)
    con = duckdb.connect()
    try:
        tbl = con.sql(sql).pl()
        return tbl
    finally:
        con.close()


def choose_chart(df: pl.DataFrame) -> str:
    cols = df.columns
    # Heuristics: prefer season/week on x, numeric on y
    if "week" in cols and df.schema.get("week") in (pl.Int32, pl.Int64, pl.UInt32, pl.UInt64):
        return "line"
    if "season" in cols and df.schema.get("season") in (pl.Int32, pl.Int64, pl.UInt32, pl.UInt64):
        return "line"
    # If one string and one numeric column, bar chart
    num_cols = [c for c, t in df.schema.items() if t in (pl.Int32, pl.Int64, pl.Float32, pl.Float64, pl.UInt32, pl.UInt64)]
    str_cols = [c for c, t in df.schema.items() if t == pl.Utf8]
    if len(str_cols) >= 1 and len(num_cols) >= 1:
        return "bar"
    # Fallback to table
    return "table"


def render_chart(df: pl.DataFrame, default_chart: Optional[str] = None) -> None:
    if df.height == 0:
        st.info("No rows returned.")
        return
    chart_type = default_chart or choose_chart(df)
    st.caption(f"Auto chart: {chart_type}")
    pdf = df.to_pandas()
    # Interactive column controls
    with st.sidebar:
        st.markdown("### Display options")
        cols = list(pdf.columns)
        visible_cols = st.multiselect("Columns", options=cols, default=cols)
        chart_choice = st.selectbox("Chart type", ["auto","line","bar","scatter","table"], index=0)
    if chart_choice != "auto":
        chart_type = chart_choice
    pdf = pdf[visible_cols] if visible_cols else pdf
    if chart_type == "line":
        x = st.selectbox("X axis", options=[c for c in pdf.columns if pdf[c].dtype.kind in ("i","f","M") or c in ("week","season")], index=0)
        y = st.multiselect("Y axis", options=[c for c in pdf.columns if pdf[c].dtype.kind in ("i","f")], default=[c for c in pdf.columns if pdf[c].dtype.kind in ("i","f")][:1])
        if x and y:
            chart = alt.Chart(pdf).mark_line().encode(x=x, y=alt.Y(alt.repeat("layer"), type='quantitative')).repeat(layer=y).interactive()
            st.altair_chart(chart, use_container_width=True)
        else:
            st.dataframe(pdf)
    elif chart_type == "bar":
        x = st.selectbox("Category", options=list(pdf.columns), index=0)
        y = st.selectbox("Value", options=[c for c in pdf.columns if pdf[c].dtype.kind in ("i","f")], index=0 if any(pdf[c].dtype.kind in ("i","f") for c in pdf.columns) else None)
        if y:
            chart = alt.Chart(pdf).mark_bar().encode(x=alt.X(x, sort='-y'), y=y, tooltip=list(pdf.columns)).interactive()
            st.altair_chart(chart, use_container_width=True)
        else:
            st.dataframe(pdf)
    elif chart_type == "scatter":
        x = st.selectbox("X", options=[c for c in pdf.columns if pdf[c].dtype.kind in ("i","f")], index=0)
        y = st.selectbox("Y", options=[c for c in pdf.columns if pdf[c].dtype.kind in ("i","f")], index=1 if len([c for c in pdf.columns if pdf[c].dtype.kind in ("i","f")])>1 else 0)
        color = st.selectbox("Color", options=[None] + list(pdf.columns), index=0)
        enc = {"x": x, "y": y}
        if color:
            enc["color"] = color
        chart = alt.Chart(pdf).mark_circle(size=60).encode(**enc).interactive()
        st.altair_chart(chart, use_container_width=True)
    else:
        st.dataframe(pdf)


@st.cache_data(show_spinner=False)
def load_research(name: str) -> pl.DataFrame:
    """Load a research output by stem name (prefers Parquet)."""
    pq = RESEARCH_DIR / f"{name}.parquet"
    csv = RESEARCH_DIR / f"{name}.csv"
    if pq.exists():
        return pl.read_parquet(pq)
    if csv.exists():
        return pl.read_csv(csv)
    return pl.DataFrame()


def render_macro_report() -> None:
    st.title("Macro Report — QA Dashboard")
    st.caption("Loads precomputed tables from research/; generate with `make macro-report`.")
    # Manual refresh to bust cache when research files change
    with st.sidebar:
        if st.button("Refresh data", help="Clear cached tables and reload from research/"):
            try:
                st.cache_data.clear()
            except Exception:
                pass
            st.rerun()
    # Load datasets (best-effort)
    league_week = load_research("league_week_metrics")
    eff_trends = load_research("league_efficiency_trends")
    league_yoy = load_research("league_yoy_metrics")
    pos_tiers = load_research("pos_tier_shares")
    flex_tiers = load_research("flex_tier_shares")
    by_roof = load_research("league_by_roof")
    team_roof = load_research("team_roof_counts")
    team_roof_week = load_research("team_roof_counts_by_week")

    tabs = st.tabs([
        "Environment",
        "Efficiency",
        "Pace & Tempo",
        "Explosives",
        "Roof Splits",
        "Shares & Tiers",
        "Raw Tables"
    ])

    # Environment (plays & touchdowns)
    with tabs[0]:
        if league_week.height == 0:
            st.info("league_week_metrics not found. Run `make macro-report`." )
        else:
            with st.expander("Definitions", expanded=False):
                st.markdown(
                    "- **plays_pg**: Offensive plays per game (league aggregate).\n"
                    "- **tds_pg**: Touchdowns per game (excludes field goals).\n"
                    "- **Season medians ± 1σ**: Median across weeks with ±1 standard deviation band.\n"
                    "- **Note**: Calendar-effect CIs will be added when modeling outputs are wired."
                )
            seasons = sorted(league_week.select("season").unique().to_series().to_list())
            s_start = st.selectbox("Season start", options=seasons, index=max(0, len(seasons)-15), key="env_start")
            s_end = st.selectbox("Season end", options=seasons, index=len(seasons)-1, key="env_end")
            sel = league_week.filter((pl.col("season")>=s_start) & (pl.col("season")<=s_end))

            view = st.radio("View", ["Weekly lines","Season aggregates","Heatmap"], index=1, horizontal=True, key="env_view")

            if view == "Weekly lines":
                has_tds = "tds_pg" in sel.columns
                cols = ["season","week","plays_pg"] + (["tds_pg"] if has_tds else (["ppg"] if "ppg" in sel.columns else []))
                pdf = sel.select(cols).to_pandas()
                if "week" in pdf.columns:
                    try:
                        pdf["week"] = pdf["week"].astype(int)
                    except Exception:
                        pass
                for col in [c for c in ["plays_pg","tds_pg","ppg"] if c in pdf.columns]:
                    try:
                        pdf[col] = pd.to_numeric(pdf[col], errors="coerce")
                    except Exception:
                        pass
                default_last = sorted(pdf["season"].unique())[-5:] if len(pdf) else []
                seasons_sel = st.multiselect("Seasons", options=sorted(pdf["season"].unique()), default=default_last, key="env_seasons_lines")
                if seasons_sel:
                    pdf = pdf[pdf["season"].isin(seasons_sel)]
                sel_leg = alt.selection_point(fields=["season"], bind="legend")
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("Plays per game")
                    line1 = alt.Chart(pdf).mark_line(size=2, interpolate='monotone').encode(
                        x=alt.X("week:O", title="Week"), y=alt.Y("plays_pg:Q", title="Plays/game"),
                        color=alt.Color("season:N", legend=alt.Legend(orient="bottom", title=None)),
                        opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O","plays_pg:Q"],
                    ).add_params(sel_leg)
                    pts1 = alt.Chart(pdf).mark_circle(size=28).encode(
                        x="week:O", y="plays_pg:Q", color="season:N",
                        opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O","plays_pg:Q"],
                    )
                    st.altair_chart((line1+pts1).interactive(), use_container_width=True)
                with c2:
                    metric_col = "tds_pg" if "tds_pg" in pdf.columns else ("ppg" if "ppg" in pdf.columns else None)
                    if metric_col is None:
                        st.info("No touchdowns or points per game available. Re-run `make macro-report`.")
                    else:
                        st.subheader("Touchdowns per game" if metric_col=="tds_pg" else "Points per game")
                        line2 = alt.Chart(pdf).mark_line(size=2, interpolate='monotone').encode(
                            x=alt.X("week:O", title="Week"), y=alt.Y(f"{metric_col}:Q", title=("TDs/game" if metric_col=="tds_pg" else "Points/game")),
                            color=alt.Color("season:N", legend=alt.Legend(orient="bottom", title=None)),
                            opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                            tooltip=["season:N","week:O", alt.Tooltip(f"{metric_col}:Q", title=("TDs/game" if metric_col=="tds_pg" else "Points/game"))],
                        ).add_params(sel_leg)
                        pts2 = alt.Chart(pdf).mark_circle(size=28).encode(
                            x="week:O", y=f"{metric_col}:Q", color="season:N",
                            opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                            tooltip=["season:N","week:O", alt.Tooltip(f"{metric_col}:Q", title=("TDs/game" if metric_col=="tds_pg" else "Points/game"))],
                        )
                        st.altair_chart((line2+pts2).interactive(), use_container_width=True)

            elif view == "Season aggregates":
                has_tds = "tds_pg" in sel.columns
                agg = sel.group_by(["season"]).agg([
                    pl.col("plays_pg").median().alias("plays_med"),
                    pl.col("plays_pg").std().alias("plays_std"),
                    (pl.col("tds_pg") if has_tds else pl.col("ppg")).median().alias("tds_med"),
                    (pl.col("tds_pg") if has_tds else pl.col("ppg")).std().alias("tds_std"),
                ]).to_pandas()
                if not agg.empty:
                    agg["plays_low"], agg["plays_high"] = agg["plays_med"] - agg["plays_std"], agg["plays_med"] + agg["plays_std"]
                    agg["tds_low"], agg["tds_high"] = agg["tds_med"] - agg["tds_std"], agg["tds_med"] + agg["tds_std"]
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("Season medians — Plays per game")
                    band = alt.Chart(agg).mark_area(opacity=0.25, color="#00E0A3").encode(
                        x="season:O", y="plays_low:Q", y2="plays_high:Q",
                        tooltip=["season:O", alt.Tooltip("plays_med:Q", title="Median"), alt.Tooltip("plays_std:Q", title="Std"), "plays_low:Q","plays_high:Q"],
                    )
                    line = alt.Chart(agg).mark_line(size=3, color="#00E0A3").encode(
                        x="season:O", y=alt.Y("plays_med:Q", title="Plays/game"), tooltip=["season:O", alt.Tooltip("plays_med:Q", title="Median"), alt.Tooltip("plays_std:Q", title="Std")],
                    )
                    st.altair_chart((band+line).interactive(), use_container_width=True)
                with c2:
                    st.subheader("Season medians — TDs per game" if has_tds else "Season medians — Points per game")
                    band2 = alt.Chart(agg).mark_area(opacity=0.25, color="#7BC8FF").encode(
                        x="season:O", y="tds_low:Q", y2="tds_high:Q",
                        tooltip=["season:O", alt.Tooltip("tds_med:Q", title="Median"), alt.Tooltip("tds_std:Q", title="Std"), "tds_low:Q","tds_high:Q"],
                    )
                    line2 = alt.Chart(agg).mark_line(size=3, color="#7BC8FF").encode(
                        x="season:O", y=alt.Y("tds_med:Q", title=("TDs/game" if has_tds else "Points/game")), tooltip=["season:O", alt.Tooltip("tds_med:Q", title="Median"), alt.Tooltip("tds_std:Q", title="Std")],
                    )
                    st.altair_chart((band2+line2).interactive(), use_container_width=True)

            else:  # Heatmap
                has_tds = "tds_pg" in sel.columns
                cols = ["season","week","plays_pg"] + (["tds_pg"] if has_tds else (["ppg"] if "ppg" in sel.columns else []))
                df = sel.select(cols).to_pandas()
                if "week" in df.columns:
                    try:
                        df["week"] = df["week"].astype(int)
                    except Exception:
                        pass
                st.subheader("Weekly pattern heatmaps")
                c1, c2 = st.columns(2)
                with c1:
                    hm1 = alt.Chart(df).mark_rect().encode(
                        x=alt.X("week:O", title="Week"), y=alt.Y("season:O", title="Season"),
                        color=alt.Color("plays_pg:Q", scale=alt.Scale(scheme='viridis')),
                        tooltip=["season:O","week:O", alt.Tooltip("plays_pg:Q", title="Plays/game")],
                    )
                    st.altair_chart(hm1.properties(height=500), use_container_width=True)
                with c2:
                    metric_col = "tds_pg" if has_tds else ("ppg" if "ppg" in df.columns else None)
                    if metric_col is None:
                        st.info("No touchdowns or points per game available. Re-run `make macro-report`.")
                    else:
                        hm2 = alt.Chart(df).mark_rect().encode(
                            x=alt.X("week:O", title="Week"), y=alt.Y("season:O", title="Season"),
                            color=alt.Color(f"{metric_col}:Q", scale=alt.Scale(scheme='viridis')),
                            tooltip=["season:O","week:O", alt.Tooltip(f"{metric_col}:Q", title=("TDs/game" if metric_col=="tds_pg" else "Points/game"))],
                        )
                        st.altair_chart(hm2.properties(height=500), use_container_width=True)

    # Efficiency (EPA splits)
    with tabs[1]:
        src = eff_trends if eff_trends.height>0 else (league_week.select(["season","week","epa_all","epa_pass","epa_rush"]) if league_week.height>0 else pl.DataFrame())
        if src.height == 0:
            st.info("No efficiency trends table available.")
        else:
            with st.expander("Definitions", expanded=False):
                st.markdown(
                    "- **EPA (Expected Points Added)**: A points-based value for each play measuring how much it changed the team’s expected points, given down, distance, field position, time, etc. Positive EPA means the offense improved its outlook; negative EPA means the defense won the play.\n"
                    "- **EPA/play**: Average EPA across plays (shown overall and split into pass vs rush). Rough guide: +0.20 is strong; 0 is average; −0.20 is poor.\n"
                    "- **Weekly lines**: Weekly series by season.\n"
                    "- **Season aggregates**: Median across weeks with ±1σ band."
                )
            seasons = sorted(src.select("season").unique().to_series().to_list())
            s_start = st.selectbox("Season start", options=seasons, index=max(0, len(seasons)-15), key="eff_start")
            s_end = st.selectbox("Season end", options=seasons, index=len(seasons)-1, key="eff_end")
            sel = src.filter((pl.col("season")>=s_start) & (pl.col("season")<=s_end))

            view = st.radio("View", ["Weekly lines","Season aggregates","Heatmap"], index=0, horizontal=True, key="eff_view")

            if view == "Weekly lines":
                pdf = sel.to_pandas()
                # Coerce types for safety
                if "week" in pdf.columns:
                    try:
                        pdf["week"] = pdf["week"].astype(int)
                    except Exception:
                        pass
                for col in ["epa_all","epa_pass","epa_rush"]:
                    if col in pdf.columns:
                        try:
                            pdf[col] = pd.to_numeric(pdf[col], errors="coerce")
                        except Exception:
                            pass
                # Default to last 5 seasons to reduce clutter
                default_last = sorted(pdf["season"].unique())[-5:] if len(pdf) else []
                seasons_sel = st.multiselect("Seasons", options=sorted(pdf["season"].unique()), default=default_last, key="eff_seasons_lines")
                if seasons_sel:
                    pdf = pdf[pdf["season"].isin(seasons_sel)]
                metric = st.selectbox("Metric", ["epa_all","epa_pass","epa_rush"], index=0)
                sel_leg = alt.selection_point(fields=["season"], bind="legend")
                line = alt.Chart(pdf).mark_line(size=2, interpolate='monotone').encode(
                    x=alt.X("week:O", title="Week"), y=alt.Y(f"{metric}:Q", title=metric),
                    color=alt.Color("season:N", legend=alt.Legend(orient="bottom", title=None)),
                    opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                    tooltip=["season:N","week:O", alt.Tooltip(f"{metric}:Q", title=metric)],
                ).add_params(sel_leg)
                pts = alt.Chart(pdf).mark_circle(size=28).encode(
                    x="week:O", y=f"{metric}:Q", color="season:N",
                    opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                    tooltip=["season:N","week:O", alt.Tooltip(f"{metric}:Q", title=metric)],
                )
                st.altair_chart((line+pts).interactive(), use_container_width=True)

            elif view == "Season aggregates":
                agg = sel.group_by("season").agg([
                    pl.col("epa_all").median().alias("all_med"),
                    pl.col("epa_all").std().alias("all_std"),
                    pl.col("epa_pass").median().alias("pass_med"),
                    pl.col("epa_pass").std().alias("pass_std"),
                    pl.col("epa_rush").median().alias("rush_med"),
                    pl.col("epa_rush").std().alias("rush_std"),
                ]).to_pandas()
                def band_and_line(df, med, std, title):
                    df[f"{med}_low"] = df[med] - df[std]
                    df[f"{med}_high"] = df[med] + df[std]
                    band = alt.Chart(df).mark_area(opacity=0.25, color="#00E0A3").encode(
                        x="season:O", y=f"{med}_low:Q", y2=f"{med}_high:Q",
                        tooltip=["season:O", alt.Tooltip(f"{med}:Q", title="Median"), alt.Tooltip(f"{std}:Q", title="Std"), alt.Tooltip(f"{med}_low:Q", title="Low (−1σ)"), alt.Tooltip(f"{med}_high:Q", title="High (+1σ)")],
                    )
                    line = alt.Chart(df).mark_line(size=3, color="#00E0A3").encode(
                        x="season:O", y=alt.Y(f"{med}:Q", title=title), tooltip=["season:O", alt.Tooltip(f"{med}:Q", title="Median"), alt.Tooltip(f"{std}:Q", title="Std")],
                    )
                    pts = alt.Chart(df).mark_point(size=60, color="#00E0A3", filled=True).encode(
                        x="season:O", y=f"{med}:Q", tooltip=["season:O", alt.Tooltip(f"{med}:Q", title="Median"), alt.Tooltip(f"{std}:Q", title="Std")],
                    )
                    return band + line + pts
                st.subheader("Season medians — EPA/play (all)")
                st.altair_chart(band_and_line(agg.copy(), "all_med", "all_std", "EPA/play"), use_container_width=True)
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("Pass EPA/play")
                    st.altair_chart(band_and_line(agg.copy(), "pass_med", "pass_std", "Pass EPA/play"), use_container_width=True)
                with c2:
                    st.subheader("Rush EPA/play")
                    st.altair_chart(band_and_line(agg.copy(), "rush_med", "rush_std", "Rush EPA/play"), use_container_width=True)

            else:  # Heatmap
                metric = st.selectbox("Metric", ["epa_all","epa_pass","epa_rush"], index=0, key="eff_heat_metric")
                df = sel.select(["season","week", metric]).to_pandas()
                if "week" in df.columns:
                    try:
                        df["week"] = df["week"].astype(int)
                    except Exception:
                        pass
                st.subheader("Weekly pattern heatmap")
                hm = alt.Chart(df).mark_rect().encode(
                    x=alt.X("week:O", title="Week"), y=alt.Y("season:O", title="Season"),
                    color=alt.Color(f"{metric}:Q", scale=alt.Scale(scheme='viridis')),
                    tooltip=["season:O","week:O", alt.Tooltip(f"{metric}:Q", title=metric)],
                )
                st.altair_chart(hm.properties(height=500), use_container_width=True)

    # Pace & Tempo
    with tabs[2]:
        if league_week.height == 0:
            st.info("league_week_metrics not found.")
        else:
            with st.expander("Definitions", expanded=False):
                st.markdown(
                    "- **sec_per_play_neutral**: Seconds per play in neutral situations.\n"
                    "- **no_huddle_rate**: Share of plays with no-huddle.\n"
                    "- **shotgun_rate**: Share of plays from shotgun."
                )
            seasons = sorted(league_week.select("season").unique().to_series().to_list())
            s_start = st.selectbox("Season start", options=seasons, index=max(0, len(seasons)-15), key="pace_start")
            s_end = st.selectbox("Season end", options=seasons, index=len(seasons)-1, key="pace_end")
            sel = league_week.filter((pl.col("season")>=s_start) & (pl.col("season")<=s_end))

            view = st.radio("View", ["Weekly lines","Season aggregates","Heatmap"], index=1, horizontal=True)

            if view == "Weekly lines":
                pdf = sel.to_pandas()
                if "week" in pdf.columns:
                    try:
                        pdf["week"] = pdf["week"].astype(int)
                    except Exception:
                        pass
                default_last = sorted(pdf["season"].unique())[-5:] if len(pdf) else []
                seasons_sel = st.multiselect("Seasons", options=sorted(pdf["season"].unique()), default=default_last, key="pace_seasons_lines")
                if seasons_sel:
                    pdf = pdf[pdf["season"].isin(seasons_sel)]
                sel_pts = alt.selection_point(fields=["season"], bind="legend")
                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("Seconds per play (neutral)")
                    line = alt.Chart(pdf).mark_line(size=2, interpolate='monotone').encode(
                        x=alt.X("week:O", title="Week"),
                        y=alt.Y("sec_per_play_neutral:Q", title="Sec/play (neutral)"),
                        color=alt.Color("season:N", legend=alt.Legend(orient="bottom", title=None)),
                        opacity=alt.condition(sel_pts, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O","sec_per_play_neutral:Q"],
                    ).add_params(sel_pts)
                    pts = alt.Chart(pdf).mark_circle(size=28).encode(
                        x="week:O", y="sec_per_play_neutral:Q", color="season:N",
                        opacity=alt.condition(sel_pts, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O","sec_per_play_neutral:Q"],
                    )
                    st.altair_chart((line+pts).interactive(), use_container_width=True)
                with c2:
                    st.subheader("No-huddle and shotgun")
                    nh = alt.Chart(pdf).mark_line(size=2, interpolate='monotone').encode(
                        x=alt.X("week:O", title="Week"), y=alt.Y("no_huddle_rate:Q", title="No-huddle rate"),
                        color=alt.Color("season:N", legend=alt.Legend(orient="bottom", title=None)),
                        opacity=alt.condition(sel_pts, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O","no_huddle_rate:Q"],
                    ).add_params(sel_pts)
                    nh_pts = alt.Chart(pdf).mark_circle(size=28).encode(
                        x="week:O", y="no_huddle_rate:Q", color="season:N",
                        opacity=alt.condition(sel_pts, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O","no_huddle_rate:Q"],
                    )
                    st.altair_chart((nh+nh_pts).interactive(), use_container_width=True)
                    sg = alt.Chart(pdf).mark_line(size=2, interpolate='monotone').encode(
                        x=alt.X("week:O", title="Week"), y=alt.Y("shotgun_rate:Q", title="Shotgun rate"),
                        color=alt.Color("season:N", legend=alt.Legend(orient="bottom", title=None)),
                        opacity=alt.condition(sel_pts, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O","shotgun_rate:Q"],
                    ).add_params(sel_pts)
                    sg_pts = alt.Chart(pdf).mark_circle(size=28).encode(
                        x="week:O", y="shotgun_rate:Q", color="season:N",
                        opacity=alt.condition(sel_pts, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O","shotgun_rate:Q"],
                    )
                    st.altair_chart((sg+sg_pts).interactive(), use_container_width=True)

            elif view == "Season aggregates":
                agg = sel.group_by("season").agg([
                    pl.col("sec_per_play_neutral").median().alias("sec_med"),
                    pl.col("sec_per_play_neutral").std().alias("sec_std"),
                    pl.col("no_huddle_rate").median().alias("nh_med"),
                    pl.col("no_huddle_rate").std().alias("nh_std"),
                    pl.col("shotgun_rate").median().alias("sg_med"),
                    pl.col("shotgun_rate").std().alias("sg_std"),
                ]).to_pandas()
                agg["sec_low"] = agg["sec_med"] - agg["sec_std"]
                agg["sec_high"] = agg["sec_med"] + agg["sec_std"]
                band = alt.Chart(agg).mark_area(opacity=0.25, color="#00E0A3").encode(
                    x="season:O", y="sec_low:Q", y2="sec_high:Q",
                    tooltip=["season:O", alt.Tooltip("sec_med:Q", title="Median"), alt.Tooltip("sec_std:Q", title="Std"), "sec_low:Q","sec_high:Q"],
                )
                line = alt.Chart(agg).mark_line(size=3, color="#00E0A3").encode(
                    x="season:O", y=alt.Y("sec_med:Q", title="Sec/play (neutral)"), tooltip=["season:O","sec_med:Q","sec_std:Q"],
                )
                st.subheader("Season medians — Seconds per play (neutral)")
                st.altair_chart((band+line).interactive(), use_container_width=True)
                agg["nh_low"] = agg["nh_med"] - agg["nh_std"]
                agg["nh_high"] = agg["nh_med"] + agg["nh_std"]
                nh_band = alt.Chart(agg).mark_area(opacity=0.25, color="#7BC8FF").encode(
                    x="season:O", y="nh_low:Q", y2="nh_high:Q",
                    tooltip=["season:O", alt.Tooltip("nh_med:Q", title="Median"), alt.Tooltip("nh_std:Q", title="Std"), "nh_low:Q","nh_high:Q"],
                )
                nh_line = alt.Chart(agg).mark_line(size=3, color="#7BC8FF").encode(
                    x="season:O", y=alt.Y("nh_med:Q", title="No-huddle rate"), tooltip=["season:O","nh_med:Q","nh_std:Q"],
                )
                agg["sg_low"] = agg["sg_med"] - agg["sg_std"]
                agg["sg_high"] = agg["sg_med"] + agg["sg_std"]
                sg_band = alt.Chart(agg).mark_area(opacity=0.25, color="#FEB06A").encode(
                    x="season:O", y="sg_low:Q", y2="sg_high:Q",
                    tooltip=["season:O", alt.Tooltip("sg_med:Q", title="Median"), alt.Tooltip("sg_std:Q", title="Std"), "sg_low:Q","sg_high:Q"],
                )
                sg_line = alt.Chart(agg).mark_line(size=3, color="#FEB06A").encode(
                    x="season:O", y=alt.Y("sg_med:Q", title="Shotgun rate"), tooltip=["season:O","sg_med:Q","sg_std:Q"],
                )
                c3, c4 = st.columns(2)
                with c3:
                    st.subheader("Season medians — No-huddle rate")
                    st.altair_chart((nh_band+nh_line).interactive(), use_container_width=True)
                with c4:
                    st.subheader("Season medians — Shotgun rate")
                    st.altair_chart((sg_band+sg_line).interactive(), use_container_width=True)

            else:  # Heatmap
                metric = st.selectbox("Metric", [
                    "sec_per_play_neutral","no_huddle_rate","shotgun_rate"
                ], index=0, key="pace_heat_metric")
                df = sel.select(["season","week", metric]).to_pandas()
                if "week" in df.columns:
                    try:
                        df["week"] = df["week"].astype(int)
                    except Exception:
                        pass
                st.subheader("Weekly pattern heatmap")
                hm = alt.Chart(df).mark_rect().encode(
                    x=alt.X("week:O", title="Week"),
                    y=alt.Y("season:O", title="Season"),
                    color=alt.Color(f"{metric}:Q", scale=alt.Scale(scheme='viridis')),
                    tooltip=["season:O","week:O", alt.Tooltip(f"{metric}:Q", title=metric)],
                )
                st.altair_chart(hm.properties(height=500), use_container_width=True)

    # Explosives
    with tabs[3]:
        if league_week.height == 0:
            st.info("league_week_metrics not found.")
        else:
            with st.expander("Definitions", expanded=False):
                st.markdown(
                    "- **explosive_pass_rate**: Share of pass plays classified as explosive.\n"
                    "- **explosive_rush10_rate / explosive_rush15_rate**: Share of rushes gaining ≥10 or ≥15 yards.\n"
                    "- **explosive_epa_share_pass/rush**: Share of total EPA from explosive pass/rush plays.\n"
                    "- **Note**: Calendar-effect CIs will be added when modeling outputs are wired."
                )
            seasons = sorted(league_week.select("season").unique().to_series().to_list())
            s_start = st.selectbox("Season start", options=seasons, index=max(0, len(seasons)-15), key="exp_start")
            s_end = st.selectbox("Season end", options=seasons, index=len(seasons)-1, key="exp_end")
            metric = st.selectbox("Explosive metric", [
                "explosive_pass_rate","explosive_rush10_rate","explosive_rush15_rate","explosive_epa_share_pass","explosive_epa_share_rush"
            ], index=0)
            sel = league_week.filter((pl.col("season")>=s_start) & (pl.col("season")<=s_end))

            view = st.radio("View", ["Weekly lines","Season aggregates","Heatmap"], index=1, horizontal=True, key="exp_view")

            if view == "Weekly lines":
                pdf = sel.select(["season","week", metric]).to_pandas()
                # enforce integer weeks and numeric metric
                if "week" in pdf.columns:
                    try:
                        pdf["week"] = pdf["week"].astype(int)
                    except Exception:
                        pass
                try:
                    pdf[metric] = pd.to_numeric(pdf[metric], errors="coerce")
                except Exception:
                    pass
                default_last = sorted(pdf["season"].unique())[-5:] if len(pdf) else []
                seasons_sel = st.multiselect("Seasons", options=sorted(pdf["season"].unique()), default=default_last, key="exp_seasons_lines")
                if seasons_sel:
                    pdf = pdf[pdf["season"].isin(seasons_sel)]
                sel_pts = alt.selection_point(fields=["season"], bind="legend")
                line = alt.Chart(pdf).mark_line(size=2, interpolate='monotone').encode(
                    x=alt.X("week:O", title="Week"), y=alt.Y(f"{metric}:Q", title=metric),
                    color=alt.Color("season:N", legend=alt.Legend(orient="bottom", title=None)),
                    opacity=alt.condition(sel_pts, alt.value(1.0), alt.value(0.2)),
                    tooltip=["season:N","week:O", alt.Tooltip(f"{metric}:Q", title=metric)],
                ).add_params(sel_pts)
                pts = alt.Chart(pdf).mark_circle(size=28).encode(
                    x="week:O", y=f"{metric}:Q", color="season:N",
                    opacity=alt.condition(sel_pts, alt.value(1.0), alt.value(0.2)),
                    tooltip=["season:N","week:O", alt.Tooltip(f"{metric}:Q", title=metric)],
                )
                st.altair_chart((line+pts).interactive(), use_container_width=True)

            elif view == "Season aggregates":
                agg = sel.group_by("season").agg([
                    pl.col(metric).median().alias("med"),
                    pl.col(metric).std().alias("std"),
                ]).to_pandas()
                agg["low"] = agg["med"] - agg["std"]
                agg["high"] = agg["med"] + agg["std"]
                band = alt.Chart(agg).mark_area(opacity=0.25, color="#00E0A3").encode(
                    x="season:O", y="low:Q", y2="high:Q",
                    tooltip=["season:O", alt.Tooltip("med:Q", title="Median"), alt.Tooltip("std:Q", title="Std"), alt.Tooltip("low:Q", title="Low (−1σ)"), alt.Tooltip("high:Q", title="High (+1σ)")],
                )
                line = alt.Chart(agg).mark_line(size=3, color="#00E0A3").encode(
                    x="season:O", y=alt.Y("med:Q", title=metric), tooltip=["season:O", alt.Tooltip("med:Q", title="Median"), alt.Tooltip("std:Q", title="Std")],
                )
                pts = alt.Chart(agg).mark_point(size=60, color="#00E0A3", filled=True).encode(
                    x="season:O", y="med:Q", tooltip=["season:O", alt.Tooltip("med:Q", title="Median"), alt.Tooltip("std:Q", title="Std")],
                )
                st.altair_chart((band+line+pts).interactive(), use_container_width=True)

            else:  # Heatmap
                df = sel.select(["season","week", metric]).to_pandas()
                if "week" in df.columns:
                    try:
                        df["week"] = df["week"].astype(int)
                    except Exception:
                        pass
                st.subheader("Weekly pattern heatmap")
                hm = alt.Chart(df).mark_rect().encode(
                    x=alt.X("week:O", title="Week"), y=alt.Y("season:O", title="Season"),
                    color=alt.Color(f"{metric}:Q", scale=alt.Scale(scheme='viridis')),
                    tooltip=["season:O","week:O", alt.Tooltip(f"{metric}:Q", title=metric)],
                )
                st.altair_chart(hm.properties(height=500), use_container_width=True)

    # Roof splits
    with tabs[4]:
        if by_roof.height == 0:
            st.info("league_by_roof not found.")
        else:
            with st.expander("Definitions", expanded=False):
                st.markdown(
                    "- **roof_class**: Stadium roof classification (e.g., open, closed, retractable).\n"
                    "- **Metrics**: Same definitions as other tabs (ppg, EPA/play, pace, explosives).\n"
                    "- **Season aggregates**: Median across weeks with ±1σ band by roof class.\n"
                    "- **Note**: 95% CIs for roof deltas will appear once modeling outputs are wired."
                )
            base_metrics = [
                "plays_pg","ppg","epa_all","epa_pass","epa_rush","sec_per_play_all","no_huddle_rate","shotgun_rate","explosive_pass_rate","explosive_rush10_rate","explosive_rush15_rate"
            ]
            metrics = [m for m in base_metrics if m in by_roof.columns]
            if "tds_pg" in by_roof.columns:
                metrics.insert(1, "tds_pg")
            metric = st.selectbox("Metric", metrics, index=0, key="roof_metric")
            seasons = sorted(by_roof.select("season").unique().to_series().to_list())
            s_start = st.selectbox("Season start", options=seasons, index=max(0, len(seasons)-15), key="roof_start")
            s_end = st.selectbox("Season end", options=seasons, index=len(seasons)-1, key="roof_end")
            sel = by_roof.filter((pl.col("season")>=s_start) & (pl.col("season")<=s_end))
            view = st.radio("View", ["Weekly lines","Season aggregates","Heatmap"], index=1, horizontal=True, key="roof_view")
            show_net = st.checkbox("Show net (Indoor − Outdoor)", value=True, key="roof_net")

            if view == "Weekly lines":
                if show_net:
                    try:
                        pdf_all = sel.select(["season","week","roof_class", metric]).to_pandas()
                        pt = pd.pivot_table(pdf_all, index=["season","week"], columns="roof_class", values=metric, aggfunc="mean").reset_index()
                        pt["INDOOR"] = pt.get("INDOOR", 0)
                        pt["OUTDOOR"] = pt.get("OUTDOOR", 0)
                        net = pt[["season","week"]].copy()
                        net["net"] = pt["INDOOR"] - pt["OUTDOOR"]
                    except Exception:
                        net = pd.DataFrame(columns=["season","week","net"])  # type: ignore
                    if "week" in net.columns:
                        try:
                            net["week"] = net["week"].astype(int)
                        except Exception:
                            pass
                    default_last = sorted(net["season"].dropna().unique())[-3:] if len(net) else []
                    seasons_sel = st.multiselect("Seasons", options=sorted(net["season"].dropna().unique()), default=default_last, key="roof_net_lines")
                    if seasons_sel:
                        net = net[net["season"].isin(seasons_sel)]
                    sel_leg = alt.selection_point(fields=["season"], bind="legend")
                    line = alt.Chart(net).mark_line(size=2, interpolate='monotone').encode(
                        x=alt.X("week:O", title="Week"), y=alt.Y("net:Q", title=f"Indoor − Outdoor ({metric})"),
                        color=alt.Color("season:N", legend=alt.Legend(orient="bottom", title=None)),
                        opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O", alt.Tooltip("net:Q", title="Net")],
                    ).add_params(sel_leg)
                    pts = alt.Chart(net).mark_circle(size=28).encode(
                        x="week:O", y="net:Q", color="season:N",
                        opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:N","week:O", alt.Tooltip("net:Q", title="Net")],
                    )
                    st.altair_chart((line+pts).interactive(), use_container_width=True)
                else:
                    pdf = sel.select(["season","week","roof_class", metric]).to_pandas()
                if "week" in pdf.columns:
                    try:
                        pdf["week"] = pdf["week"].astype(int)
                    except Exception:
                        pass
                try:
                    pdf[metric] = pd.to_numeric(pdf[metric], errors="coerce")
                except Exception:
                    pass
                default_last = sorted(pdf["season"].unique())[-3:] if len(pdf) else []
                seasons_sel = st.multiselect("Seasons", options=sorted(pdf["season"].unique()), default=default_last, key="roof_seasons_lines")
                if seasons_sel:
                    pdf = pdf[pdf["season"].isin(seasons_sel)]
                sel_leg = alt.selection_point(fields=["roof_class"], bind="legend")
                line = alt.Chart(pdf).mark_line(size=2, interpolate='monotone').encode(
                    x=alt.X("week:O", title="Week"), y=alt.Y(f"{metric}:Q", title=metric),
                    color=alt.Color("roof_class:N", legend=alt.Legend(orient="bottom", title=None)),
                    opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                    tooltip=["season:N","week:O","roof_class:N", alt.Tooltip(f"{metric}:Q", title=metric)],
                ).add_params(sel_leg)
                pts = alt.Chart(pdf).mark_circle(size=28).encode(
                    x="week:O", y=f"{metric}:Q", color="roof_class:N",
                    opacity=alt.condition(sel_leg, alt.value(1.0), alt.value(0.2)),
                    tooltip=["season:N","week:O","roof_class:N", alt.Tooltip(f"{metric}:Q", title=metric)],
                )
                st.altair_chart((line+pts).interactive(), use_container_width=True)

            elif view == "Season aggregates":
                if show_net:
                    try:
                        pdf_all = sel.select(["season","week","roof_class", metric]).to_pandas()
                        pt = pd.pivot_table(pdf_all, index=["season","week"], columns="roof_class", values=metric, aggfunc="mean").reset_index()
                        pt["INDOOR"] = pt.get("INDOOR", 0)
                        pt["OUTDOOR"] = pt.get("OUTDOOR", 0)
                        pt["net"] = pt["INDOOR"] - pt["OUTDOOR"]
                        agg = pt.groupby("season")["net"].agg(["median","std"]).reset_index().rename(columns={"median":"med","std":"std"})
                    except Exception:
                        agg = pd.DataFrame(columns=["season","med","std"])  # type: ignore
                    agg["low"], agg["high"] = agg["med"] - agg["std"], agg["med"] + agg["std"]
                    band = alt.Chart(agg).mark_area(opacity=0.25, color="#00E0A3").encode(
                        x="season:O", y="low:Q", y2="high:Q",
                        tooltip=["season:O", alt.Tooltip("med:Q", title="Median"), alt.Tooltip("std:Q", title="Std"), "low:Q","high:Q"],
                    )
                    line = alt.Chart(agg).mark_line(size=3, color="#00E0A3").encode(
                        x="season:O", y=alt.Y("med:Q", title=f"Indoor − Outdoor ({metric})"), tooltip=["season:O", alt.Tooltip("med:Q", title="Median"), alt.Tooltip("std:Q", title="Std")],
                    )
                    st.altair_chart((band+line).interactive(), use_container_width=True)
                else:
                    agg = sel.group_by(["season","roof_class"]).agg([
                        pl.col(metric).median().alias("med"),
                        pl.col(metric).std().alias("std"),
                    ]).to_pandas()
                    agg["low"] = agg["med"] - agg["std"]
                    agg["high"] = agg["med"] + agg["std"]
                    sel_leg2 = alt.selection_point(fields=["roof_class"], bind="legend")
                    band = alt.Chart(agg).mark_area(opacity=0.18).encode(
                        x="season:O", y="low:Q", y2="high:Q",
                        color=alt.Color("roof_class:N", legend=alt.Legend(orient="bottom", title=None)),
                        opacity=alt.condition(sel_leg2, alt.value(0.7), alt.value(0.05)),
                        tooltip=["season:O","roof_class:N", alt.Tooltip("med:Q", title="Median"), alt.Tooltip("std:Q", title="Std"), "low:Q","high:Q"],
                    ).add_params(sel_leg2)
                    line = alt.Chart(agg).mark_line(size=3).encode(
                        x="season:O", y=alt.Y("med:Q", title=metric), color="roof_class:N",
                        opacity=alt.condition(sel_leg2, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:O","roof_class:N", alt.Tooltip("med:Q", title="Median"), alt.Tooltip("std:Q", title="Std")],
                    )
                    pts = alt.Chart(agg).mark_point(size=60, filled=True).encode(
                        x="season:O", y="med:Q", color="roof_class:N",
                        opacity=alt.condition(sel_leg2, alt.value(1.0), alt.value(0.2)),
                        tooltip=["season:O","roof_class:N", alt.Tooltip("med:Q", title="Median"), alt.Tooltip("std:Q", title="Std")],
                    )
                    st.altair_chart((band+line+pts).interactive(), use_container_width=True)

            else:  # Heatmap
                if show_net:
                    try:
                        pdf_all = sel.select(["season","week","roof_class", metric]).to_pandas()
                        pt = pd.pivot_table(pdf_all, index=["season","week"], columns="roof_class", values=metric, aggfunc="mean").reset_index()
                        pt["INDOOR"] = pt.get("INDOOR", 0)
                        pt["OUTDOOR"] = pt.get("OUTDOOR", 0)
                        net = pt[["season","week"]].copy()
                        net["net"] = pt["INDOOR"] - pt["OUTDOOR"]
                    except Exception:
                        net = pd.DataFrame(columns=["season","week","net"])  # type: ignore
                    if "week" in net.columns:
                        try:
                            net["week"] = net["week"].astype(int)
                        except Exception:
                            pass
                    st.subheader("Weekly pattern heatmap — Net (Indoor − Outdoor)")
                    hm = alt.Chart(net).mark_rect().encode(
                        x=alt.X("week:O", title="Week"), y=alt.Y("season:O", title="Season"),
                        color=alt.Color("net:Q", scale=alt.Scale(scheme='viridis')),
                        tooltip=["season:O","week:O", alt.Tooltip("net:Q", title="Net")],
                    )
                    st.altair_chart(hm.properties(height=500), use_container_width=True)
                else:
                    classes = sorted(sel.select("roof_class").unique().to_series().to_list())
                    roof_sel = st.selectbox("Roof class", options=classes, index=0)
                    df = sel.filter(pl.col("roof_class") == roof_sel).select(["season","week", metric]).to_pandas()
                    if "week" in df.columns:
                        try:
                            df["week"] = df["week"].astype(int)
                        except Exception:
                            pass
                    st.subheader(f"Weekly pattern heatmap — {roof_sel}")
                    hm = alt.Chart(df).mark_rect().encode(
                        x=alt.X("week:O", title="Week"), y=alt.Y("season:O", title="Season"),
                        color=alt.Color(f"{metric}:Q", scale=alt.Scale(scheme='viridis')),
                        tooltip=["season:O","week:O", alt.Tooltip(f"{metric}:Q", title=metric)],
                    )
                    st.altair_chart(hm.properties(height=500), use_container_width=True)

            # Team games by roof type (counts)
            st.markdown("---")
            st.subheader("Indoor games by team (counts)")
            if team_roof.height == 0:
                st.info("team_roof_counts not found. Run make macro-report, then use Refresh data.")
            else:
                seasons_tr = sorted(team_roof.select("season").unique().to_series().to_list())
                season_pick = st.selectbox("Season", options=seasons_tr, index=len(seasons_tr)-1, key="roof_team_season")

                # New: optional filter to only count weeks 15–17 (all games)
                only_playoff_weeks = st.checkbox("Show fantasy playoffs only (Weeks 15–17)", value=False, help="Counts indoor games in Weeks 15–17 (home and away).")

                if only_playoff_weeks and team_roof_week.height > 0:
                    # Filter to selected season, weeks 15–17 (home and away)
                    filt = (
                        (pl.col("season") == season_pick) &
                        (pl.col("week").is_between(15, 17))
                    )
                    by_wk = team_roof_week.filter(filt)

                    # Build per-team, per-week indoor-only counts (0 or 1 per week)
                    indoor_week = (
                        by_wk.filter(pl.col("roof_class") == "INDOOR")
                            .group_by(["team", "week"]).agg(pl.col("games").sum().alias("games"))
                            .sort(["team", "week"])  # stable order
                    )
                    df_week = indoor_week.to_pandas()

                    # Collect team list and overall totals for sorting/filters
                    teams_all = sorted(by_wk.select("team").unique().to_series().to_list())
                    team_totals = (
                        df_week.groupby("team", as_index=False)["games"].sum().rename(columns={"games": "total_games"})
                    )
                    # Optional team filter
                    teams_sel = st.multiselect("Teams", options=teams_all, default=teams_all, key="roof_team_filter")
                    if teams_sel:
                        df_week = df_week[df_week["team"].isin(teams_sel)]
                        team_totals = team_totals[team_totals["team"].isin(teams_sel)]

                    # Stacked bars by week (15,16,17)
                    week_order = [15, 16, 17]
                    stacked = alt.Chart(df_week).mark_bar().encode(
                        x=alt.X("team:N", sort='-y', title="Team"),
                        y=alt.Y("sum(games):Q", title="Indoor games (Weeks 15–17)"),
                        color=alt.Color("week:O", title="Week", sort=week_order, legend=alt.Legend(orient="bottom")),
                        tooltip=["team:N", "week:O", alt.Tooltip("games:Q", title="Indoor games")],
                    )
                    st.altair_chart(stacked.properties(height=420), use_container_width=True)
                    # Skip the non-stacked bar below when playoffs-only view is enabled
                    return
                else:
                    # Default behavior: full-season counts by team × roof_class
                    pdf_tr = team_roof.filter(pl.col("season") == season_pick).to_pandas()
                    teams_all = sorted(pdf_tr["team"].unique().tolist())
                    indoor_counts = (
                        pdf_tr.loc[pdf_tr["roof_class"] == "INDOOR", ["team", "games"]]
                        .groupby("team", as_index=False)
                        .sum()
                    )
                # Gather all teams present in the season
                teams_all = sorted(pdf_tr["team"].unique().tolist())
                # Count only INDOOR games (includes closed retractables)
                indoor_counts = (
                    pdf_tr.loc[pdf_tr["roof_class"] == "INDOOR", ["team", "games"]]
                    .groupby("team", as_index=False)
                    .sum()
                )
                # Ensure teams with zero indoor games are included
                all_df = pd.DataFrame({"team": teams_all}).merge(indoor_counts, on="team", how="left").fillna({"games": 0})
                # Optional team filter
                teams_sel = st.multiselect("Teams", options=teams_all, default=teams_all, key="roof_team_filter")
                if teams_sel:
                    all_df = all_df[all_df["team"].isin(teams_sel)]
                # Sort by descending indoor games
                all_df = all_df.sort_values("games", ascending=False)
                bars = alt.Chart(all_df).mark_bar().encode(
                    x=alt.X("team:N", sort='-y', title="Team"),
                    y=alt.Y("games:Q", title="Indoor games"),
                    tooltip=["team:N", alt.Tooltip("games:Q", title="Indoor games")],
                )
                st.altair_chart(bars.properties(height=420), use_container_width=True)

    # Shares & Tiers
    with tabs[5]:
        if pos_tiers.height == 0 or flex_tiers.height == 0:
            st.info("pos_tier_shares or flex_tier_shares not found.")
        else:
            with st.expander("Definitions", expanded=False):
                st.markdown(
                    "- **Tier**: Rank bucket within a position (e.g., 1..12).\n"
                    "- **share**: Fraction of total positional points attributed to each tier.\n"
                    "- **QA**: Shares by season × position should sum to 1."
                )
            st.subheader("Position tier shares (Tier=12)")
            pos = st.selectbox("Position", ["QB","RB","WR","TE"], index=1)
            metric = st.selectbox("Display", ["share","mean_pts","median_pts","min_pts"], index=0)
            pdf = pos_tiers.filter(pl.col("position")==pos).to_pandas()
            chart = alt.Chart(pdf).mark_line().encode(
                x=alt.X("season:O"), y=alt.Y(f"{metric}:Q"), color="tier:N", tooltip=list(pdf.columns)
            ).interactive()
            st.altair_chart(chart, use_container_width=True)
            st.subheader("QA: Shares sum to 1 by season × position")
            qa = pos_tiers.group_by(["season","position"]).agg(pl.col("share").sum().alias("sum_share")).to_pandas()
            st.dataframe(qa)
            st.subheader("FLEX tier shares")
            pdf2 = flex_tiers.to_pandas()
            chart2 = alt.Chart(pdf2).mark_line().encode(
                x=alt.X("season:O"), y=alt.Y("share:Q"), color="tier:N"
            ).interactive()
            st.altair_chart(chart2, use_container_width=True)

    # Raw tables for download/inspection
    with tabs[6]:
        st.markdown("Download raw tables from research/")
        with st.expander("Definitions", expanded=False):
            st.markdown(
                "- **Source**: These are precomputed outputs saved under `research/`.\n"
                "- **Tip**: Use the download buttons for CSV exports."
            )
        for name, df in [
            ("league_week_metrics", league_week),
            ("league_efficiency_trends", eff_trends),
            ("league_yoy_metrics", league_yoy),
            ("league_by_roof", by_roof),
            ("team_roof_counts", team_roof),
            ("pos_tier_shares", pos_tiers),
            ("flex_tier_shares", flex_tiers),
        ]:
            st.write(f"{name}")
            if df.height == 0:
                st.caption("(missing)")
                continue
            pdf = df.to_pandas()
            st.dataframe(pdf.head(1000))
            st.download_button(f"Download {name}.csv", data=pdf.to_csv(index=False).encode("utf-8"), file_name=f"{name}.csv", mime="text/csv")


def render_query_viewer() -> None:
    st.title("Query Viewer")
    sql_files = list_sql_files()
    if not sql_files:
        st.warning("No SQL files found in queries/. Add .sql files to use this app.")
        return
    labels = [str(p.relative_to(QUERIES_DIR)) for p in sql_files]
    idx = st.sidebar.selectbox("Choose a query", options=list(range(len(labels))), format_func=lambda i: labels[i])
    path = sql_files[idx]
    st.subheader(labels[idx])
    with st.expander("Show SQL", expanded=False):
        st.code(path.read_text(), language="sql")

    # Parameter inputs discovered from :param placeholders
    sql_text = path.read_text()
    params = sorted(set(re.findall(r"\:([A-Za-z_][A-Za-z0-9_]*)\b", sql_text)))
    param_values: dict[str, str] = {}
    if params:
        st.sidebar.markdown("### Parameters")
        for p in params:
            param_values[p] = st.sidebar.text_input(p, value="")

    run = st.sidebar.button("Run query", type="primary")
    if run:
        try:
            df = run_sql(path, params={k: v for k, v in param_values.items() if v != ""})
        except Exception as exc:
            st.error(f"Query failed: {exc}")
            return
        st.success(f"Returned {df.height} rows × {df.width} cols")
        # Download button
        csv_bytes = df.to_pandas().to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", data=csv_bytes, file_name=(labels[idx].replace('/', '_')+".csv"), mime="text/csv")
        render_chart(df)


def render_fantasy_top_scorers() -> None:
    st.title("Fantasy Top Scorers")
    st.caption("Top scorers by season — PPR and DraftKings PPR (with bonuses)")

    # Discover available seasons from materialized gold table
    gold_root = Path("data/gold/player_week_fantasy")
    if not gold_root.exists():
        st.warning("Gold table not found at data/gold/player_week_fantasy. Materialize it first.")
        return
    seasons = sorted([
        int(p.name.split("=")[1])
        for p in gold_root.glob("season=*")
        if p.is_dir() and p.name.split("=")[-1].isdigit()
    ])
    if not seasons:
        st.info("No seasons detected under data/gold/player_week_fantasy.")
        return

    # Controls
    with st.sidebar:
        st.markdown("### Filters")
        season = st.selectbox("Season", options=seasons, index=len(seasons)-1)
        position = st.selectbox("Position", options=["ALL","QB","RB","WR","TE"], index=0)
        topn = st.selectbox("Top N", options=[12, 24, 36], index=0)
        playoffs_only = st.checkbox(
            "Fantasy playoffs only (Weeks 15–17)", value=False,
            help="Always regular season; enable to restrict to Weeks 15–17"
        )

    params = {
        "season": str(season),
        "position": position,
        "limit": str(topn),
        "playoffs_only": ("1" if playoffs_only else "0"),
    }

    c1, c2 = st.columns(2)

    with c1:
        sql_path = QUERIES_DIR / "fantasy/top10_ppr_by_season.sql"
        try:
            df = run_sql(sql_path, params=params)
        except Exception as exc:
            st.error(f"Failed to run PPR query: {exc}")
            return
        pdf = df.to_pandas()
        sub = f"Top {topn} — PPR ({position})"
        if playoffs_only:
            sub += " — Weeks 15–17"
        st.subheader(sub)
        if pdf.empty:
            st.info("No rows returned.")
        else:
            chart = (
                alt.Chart(pdf)
                .mark_bar()
                .encode(
                    x=alt.X("ppr_total:Q", title="PPR points"),
                    y=alt.Y("player_name:N", sort='-x', title="Player"),
                    color=alt.Color("position:N", legend=alt.Legend(orient="bottom", title=None)),
                    tooltip=list(pdf.columns),
                )
            )
            st.altair_chart(chart.properties(height=540), use_container_width=True)
            cols = [c for c in ["season","player_name","position","ppr_total","dk_total","delta"] if c in pdf.columns]
            st.dataframe(pdf[cols])

    with c2:
        sql_path = QUERIES_DIR / "fantasy/top10_dk_by_season.sql"
        try:
            df = run_sql(sql_path, params=params)
        except Exception as exc:
            st.error(f"Failed to run DraftKings query: {exc}")
            return
        pdf = df.to_pandas()
        sub = f"Top {topn} — DraftKings PPR + bonuses ({position})"
        if playoffs_only:
            sub += " — Weeks 15–17"
        st.subheader(sub)
        if pdf.empty:
            st.info("No rows returned.")
        else:
            chart = (
                alt.Chart(pdf)
                .mark_bar()
                .encode(
                    x=alt.X("dk_total:Q", title="DraftKings points"),
                    y=alt.Y("player_name:N", sort='-x', title="Player"),
                    color=alt.Color("position:N", legend=alt.Legend(orient="bottom", title=None)),
                    tooltip=list(pdf.columns),
                )
            )
            st.altair_chart(chart.properties(height=540), use_container_width=True)
            cols = [c for c in ["season","player_name","position","dk_total","ppr_total","delta"] if c in pdf.columns]
            st.dataframe(pdf[cols])


def render_workhorse_rb_report() -> None:
    st.title("Workhorse RBs — Next Season Outcomes")
    with st.expander("Definitions", expanded=False):
        st.markdown(
            "- **Workhorse season**: RB season with ≥300 carries.\n"
            "- **Next season (NY)**: The same player's stats in the following season.\n"
            "- **Delta**: Next-season value minus current-season value (positive = improvement).\n"
            "- **Goal**: Assess whether high-workload RBs sustain production or decline next year."
        )

    # Parameter: minimum carries threshold
    min_carries = int(st.sidebar.number_input("Min carries (workhorse)", min_value=200, max_value=500, value=300, step=25))

    # Run the SQL that assembles workhorse seasons and their next-year stats
    sql_path = QUERIES_DIR / "rb_300_carries_next_year.sql"
    try:
        df_pl = run_sql(sql_path, params={"min_carries": str(min_carries)})
    except Exception as exc:
        st.error(f"Failed to run query: {exc}")
        return
    if df_pl.height == 0:
        st.info("No rows returned.")
        return
    pdf = df_pl.to_pandas()

    # Season filter
    seasons = sorted(pdf["season"].unique().tolist())
    c1, c2 = st.columns(2)
    with c1:
        s_start = st.selectbox("Season start", options=seasons, index=0)
    with c2:
        s_end = st.selectbox("Season end", options=seasons, index=len(seasons)-1)
    mask = (pdf["season"] >= s_start) & (pdf["season"] <= s_end)
    pdf = pdf.loc[mask].copy()

    # Compute DraftKings fantasy points columns (current and next season)
    try:
        pdf["dk_pts_cur"] = (
            0.1 * pdf["rush_yds"].astype(float)
            + 6.0 * pdf["rush_td"].astype(float)
            + 1.0 * pdf["receptions"].astype(float)
            + 0.1 * pdf["rec_yds"].astype(float)
            + 6.0 * pdf["rec_td"].astype(float)
        )
        pdf["dk_pts_ny"] = (
            0.1 * pdf["rush_yds_ny"].astype(float)
            + 6.0 * pdf["rush_td_ny"].astype(float)
            + 1.0 * pdf["receptions_ny"].astype(float)
            + 0.1 * pdf["rec_yds_ny"].astype(float)
            + 6.0 * pdf["rec_td_ny"].astype(float)
        )
    except Exception:
        # If any column is missing, default zeros so the option still appears without crashing
        pdf["dk_pts_cur"] = 0.0
        pdf["dk_pts_ny"] = 0.0

    # Metric selector (includes DK fantasy points)
    metric_options = [
        ("Rushing yards", "rush_yds", "rush_yds_ny"),
        ("Rushing TDs", "rush_td", "rush_td_ny"),
        ("Rushing attempts", "rush_att", "rush_att_ny"),
        ("Receptions", "receptions", "receptions_ny"),
        ("Receiving yards", "rec_yds", "rec_yds_ny"),
        ("Receiving TDs", "rec_td", "rec_td_ny"),
        ("Total TDs", "td_total", "td_total_ny"),
        ("DK Points (DraftKings)", "dk_pts_cur", "dk_pts_ny"),
    ]
    label_to_cols = {label: (cur, ny) for label, cur, ny in metric_options}
    label = st.selectbox("Metric", options=[m[0] for m in metric_options], index=0)
    cur_col, ny_col = label_to_cols[label]

    # (DK points now part of the Metric selector above)

    # Compute delta (even though the query includes explicit delta_* fields) for genericity
    pdf["delta"] = pdf[ny_col] - pdf[cur_col]
    pdf["change"] = pdf["delta"].apply(lambda x: "Improved" if x > 0 else ("Declined" if x < 0 else "No change"))

    # High-level KPIs
    total = len(pdf)
    improved = int((pdf["delta"] > 0).sum())
    declined = int((pdf["delta"] < 0).sum())
    med_delta = float(pdf["delta"].median()) if total else 0.0
    st.subheader("Summary")
    k1, k2, k3 = st.columns(3)
    with k1:
        st.metric(f"Seasons (RB ≥{min_carries} carries)", value=f"{total}")
    with k2:
        st.metric("Share improved", value=f"{(improved/total*100):.1f}%" if total else "0.0%")
    with k3:
        st.metric("Median delta", value=f"{med_delta:+.1f}")

    # Charts: delta histogram, next vs current scatter, next-season histogram
    st.subheader(f"Delta: Next season − Current season ({label})")
    delta_chart = (
        alt.Chart(pdf)
        .transform_bin("bin_delta", field="delta", bin=alt.Bin(maxbins=30))
        .mark_bar()
        .encode(
            x=alt.X("bin_delta:Q", title="Delta"),
            y=alt.Y("count():Q", title="Count"),
            color=alt.Color("change:N", title="Change"),
            tooltip=[alt.Tooltip("count():Q", title="Count"), alt.Tooltip("bin_delta:Q", title="Delta")],
        )
    )
    zero_rule = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color="#F97316", strokeWidth=2).encode(x="x:Q")
    st.altair_chart((delta_chart + zero_rule), use_container_width=True)

    st.subheader(f"Next season vs Current season ({label})")
    # Determine bounds for reference line
    try:
        x_min, x_max = float(pdf[cur_col].min()), float(pdf[cur_col].max())
        y_min, y_max = float(pdf[ny_col].min()), float(pdf[ny_col].max())
        b_min, b_max = min(x_min, y_min), max(x_max, y_max)
    except Exception:
        b_min, b_max = 0.0, 1.0
    ref = pd.DataFrame({cur_col: [b_min, b_max], ny_col: [b_min, b_max]})
    scatter = alt.Chart(pdf).mark_circle(size=64).encode(
        x=alt.X(f"{cur_col}:Q", title=f"Current — {label}"),
        y=alt.Y(f"{ny_col}:Q", title=f"Next — {label}"),
        color=alt.Color("change:N", title="Change"),
        tooltip=["player_name:N","season:O", alt.Tooltip(cur_col+":Q", title="Current"), alt.Tooltip(ny_col+":Q", title="Next"), alt.Tooltip("delta:Q", title="Delta")],
    )
    line_eq = alt.Chart(ref).mark_line(color="#9AA4BF", strokeDash=[6,3]).encode(
        x=cur_col, y=ny_col
    )
    zero_x = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color="#F97316", strokeWidth=2).encode(x="x:Q")
    st.altair_chart((scatter + line_eq + zero_x).interactive(), use_container_width=True)

    st.subheader(f"Next-season distribution ({label})")
    ny_hist = (
        alt.Chart(pdf)
        .transform_bin("bin_ny", field=ny_col, bin=alt.Bin(maxbins=30))
        .mark_bar()
        .encode(
            x=alt.X("bin_ny:Q", title=f"Next — {label}"),
            y=alt.Y("count():Q", title="Count"),
            tooltip=[alt.Tooltip("count():Q", title="Count"), alt.Tooltip("bin_ny:Q", title=f"Next — {label}")],
        )
    )
    zero_rule2 = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(color="#F97316", strokeWidth=2).encode(x="x:Q")
    st.altair_chart((ny_hist + zero_rule2), use_container_width=True)

    with st.expander("Data (filtered)"):
        st.dataframe(pdf.sort_values(["season","player_name"]))

def main() -> None:
    st.set_page_config(page_title="NFL Lake — Macro Report", layout="wide", page_icon="📈")
    mode = st.sidebar.selectbox("Mode", ["Macro Report QA","Workhorse RB Report","Fantasy Top Scorers","Query Viewer"], index=0)
    if mode == "Macro Report QA":
        render_macro_report()
    elif mode == "Workhorse RB Report":
        render_workhorse_rb_report()
    elif mode == "Fantasy Top Scorers":
        render_fantasy_top_scorers()
    else:
        render_query_viewer()


if __name__ == "__main__":
    main()


