from __future__ import annotations

import altair as alt
import streamlit as st

from lib.data import (
    list_seasons,
    list_weeks,
    list_teams,
    list_positions,
    load_player_week_utilization_te,
)

st.set_page_config(page_title="TE Utilization", layout="wide")
st.title("TE Utilization")

seasons = list_seasons()
if not seasons:
    st.warning("No report data found. Run the update flow to generate gold report tables.")
    st.stop()

season = st.sidebar.selectbox("Season", seasons, index=(len(seasons) - 1))
weeks = list_weeks(season)
week = st.sidebar.multiselect("Week", weeks, default=weeks[-1:] if weeks else [])
teams = st.sidebar.multiselect("Teams", list_teams(season), default=[])
positions = st.sidebar.multiselect("Positions", list_positions(season), default=["TE"])
player_search = st.sidebar.text_input("Player search", "")

df = load_player_week_utilization_te(
    seasons=[season],
    weeks=week or None,
    teams=teams or None,
    positions=positions or None,
    player_search=player_search or None,
)

visible_columns = [
    "season",
    "week",
    "season_type",
    "team",
    "player_name",
    "position",
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "routes_run",
    "tprr",
    "yprr",
    "target_share",
    "air_yards_share",
    "wopr",
    "end_zone_target_share",
    "rz20_target_share",
    "rz10_target_share",
    "rz5_target_share",
    "third_fourth_down_target_share",
    "two_minute_target_share",
    "shotgun_target_share",
    "no_huddle_target_share",
]

st.dataframe(
    df[visible_columns] if not df.empty else df,
    width="stretch",
    hide_index=True,
)

st.subheader("Visualize a TE metric")
if not df.empty:
    candidates = [
        "target_share","yprr","tprr","adot",
        "end_zone_target_share","rz20_target_share","rz10_target_share","rz5_target_share",
        "third_fourth_down_target_share","two_minute_target_share",
        "shotgun_target_share","no_huddle_target_share",
        "p11_target_share","p12_target_share","p21_target_share",
        "targets","end_zone_targets","rz20_targets","rz10_targets","rz5_targets","wopr",
    ]
    metrics = [c for c in candidates if c in df.columns]
    default_metric = "end_zone_target_share" if "end_zone_target_share" in metrics else metrics[0]
    metric = st.selectbox("Metric", metrics, index=metrics.index(default_metric))

    mark = alt.Chart(df)
    if metric.endswith("_share") or metric in ("yprr","tprr","adot","target_share"):
        chart = mark.mark_circle(size=80).encode(
            x=alt.X(f"{metric}:Q", title=metric.replace("_"," ").title()),
            y=alt.Y("player_name:N", sort='-x'),
            color=alt.Color("team:N"),
            tooltip=["player_name","team","position",metric],
        )
    else:
        chart = mark.mark_bar().encode(
            x=alt.X(f"{metric}:Q", title=metric.replace("_"," ").title()),
            y=alt.Y("player_name:N", sort='-x'),
            color=alt.Color("team:N"),
            tooltip=["player_name","team","position",metric],
        )
    st.altair_chart(chart, width="stretch")


