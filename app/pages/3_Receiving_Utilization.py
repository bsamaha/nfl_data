from __future__ import annotations

import altair as alt
import streamlit as st

from lib.data import (
    list_seasons,
    list_weeks,
    list_teams,
    list_positions,
    load_player_week_utilization_wr,
)

st.set_page_config(page_title="WR Utilization", layout="wide")
st.title("WR Utilization")

seasons = list_seasons()
if not seasons:
    st.warning("No report data found. Run the update flow to generate gold report tables.")
    st.stop()

season = st.sidebar.selectbox("Season", seasons, index=(len(seasons) - 1))
weeks = list_weeks(season)
week = st.sidebar.multiselect("Week", weeks, default=weeks[-1:] if weeks else [])
teams = st.sidebar.multiselect("Teams", list_teams(season), default=[])
positions = st.sidebar.multiselect("Positions", list_positions(season), default=["WR","TE"])
player_search = st.sidebar.text_input("Player search", "")

df = load_player_week_utilization_wr(
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
    "target_share",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "routes_run",
    "yprr",
    "tprr",
    "air_yards_share",
    "wopr",
    "end_zone_target_share",
    "rz20_target_share",
    "rz10_target_share",
    "rz5_target_share",
    "third_fourth_down_target_share",
    "ldd_target_share",
    "sdd_target_share",
    "two_minute_target_share",
    "four_minute_target_share",
]

st.dataframe(
    df[visible_columns] if not df.empty else df,
    width="stretch",
    hide_index=True,
)

st.subheader("Visualize a receiving metric")
if not df.empty:
    candidates = [
        "target_share","adot","yprr","tprr",
        "end_zone_target_share","rz20_target_share","rz10_target_share","rz5_target_share",
        "third_fourth_down_target_share","ldd_target_share","sdd_target_share",
        "two_minute_target_share","four_minute_target_share",
        "shotgun_target_share","no_huddle_target_share",
        "p11_target_share","p12_target_share","p21_target_share",
        "targets","receiving_yards","receiving_air_yards","wopr","air_yards_share",
    ]
    metrics = [c for c in candidates if c in df.columns]
    default_metric = "target_share" if "target_share" in metrics else metrics[0]
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


