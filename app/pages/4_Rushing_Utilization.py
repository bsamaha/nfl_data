from __future__ import annotations

import altair as alt
import streamlit as st

from lib.data import (
    list_seasons,
    list_weeks,
    list_teams,
    list_positions,
    load_player_week_utilization_rushing,
)

st.set_page_config(page_title="Rushing Utilization", layout="wide")
st.title("Rushing Utilization")

seasons = list_seasons()
if not seasons:
    st.warning("No report data found. Run the update flow to generate gold report tables.")
    st.stop()

season = st.sidebar.selectbox("Season", seasons, index=(len(seasons) - 1))
weeks = list_weeks(season)
week = st.sidebar.multiselect("Week", weeks, default=weeks[-1:] if weeks else [])
teams = st.sidebar.multiselect("Teams", list_teams(season), default=[])
positions = st.sidebar.multiselect("Positions", list_positions(season), default=["RB"])
player_search = st.sidebar.text_input("Player search", "")

df = load_player_week_utilization_rushing(
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
    "carries",
    "rushing_yards",
    "rushing_tds",
    "carry_share",
    "rz20_carry_share",
    "rz10_carry_share",
    "rz5_carry_share",
]

st.dataframe(
    df[visible_columns] if not df.empty else df,
    width="stretch",
    hide_index=True,
)

st.subheader("Visualize a rushing metric")
if not df.empty:
    candidates = [
        "carry_share",
        "rz20_carry_share",
        "rz10_carry_share",
        "rz5_carry_share",
        "carries",
        "rz20_carries",
        "rz10_carries",
        "rz5_carries",
    ]
    metrics = [c for c in candidates if c in df.columns]
    metric = st.selectbox("Metric", metrics, index=metrics.index("carry_share") if "carry_share" in metrics else 0)

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(f"{metric}:Q", title=metric.replace("_", " ").title()),
            y=alt.Y("player_name:N", sort='-x'),
            color=alt.Color("team:N"),
            tooltip=["player_name","team","position",metric],
        )
    )
    st.altair_chart(chart, width="stretch")


