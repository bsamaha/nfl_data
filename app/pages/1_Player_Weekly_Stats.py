from __future__ import annotations

import streamlit as st
import altair as alt

from lib.data import (
    list_seasons,
    list_weeks,
    list_teams,
    list_positions,
    load_player_week_stats,
)

st.set_page_config(page_title="Player Weekly Stats", layout="wide")
st.title("Player Weekly Stats")

seasons = list_seasons()
if not seasons:
    st.warning("No report data found. Run the update flow to generate gold report tables.")
    st.stop()

season = st.sidebar.selectbox("Season", seasons, index=(len(seasons) - 1))
weeks = list_weeks(season)
week = st.sidebar.multiselect("Week", weeks, default=weeks[-1:] if weeks else [])
teams = st.sidebar.multiselect("Teams", list_teams(season), default=[])
positions = st.sidebar.multiselect("Positions", list_positions(season), default=[])
player_search = st.sidebar.text_input("Player search", "")

df = load_player_week_stats(
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
    "dk_ppr_points",
    "ppr_points",
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_air_yards",
    "receiving_tds",
    "rushing_yards",
    "rushing_tds",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "routes_run",
    "yprr",
    "tprr",
    "snap_share",
    "route_participation",
    "target_share",
    "air_yards_share",
    "wopr",
]

st.dataframe(
    df[visible_columns] if not df.empty else df,
    width="stretch",
    hide_index=True,
)

st.subheader("Top DK PPR scorers")
top = df.head(50)
if not top.empty:
    chart = (
        alt.Chart(top)
        .mark_bar()
        .encode(
            x=alt.X("dk_ppr_points:Q", title="DK PPR"),
            y=alt.Y("player_name:N", sort='-x'),
            color=alt.Color("position:N"),
            tooltip=["player_name", "team", "position", "dk_ppr_points", "week"],
        )
    )
    st.altair_chart(chart, width="stretch")


