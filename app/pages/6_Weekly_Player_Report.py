from __future__ import annotations

from typing import List

import altair as alt
import pandas as pd
import streamlit as st

from lib.data import (
    list_positions,
    list_seasons,
    list_teams,
    list_weeks,
    load_player_week_stats,
)


st.set_page_config(page_title="Weekly Player Report", layout="wide")
st.title("Weekly Player Report")

seasons = list_seasons()
if not seasons:
    st.warning("No report data found. Run the update flow to generate gold report tables.")
    st.stop()

season = max(seasons)
st.sidebar.markdown(f"**Season:** {season}")

weeks_available = sorted(list_weeks(season))
if not weeks_available:
    st.warning("No weekly data available for the selected season.")
    st.stop()

default_week = weeks_available[-1]
week = st.sidebar.selectbox("Week", weeks_available, index=weeks_available.index(default_week))

PRIMARY_POSITIONS = ["QB", "RB", "WR", "TE"]
position_options = [p for p in list_positions(season) if p in PRIMARY_POSITIONS]
if not position_options:
    position_options = PRIMARY_POSITIONS
selected_positions = st.sidebar.multiselect(
    "Positions",
    options=position_options,
    default=position_options,
)
positions = None if set(selected_positions) == set(position_options) else selected_positions

teams = st.sidebar.multiselect(
    "Teams",
    options=list_teams(season),
    default=[],
)

player_search = st.sidebar.text_input("Player search", "")

player_limit = st.sidebar.number_input(
    "Number of players to display",
    min_value=5,
    max_value=200,
    value=50,
    step=5,
)

df = load_player_week_stats(
    seasons=[season],
    weeks=[week],
    teams=teams or None,
    positions=positions or None,
    player_search=player_search or None,
    limit=None,
)

if df.empty:
    st.info("No player data found for the selected filters.")
    st.stop()

df["targets"] = pd.to_numeric(df.get("targets"), errors="coerce")
df["receiving_air_yards"] = pd.to_numeric(df.get("receiving_air_yards"), errors="coerce")

df["team_targets"] = df.groupby(["season", "week", "team"], dropna=False)["targets"].transform("sum")
df["team_air_yards"] = df.groupby(["season", "week", "team"], dropna=False)["receiving_air_yards"].transform("sum")
df["target_share"] = (df["targets"] / df["team_targets"].replace({0: pd.NA})) * 100
df["air_yards_share"] = (df["receiving_air_yards"] / df["team_air_yards"].replace({0: pd.NA})) * 100
df = df.drop(columns=["team_targets", "team_air_yards"], errors="ignore")


def _numeric_columns(frame: pd.DataFrame) -> List[str]:
    numeric_cols = frame.select_dtypes(include=["number"]).columns.tolist()
    # Exclude identifier columns that are numeric but not stats
    for col in ["season", "week"]:
        if col in numeric_cols:
            numeric_cols.remove(col)
    return numeric_cols


numeric_columns = _numeric_columns(df)
if not numeric_columns:
    st.info("No numeric statistics available to visualize.")
    st.stop()

default_metric = "dk_ppr_points" if "dk_ppr_points" in numeric_columns else numeric_columns[0]
stat_type = st.selectbox("Statistic to chart", options=numeric_columns, index=numeric_columns.index(default_metric))

sorted_df = df.sort_values(by=stat_type, ascending=False, na_position="last")
display_df = sorted_df.head(int(player_limit))

columns_to_hide = {
    "player_id",
    "season_type",
    "season",
    "week",
    "routes_run",
    "yprr",
    "tprr",
    "wopr",
}
visible_df = display_df.drop(columns=[c for c in columns_to_hide if c in display_df.columns], errors="ignore")

st.caption(
    "Showing top players for the selected week after applying filters. Use sidebar controls to adjust filters and chart metric."
)

st.dataframe(
    visible_df,
    width="stretch",
    hide_index=True,
)

chart_source = visible_df[["player_name", "team", stat_type]].dropna(subset=[stat_type])

if chart_source.empty:
    st.info("No chart to display because the selected statistic has no values after filtering.")
else:
    chart = (
        alt.Chart(chart_source)
        .mark_bar()
        .encode(
            x=alt.X(f"{stat_type}:Q", title=stat_type.replace("_", " ").title()),
            y=alt.Y("player_name:N", sort="-x", title="Player"),
            color=alt.Color("team:N", title="Team"),
            tooltip=["player_name", "team", stat_type],
        )
        .properties(width="container")
    )
    st.altair_chart(chart)

st.download_button(
    label="Download filtered data as CSV",
    data=visible_df.to_csv(index=False).encode("utf-8"),
    file_name=f"weekly_player_report_{season}_week_{week}.csv",
    mime="text/csv",
)

