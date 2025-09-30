from __future__ import annotations

from functools import lru_cache
from typing import List

import altair as alt
import pandas as pd
import streamlit as st

from lib.data import (
    list_defenses,
    list_positions,
    list_seasons,
    list_weeks,
    load_defense_position_points,
)


PAGE_TITLE = "Defense vs Position (Fantasy Points Allowed)"
DEFAULT_POSITIONS = ["WR", "RB", "TE", "QB"]


@lru_cache(maxsize=8)
def _position_options(season: int) -> List[str]:
    positions = list_positions(season)
    filtered = [p for p in positions if p in DEFAULT_POSITIONS]
    return filtered or DEFAULT_POSITIONS


@lru_cache(maxsize=8)
def _defense_options(season: int) -> List[str]:
    defenses = list_defenses(season)
    return defenses


st.title(PAGE_TITLE)

seasons = list_seasons()
if not seasons:
    st.warning("No report data found. Run the update flow to generate gold report tables.")
    st.stop()

season = st.sidebar.selectbox("Season", seasons, index=(len(seasons) - 1))
weeks_available = list_weeks(season)
selected_weeks = st.sidebar.multiselect(
    "Weeks",
    weeks_available,
    default=weeks_available[-4:] if len(weeks_available) >= 4 else weeks_available,
)

position_choices = _position_options(season)
selected_positions = st.sidebar.multiselect(
    "Positions",
    options=position_choices,
    default=position_choices,
)

defense_choices = _defense_options(season)
selected_defenses = st.sidebar.multiselect(
    "Defenses",
    options=defense_choices,
    default=[],
)

df = load_defense_position_points(
    seasons=[season],
    weeks=selected_weeks or None,
    positions=selected_positions or None,
    defenses=selected_defenses or None,
)

if df.empty:
    st.info("No defense vs position data available for the selected filters.")
    st.stop()


def _summarize(data: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        data.groupby(["defense_team", "position"], dropna=False)
        .agg(
            games_played=("week", pd.Series.nunique),
            total_points_allowed=("points_allowed_ppr", "sum"),
            avg_points_allowed=("points_allowed_ppr", "mean"),
        )
        .reset_index()
    )

    league_avg = (
        data.groupby("position")["points_allowed_ppr"].mean().rename("league_avg_points_allowed")
    )
    league_std = (
        data.groupby("position")["points_allowed_ppr"].std(ddof=0).rename("league_std_points_allowed")
    )

    summary = grouped.merge(league_avg, on="position", how="left")
    summary = summary.merge(league_std, on="position", how="left")
    summary["avg_vs_league"] = (
        summary["avg_points_allowed"] - summary["league_avg_points_allowed"]
    )
    summary["percent_vs_league"] = (
        summary["avg_points_allowed"] / summary["league_avg_points_allowed"] - 1.0
    )
    summary = summary.sort_values(["position", "avg_points_allowed"], ascending=[True, False])

    return summary


summary_df = _summarize(df)

st.subheader("Fantasy Points Allowed (Summary)")
st.caption(
    "Metrics recomputed for selected filters. Points reflect PPR fantasy scoring (DraftKings format)."
)

st.dataframe(
    summary_df,
    width="stretch",
    hide_index=True,
)

st.download_button(
    label="Download summary as CSV",
    data=summary_df.to_csv(index=False).encode("utf-8"),
    file_name=f"defense_vs_position_summary_{season}.csv",
    mime="text/csv",
)


st.subheader("Defense Rankings")
if not summary_df.empty:
    chart = (
        alt.Chart(summary_df)
        .mark_bar()
        .encode(
            x=alt.X("avg_points_allowed:Q", title="Avg PPR Points Allowed"),
            y=alt.Y("defense_team:N", sort="-x", title="Defense"),
            color=alt.Color("avg_vs_league:Q", scale=alt.Scale(scheme="redblue", reverse=True)),
            column=alt.Column("position:N", title="Position"),
            tooltip=[
                alt.Tooltip("defense_team:N", title="Defense"),
                alt.Tooltip("position:N", title="Position"),
                alt.Tooltip("avg_points_allowed:Q", title="Avg Points", format=".2f"),
                alt.Tooltip("avg_vs_league:Q", title="Diff vs League", format="+.2f"),
                alt.Tooltip("games_played:Q", title="Games"),
            ],
        )
        .resolve_scale(y="independent")
    )
    st.altair_chart(chart, width="stretch")


st.subheader("Weekly Detail")
st.dataframe(
    df.sort_values(["week", "position", "defense_team"], ascending=[False, True, True]),
    width="stretch",
    hide_index=True,
)


