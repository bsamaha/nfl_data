from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional

import duckdb
import polars as pl
import streamlit as st
import altair as alt


QUERIES_DIR = Path("queries")


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


def main() -> None:
    st.set_page_config(page_title="NFL Lake — Query Viewer", layout="wide")
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


if __name__ == "__main__":
    main()


