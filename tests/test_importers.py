
import pandas as pd

from src.importers import nflverse


def test_parse_years_arg_range_handles_hyphen_sequences():
    years = nflverse._parse_years_arg("2010-2012")

    assert years == [2010, 2011, 2012]


def test_parse_years_arg_handles_comma_lists():
    years = nflverse._parse_years_arg("1999,2001,2003")

    assert years == [1999, 2001, 2003]


def test_retry_params_prefers_explicit_options(monkeypatch):
    monkeypatch.setenv("IMPORTER_RETRY_ATTEMPTS", "9")
    monkeypatch.setenv("IMPORTER_RETRY_BASE_SECONDS", "10")

    attempts, base = nflverse._retry_params(options={"retry_attempts": 5, "retry_base_seconds": 2})

    assert attempts == 5
    assert base == 2


def test_retry_params_reads_environment_when_missing(monkeypatch):
    monkeypatch.setenv("IMPORTER_RETRY_ATTEMPTS", "7")
    monkeypatch.setenv("IMPORTER_RETRY_BASE_SECONDS", "11")

    attempts, base = nflverse._retry_params(options={})

    assert attempts == 7
    assert base == 11


def test_with_const_col_adds_column_and_preserves_existing_values():
    df = pd.DataFrame({"a": [1, 2]})

    result = nflverse._with_const_col(df, "season", 2024)

    assert list(result.columns) == ["a", "season"]
    assert result["season"].tolist() == [2024, 2024]


def test_with_const_col_fills_nulls_without_overwriting_values():
    df = pd.DataFrame({"team": ["BUF", None]})

    result = nflverse._with_const_col(df, "team", "BUF")

    assert result["team"].tolist() == ["BUF", "BUF"]


def test_fill_player_id_fallbacks_layers_sources(monkeypatch):
    df = pd.DataFrame({
        "player_id": [None, "abc"],
        "gsis_id": ["A", None],
        "pfr_player_id": [None, "PFR123"],
    })

    result = nflverse._fill_player_id_fallbacks(df.copy(), [("gsis_id", ""), ("pfr_player_id", "pfr_")])

    assert result.tolist() == ["A", "abc"]


def test_resolve_player_ids_prefers_direct_gsis(monkeypatch):
    lookup = pd.DataFrame({
        "gsis_id": ["00-001"],
        "pfr_id": ["PFR123"],
    })
    monkeypatch.setattr(nflverse, "_load_ids_lookup", lambda: lookup)

    df = pd.DataFrame({
        "gsis_id": ["00-001", None],
        "pfr_player_id": [None, "PFR123"],
    })

    resolved = nflverse._resolve_player_ids(df, [("gsis_id", "gsis_id"), ("pfr_player_id", "pfr_id")])

    assert resolved.tolist() == ["00-001", "00-001"]


def test_assign_weeks_from_schedule_uses_nearest_game(monkeypatch):
    schedule = pd.DataFrame({
        "team": ["BUF"],
        "game_date": [pd.Timestamp("2024-09-07")],
        "week": [1],
    })
    monkeypatch.setattr(nflverse, "_load_schedule_lookup", lambda year: schedule)

    df = pd.DataFrame({
        "team": ["BUF", "BUF"],
        "dt": [pd.Timestamp("2024-09-01T17:00:00Z"), pd.Timestamp("2024-09-08T17:00:00Z")],
    })

    weeks = nflverse._assign_weeks_from_schedule(df, 2024)

    assert weeks.tolist() == [1, 1]

