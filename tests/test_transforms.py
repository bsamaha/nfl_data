import polars as pl

from src.transforms import to_silver


def test_to_silver_weekly_renames_recent_team_and_dedupes_latest():
    df = pl.DataFrame(
        {
            "season": [2024, 2024],
            "week": [1, 1],
            "player_id": ["00-001", "00-001"],
            "recent_team": ["BUF", "BUF"],
            "ingested_at": ["2024-09-01T00:00:00Z", "2024-09-02T00:00:00Z"],
            "targets": [8, 10],
        }
    )

    result = to_silver("weekly", df)

    assert result.height == 1
    assert result["targets"].item() == 10
    assert "team" in result.columns
    assert result["team"].item() == "BUF"


def test_to_silver_weekly_drops_rows_missing_core_keys():
    df = pl.DataFrame(
        {
            "season": [2024, 2024],
            "week": [1, 1],
            "player_id": ["00-001", None],
            "recent_team": ["BUF", "BUF"],
        }
    )

    result = to_silver("weekly", df)

    assert result.height == 1
    assert result["player_id"].item() == "00-001"


def test_to_silver_weekly_upcasts_null_columns_to_utf8():
    df = pl.DataFrame(
        {
            "season": [2024],
            "week": [1],
            "player_id": ["00-001"],
            "recent_team": ["BUF"],
            "notes": [None],
        }
    )

    result = to_silver("weekly", df)

    assert result.schema["notes"] == pl.Utf8


def test_to_silver_rosters_renames_recent_team_and_keeps_team_nullable():
    df = pl.DataFrame(
        {
            "season": [2024, 2024],
            "week": [1, 1],
            "player_id": ["A", "B"],
            "recent_team": ["BUF", None],
        }
    )

    result = to_silver("rosters", df)

    assert "team" in result.columns
    first = result.filter(pl.col("player_id") == "A").select("team").item()
    second = result.filter(pl.col("player_id") == "B").select("team").item()

    assert first == "BUF"
    assert second is None

