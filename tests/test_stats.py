import hashlib

import polars as pl
import pytest

from src.lineage import compute_sha256_for_keys, record_partition_counts
from src.profiling import _compute_metrics


class TestComputeMetrics:
    def test_reports_counts_and_dtype(self):
        df = pl.DataFrame(
            {
                "season": [2023, 2023, 2024],
                "player_id": ["A", "A", "B"],
                "value": [10.0, 5.0, None],
            }
        )

        metrics = _compute_metrics(df, key_cols=["season", "player_id"])

        assert metrics["rows"] == 3
        assert metrics["columns"] == ["season", "player_id", "value"]
        assert metrics["dtypes"]["value"] == "Float64"
        assert metrics["key_nulls"] == {"season": 0, "player_id": 0}
        assert metrics["key_duplicate_rows"] == 1
        assert metrics["key_unique_rows"] == 2
        assert metrics["key_unique_ratio"] == 2 / 3
        assert metrics["season_min"] == 2023
        assert metrics["season_max"] == 2024

    def test_handles_missing_keys(self):
        df = pl.DataFrame(
            {
                "season": [2024, None],
                "player_id": ["A", None],
                "value": [1, 2],
            }
        )

        metrics = _compute_metrics(df, key_cols=["season", "player_id"])

        assert metrics["key_nulls"] == {"season": 1, "player_id": 1}
        assert metrics["key_unique_rows"] == 2
        assert metrics["key_duplicate_rows"] == 0


class TestComputeSha256:
    def test_matches_direct_hashlib(self):
        rows = ["2023|A", "2024|B"]

        expected = hashlib.sha256()
        for row in rows:
            expected.update(row.encode("utf-8"))

        assert compute_sha256_for_keys(rows) == expected.hexdigest()

    def test_ignores_none_entries(self):
        rows = ["2024|A", None, "2024|B"]

        expected = hashlib.sha256()
        expected.update("2024|A".encode("utf-8"))
        expected.update("2024|B".encode("utf-8"))

        assert compute_sha256_for_keys(rows) == expected.hexdigest()


class TestRecordPartitionCounts:
    def test_initializes_structure(self):
        lineage = {}

        updated = record_partition_counts(lineage, "weekly", "season=2024/week=1", 123)

        assert updated["weekly"]["partitions"]["season=2024/week=1"]["row_count"] == 123

    def test_updates_existing_partition(self):
        lineage = {
            "weekly": {
                "partitions": {
                    "season=2024/week=1": {"row_count": 10}
                }
            }
        }

        updated = record_partition_counts(lineage, "weekly", "season=2024/week=1", 25)

        assert updated["weekly"]["partitions"]["season=2024/week=1"]["row_count"] == 25


class TestSpotCheckStats:
    def test_passing_totals_match_pro_football_reference_sample(self):
        weekly = pl.DataFrame(
            {
                "season": [2023, 2023, 2023],
                "week": [1, 2, 3],
                "player_id": ["00-0033873", "00-0033873", "00-0033873"],
                "player_name": ["Patrick Mahomes", "Patrick Mahomes", "Patrick Mahomes"],
                "passing_yards": [226, 305, 272],
                "passing_tds": [2, 2, 3],
                "interceptions": [1, 1, 0],
                "pass_completions": [21, 29, 24],
                "pass_attempts": [39, 41, 33],
            }
        )

        totals = (
            weekly
            .group_by("season", "player_id")
            .agg([
                pl.col("passing_yards").sum().alias("passing_yards"),
                pl.col("passing_tds").sum().alias("passing_tds"),
                pl.col("interceptions").sum().alias("interceptions"),
                pl.col("pass_completions").sum().alias("completions"),
                pl.col("pass_attempts").sum().alias("attempts"),
            ])
        )

        row = totals.row(0)
        season, player_id, yards, tds, interceptions, completions, attempts = row

        assert season == 2023
        assert player_id == "00-0033873"
        assert yards == 803
        assert tds == 7
        assert interceptions == 2
        assert completions == 74
        assert attempts == 113

        # NFL passer rating calculation (aggregate) ~100.0 through Week 3, 2023
        comp_perc = completions / attempts
        yards_per_attempt = yards / attempts
        td_rate = tds / attempts
        int_rate = interceptions / attempts

        a = max(0, min(2.375, (comp_perc - 0.3) * 5))
        b = max(0, min(2.375, (yards_per_attempt - 3) * 0.25))
        c = max(0, min(2.375, td_rate * 20))
        d = max(0, min(2.375, 2.375 - int_rate * 25))
        passer_rating = ((a + b + c + d) / 6) * 100

        assert passer_rating == pytest.approx(99.54, rel=1e-3)

    def test_receiving_totals_match_pro_football_reference_sample(self):
        weekly = pl.DataFrame(
            {
                "season": [2023, 2023, 2023],
                "week": [1, 2, 3],
                "player_id": ["00-0035408", "00-0035408", "00-0035408"],
                "player_name": ["Tyreek Hill", "Tyreek Hill", "Tyreek Hill"],
                "receiving_yards": [215, 40, 157],
                "receiving_tds": [2, 1, 1],
                "receptions": [11, 5, 9],
                "targets": [15, 9, 11],
            }
        )

        totals = (
            weekly
            .group_by("season", "player_id")
            .agg([
                pl.col("receiving_yards").sum().alias("receiving_yards"),
                pl.col("receiving_tds").sum().alias("receiving_tds"),
                pl.col("receptions").sum().alias("receptions"),
                pl.col("targets").sum().alias("targets"),
            ])
        )

        row = totals.row(0)
        season, player_id, yards, tds, receptions, targets = row

        assert season == 2023
        assert player_id == "00-0035408"
        assert yards == 412
        assert tds == 4
        assert receptions == 25
        assert targets == 35

        yards_per_reception = yards / receptions
        catch_rate = receptions / targets

        assert yards_per_reception == pytest.approx(16.48, rel=1e-3)
        assert catch_rate == pytest.approx(25 / 35, rel=1e-4)


class TestTeamSpotChecks:
    def test_49ers_team_scoring_and_rushing_totals(self):
        games = pl.DataFrame(
            {
                "season": [2023, 2023, 2023],
                "week": [1, 2, 3],
                "team": ["SF", "SF", "SF"],
                "points_for": [30, 30, 30],
                "points_against": [7, 23, 12],
                "rushing_yards": [188, 159, 141],
                "rushing_tds": [2, 1, 1],
            }
        )

        totals = (
            games
            .group_by("season", "team")
            .agg([
                pl.col("points_for").sum().alias("points_for"),
                pl.col("points_against").sum().alias("points_against"),
                pl.col("rushing_yards").sum().alias("rushing_yards"),
                pl.col("rushing_tds").sum().alias("rushing_tds"),
            ])
        )

        season, team, pts_for, pts_against, rush_yards, rush_tds = totals.row(0)

        assert season == 2023
        assert team == "SF"
        assert pts_for == 90
        assert pts_against == 42
        assert rush_yards == 488
        assert rush_tds == 4

        point_diff = pts_for - pts_against
        avg_points_for = pts_for / 3
        avg_rush_yards = rush_yards / 3

        assert point_diff == 48
        assert avg_points_for == pytest.approx(30.0, rel=1e-3)
        assert avg_rush_yards == pytest.approx(162.67, rel=1e-3)

    def test_cowboys_defensive_points_allowed_spot_check(self):
        games = pl.DataFrame(
            {
                "season": [2023, 2023, 2023],
                "week": [1, 2, 3],
                "team": ["DAL", "DAL", "DAL"],
                "points_for": [40, 30, 16],
                "points_against": [0, 10, 28],
                "takeaways": [2, 4, 1],
            }
        )

        totals = (
            games
            .group_by("season", "team")
            .agg([
                pl.col("points_for").sum().alias("points_for"),
                pl.col("points_against").sum().alias("points_against"),
                pl.col("takeaways").sum().alias("takeaways"),
            ])
        )

        season, team, pts_for, pts_against, takeaways = totals.row(0)

        assert season == 2023
        assert team == "DAL"
        assert pts_for == 86
        assert pts_against == 38
        assert takeaways == 7

        avg_points_allowed = pts_against / 3
        turnover_margin = takeaways - 3  # Cowboys committed 3 giveaways through Week 3

        assert avg_points_allowed == pytest.approx(12.67, rel=1e-3)
        assert turnover_margin == 4

