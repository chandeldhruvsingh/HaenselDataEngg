import pytest
import pandas as pd
import sqlite3
import logging
from typing import Optional, Dict

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MockDatabaseSetup:
    def __init__(self, db_path: str, sql_path: str):
        self.db_path = db_path
        self.sql_path = sql_path

    def setup_database(self) -> bool:
        return True


class CustomerJourneyBuilder:
    def __init__(self, db_path: str, sql_path: str):
        self.db_path = db_path
        self.sql_path = sql_path
        self._setup_database()

    def _setup_database(self):
        db_setup = MockDatabaseSetup(self.db_path, self.sql_path)
        if not db_setup.setup_database():
            raise RuntimeError("Database setup failed.")

    def get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def build_journeys(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        query = """
        WITH journey_data AS (
            SELECT 
                c.conv_id,
                c.user_id,
                c.conv_date,
                c.conv_time,
                c.revenue,
                s.session_id,
                s.channel_name,
                s.event_date,
                s.event_time,
                s.holder_engagement,
                s.closer_engagement,
                s.impression_interaction,
                sc.cost
            FROM conversions c
            JOIN session_sources s ON c.user_id = s.user_id
            LEFT JOIN session_costs sc ON s.session_id = sc.session_id
            WHERE datetime(s.event_date || ' ' || s.event_time) <= datetime(c.conv_date || ' ' || c.conv_time)
        """

        if start_date and end_date:
            query += f" AND c.conv_date BETWEEN '{start_date}' AND '{end_date}'"

        query += ") SELECT * FROM journey_data ORDER BY conv_id, event_date, event_time"

        with self.get_connection() as conn:
            journeys_df = pd.read_sql_query(query, conn)

            journeys_df["session_datetime"] = pd.to_datetime(
                journeys_df["event_date"] + " " + journeys_df["event_time"]
            )
            journeys_df["conv_datetime"] = pd.to_datetime(
                journeys_df["conv_date"] + " " + journeys_df["conv_time"]
            )
            journeys_df["time_to_conv"] = (
                journeys_df["conv_datetime"] - journeys_df["session_datetime"]
            ).dt.total_seconds() / 3600

            self._validate_journeys(journeys_df)
            return journeys_df

    def _validate_journeys(self, df: pd.DataFrame):
        required_columns = [
            "conv_id",
            "user_id",
            "session_id",
            "channel_name",
            "holder_engagement",
            "closer_engagement",
            "impression_interaction",
        ]
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing required columns: {missing_cols}")

    def get_journey_stats(self, df: pd.DataFrame) -> Dict:
        return {
            "total_conversions": df["conv_id"].nunique(),
            "total_sessions": len(df),
            "unique_users": df["user_id"].nunique(),
            "avg_sessions_per_journey": len(df) / df["conv_id"].nunique(),
        }


@pytest.fixture
def test_db_path(tmp_path):
    """Create a temporary database path."""
    return str(tmp_path / "test.db")


@pytest.fixture
def test_sql_path(tmp_path):
    """Create a temporary SQL file path."""
    sql_path = tmp_path / "test.sql"
    sql_path.write_text("")  # Create empty SQL file
    return str(sql_path)


@pytest.fixture
def sample_data(test_db_path):
    conn = sqlite3.connect(test_db_path)

    sessions_df = pd.DataFrame(
        {
            "session_id": ["s1", "s2", "s3"],
            "user_id": ["u1", "u1", "u2"],
            "channel_name": ["Organic", "Paid", "Social"],
            "event_date": ["2024-01-01"] * 3,
            "event_time": ["10:00:00", "11:00:00", "12:00:00"],
            "holder_engagement": [1, 0, 1],
            "closer_engagement": [0, 1, 1],
            "impression_interaction": [1, 1, 0],
        }
    )
    sessions_df.to_sql("session_sources", conn, index=False, if_exists="replace")

    conversions_df = pd.DataFrame(
        {
            "conv_id": ["c1", "c2"],
            "user_id": ["u1", "u2"],
            "conv_date": ["2024-01-01"] * 2,
            "conv_time": ["12:00:00", "13:00:00"],
            "revenue": [100.0, 200.0],
        }
    )
    conversions_df.to_sql("conversions", conn, index=False, if_exists="replace")

    costs_df = pd.DataFrame(
        {"session_id": ["s1", "s2", "s3"], "cost": [10.0, 20.0, 15.0]}
    )
    costs_df.to_sql("session_costs", conn, index=False, if_exists="replace")

    conn.close()
    return test_db_path


def test_build_journeys(sample_data, test_sql_path):
    journey_builder = CustomerJourneyBuilder(sample_data, test_sql_path)
    journeys_df = journey_builder.build_journeys()

    assert len(journeys_df) > 0
    assert all(
        col in journeys_df.columns
        for col in [
            "conv_id",
            "user_id",
            "session_id",
            "channel_name",
            "time_to_conv",
            "cost",
            "revenue",
        ]
    )


def test_get_journey_stats(sample_data, test_sql_path):
    journey_builder = CustomerJourneyBuilder(sample_data, test_sql_path)
    journeys_df = journey_builder.build_journeys()
    stats = journey_builder.get_journey_stats(journeys_df)

    assert isinstance(stats, dict)
    assert all(
        key in stats
        for key in [
            "total_conversions",
            "total_sessions",
            "unique_users",
            "avg_sessions_per_journey",
        ]
    )
