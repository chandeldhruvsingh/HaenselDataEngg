import pytest
import pandas as pd
import sqlite3
import os
import logging
from typing import List, Dict, Optional
from unittest.mock import Mock, patch
import shutil

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IHCAttributionClient:
    def __init__(self, api_key: str, conv_type_id: str = "data_engineering_challenge"):
        self.api_key = api_key
        self.conv_type_id = conv_type_id


class AttributionProcessor:
    def __init__(
        self,
        db_path: str,
        api_key: str,
        conv_type_id: str = "data_engineering_challenge",
    ):
        self.db_path = db_path
        self.api_client = IHCAttributionClient(api_key, conv_type_id)

    def process_batch(self, batch_response: Dict):
        if batch_response["statusCode"] != 200:
            logger.error(f"Batch processing failed: {batch_response}")
            return

        with sqlite3.connect(self.db_path) as conn:
            attribution_data = []
            for attribution in batch_response["value"]:
                attribution_data.append(
                    {
                        "conv_id": attribution["conversion_id"],
                        "session_id": attribution["session_id"],
                        "ihc": attribution["ihc"],
                    }
                )

            if attribution_data:
                df = pd.DataFrame(attribution_data)
                df.to_sql(
                    "attribution_customer_journey",
                    conn,
                    if_exists="append",
                    index=False,
                )

    def update_channel_reporting(self):
        query = """
        WITH daily_channels AS (
            SELECT 
                ss.channel_name,
                ss.event_date as date,
                SUM(COALESCE(sc.cost, 0)) as cost,
                SUM(COALESCE(acj.ihc, 0)) as ihc,
                SUM(COALESCE(acj.ihc * c.revenue, 0)) as ihc_revenue
            FROM session_sources ss
            LEFT JOIN session_costs sc ON ss.session_id = sc.session_id
            LEFT JOIN attribution_customer_journey acj ON ss.session_id = acj.session_id
            LEFT JOIN conversions c ON acj.conv_id = c.conv_id
            GROUP BY ss.channel_name, ss.event_date
        )
        
        INSERT OR REPLACE INTO channel_reporting (channel_name, date, cost, ihc, ihc_revenue)
        SELECT 
            channel_name,
            date,
            cost,
            ihc,
            ihc_revenue
        FROM daily_channels
        """

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(query)

    def export_channel_report(self, output_path: str):
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                """
                SELECT * FROM channel_reporting
                ORDER BY date, channel_name
            """,
                conn,
            )

            df["CPO"] = df.apply(
                lambda row: row["cost"] / row["ihc"]
                if row["ihc"] > 0
                else float("inf"),
                axis=1,
            )
            df["ROAS"] = df.apply(
                lambda row: row["ihc_revenue"] / row["cost"] if row["cost"] > 0 else 0,
                axis=1,
            )

            df.to_csv(output_path, index=False)


# Test fixtures and setup
@pytest.fixture
def test_db_path(tmp_path):
    db_path = tmp_path / "test.db"

    # Create test database with required schema
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS session_sources (
            session_id TEXT PRIMARY KEY,
            channel_name TEXT,
            event_date TEXT
        )
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS session_costs (
            session_id TEXT PRIMARY KEY,
            cost REAL
        )
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS conversions (
            conv_id TEXT PRIMARY KEY,
            revenue REAL
        )
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS attribution_customer_journey (
            conv_id TEXT,
            session_id TEXT,
            ihc REAL
        )
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_reporting (
            channel_name TEXT,
            date TEXT,
            cost REAL,
            ihc REAL,
            ihc_revenue REAL,
            PRIMARY KEY (channel_name, date)
        )
    """
    )

    # Insert sample data
    conn.execute("INSERT INTO session_sources VALUES ('s1', 'Organic', '2024-01-01')")
    conn.execute("INSERT INTO session_sources VALUES ('s2', 'Paid', '2024-01-01')")
    conn.execute("INSERT INTO session_costs VALUES ('s1', 100)")
    conn.execute("INSERT INTO session_costs VALUES ('s2', 200)")
    conn.execute("INSERT INTO conversions VALUES ('c1', 1000)")

    conn.commit()
    conn.close()

    return str(db_path)


@pytest.fixture
def sample_api_response():
    return {
        "statusCode": 200,
        "value": [
            {"conversion_id": "c1", "session_id": "s1", "ihc": 0.5},
            {"conversion_id": "c1", "session_id": "s2", "ihc": 0.5},
        ],
    }


@pytest.fixture(autouse=True)
def cleanup_files(request, tmp_path):
    def cleanup():
        for file in tmp_path.glob("*"):
            if file.is_file():
                file.unlink()
            elif file.is_dir():
                shutil.rmtree(file)

    request.addfinalizer(cleanup)


# Tests
def test_process_batch(test_db_path, sample_api_response):
    processor = AttributionProcessor(test_db_path, "test_key")
    processor.process_batch(sample_api_response)

    conn = sqlite3.connect(test_db_path)
    df = pd.read_sql_query("SELECT * FROM attribution_customer_journey", conn)
    conn.close()

    assert len(df) == 2
    assert all(col in df.columns for col in ["conv_id", "session_id", "ihc"])


def test_update_channel_reporting(test_db_path):
    # First insert some attribution data
    processor = AttributionProcessor(test_db_path, "test_key")
    processor.process_batch(
        {
            "statusCode": 200,
            "value": [
                {"conversion_id": "c1", "session_id": "s1", "ihc": 0.5},
                {"conversion_id": "c1", "session_id": "s2", "ihc": 0.5},
            ],
        }
    )

    processor.update_channel_reporting()

    conn = sqlite3.connect(test_db_path)
    df = pd.read_sql_query("SELECT * FROM channel_reporting", conn)
    conn.close()

    assert len(df) > 0
    assert all(
        col in df.columns
        for col in ["channel_name", "date", "cost", "ihc", "ihc_revenue"]
    )


def test_export_channel_report(test_db_path, tmp_path):
    output_path = str(tmp_path / "report.csv")
    processor = AttributionProcessor(test_db_path, "test_key")

    # First insert and process some data
    processor.process_batch(
        {
            "statusCode": 200,
            "value": [
                {"conversion_id": "c1", "session_id": "s1", "ihc": 0.5},
                {"conversion_id": "c1", "session_id": "s2", "ihc": 0.5},
            ],
        }
    )
    processor.update_channel_reporting()

    # Now export
    processor.export_channel_report(output_path)

    assert os.path.exists(output_path)
    df = pd.read_csv(output_path)
    assert all(
        col in df.columns
        for col in ["channel_name", "date", "cost", "ihc", "ihc_revenue", "CPO", "ROAS"]
    )
