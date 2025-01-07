import pytest
import sqlite3
from ..pipeline.setup_db import DatabaseSetup


@pytest.fixture
def test_db_path(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture
def test_sql_path(tmp_path):
    sql_content = """
    CREATE TABLE session_sources (
        session_id TEXT PRIMARY KEY,
        user_id TEXT,
        channel_name TEXT,
        event_date TEXT,
        event_time TEXT,
        holder_engagement INTEGER,
        closer_engagement INTEGER,
        impression_interaction INTEGER
    );
    
    CREATE TABLE conversions (
        conv_id TEXT PRIMARY KEY,
        user_id TEXT,
        conv_date TEXT,
        conv_time TEXT,
        revenue REAL
    );
    
    CREATE TABLE session_costs (
        session_id TEXT PRIMARY KEY,
        cost REAL
    );
    
    CREATE TABLE attribution_customer_journey (
        conv_id TEXT,
        session_id TEXT,
        ihc REAL,
        PRIMARY KEY (conv_id, session_id)
    );
    
    CREATE TABLE channel_reporting (
        channel_name TEXT,
        date TEXT,
        cost REAL,
        ihc REAL,
        ihc_revenue REAL,
        PRIMARY KEY (channel_name, date)
    );
    """
    sql_path = tmp_path / "test_create.sql"
    sql_path.write_text(sql_content)
    return str(sql_path)


def test_database_setup_successful(test_db_path, test_sql_path):
    db_setup = DatabaseSetup(test_db_path, test_sql_path)
    assert db_setup.setup_database() == True

    # Verify tables exist
    conn = sqlite3.connect(test_db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}

    expected_tables = {
        "session_sources",
        "conversions",
        "session_costs",
        "attribution_customer_journey",
        "channel_reporting",
    }

    assert tables == expected_tables
    conn.close()


def test_database_setup_invalid_sql_path(test_db_path):
    db_setup = DatabaseSetup(test_db_path, "invalid_path.sql")
    assert db_setup.setup_database() == False
