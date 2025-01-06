import pytest
from attribution_processor import AttributionProcessor

@pytest.fixture
def sample_api_response():
    return {
        "statusCode": 200,
        "value": [
            {
                "conversion_id": "c1",
                "session_id": "s1",
                "ihc": 0.5
            },
            {
                "conversion_id": "c1",
                "session_id": "s2",
                "ihc": 0.5
            }
        ]
    }

def test_process_batch(test_db_path, sample_api_response):
    processor = AttributionProcessor(test_db_path, "test_key")
    processor.process_batch(sample_api_response)
    
    conn = sqlite3.connect(test_db_path)
    df = pd.read_sql_query("SELECT * FROM attribution_customer_journey", conn)
    conn.close()
    
    assert len(df) == 2
    assert all(col in df.columns for col in ['conv_id', 'session_id', 'ihc'])

def test_update_channel_reporting(test_db_path):
    processor = AttributionProcessor(test_db_path, "test_key")
    processor.update_channel_reporting()
    
    conn = sqlite3.connect(test_db_path)
    df = pd.read_sql_query("SELECT * FROM channel_reporting", conn)
    conn.close()
    
    assert len(df) > 0
    assert all(col in df.columns for col in [
        'channel_name', 'date', 'cost', 'ihc', 'ihc_revenue'
    ])

def test_export_channel_report(test_db_path, tmp_path):
    output_path = str(tmp_path / "report.csv")
    processor = AttributionProcessor(test_db_path, "test_key")
    processor.export_channel_report(output_path)
    
    assert os.path.exists(output_path)
    df = pd.read_csv(output_path)
    assert all(col in df.columns for col in [
        'channel_name', 'date', 'cost', 'ihc', 'ihc_revenue', 'CPO', 'ROAS'
    ])

# conftest.py
import pytest
import os
import shutil

@pytest.fixture(autouse=True)
def cleanup_files(request, tmp_path):
    def cleanup():
        for file in tmp_path.glob("*"):
            if file.is_file():
                file.unlink()
            elif file.is_dir():
                shutil.rmtree(file)
    request.addfinalizer(cleanup)