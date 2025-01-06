import pytest
import pandas as pd
from build_customer_journey import CustomerJourneyBuilder

@pytest.fixture
def sample_data(test_db_path):
    # Create test data
    conn = sqlite3.connect(test_db_path)
    
    # Sample session sources
    sessions_df = pd.DataFrame({
        'session_id': ['s1', 's2', 's3'],
        'user_id': ['u1', 'u1', 'u2'],
        'channel_name': ['Organic', 'Paid', 'Social'],
        'event_date': ['2024-01-01'] * 3,
        'event_time': ['10:00:00', '11:00:00', '12:00:00'],
        'holder_engagement': [1, 0, 1],
        'closer_engagement': [0, 1, 1],
        'impression_interaction': [1, 1, 0]
    })
    sessions_df.to_sql('session_sources', conn, index=False, if_exists='replace')
    
    # Sample conversions
    conversions_df = pd.DataFrame({
        'conv_id': ['c1', 'c2'],
        'user_id': ['u1', 'u2'],
        'conv_date': ['2024-01-01'] * 2,
        'conv_time': ['12:00:00', '13:00:00'],
        'revenue': [100.0, 200.0]
    })
    conversions_df.to_sql('conversions', conn, index=False, if_exists='replace')
    
    # Sample session costs
    costs_df = pd.DataFrame({
        'session_id': ['s1', 's2', 's3'],
        'cost': [10.0, 20.0, 15.0]
    })
    costs_df.to_sql('session_costs', conn, index=False, if_exists='replace')
    
    conn.close()
    return test_db_path

def test_build_journeys(sample_data, test_sql_path):
    journey_builder = CustomerJourneyBuilder(sample_data, test_sql_path)
    journeys_df = journey_builder.build_journeys()
    
    assert len(journeys_df) > 0
    assert all(col in journeys_df.columns for col in [
        'conv_id', 'user_id', 'session_id', 'channel_name',
        'time_to_conv', 'cost', 'revenue'
    ])

def test_get_journey_stats(sample_data, test_sql_path):
    journey_builder = CustomerJourneyBuilder(sample_data, test_sql_path)
    journeys_df = journey_builder.build_journeys()
    stats = journey_builder.get_journey_stats(journeys_df)
    
    assert isinstance(stats, dict)
    assert all(key in stats for key in [
        'total_conversions',
        'total_sessions',
        'unique_users',
        'avg_sessions_per_journey'
    ])
