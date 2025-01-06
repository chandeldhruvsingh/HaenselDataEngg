import pytest
from unittest.mock import patch, Mock
from send_to_api import IHCAttributionClient

@pytest.fixture
def sample_journey_data():
    return pd.DataFrame({
        'conv_id': ['c1', 'c1', 'c2'],
        'session_id': ['s1', 's2', 's3'],
        'channel_name': ['Organic', 'Paid', 'Social'],
        'event_date': ['2024-01-01'] * 3,
        'event_time': ['10:00:00', '11:00:00', '12:00:00'],
        'holder_engagement': [1, 0, 1],
        'closer_engagement': [0, 1, 1],
        'impression_interaction': [1, 1, 0]
    })

def test_format_journey_data(sample_journey_data):
    client = IHCAttributionClient("test_key")
    formatted = client.format_journey_data(sample_journey_data)
    
    assert isinstance(formatted, list)
    assert len(formatted) > 0
    assert all(isinstance(journey, dict) for journey in formatted)
    
    # Check required fields
    required_fields = {
        'conversion_id', 'session_id', 'timestamp',
        'channel_label', 'holder_engagement', 'closer_engagement',
        'conversion', 'impression_interaction'
    }
    assert all(all(field in journey for field in required_fields) 
              for journey in formatted)

@patch('requests.post')
def test_send_to_api_success(mock_post, sample_journey_data):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"statusCode": 200, "value": []}
    mock_post.return_value = mock_response
    
    client = IHCAttributionClient("test_key")
    formatted = client.format_journey_data(sample_journey_data)
    response = client.send_to_api(formatted)
    
    assert response is not None
    assert response["statusCode"] == 200
    mock_post.assert_called_once()

@patch('requests.post')
def test_send_to_api_retry(mock_post, sample_journey_data):
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = Exception("API Error")
    mock_post.return_value = mock_response
    
    client = IHCAttributionClient("test_key")
    formatted = client.format_journey_data(sample_journey_data)
    response = client.send_to_api(formatted, retry_count=3)
    
    assert response is None
    assert mock_post.call_count == 3