import pytest
import pandas as pd
from unittest.mock import patch, Mock
import requests
import logging
from typing import List, Dict, Optional
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IHCAttributionClient:
    def __init__(self, api_key: str, conv_type_id: str = "data_engineering_challenge"):
        self.api_key = api_key
        self.conv_type_id = conv_type_id
        self.base_url = "https://api.ihc-attribution.com/v1/compute_ihc"
        self.headers = {"Content-Type": "application/json", "x-api-key": api_key}

    @property
    def api_url(self) -> str:
        return f"{self.base_url}?conv_type_id={self.conv_type_id}"

    def format_journey_data(self, df: pd.DataFrame) -> List[Dict]:
        df_sorted = df.sort_values(["conv_id", "event_date", "event_time"])

        formatted_journeys = []
        current_conversion = None
        current_sessions = []

        for _, row in df_sorted.iterrows():
            if current_conversion != row["conv_id"]:
                if current_conversion and current_sessions:
                    journey = {
                        "conversion_id": str(current_conversion),
                        "session_id": current_sessions[0]["session_id"],
                        "timestamp": current_sessions[0]["timestamp"],
                        "channel_label": current_sessions[0]["channel_label"],
                        "holder_engagement": current_sessions[0]["holder_engagement"],
                        "closer_engagement": current_sessions[0]["closer_engagement"],
                        "conversion": 1
                        if current_sessions[-1]["closer_engagement"]
                        else 0,
                        "impression_interaction": current_sessions[0][
                            "impression_interaction"
                        ],
                    }
                    formatted_journeys.append(journey)

                current_conversion = row["conv_id"]
                current_sessions = []

            session = {
                "session_id": str(row["session_id"]),
                "timestamp": f"{row['event_date']} {row['event_time']}",
                "channel_label": str(row["channel_name"]),
                "holder_engagement": bool(row["holder_engagement"]),
                "closer_engagement": bool(row["closer_engagement"]),
                "impression_interaction": bool(row["impression_interaction"]),
            }
            current_sessions.append(session)

        if current_conversion and current_sessions:
            journey = {
                "conversion_id": str(current_conversion),
                "session_id": current_sessions[0]["session_id"],
                "timestamp": current_sessions[0]["timestamp"],
                "channel_label": current_sessions[0]["channel_label"],
                "holder_engagement": current_sessions[0]["holder_engagement"],
                "closer_engagement": current_sessions[0]["closer_engagement"],
                "conversion": 1 if current_sessions[-1]["closer_engagement"] else 0,
                "impression_interaction": current_sessions[0]["impression_interaction"],
            }
            formatted_journeys.append(journey)

        return formatted_journeys

    def send_to_api(
        self, journeys: List[Dict], retry_count: int = 1, retry_delay: int = 5
    ) -> Optional[Dict]:
        payload = {
            "customer_journeys": journeys,
            "redistribution_parameter": {
                "initializer": {
                    "direction": "earlier_sessions_only",
                    "receive_threshold": 0,
                    "redistribution_channel_labels": ["Direct", "Email_Newsletter"],
                },
                "holder": {
                    "direction": "any_session",
                    "receive_threshold": 0,
                    "redistribution_channel_labels": ["Direct", "Email_Newsletter"],
                },
                "closer": {
                    "direction": "later_sessions_only",
                    "receive_threshold": 0.1,
                    "redistribution_channel_labels": ["SEO - Brand"],
                },
            },
        }

        for attempt in range(retry_count):
            try:
                response = requests.post(
                    self.api_url, headers=self.headers, json=payload, timeout=30
                )
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt < retry_count - 1:
                    time.sleep(retry_delay)
                else:
                    return None


# Test fixtures and functions
@pytest.fixture
def sample_journey_data():
    return pd.DataFrame(
        {
            "conv_id": ["c1", "c1", "c2"],
            "session_id": ["s1", "s2", "s3"],
            "channel_name": ["Organic", "Paid", "Social"],
            "event_date": ["2024-01-01"] * 3,
            "event_time": ["10:00:00", "11:00:00", "12:00:00"],
            "holder_engagement": [1, 0, 1],
            "closer_engagement": [0, 1, 1],
            "impression_interaction": [1, 1, 0],
        }
    )


def test_format_journey_data(sample_journey_data):
    client = IHCAttributionClient("test_key")
    formatted = client.format_journey_data(sample_journey_data)

    assert isinstance(formatted, list)
    assert len(formatted) > 0
    assert all(isinstance(journey, dict) for journey in formatted)

    required_fields = {
        "conversion_id",
        "session_id",
        "timestamp",
        "channel_label",
        "holder_engagement",
        "closer_engagement",
        "conversion",
        "impression_interaction",
    }
    assert all(
        all(field in journey for field in required_fields) for journey in formatted
    )


@patch("requests.post")
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


@patch("requests.post")
def test_send_to_api_retry(mock_post, sample_journey_data):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.RequestException(
        "API Error"
    )
    mock_post.return_value = mock_response

    client = IHCAttributionClient("test_key")
    formatted = client.format_journey_data(sample_journey_data)

    with patch("time.sleep") as mock_sleep:
        response = client.send_to_api(formatted, retry_count=3)

        assert mock_post.call_count == 3
        assert mock_sleep.call_count == 2
        assert response is None


@patch("requests.post")
def test_send_to_api_retry_success(mock_post, sample_journey_data):
    mock_failure = Mock()
    mock_failure.raise_for_status.side_effect = requests.exceptions.RequestException(
        "API Error"
    )

    mock_success = Mock()
    mock_success.status_code = 200
    mock_success.json.return_value = {"statusCode": 200, "value": []}

    mock_post.side_effect = [mock_failure, mock_failure, mock_success]

    client = IHCAttributionClient("test_key")
    formatted = client.format_journey_data(sample_journey_data)

    with patch("time.sleep") as mock_sleep:
        response = client.send_to_api(formatted, retry_count=3)

        assert mock_post.call_count == 3
        assert mock_sleep.call_count == 2
        assert response is not None
        assert response["statusCode"] == 200
