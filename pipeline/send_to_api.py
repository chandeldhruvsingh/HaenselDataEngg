import json
import logging
import time
import pandas as pd
import requests

from typing import Dict, List, Optional
from build_customer_journey import CustomerJourneyBuilder
from config import config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IHCAttributionClient:
    def __init__(self, api_key: str, conv_type_id: str, base_url: str):
        """
        Initialize the IHC Attribution API client.

        Args:
            api_key: API authentication key
            conv_type_id: Conversion type identifier
            base_url: IHC API URL
        """
        self.api_key = api_key
        self.conv_type_id = conv_type_id
        self.base_url = base_url
        self.headers = {"Content-Type": "application/json", "x-api-key": api_key}

    @property
    def api_url(self) -> str:
        """Get the full API URL with query parameters."""
        return f"{self.base_url}?conv_type_id={self.conv_type_id}"

    def format_journey_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Format journey data exactly as expected by the API.

        Args:
            df: DataFrame containing journey data

        Returns:
            Formatted journey data
        """
        # Sort DataFrame by conversion_id and timestamp
        df_sorted = df.sort_values(["conv_id", "event_date", "event_time"])

        formatted_journeys = []

        for conversion_id, group in df_sorted.groupby("conv_id"):
            # Process each group of sessions for the same conversion ID
            sessions = []
            for _, row in group.iterrows():
                session = {
                    "conversion_id": str(row["conv_id"]),
                    "session_id": str(row["session_id"]),
                    "timestamp": f"{row['event_date']} {row['event_time']}",
                    "channel_label": str(row["channel_name"]),
                    "holder_engagement": int(row["holder_engagement"]),
                    "closer_engagement": int(row["closer_engagement"]),
                    "conversion": 0,  # Default to 0; updated for the last session
                    "impression_interaction": int(row["impression_interaction"]),
                }
                sessions.append(session)

            # Mark the last session's "conversion" as 1
            if sessions:
                sessions[-1]["conversion"] = 1

            # Append all sessions to the formatted journeys
            formatted_journeys.extend(sessions)

        return formatted_journeys


    def send_to_api(
        self, journeys: List[Dict], retry_count: int = 1, retry_delay: int = 5
    ) -> Optional[Dict]:
        """
        Send journeys to API with retry logic and debugging.

        Args:
            journeys: List of formatted journey dictionaries
            retry_count: Number of retries for failed requests
            retry_delay: Delay between retries in seconds

        Returns:
            API response if successful, None if all retries fail
        """
        # Prepare request payload - note conv_type_id is now in URL, not payload
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

        # Log the request details for debugging
        logger.debug(f"Request URL: {self.api_url}")
        logger.debug(f"Request payload: {json.dumps(payload, indent=2)}")

        for attempt in range(retry_count):
            try:
                response = requests.post(
                    self.api_url, headers=self.headers, json=payload, timeout=30
                )

                # Log the response for debugging
                logger.debug(f"Response status: {response.status_code}")
                logger.debug(f"Response content: {response.text}")

                if response.status_code == 400:
                    logger.error(f"Bad request - Response content: {response.text}")

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt < retry_count - 1:
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{retry_count}): {str(e)}"
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(
                        f"Request failed after {retry_count} attempts: {str(e)}"
                    )
                    return None

    def process_journeys(self, df: pd.DataFrame, batch_size: int = 200) -> List[Dict]:
        """
        Process journeys in batches and send to API.

        Args:
            df: DataFrame containing journey data
            batch_size: Maximum number of journeys to process in each batch

        Returns:
            List of API responses
        """
        logger.info("Starting journey processing...")

        formatted_journeys = self.format_journey_data(df)
        total_journeys = len(formatted_journeys)
        logger.info(f"Formatted {total_journeys} journeys")

        # Process in batches
        responses = []
        for i in range(0, total_journeys, batch_size):
            batch = formatted_journeys[i : i + batch_size]
            logger.info(
                f"Processing batch {i//batch_size + 1} with {len(batch)} journeys"
            )

            if batch:
                logger.debug(f"Sample journey data: {json.dumps(batch[0], indent=2)}")

            # Send batch to API
            response = self.send_to_api(batch)
            if response:
                logger.info(
                    f"Successfully received API response for batch {i//batch_size + 1}"
                )
                responses.append(response)
            else:
                logger.error(
                    f"Failed to get API response for batch {i//batch_size + 1}"
                )

        if responses:
            logger.info(f"Successfully processed {len(responses)} batches")
        else:
            logger.error("No batches were successfully processed")

        return responses


def main():
    logging.getLogger(__name__).setLevel(logging.DEBUG)

    # load config
    api_key = config.api.api_key
    conv_type_id = config.api.conv_type_id
    base_url = config.api.base_url
    batch_size = config.api.batch_size

    db_path = config.database.db_path
    sql_path = config.database.sql_path

    try:
        # Initialize the journey builder
        journey_builder = CustomerJourneyBuilder(db_path, sql_path)

        # Build customer journeys
        journeys_df = journey_builder.build_journeys()

        # Initialize the IHC Attribution API client
        client = IHCAttributionClient(api_key, conv_type_id, base_url)

        # Process and send the journeys to the API in batches
        responses = client.process_journeys(journeys_df, batch_size)

        # Save API responses to a file
        if responses:
            with open("attribution_results.json", "w") as f:
                json.dump(responses, f, indent=2)
            logger.info(f"Successfully saved results from {len(responses)} batches")

    except Exception as e:
        logger.error(f"Error processing journeys: {str(e)}")


if __name__ == "__main__":
    main()
