import requests
import pandas as pd
import logging
import json
import os
from typing import List, Dict, Optional
import time
from build_customer_journey import CustomerJourneyBuilder

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IHCAttributionClient:
    def __init__(self, api_key: str, conv_type_id: str = 'data_engineering_challenge'):
        """
        Initialize the IHC Attribution API client.
        
        Args:
            api_key: API authentication key
            conv_type_id: Conversion type identifier
        """
        self.api_key = api_key
        self.conv_type_id = conv_type_id
        self.base_url = "https://api.ihc-attribution.com/v1/compute_ihc"
        self.headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key
        }
        
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
        df_sorted = df.sort_values(['conv_id', 'event_date', 'event_time'])
        
        formatted_journeys = []
        current_conversion = None
        current_sessions = []
        
        for _, row in df_sorted.iterrows():
            # If we've moved to a new conversion
            if current_conversion != row['conv_id']:
                # Save the previous conversion if it exists
                if current_conversion and current_sessions:
                    journey = {
                        "conversion_id": str(current_conversion),
                        "session_id": current_sessions[0]["session_id"],
                        "timestamp": current_sessions[0]["timestamp"],
                        "channel_label": current_sessions[0]["channel_label"],
                        "holder_engagement": current_sessions[0]["holder_engagement"],
                        "closer_engagement": current_sessions[0]["closer_engagement"],
                        "conversion": 1 if current_sessions[-1]["closer_engagement"] else 0,
                        "impression_interaction": current_sessions[0]["impression_interaction"]
                    }
                    formatted_journeys.append(journey)
                
                current_conversion = row['conv_id']
                current_sessions = []
            
            # Format the session data
            session = {
                "session_id": str(row['session_id']),
                "timestamp": f"{row['event_date']} {row['event_time']}",
                "channel_label": str(row['channel_name']),
                "holder_engagement": bool(row['holder_engagement']),
                "closer_engagement": bool(row['closer_engagement']),
                "impression_interaction": bool(row['impression_interaction'])
            }
            current_sessions.append(session)
        
        # Add the last conversion
        if current_conversion and current_sessions:
            journey = {
                "conversion_id": str(current_conversion),
                "session_id": current_sessions[0]["session_id"],
                "timestamp": current_sessions[0]["timestamp"],
                "channel_label": current_sessions[0]["channel_label"],
                "holder_engagement": current_sessions[0]["holder_engagement"],
                "closer_engagement": current_sessions[0]["closer_engagement"],
                "conversion": 1 if current_sessions[-1]["closer_engagement"] else 0,
                "impression_interaction": current_sessions[0]["impression_interaction"]
            }
            formatted_journeys.append(journey)
        
        return formatted_journeys

    def send_to_api(self, journeys: List[Dict], retry_count: int = 1, retry_delay: int = 5) -> Optional[Dict]:
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
                    "redistribution_channel_labels": ["Direct", "Email_Newsletter"]
                },
                "holder": {
                    "direction": "any_session",
                    "receive_threshold": 0,
                    "redistribution_channel_labels": ["Direct", "Email_Newsletter"]
                },
                "closer": {
                    "direction": "later_sessions_only",
                    "receive_threshold": 0.1,
                    "redistribution_channel_labels": ["SEO - Brand"]
                }
            }
        }
        
        # Log the request details for debugging
        logger.debug(f"Request URL: {self.api_url}")
        logger.debug(f"Request payload: {json.dumps(payload, indent=2)}")
        
        for attempt in range(retry_count):
            try:
                response = requests.post(
                    self.api_url,  # Using the URL with query parameter
                    headers=self.headers,
                    json=payload,
                    timeout=30
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
                    logger.warning(f"Request failed (attempt {attempt + 1}/{retry_count}): {str(e)}")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Request failed after {retry_count} attempts: {str(e)}")
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
        
        # Format the journey data
        formatted_journeys = self.format_journey_data(df)
        total_journeys = len(formatted_journeys)
        logger.info(f"Formatted {total_journeys} journeys")
        
        # Process in batches
        responses = []
        for i in range(0, total_journeys, batch_size):
            batch = formatted_journeys[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} journeys")
            
            # Add debug logging
            if batch:
                logger.debug(f"Sample journey data: {json.dumps(batch[0], indent=2)}")
            
            # Send batch to API
            response = self.send_to_api(batch)
            if response:
                logger.info(f"Successfully received API response for batch {i//batch_size + 1}")
                responses.append(response)
            else:
                logger.error(f"Failed to get API response for batch {i//batch_size + 1}")
        
        if responses:
            logger.info(f"Successfully processed {len(responses)} batches")
        else:
            logger.error("No batches were successfully processed")
        
        return responses

def main():
    # Set debug logging
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    
    # Your API key and conv_type_id
    api_key = "17e9cc65-2e46-4345-9210-36860a63f435"
    conv_type_id = "data_engineering_challenge"
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Construct paths
    db_path = os.path.join(project_root, 'data', 'challenge.db')
    sql_path = os.path.join(project_root, 'data', 'challenge_db_create.sql')
    
    try:
        # Initialize the journey builder
        journey_builder = CustomerJourneyBuilder(db_path, sql_path)
        
        # Build customer journeys
        journeys_df = journey_builder.build_journeys()
        
        # Initialize the IHC Attribution API client
        client = IHCAttributionClient(api_key, conv_type_id)
        
        # Process and send the journeys to the API in batches
        responses = client.process_journeys(journeys_df, batch_size=200)
        
        # Save API responses to a file
        if responses:
            with open('attribution_results.json', 'w') as f:
                json.dump(responses, f, indent=2)
            logger.info(f"Successfully saved results from {len(responses)} batches")
        
    except Exception as e:
        logger.error(f"Error processing journeys: {str(e)}")

if __name__ == "__main__":
    main()
