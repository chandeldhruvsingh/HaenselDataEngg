import sqlite3
import pandas as pd
import logging
import os
from typing import Optional, Dict
from datetime import datetime
from setup_db import DatabaseSetup  # Import the DatabaseSetup class

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CustomerJourneyBuilder:
    def __init__(self, db_path: str, sql_path: str):
        """
        Initialize the Customer Journey Builder.
        
        Args:
            db_path: Path to SQLite database
            sql_path: Path to SQL file for database setup
        """
        self.db_path = db_path
        self.sql_path = sql_path
        self._setup_database()

    def _setup_database(self):
        """
        Ensure the database is set up before building journeys.
        """
        logger.info("Setting up the database if required...")
        db_setup = DatabaseSetup(self.db_path, self.sql_path)
        if not db_setup.setup_database():
            logger.error("Database setup failed. Exiting.")
            raise RuntimeError("Database setup failed.")
        logger.info("Database setup completed successfully!")

    def get_connection(self) -> sqlite3.Connection:
        """Create database connection."""
        return sqlite3.connect(self.db_path)

    def build_journeys(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Build customer journeys by joining sessions with conversions.
        
        Args:
            start_date: Optional start date filter (YYYY-MM-DD)
            end_date: Optional end date filter (YYYY-MM-DD)
            
        Returns:
            DataFrame containing customer journeys
        """
        logger.info("Building customer journeys...")
        
        # Construct the base query
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
        
        # Add date filters if provided
        if start_date and end_date:
            query += f" AND c.conv_date BETWEEN '{start_date}' AND '{end_date}'"
            
        query += ") SELECT * FROM journey_data ORDER BY conv_id, event_date, event_time"
        
        try:
            with self.get_connection() as conn:
                # Read query into DataFrame
                journeys_df = pd.read_sql_query(query, conn)
                
                # Add time difference between session and conversion
                journeys_df['session_datetime'] = pd.to_datetime(
                    journeys_df['event_date'] + ' ' + journeys_df['event_time']
                )
                journeys_df['conv_datetime'] = pd.to_datetime(
                    journeys_df['conv_date'] + ' ' + journeys_df['conv_time']
                )
                journeys_df['time_to_conv'] = (
                    journeys_df['conv_datetime'] - journeys_df['session_datetime']
                ).dt.total_seconds() / 3600  # Convert to hours
                
                logger.info(f"Built {len(journeys_df)} journey records for {journeys_df['conv_id'].nunique()} conversions")
                
                # Basic data validation
                self._validate_journeys(journeys_df)
                
                return journeys_df
                
        except Exception as e:
            logger.error(f"Error building customer journeys: {str(e)}")
            raise

    def _validate_journeys(self, df: pd.DataFrame):
        """
        Validate the built customer journeys.
        
        Args:
            df: DataFrame containing customer journeys
        """
        # Check for required columns
        required_columns = [
            'conv_id', 'user_id', 'session_id', 'channel_name',
            'holder_engagement', 'closer_engagement', 'impression_interaction'
        ]
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing required columns: {missing_cols}")
            
        # Check for null values in key columns
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            logger.warning(f"Null values found:\n{null_counts[null_counts > 0]}")
            
        # Verify engagement indicators are binary
        for col in ['holder_engagement', 'closer_engagement', 'impression_interaction']:
            invalid = ~df[col].isin([0, 1])
            if invalid.any():
                logger.warning(f"Invalid values in {col}: {df[col][invalid].unique()}")

    def get_journey_stats(self, df: pd.DataFrame) -> Dict:
        """
        Generate statistics about the customer journeys.
        
        Args:
            df: DataFrame containing customer journeys
            
        Returns:
            Dictionary containing journey statistics
        """
        stats = {
            'total_conversions': df['conv_id'].nunique(),
            'total_sessions': len(df),
            'unique_users': df['user_id'].nunique(),
            'avg_sessions_per_journey': len(df) / df['conv_id'].nunique(),
            'channels': df['channel_name'].unique().tolist(),
            'date_range': f"{df['event_date'].min()} to {df['event_date'].max()}",
            'total_revenue': df.groupby('conv_id')['revenue'].first().sum(),
            'avg_revenue_per_conversion': df.groupby('conv_id')['revenue'].first().mean()
        }
        
        return stats

def main():
    # Get the absolute path to the project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(project_root, 'data', 'challenge.db')
    sql_path = os.path.join(project_root, 'data', 'challenge_db_create.sql')
    
    # Initialize journey builder
    journey_builder = CustomerJourneyBuilder(db_path, sql_path)
    
    try:
        # Build journeys
        journeys_df = journey_builder.build_journeys()
        
        # Get and print statistics
        stats = journey_builder.get_journey_stats(journeys_df)
        logger.info("\nJourney Statistics:")
        for key, value in stats.items():
            logger.info(f"{key}: {value}")
            
    except Exception as e:
        logger.error(f"Failed to build customer journeys: {e}")

if __name__ == "__main__":
    main()
