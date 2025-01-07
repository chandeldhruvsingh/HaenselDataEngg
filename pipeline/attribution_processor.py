import pandas as pd
import sqlite3
import os
import logging
import argparse
from datetime import datetime
from pipeline.send_to_api import IHCAttributionClient
from pipeline.build_customer_journey import CustomerJourneyBuilder
from typing import List, Dict, Optional
from config import config


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AttributionProcessor:
    def __init__(self, db_path: str, api_key: str, conv_type_id: str, base_url: str):
        """
        Initialize the processor with database and API settings.

        Args:
            db_path: Path to the SQLite database
            api_key: API authentication key
            conv_type_id: Conversion type identifier
        """
        self.db_path = db_path
        self.api_client = IHCAttributionClient(api_key, conv_type_id, base_url)

    def process_batch(self, batch_response: Dict):
        """
        Process a single batch response from the API.

        Args:
            batch_response: API response for a single batch
        """
        if batch_response["statusCode"] != 200:
            logger.error(f"Batch processing failed: {batch_response}")
            return

        with sqlite3.connect(self.db_path) as conn:
            # Insert attribution results
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
                logger.info(f"Processed {len(df)} attributions from batch")

    def update_channel_reporting(self):
        """
        Update channel_reporting table using latest attribution data.
        """
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
            logger.info("Channel reporting table updated")

    def export_channel_report(self, output_path: str):
        """
        Export final channel report with CPO and ROAS metrics.

        Args:
            output_path: Path where CSV file will be saved
        """
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                """
                SELECT * FROM channel_reporting
                ORDER BY date, channel_name
            """,
                conn,
            )

            # Calculate CPO and ROAS
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
            logger.info(f"Channel report exported to {output_path}")


def validate_dates(start_date, end_date):
    if start_date and end_date:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        if start > end:
            raise ValueError("Start date cannot be later than end date")
    return True


def load_config():
    """
    Load configuration settings from config file.

    Returns:
        tuple: Contains (api_key, conv_type_id, base_url, db_path, sql_path, output_path, batch_size)
    """
    return (
        config.api.api_key,
        config.api.conv_type_id,
        config.api.base_url,
        config.database.db_path,
        config.database.sql_path,
        config.output.file_path,
        config.api.batch_size,
    )


def main():
    # Parse start and end date if provided
    parser = argparse.ArgumentParser(
        description="Attribution Processor with time-range support."
    )
    parser.add_argument(
        "--start_date", type=str, help="Start date in YYYY-MM-DD format."
    )
    parser.add_argument("--end_date", type=str, help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()

    # loading config
    (
        api_key,
        conv_type_id,
        base_url,
        db_path,
        sql_path,
        output_path,
        batch_size,
    ) = load_config()

    try:
        # Validate dates first
        try:
            validate_dates(args.start_date, args.end_date)
        except ValueError as date_error:
            logger.error(str(date_error))
            return
        # Initialize processor
        processor = AttributionProcessor(db_path, api_key, conv_type_id, base_url)

        # Get journey data
        journey_builder = CustomerJourneyBuilder(db_path, sql_path)
        journeys_df = journey_builder.build_journeys(
            start_date=args.start_date, end_date=args.end_date
        )

        # Process journeys in batches
        formatted_journeys = processor.api_client.format_journey_data(journeys_df)
        total_journeys = len(formatted_journeys)

        logger.info(f"Processing {total_journeys} journeys in batches of {batch_size}")

        for i in range(0, total_journeys, batch_size):
            batch = formatted_journeys[i : i + batch_size]
            logger.info(
                f"Processing batch {i//batch_size + 1} of {(total_journeys + batch_size - 1)//batch_size}"
            )

            # Get attribution for batch
            response = processor.api_client.send_to_api(batch)
            if response:
                # Process batch results
                processor.process_batch(response)

            # Update channel reporting after each batch
            processor.update_channel_reporting()

        # Export final report
        processor.export_channel_report(output_path)
        logger.info("Processing completed successfully")

    except Exception as e:
        logger.error(f"Error in processing: {str(e)}")
        raise


if __name__ == "__main__":
    main()
