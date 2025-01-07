import logging
import os
import re
import sqlite3

from config import config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DatabaseSetup:
    def __init__(self, db_path: str, sql_path: str):
        self.db_path = db_path
        self.sql_path = sql_path

    def clean_sql_script(self, sql_content: str) -> str:
        """
        Clean SQL script by removing comments and empty lines.

        Args:
            sql_content: Raw SQL script content

        Returns:
            Cleaned SQL script
        """
        # Remove content between triple quotes
        sql_content = re.sub(r'""".*?"""', "", sql_content, flags=re.DOTALL)

        # Remove empty lines and single line comments
        lines = []
        for line in sql_content.split("\n"):
            line = line.strip()
            if line and not line.startswith("#"):
                lines.append(line)

        return "\n".join(lines)

    def setup_database(self) -> bool:
        try:
            if not os.path.exists(self.sql_path):
                logger.error(f"SQL file not found at {self.sql_path}")
                return False

            # Print the paths for verification
            logger.info(f"Using database path: {self.db_path}")
            logger.info(f"Using SQL file path: {self.sql_path}")

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Read and clean SQL script
            with open(self.sql_path, "r") as sql_file:
                sql_script = sql_file.read()
                clean_script = self.clean_sql_script(sql_script)

                statements = [
                    stmt.strip() for stmt in clean_script.split(";") if stmt.strip()
                ]
                for statement in statements:
                    cursor.execute(statement)

            conn.commit()
            logger.info("Database setup completed successfully!")

            # Print Table Info
            self.verify_tables(cursor)

            conn.close()
            return True

        except sqlite3.Error as e:
            logger.error(f"Database error occurred: {e}")
            logger.error(
                f"Failed statement: {statement if 'statement' in locals() else 'Unknown'}"
            )
            return False
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return False

    def verify_tables(self, cursor: sqlite3.Cursor):
        expected_tables = [
            "session_sources",
            "conversions",
            "session_costs",
            "attribution_customer_journey",
            "channel_reporting",
        ]

        logger.info("\nVerifying tables:")
        for table in expected_tables:
            cursor.execute(
                f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table}'"
            )
            if cursor.fetchone()[0] == 1:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                logger.info(f"✓ {table:<30} (rows: {row_count})")
            else:
                logger.warning(f"✗ Table '{table}' not found!")


def main():

    db_path = config.database.db_path
    sql_path = config.database.sql_path

    # Initialize and run setup
    db_setup = DatabaseSetup(db_path, sql_path)
    if db_setup.setup_database():
        logger.info("Database setup completed successfully!")
    else:
        logger.error("Database setup failed!")


if __name__ == "__main__":
    main()
