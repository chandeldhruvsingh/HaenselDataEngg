import os
import yaml
from dataclasses import dataclass


@dataclass
class APIConfig:
    base_url: str
    api_key: str
    conv_type_id: str
    batch_size: int
    retry_count: int
    retry_delay: int
    timeout: int


@dataclass
class DatabaseConfig:
    db_name: str
    sql_name: str
    data_dir: str

    @property
    def project_root(self) -> str:
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    @property
    def db_path(self) -> str:
        return os.path.join(self.project_root, self.data_dir, self.db_name)

    @property
    def sql_path(self) -> str:
        return os.path.join(self.project_root, self.data_dir, self.sql_name)


@dataclass
class LoggingConfig:
    level: str
    format: str


@dataclass
class OutputConfig:
    file_path: str

    @property
    def output_path(self) -> str:
        return os.path.join(DatabaseConfig.project_root, self.directory, self.filename)


class Config:
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "config.yaml"
            )

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at: {config_path}")

        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        self.api = APIConfig(**config_data["api"])
        self.database = DatabaseConfig(**config_data["database"])
        self.logging = LoggingConfig(**config_data["logging"])
        self.output = OutputConfig(**config_data["output"])


# Create and export the config instance
config = Config()

# Add this for debugging
if __name__ == "__main__":
    print("Config loaded successfully")
    print(f"Database path: {config.database.db_path}")
    print(f"SQL path: {config.database.sql_path}")
