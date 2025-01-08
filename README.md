# Marketing Attribution Pipeline

## Overview
This project implements a marketing attribution pipeline that processes customer journey data and calculates channel attribution using the IHC Attribution API. The pipeline handles data from raw session and conversion events through to final channel performance reporting.

## Project Structure

      root/
      ├── data/
      │   ├── challenge.db
      │   ├── challenge_db_create.sql
      ├── output/
      │   └── channel_reporting.csv
      ├── pipeline/
      │   ├── setup_db.py
      │   ├── build_customer_journey.py
      │   ├── send_to_api.py
      │   └── attribution_processor.py
      ├── tests/
      │   ├── test_setup_db.py
      │   ├── test_build_customer_journey.py
      │   ├── test_send_to_api.py
      │   └── test_attribution_processor.py
      ├── airflow/
      │   ├── docker-compose.yml
      │   ├── dags/
      │   │   └── attribution_pipeline_dag.py
      │   └── data/
      │       ├── challenge.db
      │       └── challenge_db_create.sql
      ├── config/
      │   ├── config.yaml
      │   └── config.py
      ├── requirements.txt
      ├── Dockerfile
      └── Makefile

## Architecture Diagram
![ArchitectureDiagram](https://github.com/user-attachments/assets/f90ba33d-20ba-4c45-828e-92c162abb88a)


## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <project-directory>
   ```

## Configuration

1. **Database Setup**:
   - Place your SQLite database file in `data/challenge.db`.

2. **API Configuration**:
   - Set your API key in the `config.yaml` file inside the `config` directory.

## Running the Pipeline

The pipeline can be executed through the Makefile for ease of use. Below are the available commands:

### Build the Docker Image
```bash
make build
```

### Run the Pipeline
```bash
make run
```
- Optionally, you can specify a date range:
   ```bash
   make run START_DATE=2023-01-07 END_DATE=2023-02-21
   ```

### Stop the Running Container
```bash
make stop
```

### Clean Up Docker Images
```bash
make clean
```

## Testing

Run all tests:
```bash
make test
```


Run a specific test file:
```bash
pytest tests/test_setup_db.py
```

## Pipeline Components

### 1. Database Setup (`setup_db.py`)
- Initializes the SQLite database.
- Creates required tables.
- Validates database schema.

### 2. Customer Journey Builder (`build_customer_journey.py`)
- Constructs customer journeys from session data.
- Joins conversion and cost data.
- Calculates time-based metrics.

### 3. API Integration (`send_to_api.py`)
- Formats journey data for API.
- Handles API communication.
- Implements retry logic.
- Processes data in batches.

### 4. Attribution Processor (`attribution_processor.py`)
- Processes API responses.
- Updates attribution results.
- Generates channel reporting.
- Calculates key metrics (CPO, ROAS).

## Triggering the Airflow DAGs

To deploy and trigger the Airflow DAGs:

1. **Set Up Airflow Environment**:
   - Navigate to the `airflow/` directory.
   - Place the .db database file inside the data directory.
   - Start Airflow services using Docker Compose:
     ```bash
     cd airflow
     docker-compose up -d
     ```

2. **Access the Airflow Web Interface**:
   - Open your browser and go to `http://localhost:8080`.
   - Use the default credentials:
     - Username: `airflow`
     - Password: `airflow`

3. **Activate the DAG**:
   - Locate the `attribution_pipeline_dag` in the Airflow interface.
   - Toggle the DAG to the `On` state.

4. **Trigger the DAG**:
   - Click the play button ▶️ next to the `attribution_pipeline_dag`.
   - Monitor task progress in the Airflow interface.

## Expected Output

The pipeline generates:
1. SQLite database with processed data in attribution_customer_journey and channel_reporting tables .
2. Channel reporting CSV file located in the `output/` directory containing:
   - Channel performance metrics.
   - Cost per Order (CPO).
   - Return on Ad Spend (ROAS).


## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Commit your changes.
4. Push to the branch.
5. Create a Pull Request.

## Contact

chandeldhruvsingh@gmail.com
