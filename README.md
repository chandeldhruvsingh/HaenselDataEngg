# Marketing Attribution Pipeline

## Overview
This project implements a marketing attribution pipeline that processes customer journey data and calculates channel attribution using the IHC Attribution API. The pipeline handles data from raw session and conversion events through to final channel performance reporting.

## Project Structure
\```
project_root/
├── data/
│   ├── challenge.db
│   └── challenge_db_create.sql
├── pipeline/
│   ├── __init__.py
│   ├── setup_db.py
│   ├── build_customer_journey.py
│   ├── send_to_api.py
│   └── attribution_processor.py
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_setup_db.py
    ├── test_build_customer_journey.py
    ├── test_send_to_api.py
    └── test_attribution_processor.py
\```

## Installation

1. Clone the repository:
\```bash
git clone <repository-url>
cd <project-directory>
\```

2. Create a virtual environment:
\```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
\```

3. Install dependencies:
\```bash
pip install -r requirements.txt
\```

## Configuration

1. Database Setup:
   - Place your SQLite database in \`data/challenge.db\`
   - SQL schema file should be in \`data/challenge_db_create.sql\`

2. API Configuration:
   - Set your API key in the environment:
   \```bash
   export IHC_API_KEY="your-api-key"
   \```

## Pipeline Components

### 1. Database Setup (\`setup_db.py\`)
- Initializes SQLite database
- Creates required tables
- Validates database schema

### 2. Customer Journey Builder (\`build_customer_journey.py\`)
- Constructs customer journeys from session data
- Joins conversion and cost data
- Calculates time-based metrics

### 3. API Integration (\`send_to_api.py\`)
- Formats journey data for API
- Handles API communication
- Implements retry logic
- Processes data in batches

### 4. Attribution Processor (\`attribution_processor.py\`)
- Processes API responses
- Updates attribution results
- Generates channel reporting
- Calculates key metrics (CPO, ROAS)

## Usage

1. Run the complete pipeline:
\```bash
python pipeline/attribution_processor.py
\```

2. Run individual components:
\```bash
# Setup database
python pipeline/setup_db.py

# Build customer journeys
python pipeline/build_customer_journey.py

# Send to API
python pipeline/send_to_api.py
\```

## Testing

Run all tests:
\```bash
pytest tests/
\```

Run tests with coverage:
\```bash
pytest --cov=pipeline tests/
\```

Run specific test file:
\```bash
pytest tests/test_setup_db.py
\```

## Expected Output

The pipeline generates:
1. SQLite database with processed data
2. Channel reporting CSV file with:
   - Channel performance metrics
   - Cost per Order (CPO)
   - Return on Ad Spend (ROAS)

## Data Schema

### Input Tables
1. session_sources
   - session_id (PRIMARY KEY)
   - user_id
   - channel_name
   - event_date
   - event_time
   - holder_engagement
   - closer_engagement
   - impression_interaction

2. conversions
   - conv_id (PRIMARY KEY)
   - user_id
   - conv_date
   - conv_time
   - revenue

3. session_costs
   - session_id (PRIMARY KEY)
   - cost

### Output Tables
1. attribution_customer_journey
   - conv_id
   - session_id
   - ihc

2. channel_reporting
   - channel_name
   - date
   - cost
   - ihc
   - ihc_revenue
   - CPO
   - ROAS

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your chosen license]

## Contact

[Your contact information]