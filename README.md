# ETL Manager

A comprehensive ETL job management system with web interface.

## Project Structure

```
ETL_MANAGER/
├── app/                    # Application core
│   ├── config.py          # Configuration management
│   ├── database_setup.py  # Database initialization
│   ├── database.py        # Database operations
│   ├── models.py          # SQLAlchemy models
│   ├── scheduler.py       # Job scheduler
│   └── streamlit_app.py   # Web interface
├── config/                # Configuration files
│   └── etl_config.yaml   # ETL configuration
├── logs/                  # Log files
│   ├── etl_manager.log
│   └── etl_scheduler.log
├── scripts/              # ETL Scripts
│   ├── templates/        # Script templates
│   ├── api_to_database.py
│   └── csv_to_database.py
├── data/                 # Data directory
├── .env                  # Environment variables
├── requirements.txt      # Project dependencies
└── README.md            # Project documentation
```

## Setup

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment:
- Copy `.env.example` to `.env`
- Update settings as needed

4. Initialize database:
```bash
python -c "from app.models import init_db; init_db()"
```

5. Run the application:
```bash
streamlit run app/streamlit_app.py
```

## Features

- Web-based ETL job management
- Job scheduling and monitoring
- Support for multiple database types
- Template-based ETL scripts
- Job dependency management
- Execution logging and metrics

## Development

1. Code Style:
```bash
black app/
flake8 app/
```

2. Run Tests:
```bash
pytest tests/
```

## Adding New ETL Jobs

1. Create new script in `scripts/` directory
2. Use template from `scripts/templates/`
3. Add job through web interface

## License

MIT License
