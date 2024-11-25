import os
from pathlib import Path
from typing import Dict, Any
import yaml
from datetime import timedelta

class Config:
    """Configuration management for ETL Manager"""

    def __init__(self):
        # Base directories
        self.BASE_DIR = Path(__file__).parent.parent
        self.CONFIG_DIR = self.BASE_DIR / "config"
        self.SCRIPTS_DIR = self.BASE_DIR / "etl_scripts"
        self.LOGS_DIR = self.BASE_DIR / "logs"

        # Ensure directories exist
        self._create_directories()

        # Load configuration
        self.config_file = self.CONFIG_DIR / "app_config.yaml"
        self.config = self._load_config()

    def _create_directories(self):
        """Create necessary directories if they don't exist"""
        for directory in [self.CONFIG_DIR, self.SCRIPTS_DIR, self.LOGS_DIR]:
            directory.mkdir(exist_ok=True)

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file or create default"""
        if not self.config_file.exists():
            return self._create_default_config()

        with open(self.config_file, 'r') as f:
            return yaml.safe_load(f)

    def _create_default_config(self) -> Dict[str, Any]:
        """Create and save default configuration"""
        default_config = {
            'database': {
                'type': 'sqlite',  # or 'postgresql'
                'name': 'etl_manager.db',
                'host': 'localhost',
                'port': 5432,
                'user': 'etl_user',
                'password': '',  # Set through environment variable
            },
            'scheduler': {
                'max_concurrent_jobs': 3,
                'job_timeout': 3600,  # 1 hour in seconds
                'retry_limit': 3,
                'retry_delay': 300,  # 5 minutes in seconds
            },
            'logging': {
                'level': 'INFO',
                'retention_days': 30,
                'max_size_mb': 100,
            },
            'email_notifications': {
                'enabled': False,
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'sender_email': '',
                'sender_password': '',  # Set through environment variable
                'recipients': [],
            },
            'security': {
                'secret_key': os.urandom(24).hex(),
                'session_duration': 3600,  # 1 hour in seconds
                'require_auth': True,
            },
            'api': {
                'enabled': True,
                'rate_limit': 100,  # requests per minute
                'require_key': True,
            }
        }

        # Save default configuration
        self.save_config(default_config)
        return default_config

    def save_config(self, config: Dict[str, Any]):
        """Save configuration to YAML file"""
        with open(self.config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
        self.config = config

    def get_database_url(self) -> str:
        """Get database URL based on configuration"""
        db_config = self.config['database']

        if db_config['type'] == 'sqlite':
            db_path = self.BASE_DIR / db_config['name']
            return f"sqlite:///{db_path}"

        elif db_config['type'] == 'postgresql':
            password = os.getenv('ETL_DB_PASSWORD', db_config['password'])
            return f"postgresql://{db_config['user']}:{password}@{db_config['host']}:{db_config['port']}/{db_config['name']}"

    def get_scheduler_config(self) -> Dict[str, Any]:
        """Get scheduler-specific configuration"""
        return self.config['scheduler']

    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging-specific configuration"""
        return self.config['logging']

    def get_notification_config(self) -> Dict[str, Any]:
        """Get notification-specific configuration"""
        config = self.config['email_notifications']
        if config['enabled']:
            config['sender_password'] = os.getenv('ETL_SMTP_PASSWORD', '')
        return config

    def get_security_config(self) -> Dict[str, Any]:
        """Get security-specific configuration"""
        return self.config['security']

    def get_api_config(self) -> Dict[str, Any]:
        """Get API-specific configuration"""
        return self.config['api']

    def update_section(self, section: str, updates: Dict[str, Any]):
        """Update a specific section of the configuration"""
        if section in self.config:
            self.config[section].update(updates)
            self.save_config(self.config)
        else:
            raise KeyError(f"Configuration section '{section}' not found")

    def get_job_timeout(self) -> timedelta:
        """Get job timeout duration"""
        return timedelta(seconds=self.config['scheduler']['job_timeout'])

    def get_retry_delay(self) -> timedelta:
        """Get retry delay duration"""
        return timedelta(seconds=self.config['scheduler']['retry_delay'])

# Create global config instance
config = Config()
