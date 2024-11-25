from datetime import datetime
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base, joinedload
from sqlalchemy.pool import QueuePool
from pathlib import Path
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration and connection management"""

    def __init__(self):
        self._setup_paths()
        self._load_config()
        self._setup_engine()
        self.Base = declarative_base()

    def _setup_paths(self):
        """Setup necessary paths"""
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.data_dir.mkdir(exist_ok=True)

    def _load_config(self):
        """Load database configuration from environment variables"""
        self.db_type = os.getenv('DB_TYPE', 'sqlite').lower()

        if self.db_type == 'sqlite':
            db_path = self.data_dir / 'etl_manager.db'
            self.database_url = f'sqlite:///{db_path}'
        else:
            # PostgreSQL configuration
            self.database_url = (
                f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
                f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )

        # Pool configuration
        self.pool_size = int(os.getenv('DB_POOL_SIZE', 5))
        self.max_overflow = int(os.getenv('DB_MAX_OVERFLOW', 10))
        self.pool_timeout = int(os.getenv('DB_POOL_TIMEOUT', 30))
        self.pool_recycle = int(os.getenv('DB_POOL_RECYCLE', 1800))

    def _setup_engine(self):
        """Setup SQLAlchemy engine with appropriate configuration"""
        try:
            if self.db_type == 'sqlite':
                # SQLite specific configuration
                self.engine = create_engine(
                    self.database_url,
                    connect_args={'check_same_thread': False},
                    pool_pre_ping=True
                )
            else:
                # PostgreSQL configuration with connection pooling
                self.engine = create_engine(
                    self.database_url,
                    poolclass=QueuePool,
                    pool_size=self.pool_size,
                    max_overflow=self.max_overflow,
                    pool_timeout=self.pool_timeout,
                    pool_recycle=self.pool_recycle,
                    pool_pre_ping=True
                )

            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )

            logger.info(f"Database engine created successfully for {self.db_type}")

        except Exception as e:
            logger.error(f"Failed to create database engine: {str(e)}")
            raise

    def create_tables(self):
        """Create all database tables"""
        try:
            self.Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create database tables: {str(e)}")
            raise

    def get_session(self):
        """Get a new database session"""
        return self.SessionLocal()

    def dispose_engine(self):
        """Dispose of the database engine"""
        self.engine.dispose()
        logger.info("Database engine disposed")

    @property
    def metadata(self):
        """Get database metadata"""
        return MetaData()

    def verify_connection(self):
        """Verify database connection"""
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            logger.info("Database connection verified successfully")
            return True
        except Exception as e:
            logger.error(f"Database connection verification failed: {str(e)}")
            return False

    def get_table_names(self):
        """Get list of all tables in the database"""
        return self.engine.table_names()

    def clear_all_tables(self):
        """Clear all tables in the database (for testing purposes)"""
        if os.getenv('ENVIRONMENT') != 'testing':
            raise ValueError("Cannot clear tables in non-testing environment")

        self.Base.metadata.drop_all(self.engine)
        self.Base.metadata.create_all(self.engine)

    def backup_database(self, backup_path: str = None):
        """Create a backup of the database"""
        if self.db_type == 'sqlite':
            import shutil
            source_path = self.data_dir / 'etl_manager.db'
            if not backup_path:
                backup_path = self.data_dir / f'etl_manager_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
            shutil.copy2(source_path, backup_path)
            logger.info(f"Database backed up to {backup_path}")
        else:
            logger.warning("Database backup not implemented for PostgreSQL")

# Create global database configuration instance
db_config = DatabaseConfig()

# Helper function to get database session
def get_db():
    """Get database session with automatic cleanup"""
    db = db_config.get_session()
    try:
        yield db
    finally:
        db.close()
