from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Text, JSON, ForeignKey,
    Enum, Float, Table, UniqueConstraint
)
from sqlalchemy.orm import relationship, validates
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property
from datetime import datetime
import enum
from database_setup import db_config
import json

# Enums for constrained fields
class JobStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

class DataSourceType(enum.Enum):
    DATABASE = "database"
    API = "api"
    FILE = "file"
    S3 = "s3"
    SFTP = "sftp"
    HTTP = "http"

# Association tables for many-to-many relationships
job_tags = Table('job_tags', db_config.Base.metadata,
    Column('job_id', Integer, ForeignKey('etl_jobs.id')),
    Column('tag_id', Integer, ForeignKey('tags.id'))
)

class Tag(db_config.Base):
    """Tags for organizing ETL jobs"""
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    color = Column(String(7))  # Hex color code

    # Relationships
    jobs = relationship('ETLJob', secondary=job_tags, back_populates='tags')

class ETLJob(db_config.Base):
    """Enhanced ETL Job definition"""
    __tablename__ = 'etl_jobs'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    script_path = Column(String(255), nullable=False)
    schedule = Column(String(100), nullable=False)
    description = Column(Text)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    last_run = Column(DateTime(timezone=True))
    parameters = Column(JSON)

    # New fields
    priority = Column(Integer, default=1)  # Higher number = higher priority
    timeout = Column(Integer)  # Timeout in seconds
    max_retries = Column(Integer, default=3)
    retry_delay = Column(Integer, default=300)  # Delay between retries in seconds
    owner = Column(String(100))
    notification_emails = Column(JSON)  # List of email addresses
    version = Column(String(20))  # Version tracking
    is_template = Column(Boolean, default=False)  # Template flag
    parent_id = Column(Integer, ForeignKey('etl_jobs.id'), nullable=True)  # For template inheritance

    # Resource constraints
    max_memory_mb = Column(Integer)
    max_cpu_cores = Column(Float)

    # Relationships
    executions = relationship("JobExecution", back_populates="job", cascade="all, delete-orphan")
    tags = relationship("Tag", secondary=job_tags, back_populates="jobs")
    dependencies = relationship("JobDependency",
                              foreign_keys="[JobDependency.job_id]",
                              back_populates="job",
                              cascade="all, delete-orphan")
    dependent_jobs = relationship("JobDependency",
                                foreign_keys="[JobDependency.depends_on_job_id]",
                                backref="dependent_job")
    children = relationship("ETLJob", backref=db_config.Base.backref('parent', remote_side=[id]))

    @validates('schedule')
    def validate_schedule(self, key, schedule):
        """Validate schedule format"""
        valid_formats = ['daily at', 'every', 'cron']
        if not any(schedule.startswith(fmt) for fmt in valid_formats):
            raise ValueError(f"Invalid schedule format. Must start with: {', '.join(valid_formats)}")
        return schedule

    @hybrid_property
    def success_rate(self):
        """Calculate job success rate"""
        total_runs = len(self.executions)
        if total_runs == 0:
            return 0
        successful_runs = sum(1 for e in self.executions if e.status == JobStatus.COMPLETED.value)
        return (successful_runs / total_runs) * 100

    def to_dict(self):
        """Convert job to dictionary"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'schedule': self.schedule,
            'enabled': self.enabled,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'priority': self.priority,
            'success_rate': self.success_rate,
            'tags': [tag.name for tag in self.tags],
            'version': self.version
        }

class JobExecution(db_config.Base):
    """Enhanced Job Execution Records"""
    __tablename__ = 'job_executions'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('etl_jobs.id'))
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True))
    status = Column(Enum(JobStatus), nullable=False, default=JobStatus.PENDING)
    error_message = Column(Text)
    logs = Column(Text)

    # New fields
    retry_count = Column(Integer, default=0)
    duration = Column(Float)  # Duration in seconds
    rows_processed = Column(Integer)
    memory_usage_mb = Column(Float)
    cpu_usage_percent = Column(Float)

    # Metrics storage
    metrics = Column(JSON)  # Store detailed metrics

    # Relationships
    job = relationship("ETLJob", back_populates="executions")

    @hybrid_property
    def duration_seconds(self):
        """Calculate execution duration"""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def add_metric(self, name: str, value: any):
        """Add a metric to the execution"""
        if self.metrics is None:
            self.metrics = {}
        self.metrics[name] = value

    def to_dict(self):
        """Convert execution to dictionary"""
        return {
            'id': self.id,
            'job_id': self.job_id,
            'status': self.status.value,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': self.duration_seconds,
            'error_message': self.error_message,
            'metrics': self.metrics
        }

class JobDependency(db_config.Base):
    """Enhanced Job Dependencies"""
    __tablename__ = 'job_dependencies'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('etl_jobs.id'))
    depends_on_job_id = Column(Integer, ForeignKey('etl_jobs.id'))
    condition = Column(String(50))  # success, completed, failed

    # New fields
    timeout = Column(Integer)  # Timeout waiting for dependency
    optional = Column(Boolean, default=False)  # Is this dependency optional?

    # Relationships
    job = relationship("ETLJob", foreign_keys=[job_id], back_populates="dependencies")

    __table_args__ = (
        UniqueConstraint('job_id', 'depends_on_job_id', name='unique_job_dependency'),
    )

class DataSource(db_config.Base):
    """Enhanced Data Source configuration"""
    __tablename__ = 'data_sources'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    type = Column(Enum(DataSourceType), nullable=False)
    configuration = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # New fields
    enabled = Column(Boolean, default=True)
    description = Column(Text)
    owner = Column(String(100))
    last_tested = Column(DateTime(timezone=True))
    test_status = Column(Boolean)
    credentials_key = Column(String(255))  # Reference to secured credentials

    @validates('configuration')
    def validate_configuration(self, key, configuration):
        """Validate configuration based on source type"""
        if isinstance(configuration, str):
            configuration = json.loads(configuration)

        required_fields = {
            DataSourceType.DATABASE: ['host', 'port', 'database', 'username'],
            DataSourceType.API: ['url', 'method'],
            DataSourceType.S3: ['bucket', 'region'],
            DataSourceType.SFTP: ['host', 'username']
        }

        if self.type in required_fields:
            missing = [field for field in required_fields[self.type]
                      if field not in configuration]
            if missing:
                raise ValueError(f"Missing required fields for {self.type}: {missing}")

        return configuration

    def to_dict(self):
        """Convert data source to dictionary"""
        return {
            'id': self.id,
            'name': self.name,
            'type': self.type.value,
            'description': self.description,
            'enabled': self.enabled,
            'last_tested': self.last_tested.isoformat() if self.last_tested else None,
            'test_status': self.test_status,
            'owner': self.owner
        }

def init_db():
    """Initialize database with enhanced error handling"""
    try:
        db_config.create_tables()
        print("Database initialized successfully")
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        raise

# Additional helper functions for common operations
def get_job_with_dependencies(job_id: int):
    """Get a job with all its dependencies loaded"""
    session = db_config.SessionLocal()
    try:
        return session.query(ETLJob)\
            .options(db_config.joinedload(ETLJob.dependencies))\
            .options(db_config.joinedload(ETLJob.executions))\
            .filter(ETLJob.id == job_id)\
            .first()
    finally:
        session.close()

def get_active_jobs():
    """Get all active jobs"""
    session = db_config.SessionLocal()
    try:
        return session.query(ETLJob)\
            .filter(ETLJob.enabled == True)\
            .all()
    finally:
        session.close()
