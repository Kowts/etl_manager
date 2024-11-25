from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Text, ForeignKey, JSON, and_, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, joinedload
from sqlalchemy.sql import func
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime, timedelta
from config import config
import logging

Base = declarative_base()

class ETLJob(Base):
    """ETL Job definition"""
    __tablename__ = 'etl_jobs'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(Text)
    script_path = Column(String(255), nullable=False)
    schedule = Column(String(100), nullable=False)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    parameters = Column(JSON)
    timeout = Column(Integer)  # in seconds
    max_retries = Column(Integer, default=3)

    # Relationships
    executions = relationship("JobExecution", back_populates="job")
    dependencies = relationship("JobDependency", back_populates="job")

class JobExecution(Base):
    """Record of job executions"""
    __tablename__ = 'job_executions'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('etl_jobs.id'))
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True))
    status = Column(String(20))  # 'running', 'completed', 'failed', 'timeout'
    error_message = Column(Text)
    retries = Column(Integer, default=0)
    metrics = Column(JSON)  # Store execution metrics (memory usage, duration, etc.)

    # Relationships
    job = relationship("ETLJob", back_populates="executions")

class JobDependency(Base):
    """Job dependencies configuration"""
    __tablename__ = 'job_dependencies'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('etl_jobs.id'))
    depends_on_job_id = Column(Integer, ForeignKey('etl_jobs.id'))
    condition = Column(String(50))  # 'success', 'completed', 'failed'

    # Relationships
    job = relationship("ETLJob", back_populates="dependencies")

class DataSource(Base):
    """Data source configuration"""
    __tablename__ = 'data_sources'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    type = Column(String(50))  # 'database', 'api', 'file', etc.
    configuration = Column(JSON)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class DatabaseManager:
    """Enhanced Database management class"""

    def __init__(self):
        self.engine = create_engine(
            config.get_database_url(),
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.logger = logging.getLogger(__name__)

        # Create all tables
        try:
            Base.metadata.create_all(self.engine)
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to create database tables: {str(e)}")
            raise

    @contextmanager
    def get_session(self):
        """Enhanced transactional scope with better error handling"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error: {str(e)}")
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Unexpected error: {str(e)}")
            raise
        finally:
            session.close()

    # Enhanced job management methods
    def create_job(self, name: str, script_path: str, schedule: str, **kwargs) -> ETLJob:
        """Create a new ETL job with validation"""
        try:
            # Validate schedule format
            self._validate_schedule(schedule)

            with self.get_session() as session:
                # Check for existing job with same name
                existing_job = session.query(ETLJob).filter(ETLJob.name == name).first()
                if existing_job:
                    raise ValueError(f"Job with name '{name}' already exists")

                job = ETLJob(
                    name=name,
                    script_path=script_path,
                    schedule=schedule,
                    description=kwargs.get('description'),
                    parameters=kwargs.get('parameters', {}),
                    timeout=kwargs.get('timeout'),
                    max_retries=kwargs.get('max_retries', 3),
                    enabled=kwargs.get('enabled', True)
                )
                session.add(job)
                session.commit()
                self.logger.info(f"Created new job: {name}")
                return job
        except Exception as e:
            self.logger.error(f"Failed to create job {name}: {str(e)}")
            raise

    def bulk_create_jobs(self, jobs_data: List[Dict[str, Any]]) -> List[ETLJob]:
        """Bulk create multiple ETL jobs"""
        created_jobs = []
        with self.get_session() as session:
            for job_data in jobs_data:
                try:
                    job = ETLJob(**job_data)
                    session.add(job)
                    created_jobs.append(job)
                except Exception as e:
                    self.logger.error(f"Failed to create job {job_data.get('name')}: {str(e)}")
            session.commit()
        return created_jobs

    def search_jobs(self,
                   search_term: str = None,
                   status: str = None,
                   date_range: Tuple[datetime, datetime] = None) -> List[ETLJob]:
        """Search jobs with multiple criteria"""
        with self.get_session() as session:
            query = session.query(ETLJob)

            if search_term:
                query = query.filter(
                    or_(
                        ETLJob.name.ilike(f"%{search_term}%"),
                        ETLJob.description.ilike(f"%{search_term}%")
                    )
                )

            if status:
                query = query.filter(ETLJob.enabled == (status == 'enabled'))

            if date_range:
                start_date, end_date = date_range
                query = query.filter(
                    and_(
                        ETLJob.created_at >= start_date,
                        ETLJob.created_at <= end_date
                    )
                )

            return query.all()

    def get_job_status_summary(self) -> Dict[str, int]:
        """Get summary of job statuses"""
        with self.get_session() as session:
            total_jobs = session.query(ETLJob).count()
            active_jobs = session.query(ETLJob).filter(ETLJob.enabled == True).count()
            failed_jobs = session.query(JobExecution)\
                .filter(JobExecution.status == 'failed')\
                .distinct(JobExecution.job_id).count()

            return {
                'total': total_jobs,
                'active': active_jobs,
                'disabled': total_jobs - active_jobs,
                'failed': failed_jobs
            }

    def get_job_metrics(self, job_id: int, days: int = 30) -> Dict[str, Any]:
        """Get job performance metrics"""
        with self.get_session() as session:
            start_date = datetime.now() - timedelta(days=days)

            executions = session.query(JobExecution)\
                .filter(
                    JobExecution.job_id == job_id,
                    JobExecution.start_time >= start_date
                ).all()

            total_runs = len(executions)
            successful_runs = sum(1 for e in executions if e.status == 'completed')
            failed_runs = sum(1 for e in executions if e.status == 'failed')

            # Calculate average duration
            durations = []
            for e in executions:
                if e.end_time and e.start_time:
                    duration = (e.end_time - e.start_time).total_seconds()
                    durations.append(duration)

            avg_duration = sum(durations) / len(durations) if durations else 0

            return {
                'total_runs': total_runs,
                'successful_runs': successful_runs,
                'failed_runs': failed_runs,
                'success_rate': (successful_runs / total_runs * 100) if total_runs > 0 else 0,
                'average_duration': avg_duration,
                'last_run_status': executions[0].status if executions else None
            }

    def cleanup_old_executions(self, days: int = 30) -> int:
        """Clean up old job execution records"""
        with self.get_session() as session:
            cutoff_date = datetime.now() - timedelta(days=days)
            deleted_count = session.query(JobExecution)\
                .filter(JobExecution.start_time < cutoff_date)\
                .delete()
            session.commit()
            return deleted_count

    def _validate_schedule(self, schedule: str) -> bool:
        """Validate schedule format"""
        valid_formats = [
            'daily at', 'every', 'cron'
        ]

        if not any(schedule.startswith(fmt) for fmt in valid_formats):
            raise ValueError(
                f"Invalid schedule format. Must start with one of: {', '.join(valid_formats)}"
            )
        return True

    # Data Source Management
    def update_data_source(self, name: str, updates: Dict[str, Any]) -> Optional[DataSource]:
        """Update data source configuration"""
        with self.get_session() as session:
            data_source = session.query(DataSource).filter(DataSource.name == name).first()
            if data_source:
                for key, value in updates.items():
                    if hasattr(data_source, key):
                        setattr(data_source, key, value)
                session.commit()
            return data_source

    def test_data_source(self, name: str) -> bool:
        """Test data source connection"""
        data_source = self.get_data_source(name)
        if not data_source:
            return False

        try:
            # Implementation depends on data source type
            if data_source.type == 'database':
                test_engine = create_engine(data_source.configuration['connection_string'])
                with test_engine.connect() as conn:
                    conn.execute("SELECT 1")
                return True
            # Add other data source type tests as needed
            return False
        except Exception as e:
            self.logger.error(f"Data source test failed: {str(e)}")
            return False

# Create global database instance
db = DatabaseManager()
