from abc import ABC, abstractmethod
import logging
import time
from typing import Dict, Any
from datetime import datetime
import pandas as pd

class BaseETL(ABC):
    """Base class for all ETL jobs"""

    def __init__(self, job_name: str, parameters: Dict[str, Any] = None):
        self.job_name = job_name
        self.parameters = parameters or {}
        self.logger = self._setup_logger()
        self.start_time = None
        self.end_time = None

    def _setup_logger(self) -> logging.Logger:
        """Setup job-specific logger"""
        logger = logging.getLogger(f"ETL.{self.job_name}")
        logger.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Create file handler
        fh = logging.FileHandler(f"logs/{self.job_name}.log")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        return logger

    def execute(self):
        """Main execution method"""
        try:
            self.start_time = datetime.now()
            self.logger.info(f"Starting ETL job: {self.job_name}")

            # Pre-execution hooks
            self.pre_execute()

            # Main ETL steps
            data = self.extract()
            transformed_data = self.transform(data)
            self.load(transformed_data)

            # Post-execution hooks
            self.post_execute()

            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            self.logger.info(f"ETL job completed successfully in {duration} seconds")

            return True

        except Exception as e:
            self.logger.error(f"ETL job failed: {str(e)}", exc_info=True)
            raise

    def pre_execute(self):
        """Hook for pre-execution tasks"""
        pass

    def post_execute(self):
        """Hook for post-execution tasks"""
        pass

    @abstractmethod
    def extract(self):
        """Data extraction step"""
        pass

    @abstractmethod
    def transform(self, data):
        """Data transformation step"""
        pass

    @abstractmethod
    def load(self, data):
        """Data loading step"""
        pass

    def get_parameter(self, key: str, default: Any = None) -> Any:
        """Safely get a parameter value"""
        return self.parameters.get(key, default)

    def validate_parameters(self, required_params: list):
        """Validate that all required parameters are present"""
        missing_params = []
        for param in required_params:
            if param not in self.parameters:
                missing_params.append(param)

        if missing_params:
            raise ValueError(f"Missing required parameters: {', '.join(missing_params)}")
