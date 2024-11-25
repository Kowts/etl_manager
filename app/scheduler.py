from datetime import datetime, timedelta
import yaml
import logging
import schedule
import time
import importlib.util
from pathlib import Path
import threading

class ETLScheduler:
    def __init__(self, config_path="config/etl_config.yaml", scripts_dir="etl_scripts"):
        """
        Initialize the ETL Scheduler with enhanced features
        """
        self.config_path = Path(config_path)
        self.scripts_dir = Path(scripts_dir)
        self.logger = self._setup_logging()

        # Create necessary directories
        self.config_path.parent.mkdir(exist_ok=True)
        self.scripts_dir.mkdir(exist_ok=True)

        # Initialize empty config with jobs dictionary
        self.config = {'jobs': {}}

        # Load initial configuration
        self._load_config()

        # Initialize scheduler thread
        self.scheduler_thread = None
        self.is_running = False

    def _setup_logging(self):
        """Set up logging configuration"""
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/etl_scheduler.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger('ETLScheduler')

    def _load_config(self):
        """Load the YAML configuration file"""
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r') as file:
                    loaded_config = yaml.safe_load(file)
                    if loaded_config and isinstance(loaded_config, dict):
                        # Ensure 'jobs' key exists
                        if 'jobs' not in loaded_config:
                            loaded_config['jobs'] = {}
                        self.config = loaded_config
                    else:
                        self.logger.warning("Invalid configuration format. Using default config.")
            else:
                self.logger.info(f"No configuration file found at {self.config_path}. Using default config.")
                self._save_config()
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}. Using default config.")
            self._save_config()

    def _save_config(self):
        """Save the configuration to the YAML file"""
        try:
            with open(self.config_path, 'w') as file:
                yaml.dump(self.config, file, default_flow_style=False)
        except Exception as e:
            self.logger.error(f"Error saving config: {str(e)}")

    def get_all_jobs(self):
        """Return all configured jobs"""
        return self.config.get('jobs', {})

    def get_job(self, name):
        """Get a specific job configuration"""
        return self.config['jobs'].get(name)

    def get_next_run(self, job_name):
        """Calculate the next run time for a job"""
        job = self.get_job(job_name)
        if not job or not job.get('enabled', False):
            return "N/A"

        schedule_time = job.get('schedule')
        if not schedule_time:
            return "N/A"

        now = datetime.now()

        try:
            if schedule_time.startswith('daily at'):
                time_str = schedule_time.replace('daily at ', '')
                next_run = datetime.strptime(f"{now.date()} {time_str}", "%Y-%m-%d %H:%M")
                if next_run < now:
                    next_run += timedelta(days=1)
            elif schedule_time.startswith('every'):
                parts = schedule_time.split()
                if len(parts) >= 3:
                    interval = int(parts[1])
                    unit = parts[2]

                    if unit == 'minutes':
                        delta = timedelta(minutes=interval)
                    elif unit == 'hours':
                        delta = timedelta(hours=interval)
                    else:
                        return "Invalid schedule"

                    next_run = now + delta
                else:
                    return "Invalid schedule format"
            else:
                return "Invalid schedule"

            return next_run.strftime("%Y-%m-%d %H:%M")

        except Exception as e:
            self.logger.error(f"Error calculating next run for {job_name}: {str(e)}")
            return "Error"

    def add_job(self, name, schedule_time, script_path, enabled=True):
        """Add a new ETL job to the scheduler"""
        script_path = Path(script_path)
        if not script_path.exists():
            raise FileNotFoundError(f"Script not found: {script_path}")

        if 'jobs' not in self.config:
            self.config['jobs'] = {}

        self.config['jobs'][name] = {
            'schedule': schedule_time,
            'script_path': str(script_path),
            'enabled': enabled,
            'last_run': None
        }

        self._save_config()
        self.logger.info(f"Added new job: {name}")

    def remove_job(self, name):
        """Remove a job from the scheduler"""
        if name in self.config['jobs']:
            del self.config['jobs'][name]
            self._save_config()
            self.logger.info(f"Removed job: {name}")
            return True
        return False

    def toggle_job(self, name):
        """Toggle job enabled/disabled status"""
        if name in self.config['jobs']:
            self.config['jobs'][name]['enabled'] = not self.config['jobs'][name]['enabled']
            self._save_config()
            status = "enabled" if self.config['jobs'][name]['enabled'] else "disabled"
            self.logger.info(f"Job {name} {status}")
            return True
        return False

    def _run_job(self, name, job_config):
        """Execute an ETL job"""
        try:
            self.logger.info(f"Starting job: {name}")

            script_path = job_config['script_path']

            # Import the script module
            spec = importlib.util.spec_from_file_location(name, script_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Execute the main function
            if hasattr(module, 'main'):
                module.main()
            else:
                self.logger.error(f"No main() function found in {script_path}")
                return

            # Update last run time
            self.config['jobs'][name]['last_run'] = datetime.now().isoformat()
            self._save_config()

            self.logger.info(f"Completed job: {name}")

        except Exception as e:
            self.logger.error(f"Error running job {name}: {str(e)}")

    def start(self):
        """Start the scheduler"""
        if self.is_running:
            return

        self.logger.info("Starting ETL Scheduler")
        self.is_running = True

        # Schedule all enabled jobs
        self._schedule_jobs()

        # Start the scheduler in a separate thread
        self.scheduler_thread = threading.Thread(target=self._run_scheduler)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()

    def stop(self):
        """Stop the scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join()
        self.logger.info("Scheduler stopped")

    def _schedule_jobs(self):
        """Schedule all enabled jobs"""
        schedule.clear()

        for name, job_config in self.config['jobs'].items():
            if job_config.get('enabled', True):
                schedule_time = job_config['schedule']

                if schedule_time.startswith('daily at'):
                    time_str = schedule_time.replace('daily at ', '')
                    schedule.every().day.at(time_str).do(self._run_job, name, job_config)
                elif schedule_time.startswith('every'):
                    parts = schedule_time.split()
                    if len(parts) >= 3:
                        interval = int(parts[1])
                        unit = parts[2]
                        if unit == 'minutes':
                            schedule.every(interval).minutes.do(self._run_job, name, job_config)
                        elif unit == 'hours':
                            schedule.every(interval).hours.do(self._run_job, name, job_config)

                self.logger.info(f"Scheduled job: {name} - {schedule_time}")

    def _run_scheduler(self):
        """Run the scheduler loop"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(1)

# Create global scheduler instance
scheduler = ETLScheduler()
