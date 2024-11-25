import streamlit as st
import pandas as pd
import yaml
import logging
from pathlib import Path
from datetime import datetime
import time
import plotly.graph_objects as go
from scheduler import ETLScheduler

class ETLManagerApp:
    def __init__(self):
        st.set_page_config(
            page_title="ETL Manager",
            page_icon="🔄",
            layout="wide"
        )

        # Initialize scheduler
        self.scheduler = ETLScheduler()

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/etl_manager.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('ETLManager')

    def run(self):
        st.title("ETL Manager Dashboard 🔄")

        # Sidebar
        self.render_sidebar()

        # Main content tabs
        tabs = st.tabs(["Dashboard", "Job Management", "Logs", "Settings"])

        with tabs[0]:
            self.render_dashboard()

        with tabs[1]:
            self.render_job_management()

        with tabs[2]:
            self.render_logs()

        with tabs[3]:
            self.render_settings()

    def render_sidebar(self):
        st.sidebar.title("ETL Manager")
        st.sidebar.info(
            "Welcome to ETL Manager. Monitor and manage your ETL jobs easily."
        )

        # Quick actions
        st.sidebar.subheader("Quick Actions")
        if st.sidebar.button("▶️ Run Selected Job"):
            selected_job = st.session_state.get('selected_job')
            if selected_job:
                self.run_job_manually(selected_job)

        if st.sidebar.button("🔄 Refresh Dashboard"):
            st.experimental_rerun()

    def render_dashboard(self):
        # Layout with columns
        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("Job Status Overview")
            jobs = self.scheduler.get_all_jobs()

            if jobs:
                # Create status DataFrame
                status_data = []
                for name, config in jobs.items():
                    status_data.append({
                        "Job Name": name,
                        "Status": "Active" if config['enabled'] else "Disabled",
                        "Last Run": config.get('last_run', 'Never'),
                        "Schedule": config['schedule'],
                        "Next Run": self.scheduler.get_next_run(name)
                    })

                df = pd.DataFrame(status_data)
                st.dataframe(df, use_container_width=True)

                # Create status chart
                fig = go.Figure(data=[
                    go.Pie(
                        labels=['Active', 'Disabled', 'Failed'],
                        values=[
                            sum(1 for job in jobs.values() if job['enabled']),
                            sum(1 for job in jobs.values() if not job['enabled']),
                            0  # Add failure tracking in future
                        ],
                        hole=.3
                    )
                ])
                fig.update_layout(title="Job Status Distribution")
                st.plotly_chart(fig, use_container_width=True)

            else:
                st.info("No jobs configured yet. Add some jobs in the Job Management tab.")

        with col2:
            st.subheader("Recent Activity")
            try:
                with open('logs/etl_manager.log', 'r') as log_file:
                    recent_logs = log_file.readlines()[-5:]  # Last 5 logs
                    for log in recent_logs:
                        st.text(log.strip())
            except FileNotFoundError:
                st.info("No recent activity logs found.")

    def render_job_management(self):
        st.subheader("Job Management")

        # Add new job form
        with st.expander("Add New Job", expanded=False):
            with st.form("new_job_form"):
                job_name = st.text_input("Job Name")

                # Script upload
                script_file = st.file_uploader("Upload ETL Script", type=['py'])

                # Schedule options
                schedule_type = st.selectbox(
                    "Schedule Type",
                    ["Daily", "Hourly", "Custom"]
                )

                if schedule_type == "Daily":
                    time_input = st.time_input("Run at")
                    schedule_time = f"daily at {time_input.strftime('%H:%M')}"
                elif schedule_type == "Hourly":
                    hours = st.number_input("Every N hours", min_value=1, value=1)
                    schedule_time = f"every {hours} hours"
                else:
                    schedule_time = st.text_input("Custom Schedule (e.g., 'every 30 minutes')")

                enabled = st.checkbox("Enable Job", value=True)

                if st.form_submit_button("Add Job"):
                    if job_name and script_file and schedule_time:
                        # Save uploaded script
                        script_path = Path("etl_scripts") / f"{job_name}.py"
                        script_path.parent.mkdir(exist_ok=True)
                        script_path.write_bytes(script_file.getvalue())

                        # Add job to scheduler
                        self.scheduler.add_job(
                            name=job_name,
                            schedule_time=schedule_time,
                            script_path=str(script_path),
                            enabled=enabled
                        )
                        st.success(f"Added new job: {job_name}")
                        self.logger.info(f"Added new job: {job_name}")
                    else:
                        st.error("Please fill in all required fields")

        # Existing jobs management
        st.subheader("Existing Jobs")
        jobs = self.scheduler.get_all_jobs()

        if jobs:
            for name, config in jobs.items():
                with st.expander(f"Job: {name}", expanded=False):
                    col1, col2 = st.columns([3, 1])

                    with col1:
                        st.write("Schedule:", config['schedule'])
                        st.write("Status:", "Enabled" if config['enabled'] else "Disabled")
                        st.write("Last Run:", config.get('last_run', 'Never'))

                        if st.button(f"Toggle Status###{name}"):
                            self.scheduler.toggle_job(name)
                            st.experimental_rerun()

                    with col2:
                        if st.button(f"Delete###{name}"):
                            self.scheduler.remove_job(name)
                            st.experimental_rerun()
        else:
            st.info("No jobs configured yet")

    def render_logs(self):
        st.subheader("Log Viewer")

        # Log filter options
        col1, col2 = st.columns([2, 1])
        with col1:
            log_filter = st.text_input("Filter logs", "")
        with col2:
            log_level = st.selectbox("Log Level", ["ALL", "INFO", "ERROR"])

        try:
            with open('logs/etl_manager.log', 'r') as log_file:
                logs = log_file.readlines()

                # Apply filters
                if log_filter:
                    logs = [log for log in logs if log_filter.lower() in log.lower()]
                if log_level != "ALL":
                    logs = [log for log in logs if log_level in log]

                # Display logs in scrollable container
                st.text_area("Logs", value="".join(logs), height=400)
        except FileNotFoundError:
            st.info("No logs found")

    def render_settings(self):
        st.subheader("Settings")

        # General settings
        with st.expander("General Settings", expanded=True):
            log_retention = st.number_input(
                "Log Retention (days)",
                min_value=1,
                value=30
            )

            max_retries = st.number_input(
                "Maximum Job Retries",
                min_value=0,
                value=3
            )

            if st.button("Save Settings"):
                # Save settings logic here
                st.success("Settings saved successfully")

        # Backup & Restore
        with st.expander("Backup & Restore", expanded=False):
            if st.button("Export Configuration"):
                # Add export logic
                st.info("Export functionality coming soon")

            uploaded_file = st.file_uploader("Import Configuration", type=['yaml'])
            if uploaded_file is not None:
                # Add import logic
                st.info("Import functionality coming soon")

    def run_job_manually(self, job_name):
        try:
            self.scheduler._run_job(job_name, self.scheduler.config['jobs'][job_name])
            st.sidebar.success(f"Manually triggered job: {job_name}")
        except Exception as e:
            st.sidebar.error(f"Error running job: {str(e)}")

if __name__ == "__main__":
    app = ETLManagerApp()
    app.run()
