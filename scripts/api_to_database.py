from templates.base_etl import BaseETL
import pandas as pd
import requests
import sqlalchemy
from datetime import datetime, timedelta
import time

class APIToDatabaseETL(BaseETL):
    """ETL job to load data from an API into a database"""

    def __init__(self, job_name: str, parameters: dict):
        super().__init__(job_name, parameters)
        self.required_params = [
            'api_url',
            'target_table',
            'database_url'
        ]
        self.validate_parameters(self.required_params)

    def extract(self):
        """Extract data from API"""
        self.logger.info("Starting data extraction from API")

        headers = self.parameters.get('headers', {})
        params = self.parameters.get('params', {})
        pagination = self.parameters.get('pagination', {})

        all_data = []

        try:
            if pagination:
                page = pagination.get('start_page', 1)
                page_size = pagination.get('page_size', 100)
                max_pages = pagination.get('max_pages', float('inf'))

                while page <= max_pages:
                    params.update({
                        pagination['page_param']: page,
                        pagination['size_param']: page_size
                    })

                    response = requests.get(
                        self.parameters['api_url'],
                        headers=headers,
                        params=params
                    )
                    response.raise_for_status()

                    data = response.json()
                    results = self._extract_data_from_response(data)

                    if not results:
                        break

                    all_data.extend(results)
                    self.logger.info(f"Fetched page {page}, got {len(results)} records")

                    # Check if we've reached the last page
                    if len(results) < page_size:
                        break

                    page += 1

                    # Respect rate limits
                    if 'rate_limit' in self.parameters:
                        time.sleep(1 / self.parameters['rate_limit'])

            else:
                response = requests.get(
                    self.parameters['api_url'],
                    headers=headers,
                    params=params
                )
                response.raise_for_status()

                data = response.json()
                all_data = self._extract_data_from_response(data)

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise

        df = pd.DataFrame(all_data)
        self.logger.info(f"Extracted {len(df)} records from API")

        return df

    def _extract_data_from_response(self, response_data):
        """Extract relevant data from API response"""
        data_path = self.parameters.get('data_path', '')

        if data_path:
            for key in data_path.split('.'):
                response_data = response_data[key]

        return response_data

    def transform(self, data):
        """Transform the API data"""
        self.logger.info("Starting data transformation")

        # Apply column mappings
        column_mappings = self.parameters.get('column_mappings', {})
        if column_mappings:
            data = data.rename(columns=column_mappings)

        # Select specific columns if specified
        columns = self.parameters.get('columns', [])
        if columns:
            data = data[columns]

        # Apply filters if specified
        filters = self.parameters.get('filters', [])
        for filter_condition in filters:
            column = filter_condition['column']
            operator = filter_condition['operator']
            value = filter_condition['value']

            if operator == 'equals':
                data = data[data[column] == value]
            elif operator == 'greater_than':
                data = data[data[column] > value]
            elif operator == 'less_than':
                data = data[data[column] < value]
            elif operator == 'in':
                data = data[data[column].isin(value)]

        # Apply transformations
        transformations = self.parameters.get('transformations', [])
        for transformation in transformations:
            if transformation['type'] == 'datetime':
                data[transformation['column']] = pd.to_datetime(
                    data[transformation['column']],
                    format=transformation.get('format')
                )
            elif transformation['type'] == 'numeric':
                data[transformation['column']] = pd.to_numeric(
                    data[transformation['column']],
                    errors=transformation.get('errors', 'raise')
                )

        self.logger.info(f"Transformation complete. Final record count: {len(data)}")
        return data

    def load(self, data):
        """Load data into the database"""
        self.logger.info("Starting data load")

        engine = sqlalchemy.create_engine(self.parameters['database_url'])
        table_name = self.parameters['target_table']

        try:
            # Load data in chunks
            chunk_size = self.parameters.get('chunk_size', 1000)
            data.to_sql(
                name=table_name,
                con=engine,
                if_exists=self.parameters.get('if_exists', 'append'),
                index=False,
                chunksize=chunk_size
            )

            self.logger.info(f"Successfully loaded {len(data)} records to {table_name}")

        except Exception as e:
            self.logger.error(f"Error loading data to database: {str(e)}")
            raise

        finally:
            engine.dispose()

