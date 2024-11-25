from templates.base_etl import BaseETL
import pandas as pd
import sqlalchemy
from pathlib import Path
import glob

class CSVToDatabaseETL(BaseETL):
    """ETL job to load CSV files into a database"""

    def __init__(self, job_name: str, parameters: dict):
        super().__init__(job_name, parameters)
        self.required_params = [
            'source_path',
            'file_pattern',
            'target_table',
            'database_url'
        ]
        self.validate_parameters(self.required_params)

    def extract(self):
        """Extract data from CSV files"""
        self.logger.info("Starting data extraction from CSV")

        source_path = Path(self.parameters['source_path'])
        file_pattern = self.parameters['file_pattern']

        # Get list of files matching pattern
        files = glob.glob(str(source_path / file_pattern))

        if not files:
            raise FileNotFoundError(f"No files found matching pattern: {file_pattern}")

        # Read and combine all matching CSV files
        dfs = []
        for file in files:
            self.logger.info(f"Reading file: {file}")
            df = pd.read_csv(
                file,
                encoding=self.parameters.get('encoding', 'utf-8'),
                delimiter=self.parameters.get('delimiter', ',')
            )
            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Extracted {len(combined_df)} records from {len(files)} files")

        return combined_df

    def transform(self, data):
        """Transform the data"""
        self.logger.info("Starting data transformation")

        # Apply any column mappings
        column_mappings = self.parameters.get('column_mappings', {})
        if column_mappings:
            data = data.rename(columns=column_mappings)

        # Apply data type conversions
        dtype_mappings = self.parameters.get('dtype_mappings', {})
        if dtype_mappings:
            data = data.astype(dtype_mappings)

        # Handle missing values
        null_replacements = self.parameters.get('null_replacements', {})
        if null_replacements:
            data = data.fillna(null_replacements)

        # Apply custom transformations
        custom_transformations = self.parameters.get('custom_transformations', [])
        for transformation in custom_transformations:
            if transformation == 'drop_duplicates':
                subset = self.parameters.get('duplicate_subset')
                data = data.drop_duplicates(subset=subset)
            elif transformation == 'drop_na':
                subset = self.parameters.get('na_subset')
                data = data.dropna(subset=subset)

        self.logger.info(f"Transformation complete. Final record count: {len(data)}")
        return data

    def load(self, data):
        """Load data into the database"""
        self.logger.info("Starting data load")

        engine = sqlalchemy.create_engine(self.parameters['database_url'])
        table_name = self.parameters['target_table']

        # Determine if table should be replaced or appended
        if_exists = self.parameters.get('if_exists', 'append')

        try:
            # Load data in chunks
            chunk_size = self.parameters.get('chunk_size', 1000)
            data.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunk_size
            )

            self.logger.info(f"Successfully loaded {len(data)} records to {table_name}")

        except Exception as e:
            self.logger.error(f"Error loading data to database: {str(e)}")
            raise

        finally:
            engine.dispose()

def main():
    """Main entry point for the ETL job"""
    # Example parameters
    parameters = {
        'source_path': 'data/raw',
        'file_pattern': '*.csv',
        'target_table': 'sales_data',
        'database_url': 'postgresql://user:password@localhost:5432/mydatabase',
        'encoding': 'utf-8',
        'delimiter': ',',
        'column_mappings': {
            'Customer ID': 'customer_id',
            'Product ID': 'product_id',
            'Sale Amount': 'sale_amount'
        },
        'dtype_mappings': {
            'customer_id': 'int64',
            'sale_amount': 'float64'
        },
        'null_replacements': {
            'sale_amount': 0.0
        },
        'custom_transformations': ['drop_duplicates'],
        'duplicate_subset': ['customer_id', 'product_id'],
        'if_exists': 'append',
        'chunk_size': 1000
    }

    job = CSVToDatabaseETL('csv_to_database', parameters)
    job.execute()

if __name__ == "__main__":
    main()
