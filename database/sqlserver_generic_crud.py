from typing import Any, Dict, List, Optional, Tuple
import logging
from datetime import date, datetime
from helpers.utils import retry

logger = logging.getLogger(__name__)

class SQLServerGenericCRUD:
    """Generic CRUD operations for any table in SQL Server."""

    def __init__(self, db_client):
        """
        Initialize the SQLServerGenericCRUD class.

        Args:
            db_client: An instance of a database client (e.g., SQLServerClient).
        """
        self.db_client = db_client

    def _escape_column_name(self, column: str) -> str:
        """
        Escape column names that contain spaces or special characters.

        Args:
            column (str): The column name to escape.

        Returns:
            str: Properly escaped column name for SQL Server.
        """
        return f"[{column}]" if ' ' in column or any(c in column for c in '.-+/\\') else column

    def _get_table_columns(self, table: str, show_id: bool = False) -> List[str]:
        """
        Get the column names of a table, optionally including the 'id' column.

        Args:
            table (str): The table name.
            show_id (bool): If True, include the 'id' column. Default is False.

        Returns:
            list: List of column names.
        """
        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        """
        if not show_id:
            query += " AND COLUMN_NAME != 'ID'"
        query += " ORDER BY ORDINAL_POSITION"

        try:
            result = self.db_client.execute_query(query, (table,), fetch_as_dict=True)
            return [row['COLUMN_NAME'] for row in result] if result else []
        except Exception as e:
            logger.error(f"Failed to get table columns. Error: {e}")
            return []

    def _format_dates(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format date fields in a record to a readable string format.

        Args:
            record (dict): The record with potential date fields.

        Returns:
            dict: The record with formatted date fields.
        """
        for key, value in record.items():
            if isinstance(value, (date, datetime)):
                record[key] = value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime) else value.strftime('%Y-%m-%d')
        return record

    def _infer_column_types(self, values: List[Tuple[Any]], columns: List[str]) -> Dict[str, str]:
        """
        Infer column data types based on the values provided.
        """
        types = {}
        if not values:
            return {col: 'VARCHAR(MAX)' for col in columns}

        for col_idx, column in enumerate(columns):
            column_type = 'VARCHAR(MAX)'  # default type

            # Check first non-null value for type inference
            for row in values:
                if col_idx >= len(row):
                    continue

                value = row[col_idx]
                if value is not None:
                    if isinstance(value, bool):
                        column_type = 'BIT'
                    elif isinstance(value, int):
                        if abs(value) <= 2147483647:
                            column_type = 'INT'
                        else:
                            column_type = 'BIGINT'
                    elif isinstance(value, float):
                        column_type = 'FLOAT'
                    elif isinstance(value, datetime):
                        column_type = 'DATETIME2'
                    elif isinstance(value, date):
                        column_type = 'DATE'
                    elif isinstance(value, str):
                        max_length = max(len(str(val[col_idx])) for val in values if val[col_idx] is not None)
                        if max_length <= 4000:
                            column_type = f'VARCHAR({max(max_length * 2, 50)})'
                        else:
                            column_type = 'VARCHAR(MAX)'
                    break

            types[column] = column_type

        return types

    def create_table_if_not_exists(self, table: str, columns: List[str], values: List[Tuple[Any]]) -> bool:
        """
        Create a table with the specified columns if it does not already exist.

        Args:
            table (str): The name of the table to create.
            columns (list): List of column names.
            values (list of tuples): List of tuples representing the values to insert.

        Returns:
            bool: True if the table was created, False if it already existed.
        """
        try:
            # Check if table exists
            existing_columns = self._get_table_columns(table)
            if existing_columns:
                logger.info(f"Table '{table}' already exists.")
                return False

            # Infer column types
            column_types = self._infer_column_types(values, columns)

            # Format column definitions with proper escaping
            columns_def = []
            for col in columns:
                escaped_col = self._escape_column_name(col)
                dtype = column_types[col]
                columns_def.append(f"{escaped_col} {dtype}")

            # Create the table
            create_query = f"CREATE TABLE {table} ({', '.join(columns_def)})"
            self.db_client.execute_query(create_query)
            logger.info(f"Table '{table}' created successfully.")
            return True

        except Exception as e:
            logger.error(f"Failed to create table '{table}'. Error: {e}")
            raise

    @retry(max_retries=3, delay=2, backoff=1.5, exceptions=(Exception,), logger=logger)
    def table_exists(self, table: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        query = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = ?
        """

        try:
            result = self.db_client.execute_query(query, (table,), fetch_as_dict=False)
            return result[0][0] > 0
        except Exception as e:
            logger.error(f"Failed to check if table '{table}' exists. Error: {e}")
            raise

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def create(self, table: str, values: List[Tuple[Any]], columns: List[str] = None) -> bool:
        """
        Create new records in the specified table.

        Args:
            table (str): The table name.
            values (list of tuples): List of tuples of values to insert.
            columns (list, optional): List of column names.

        Returns:
            bool: True if records were inserted successfully, False otherwise.
        """
        try:
            if columns is None:
                columns = self._get_table_columns(table)

            if not isinstance(values, list):
                values = [values]
            elif not all(isinstance(v, tuple) for v in values):
                values = [tuple(v) for v in values]

            # Create table if it doesn't exist
            self.create_table_if_not_exists(table, columns, values)

            # Validate value lengths
            for value_tuple in values:
                if len(value_tuple) != len(columns):
                    raise ValueError(f"Number of values {len(value_tuple)} does not match number of columns {len(columns)}")

            # Escape column names and build INSERT query
            escaped_columns = [self._escape_column_name(col) for col in columns]
            columns_str = ", ".join(escaped_columns)
            placeholders = ", ".join(["?" for _ in columns])

            # Build and execute the insert query
            query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

            # Execute batch insert
            self.db_client.execute_batch_query(query, values)
            logger.info(f"Successfully inserted {len(values)} records into {table}")
            return True

        except Exception as e:
            logger.error(f"Failed to insert records. Error: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def read(self, table: str, columns: List[str] = None, where: str = "", params: Tuple[Any] = None, show_id: bool = False, batch_size: int = None) -> List[Dict[str, Any]]:
        """
        Read records from the specified table with optional batch support.

        Args:
            table (str): The table name.
            columns (list, optional): List of column names to retrieve. If None, all columns will be retrieved.
            where (str, optional): WHERE clause for filtering records.
            params (tuple, optional): Tuple of parameters for the WHERE clause.
            show_id (bool, optional): If True, include the 'id' column. Default is False.
            batch_size (int, optional): If provided, limits the number of records returned per batch.

        Returns:
            list: List of records as dictionaries.
        """
        if columns is None:
            columns = self._get_table_columns(table, show_id=show_id)

        columns_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {columns_str} FROM {table}"
        if where:
            query += f" WHERE {where}"
        if batch_size:
            query += f" ORDER BY {columns[0]} OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"

        try:
            result = self.db_client.execute_query(query, params, fetch_as_dict=True)
            records = [self._format_dates(row) for row in result]
            logger.info(f"Records read successfully, {len(records)} rows found.")
            return records
        except Exception as e:
            logger.error(f"Failed to read records. Error: {e}")
            raise

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def update(self, table: str, updates: Dict[str, Any], where: str, params: Tuple[Any]) -> bool:
        """
        Update records in the specified table.

        Args:
            table (str): The table name.
            updates (dict): Dictionary of columns and their new values.
            where (str): WHERE clause for identifying records to update.
            params (tuple): Tuple of parameters for the WHERE clause.

        Returns:
            bool: True if records were updated successfully, False otherwise.
        """
        set_clause = ", ".join([f"{col} = ?" for col in updates.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        values = tuple(updates.values()) + params
        try:
            self.db_client.execute_query(query, values)
            logger.info("Records updated successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to update records. Error: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def delete(self, table: str, where: str = "", params: Tuple[Any] = None, batch_size: int = None) -> bool:
        """
        Delete records from the specified table with optional batch processing.

        Args:
            table (str): The table name.
            where (str, optional): WHERE clause for identifying records to delete. If empty, all records will be deleted.
            params (tuple, optional): Tuple of parameters for the WHERE clause.
            batch_size (int, optional): If provided, deletes records in batches.

        Returns:
            bool: True if records were deleted successfully, False otherwise.
        """

        # Check if the table exists before attempting to delete
        if not self.table_exists(table):
            logger.warning(f"Table '{table}' does not exist. Delete operation aborted.")
            return False

        query = f"DELETE FROM {table}"
        if where:
            query += f" WHERE {where}"
        if batch_size:
            query += f" ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"

        try:
            self.db_client.execute_query(query, params)
            logger.info("Records deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to delete records. Error: {e}")
            return False
