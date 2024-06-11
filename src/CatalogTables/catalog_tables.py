"""
External tables that are defined by the path to the parquet files containing the table data.
"""
import os

from pyspark.sql import SparkSession

from src.utils.define_spark import spark
from src.utils.read_config import read

import os


project_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')).replace('\\', '/')
output_dir_path = project_root_path + read('PATHS', 'output_path')
delta_table_path = output_dir_path + "/delta_table"

def external_table(delta_table_path):
    """
    Creates an external Delta Lake table in the 'bank_tnx' database.

    Args:
        delta_table_path (str): The path where the Delta Lake table files will be stored.

    Returns:
        None

    This function creates a database named 'bank_tnx' if it doesn't already exist, and then
    creates an external Delta Lake table named 'ExternalTnx' within that database. The table
    files are stored at the specified `delta_table_path` location.

    After creating the table, it doesn't return anything, but you can use the `spark.sql()`
    function to execute SQL statements on the created table, such as `DESCRIBE EXTENDED` or
    `SELECT` queries.

    Example:
        delta_table_path = "/path/to/delta/table"
        external_table(delta_table_path)
        spark.sql("DESCRIBE EXTENDED bank_tnx.ExternalTnx").show(truncate=False)
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS bank_tnx")
    spark.sql("CREATE TABLE bank_tnx.ExternalTnx USING DELTA LOCATION '{}'".format(delta_table_path))



def manage_table(delta_table_path, operation, condition=None, update_condition=None, update_expression=None):
    """
    Performs various operations on a Delta Lake table.

    Args:
        delta_table_path (str): The path where the Delta Lake table is stored.
        operation (str): The operation to perform on the table. Accepted values are:
            'describe': Describes the table schema
            'select': Selects data from the table
            'update': Updates data in the table
            'delete': Deletes data from the table
        condition (str, optional): The condition for the 'select' operation.
            It filters the rows to retrieve.
        update_condition (str, optional): The condition for the 'update' operation.
            It specifies the rows to update.
        update_expression (str, optional): The expression for the 'update' operation.
            It specifies how to update the selected rows.

    Returns:
        None

    This function performs the specified operation on the Delta Lake table located at the given
    `delta_table_path`. The operation can be 'describe', 'select', 'update', or 'delete'.

    For the 'describe' operation, it prints the table schema using the 'DESCRIBE EXTENDED' SQL command.

    For the 'select' operation, it retrieves data from the table based on the provided `condition`.
    If no condition is specified, it selects all rows.

    For the 'update' operation, it updates the data in the table based on the provided `update_condition`
    and `update_expression`.

    For the 'delete' operation, it deletes data from the table based on the provided `condition`.

    Examples:
        # Describe the table schema
        manage_table("/path/to/delta/table", "describe")

        # Select rows where amount is greater than 500
        manage_table("/path/to/delta/table", "select", "amount > 500")

        # Update amount to 10% if transaction_id is odd
        manage_table("/path/to/delta/table", "update", update_condition="transaction_id % 2 != 0", update_expression="amount = amount * 1.1")

        # Delete rows where amount is less than 100
        manage_table("/path/to/delta/table", "delete", "amount < 100")
    """
    if operation == "describe":
        spark.sql("DESCRIBE EXTENDED delta.`{}`".format(delta_table_path)).show(truncate=False)
    elif operation == "select":
        condition = "1=1" if condition is None else condition
        spark.sql("SELECT * FROM delta.`{}` WHERE {}".format(delta_table_path, condition)).show(truncate=False)
    elif operation == "update":
        if update_condition and update_expression:
            spark.sql("UPDATE delta.`{}` SET {} WHERE {}".format(delta_table_path, update_expression, update_condition))
        else:
            print("Please provide update_condition and update_expression for the 'update' operation.")
    elif operation == "delete":
        if condition:
            spark.sql("DELETE FROM delta.`{}` WHERE {}".format(delta_table_path, condition))
            print(f"Deleted rows where: {condition}")
        else:
            print("Please provide a condition for the 'delete' operation.")
    else:
        print("Invalid operation specified.")

    # Show the updated/deleted result
    print("{0} table: ".format(operation))
    spark.sql("SELECT * FROM delta.`{}`".format(delta_table_path)).show(truncate=False)


external_table(delta_table_path)
print("Table Description: ")
spark.sql("DESCRIBE EXTENDED bank_tnx.ExternalTnx").show(truncate=False)

print("Filter records using external table: ")
spark.sql("SELECT * FROM bank_tnx.ExternalTnx WHERE amount > 500").show(n=5,truncate=False)

# Example usage
manage_table(delta_table_path, "update", update_condition="transaction_id % 2 != 0", update_expression="amount = amount * 1.1")
manage_table(delta_table_path, "delete", "transaction_id % 2 = 0")


