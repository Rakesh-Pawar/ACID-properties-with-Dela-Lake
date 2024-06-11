import configparser
import logging
import os

from delta import tables, DeltaTable
from pyspark.sql.types import *

from src.Transactions.simulate_transactions import generate_dummy_transactions
from src.utils.read_config import read

project_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
output_dir_path = project_root_path + read('PATHS', 'output_path')
try:
    delta_table_path = output_dir_path + "/delta_table"
    logging.info("Delta table path:", delta_table_path)
except configparser.NoSectionError as e:
    logging.info(f"Error: {e}")
    raise

schema=StructType([
    StructField("transaction_id", IntegerType()),
    StructField("account_from", IntegerType()),
    StructField("account_to", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType())
])

def delta_table(spark) -> DeltaTable:
    """

    :param spark: Spark session object
    :return: delta table
    """
    try:
        delta_table_path = output_dir_path + "/delta_table"
        logging.info("Delta table path:", delta_table_path)
    except configparser.NoSectionError as e:
        logging.info(f"Error: {e}")
        raise

    # Generate dummy transactions
    num_transactions = 100
    transactions = generate_dummy_transactions(num_transactions)

    # Convert transactions to Spark DataFrame
    df_transactions = spark.createDataFrame(transactions, schema)
    # Write data to Delta Lake
    df_transactions.write.format("delta") \
        .mode("overwrite") \
        .save(delta_table_path)

    # Create DeltaTable object
    delta_table = tables.DeltaTable.forPath(spark, delta_table_path)
    return delta_table