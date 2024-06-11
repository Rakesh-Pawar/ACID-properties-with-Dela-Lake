import configparser
import logging
import os

from delta import tables

from src.Transactions.simulate_transactions import df_transactions
from src.utils.define_spark import spark
from src.utils.read_config import read

project_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
output_dir_path = project_root_path + read('PATHS', 'output_path')
try:
    delta_table_path = output_dir_path + "/delta_table"
    logging.info("Delta table path:", delta_table_path)
    checkpointPath = delta_table_path + read('PATHS', "checkpointPath")
    logging.info("checkpoint location: ", checkpointPath)
except configparser.NoSectionError as e:
    logging.info(f"Error: {e}")
    raise

# Write data to Delta Lake
df_transactions.write.format("delta") \
    .mode("overwrite") \
    .option('checkpointLocation', checkpointPath) \
    .save(delta_table_path)

# Create DeltaTable object
delta_table = tables.DeltaTable.forPath(spark, delta_table_path)
