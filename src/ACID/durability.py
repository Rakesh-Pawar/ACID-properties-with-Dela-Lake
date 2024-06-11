import logging
import os

from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession

from src.Transactions.simulate_transactions import generate_dummy_transactions
from src.utils.define_spark import spark
from src.utils.implement_delta_lake import delta_table_path, delta_table
from src.utils.read_config import read


def simulate_system_crash(delta_table, spark: SparkSession):
    """
    Simulate a system crash and verify durability of the Delta table by performing a group analysis.

        :param delta_table: The DeltaTable to check.
        :param spark: SparkSession object
    """

    try:
        # Perform some operations
        new_transactions = generate_dummy_transactions(5)
        df_new_transactions = spark.createDataFrame(new_transactions)
        df_new_transactions.write.format("delta").mode("append").save(delta_table_path)

        # Simulate system crash by stopping and restarting Spark session
        spark.stop()

        # Restart Spark session
        builder = SparkSession.builder \
            .appName("DeltaLakeACID") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        delta_table = DeltaTable.forPath(spark, delta_table_path)

        # Perform a group analysis to verify durability
        grouped_df = (delta_table.toDF()
                      .withColumnRenamed("account from", "account_from")
                      .withColumnRenamed("account to", "account_to")
                      .groupBy("account_from").sum("amount")
                      .withColumnRenamed("sum(amount)", "total_amount_from")
                      )
        grouped_df.show()

        grouped_df.printSchema()

        logging.info("Group analysis completed successfully after simulated crash, ensuring durability.")

        project_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        output_dir_path = project_root_path+read('PATHS', 'output_path')
        grouped_count_path = output_dir_path + read('PATHS', 'groupedCountPath')
        grouped_checkpoint_path = read('PATHS', "groupedCountCheckpoint")

        (grouped_df.write.format('delta')
         .option('checkpointLocation', grouped_checkpoint_path)
         .mode("overwrite")
         .save(grouped_count_path))

        logging.info("Analysis result write successfully !")
    except Exception as e:
        print(f"Durability check failed: {e}")


# Simulate system crash and check durability
simulate_system_crash(delta_table, spark)

"""
Durability ensures that once a transaction is committed, it will remain so, even in the event of a system crash.
Delta Lake ensures durability through its log-based storage.
"""
