import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

from src.Transactions.simulate_transactions import generate_dummy_transactions
from src.utils.define_spark import spark
from src.utils.read_config import read


class ConditionalUpdateWithoutOverwrite:
    """
    Class to perform conditional updates, deletes, merges, and load data from specific versions in a Delta table.
    """

    def __init__(self, delta_table_path, spark):
        """
        Initializes the ConditionalUpdateWithoutOverwrite class.

        Args:
            delta_table_path (str): The path to the Delta table.
            spark (SparkSession): SparkSession object
        """
        self.delta_table_path = delta_table_path
        self.spark = spark
        self.delta_table = DeltaTable.forPath(self.spark, self.delta_table_path)

    def show_affected_data(self, operation_description):
        """
        Shows the affected data after an operation.

        Args:
            operation_description (str): Description of the operation performed.
        """
        print(f"\n{operation_description}:")
        self.delta_table.toDF().show()

    def update_even_values(self):
        """
        Updates the 'amount' column by adding 100 to every even 'transaction_id'.
        This operation commits changes permanently and creates a new version of the Delta table.
        """
        self.delta_table.update(
            condition=expr("transaction_id % 2 == 0"),
            set={"amount": expr("amount + 100")}
        )
        self.show_affected_data("Updated even transaction_ids")

    def delete_even_values(self):
        """
        Deletes rows with even 'transaction_id'.
        This operation commits changes permanently and creates a new version of the Delta table.
        """
        self.delta_table.delete(condition=expr("transaction_id % 2 == 0"))
        self.show_affected_data("Deleted even transaction_ids")

    def merge_new_data(self):
        """
        Merges new data into the Delta table, updating existing records and inserting new ones.
        This operation commits changes permanently and creates a new version of the Delta table.
        """
        new_data = generate_dummy_transactions(5)
        newDf = spark.createDataFrame(new_data)
        self.delta_table.alias("oldData") \
            .merge(
            newDf.alias("newData"),
            "oldData.transaction_id = newData.transaction_id"
        ) \
            .whenMatchedUpdate(set={"transaction_id": col("newData.transaction_id")}) \
            .whenNotMatchedInsert(values={"transaction_id": col("newData.transaction_id")}) \
            .execute()
        self.show_affected_data("Merged new data")

    def load_data_from_version(self, version):
        """
        Loads data from a specific version of the Delta table.

        Args:
            version (int): The version of the Delta table to load data from.
        """
        df = self.spark.read.format("delta").option("versionAsOf", version).load(self.delta_table_path)
        df.show()


project_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
output_dir_path = project_root_path + read('PATHS', 'output_path')
delta_table_path = output_dir_path + "/delta_table"

# Example usage:
conditional_update = ConditionalUpdateWithoutOverwrite(delta_table_path, spark)
conditional_update.update_even_values()
conditional_update.delete_even_values()
conditional_update.merge_new_data()
conditional_update.load_data_from_version(1)
