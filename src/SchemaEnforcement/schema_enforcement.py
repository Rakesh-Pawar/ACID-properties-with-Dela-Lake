import os

from delta import configure_spark_with_delta_pip
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from src.utils.define_spark import spark
from src.utils.read_config import read

project_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')).replace('\\', '/')
output_dir_path = project_root_path + read('PATHS', 'output_path')
delta_table_path = output_dir_path + "/delta_table"

# stop existing spark session
spark.stop()

# create new spark session with required configuration.
builder = SparkSession.builder \
    .appName("DeltaLakeACID") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.repositories", "https://repo1.maven.org/maven2/")

# Enable Delta Lake support
spark: SparkSession = configure_spark_with_delta_pip(builder).getOrCreate()


def initial_():
    df = spark.createDataFrame([("bob", 47), ("li", 23), ("leonard", 51)]).toDF(
        "first_name", "age"
    )

    df.write.format("delta").mode('overwrite').save(delta_table_path + "/fun_people")

    funDF = spark.read.format('delta').load(delta_table_path + "/fun_people")
    return funDF


df = initial_()
df.show()


def add_data():
    """
    Now try to append a DataFrame with a different schema to the existing Delta table.
    This DataFrame will contain first_name, age, and country columns.

    :return: This code errors out with an AnalysisException.
    Delta Lake does not allow you to append data with mismatched schema by default
    """
    try:
        df = spark.createDataFrame([("frank", 68, "usa"), ("jordana", 26, "brasil")]).toDF(
            "first_name", "age", "country"
        )

        df.write.format("delta").mode("append").save(delta_table_path + "/fun_people")
    except AnalysisException as e:
        print(f"Schema enforcement error: {e}")

    return df


newDF = add_data()


# Delta lake using schema evaluation with mergeSchema = true
def merge_schema_evol(df):
    """
    Appends a DataFrame to an existing Delta Lake table while merging the schema.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to be appended to the Delta Lake table.

    Returns:
        pyspark.sql.DataFrame: The updated Delta Lake table with the merged schema.

    This function appends the given DataFrame `df` to an existing Delta Lake table
    located at `delta_table_path + "/fun_people"`. It uses the `option("mergeSchema", "true")`
    to merge the schema of the DataFrame with the existing schema of the Delta Lake table.

    If the schemas are compatible, the new columns from the DataFrame will be added to
    the Delta Lake table, and existing columns with compatible data types will be preserved.

    After appending the DataFrame, the function reads the updated Delta Lake table and
    returns it as a new DataFrame.

    Note: If there are any incompatible schema changes, an exception will be raised by
    Delta Lake, and the write operation will be aborted.

    Example:
        # Create a DataFrame
        df = spark.createDataFrame([("frank", 68, "usa"), ("jordana", 26, "brasil")]).toDF(
            "first_name", "age", "country"
        )

        # Append the DataFrame to the Delta Lake table and merge the schema
        updatedDF = merge_schema_(df)

        # Display the updated DataFrame
        updatedDF.show()
    """
    df.write.option("mergeSchema", "true").mode("append").format("delta").save(
        delta_table_path + "/fun_people"
    )
    return spark.read.format('delta').load(delta_table_path + "/fun_people")


mergeSchemaDF = merge_schema_evol(newDF)
print("Delta Lake schema evolution with merge schema: ")
mergeSchemaDF.show()


# Delta Lake schema evolution with autoMerge
def autoMerge_schema_evol(df):
    """
    Appends a DataFrame to an existing Delta Lake table with automatic schema evolution.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to be appended to the Delta Lake table.

    Returns:
        pyspark.sql.DataFrame: The updated Delta Lake table with the evolved schema.

    This function enables automatic schema evolution for Delta Lake by setting the
    `spark.databricks.delta.schema.autoMerge.enabled` configuration to `true`.

    It then creates a new DataFrame `df` with a single column "first_name" containing
    two string values.

    The new DataFrame `df` is appended to an existing Delta Lake table located at
    "tmp/fun_people" using the `write.format("delta").mode("append").save()` method.

    Delta Lake will automatically merge the schema of the new DataFrame with the existing
    schema of the Delta Lake table, adding any new columns or updating the data types of
    existing columns if necessary.

    After appending the DataFrame, the function reads the updated Delta Lake table and
    returns it as a new DataFrame.

    Note: If there are any incompatible schema changes that cannot be automatically resolved,
    an exception will be raised by Delta Lake, and the write operation will be aborted.

    Example:
        # Create a sample DataFrame
        df = spark.createDataFrame([("alice", 30), ("bob", 45)]).toDF("name", "age")

        # Append the DataFrame to the Delta Lake table with automatic schema evolution
        updatedDF = autoMerge_schema_evol(df)

        # Display the updated DataFrame
        updatedDF.show()
    """
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    df = spark.createDataFrame([("dahiana",), ("sabrina",)]).toDF("first_name")

    df.write.format("delta").mode("append").save("tmp/fun_people")
    return spark.read.format("delta").load("tmp/fun_people")


autoMergeSchemaDF = autoMerge_schema_evol(newDF)
print("Delta Lake schema evolution with autoMerge schema: ")

autoMergeSchemaDF.show()
