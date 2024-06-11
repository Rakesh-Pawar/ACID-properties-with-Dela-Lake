# Initialize Spark session
import findspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

findspark.init()
'''
    def configure_spark_with_delta_pip(spark_session_builder: Builder,
                                       extra_packages: list[str] | None = None) -> Builder
        Utility function to configure a SparkSession builder such that the generated SparkSession will automatically download the required Delta Lake JARs from Maven. This function is required when you want to
        Install Delta Lake locally using pip, and
        Execute your Python code using Delta Lake + Pyspark directly, that is, not using spark-submit –packages io. delta:... or pyspark –packages io. delta:....
         
        builder = SparkSession. builder
        .master("local[*]") .appName("test")
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        If you would like to add more packages, use the extra_packages parameter.
         
        builder = SparkSession. builder
        .master("local[*]") .appName("test")
        my_packages = ["org. apache. spark:spark-sql-kafka-0-10_2.12:x. y. z"] spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
        Note
        Evolving
        
    Params:
        spark_session_builder – SparkSession. Builder object being used to configure and create a SparkSession.
        extra_packages – Set other packages to add to Spark session besides Delta Lake.
        
    Returns:
        Updated SparkSession. Builder object
    
'''

builder = SparkSession.builder \
    .appName("DeltaLakeACID") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.repositories", "https://repo1.maven.org/maven2/")

# Enable Delta Lake support
spark: SparkSession = configure_spark_with_delta_pip(builder).getOrCreate()

# Enabling autoMerge in spark configuration
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
