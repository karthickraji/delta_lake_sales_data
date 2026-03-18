from pyspark.sql import SparkSession
# from delta import configure_spark_with_delta_pip

def get_spark_session():
    builder = (
        SparkSession.builder
        .appName("DeltaLakeProject")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    # spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate() # It uses in local for development
    spark = builder.enableHiveSupport().getOrCreate()
    return spark