from config.basic_config import HDFS_BRONZE_PATH, HDFS_SOURCE_DATA_PATH
from config.spark_config import get_spark_session

spark_session = get_spark_session()

users_df = (
    spark_session.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(f"{HDFS_SOURCE_DATA_PATH}/users_data.csv")
)
users_df.write.format("delta").mode("overwrite").save(f"{HDFS_BRONZE_PATH}/users_data")

orders_df = (
    spark_session.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{HDFS_SOURCE_DATA_PATH}/orders_data.csv")
)
orders_df.write.format("delta").mode("overwrite").save(f"{HDFS_BRONZE_PATH}/orders_data")

spark_session.stop()