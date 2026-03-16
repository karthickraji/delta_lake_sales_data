from config.basic_config import HDFS_BRONZE_PATH, HDFS_SOURCE_DATA_PATH
from config.spark_config import get_spark_session
from config.logging_config import setup_logging
import logging

setup_logging()

logger = logging.getLogger(__name__)

def extract_user_data(spark):
    users_df = (
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{HDFS_SOURCE_DATA_PATH}/users_data.csv")
    )
    logging.info(f"Raw users data has ingested {len(users_df)} rows")
    users_df.write.format("delta").mode("overwrite").save(f"{HDFS_BRONZE_PATH}/users_data")
    logging.info(f"Raw users data has been written into Bronze folder!")

def extract_orders_data(spark):
    orders_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{HDFS_SOURCE_DATA_PATH}/orders_data.csv")
    )
    logging.info(f"Raw orders data has ingested {len(orders_df)} rows")
    orders_df.write.format("delta").mode("overwrite").save(f"{HDFS_BRONZE_PATH}/orders_data")
    logging.info(f"Raw orders data has been written into Bronze folder!")

if __name__ == "__main__":
    spark_session = get_spark_session()
    extract_user_data(spark_session)
    extract_orders_data(spark_session)
    spark_session.stop()