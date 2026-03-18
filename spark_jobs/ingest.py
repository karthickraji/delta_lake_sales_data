from config.basic_config import HDFS_BRONZE_PATH, HDFS_SOURCE_DATA_PATH
from config.spark_config import get_spark_session
from config.logging_config import setup_logging
import logging


def extract_user_data(spark):
    users_df = (
        spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{HDFS_SOURCE_DATA_PATH}/users_data.csv")
    )
    users_row_count = users_df.count()
    logger.info(f"Ingested {users_row_count} rows")
    users_df.write.format("delta").mode("overwrite").save(f"{HDFS_BRONZE_PATH}/users_data")
    logger.info(f"Raw users data has been written into Bronze folder!")

def extract_orders_data(spark):
    orders_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{HDFS_SOURCE_DATA_PATH}/orders_data.csv")
    )
    orders_row_count = orders_df.count()
    logger.info(f"Ingested {orders_row_count} rows")
    orders_df.write.format("delta").mode("overwrite").save(f"{HDFS_BRONZE_PATH}/orders_data")
    logger.info(f"Raw orders data has been written into Bronze folder!")

def main():
    print("Extraction Job Started...")
    spark_session = get_spark_session()
    try:
        extract_user_data(spark_session)
        extract_orders_data(spark_session)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"Extraction Job is getting error : {e}")
    finally:
        spark_session.stop()
        print("Extraction Job Ended")


if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    main()