from config.basic_config import HDFS_BRONZE_PATH, HDFS_SILVER_PATH
from config.spark_config import get_spark_session
from pyspark.sql.functions import col
from config.logging_config import setup_logging
import logging

def remove_negative_values(df, field):
    return df.filter(col(field) >= 0)

def remove_null_values(df):
    return df.dropna()

def remove_duplicates(df, data_fields):
    return df.dropDuplicates(data_fields)

def clean_and_transform_users_data(spark):
    users_df = (
        spark.read.format("delta").load(f"{HDFS_BRONZE_PATH}/users_data")
    )
    users_df_clean = remove_null_values(users_df)
    users_df_clean = remove_duplicates(users_df_clean, ["name", "email"])
    users_df_clean.write.format("delta").mode("overwrite").save(f"{HDFS_SILVER_PATH}/users_data_clean")
    logger.info("Cleaned and transformed users data")

def clean_and_transform_orders_data(spark):
    orders_df = (
        spark.read.format("delta").load(f"{HDFS_BRONZE_PATH}/orders_data")
    )
    orders_df_clean = remove_negative_values(orders_df, "quantity")
    orders_df_clean = remove_null_values(orders_df_clean)
    orders_df_clean = remove_duplicates(orders_df_clean, ["quantity"])
    orders_df_clean.write.format("delta").mode("overwrite").save(f"{HDFS_SILVER_PATH}/orders_data_clean")
    logger.info("Cleaned and transformed orders data")

def main():
    print("Clean and Transform Job Started...")
    spark_session = get_spark_session()
    try:
        clean_and_transform_users_data(spark_session)
        clean_and_transform_orders_data(spark_session)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"Clean and Transform Job is getting error : {e}")
    finally:
        spark_session.stop()
        print("Clean and Transform Ended")

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    main()