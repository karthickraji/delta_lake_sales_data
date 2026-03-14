from config.basic_config import HDFS_BRONZE_PATH, HDFS_SILVER_PATH
from config.spark_config import get_spark_session

spark_session = get_spark_session()

users_df = (
    spark_session.read.format("delta").load(f"{HDFS_BRONZE_PATH}/users_data")
)

users_df_clean = users_df.dropna()
users_df_clean = users_df_clean.dropDuplicates(["name", "email"])
users_df_clean.write.format("delta").save(f"{HDFS_SILVER_PATH}/users_data_clean")

orders_df = (
    spark_session.read.format("delta").load(f"{HDFS_BRONZE_PATH}/orders_data")
)

orders_df_clean = orders_df.filter(orders_df.quantity > 0)
orders_df_clean = orders_df_clean.dropna()
orders_df_clean = orders_df_clean.dropDuplicates(["quantity"])
orders_df_clean.write.format("delta").save(f"{HDFS_SILVER_PATH}/orders_data_clean")


spark_session.stop()
