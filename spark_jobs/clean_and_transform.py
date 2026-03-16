from config.basic_config import HDFS_BRONZE_PATH, HDFS_SILVER_PATH
from config.spark_config import get_spark_session

def remove_negative_values(df, field):
    return df[df[f'{field}'] > 0]

def remove_null_values(df):
    return df.dropna()

def remove_duplicates(df, data_fields):
    return df.dropDuplicates(data_fields)

def clean_and_transform_users_data():
    users_df = (
        spark_session.read.format("delta").load(f"{HDFS_BRONZE_PATH}/users_data")
    )
    users_df_clean = remove_null_values(users_df)
    users_df_clean = remove_duplicates(users_df_clean, ["name", "email"])
    users_df_clean.write.format("delta").save(f"{HDFS_SILVER_PATH}/users_data_clean")

def clean_and_transform_orders_data():
    orders_df = (
        spark_session.read.format("delta").load(f"{HDFS_BRONZE_PATH}/orders_data")
    )

    orders_df_clean = remove_negative_values(orders_df, "quantity")
    orders_df_clean = remove_null_values(orders_df_clean)
    orders_df_clean = remove_duplicates(orders_df_clean, ["quantity"])
    orders_df_clean.write.format("delta").save(f"{HDFS_SILVER_PATH}/orders_data_clean")

if __name__ == "__main__":
    spark_session = get_spark_session()
    clean_and_transform_users_data()
    clean_and_transform_orders_data()
    spark_session.stop()