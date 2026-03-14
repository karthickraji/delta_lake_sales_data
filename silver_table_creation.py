from config.spark_config import get_spark_session
from config.basic_config import HDFS_SILVER_PATH

spark_session = get_spark_session()

spark_session.sql("CREATE DATABASE IF NOT EXISTS spark_sales_db")

# Registered table
spark_session.sql(f"""
CREATE TABLE IF NOT EXISTS spark_sales_db.users_data
USING DELTA
LOCATION '{HDFS_SILVER_PATH}/users_data_clean'
""")

# Registered table
spark_session.sql(f"""
CREATE TABLE IF NOT EXISTS spark_sales_db.orders_data
USING DELTA
LOCATION '{HDFS_SILVER_PATH}/orders_data_clean'
""")


