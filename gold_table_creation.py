from config.spark_config import get_spark_session
from config.basic_config import HDFS_GOLD_PATH

spark_session = get_spark_session()

spark_session.sql("CREATE DATABASE IF NOT EXISTS spark_sales_gold_db")

# Registered table
spark_session.sql(f"""
CREATE TABLE IF NOT EXISTS spark_sales_gold_db.daily_revenue
USING DELTA
LOCATION '{HDFS_GOLD_PATH}/daily_revenue'
""")

# Registered table
spark_session.sql(f"""
CREATE TABLE IF NOT EXISTS spark_sales_gold_db.category_wise_revenue
USING DELTA
LOCATION '{HDFS_GOLD_PATH}/category_wise_revenue'
""")

# Registered table
spark_session.sql(f"""
CREATE TABLE IF NOT EXISTS spark_sales_gold_db.customer_wise_data
USING DELTA
LOCATION '{HDFS_GOLD_PATH}/customer_wise_data'
""")

