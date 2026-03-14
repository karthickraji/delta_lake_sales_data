from config.spark_config import get_spark_session

spark_session = get_spark_session()

spark_session.sql("DROP TABLE spark_sales_db.users_data")

spark_session.sql("DROP TABLE spark_sales_db.orders_data")

spark_session.sql("DROP DATABASE spark_sales_db")