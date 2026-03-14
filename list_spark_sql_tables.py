from config.spark_config import get_spark_session

spark_session = get_spark_session()
spark_session.sql("USE spark_sales_db")
spark_session.sql("SHOW TABLES").show()

