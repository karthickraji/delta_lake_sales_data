from config.spark_config import get_spark_session

spark_session = get_spark_session()

# spark_session.sql("DESCRIBE FORMATTED spark_sales_db.users_data").show(truncate=False)
result_df = spark_session.sql("SELECT * FROM spark_sales_gold_db.customer_wise_data")
result_df.show()


