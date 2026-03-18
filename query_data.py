from config.spark_config import get_spark_session

spark_session = get_spark_session()

# spark_session.sql("DESCRIBE FORMATTED spark_sales_db.users_data").show(truncate=False)
print("---"*20)
print("---daily_revenue---")
daily_revenue_df = spark_session.sql("SELECT * FROM spark_sales_gold_db.daily_revenue")
daily_revenue_df.show()
print("---"*20)

print("---"*20)
print("---customer_wise_data---")
result_df = spark_session.sql("SELECT * FROM spark_sales_gold_db.customer_wise_data")
result_df.show()
print("---"*20)

print("---"*20)
print("---category_wise_revenue---")
result_df = spark_session.sql("SELECT * FROM spark_sales_gold_db.category_wise_revenue")
result_df.show()
print("---"*20)


