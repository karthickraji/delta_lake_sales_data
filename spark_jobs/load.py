from config.basic_config import HDFS_GOLD_PATH
from config.spark_config import get_spark_session

spark_session = get_spark_session()



# customer wise revenue
customer_wise_revenue = spark_session.sql(f"""
select u.name, u.country, sum(o.price * o.quantity) as total_revenue 
from spark_sales_db.users_data u join spark_sales_db.orders_data o on u.user_id = o.user_id group by u.name, u.country order by total_revenue desc;
""")

customer_wise_revenue.write.format("delta").save(f"{HDFS_GOLD_PATH}/customer_wise_data")

spark_session.stop()



