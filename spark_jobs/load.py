from config.basic_config import HDFS_GOLD_PATH
from config.spark_config import get_spark_session
from config.logging_config import setup_logging
import logging


def load_daily_revenue(spark):
    # daily revenue
    daily_revenue = spark.sql(f"""
    select order_date, sum(price * quantity) as daily_revenue 
    from spark_sales_db.orders_data group by order_date order by daily_revenue desc;
    """)
    daily_revenue.write.format("delta").save(f"{HDFS_GOLD_PATH}/daily_revenue")
    logger.info("Loaded the daily revenue data")

def load_category_wise_revenue(spark):
    # category wise revenue
    category_wise_revenue = spark.sql(f"""
    select category, sum(price * quantity) as total_revenue from spark_sales_db.orders_data group by category order by total_revenue desc;
    """)
    category_wise_revenue.write.format("delta").mode("overwrite").save(f"{HDFS_GOLD_PATH}/category_wise_revenue")
    logger.info("Loaded the category wise revenue data")

def load_customer_wise_revenue(spark):
    # customer wise revenue
    customer_wise_revenue = spark.sql(f"""
    select u.name, u.country, sum(o.price * o.quantity) as total_revenue 
    from spark_sales_db.users_data u join spark_sales_db.orders_data o on u.user_id = o.user_id group by u.name, u.country order by total_revenue desc;
    """)
    customer_wise_revenue.write.format("delta").mode("overwrite").save(f"{HDFS_GOLD_PATH}/customer_wise_data")
    logger.info("Loaded the customer wise revenue data")

def main():
    print("Load Job Started...")
    spark_session = get_spark_session()
    try:
        load_daily_revenue(spark_session)
        load_category_wise_revenue(spark_session)
        load_customer_wise_revenue(spark_session)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"Load Job is getting error : {e}")
    finally:
        spark_session.stop()
        print("Load Job Ended")

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    main()

