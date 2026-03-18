# delta_lake_sales_data

spark-submit \
  --master yarn \
  --deploy-mode client \
  --py-files /home/karthick/PycharmProjects/sales_data_pipeline_using_spark/necessary_files.zip \
  /home/karthick/PycharmProjects/sales_data_pipeline_using_spark/mysql_scripts/load_to_mysql.py
