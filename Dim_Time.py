# Databricks notebook source
# MAGIC %md
# MAGIC ## DIM_TIME TRANSFORMATIONS AND CLEANING (Rows 88-99)

# COMMAND ----------

# DBTITLE 1,IMPORT
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,READ
time_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/bronze/dim_time_10k.csv")
time_df.display()

# COMMAND ----------

# DBTITLE 1,ARCHIVE CODE
#select column with atleat one null value and form new df 
null_columns = [c for c in time_df.columns if time_df.filter(col(c).isNull()).count() > 0]
archive_time_df = time_df.select(*null_columns)
archive_time_df.display()

#write the df
archive_time_df.coalesce(1).write.mode("append").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/archive/time")

# COMMAND ----------

# DBTITLE 1,AUDIT BRONZE
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [time_df.filter(col(c).isNull()).count() for c in time_df.columns]
row_count = time_df.count()
col_count = len(time_df.columns)

# single audit record

adudit_data = [{
    "audit_id": 1,
    "table_name": "time",
    "row_count": row_count,
    "col_count": col_count,
    "null_count": str(null_count)
}]
#create dataframe
audit_time_df = spark.createDataFrame(adudit_data)
audit_time_df.display()

#write the df 
audit_time_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/time/bronze")

# COMMAND ----------

# DBTITLE 1,TRANSFORMATIONS
# Row 88: time_id - Sequential ID generation using ROW_NUMBER
window_spec = Window.orderBy("full_date") 

# COMMAND ----------

# Row 90: day_of_week - Calculate day of week
result_df1 = time_df.withColumn("day_of_week", dayofweek(col("full_date")))

# COMMAND ----------

 # Row 91: day_name - Calculate day name
result_df2 = result_df1.withColumn("day_name", date_format(col("full_date"), "EEEE"))

# COMMAND ----------

# Row 92: month_number - Calculate month
result_df3 = result_df2.withColumn("month_number", month(col("full_date")))

# COMMAND ----------

 #Row 93: month_name - Calculate month name
result_df4 = result_df3.withColumn("month_name", date_format(col("full_date"), "MMMM"))

# COMMAND ----------

 # Row 94: quarter - Calculate quarter
result_df5 = result_df4.withColumn("quarter", quarter(col("full_date")))
    

# COMMAND ----------

# Row 95: year - Calculate year
result_df6 = result_df5.withColumn("year", year(col("full_date")))

# COMMAND ----------

 # Row 96: fiscal_year - Calculate fiscal year (Indian: April-March)
result_df7 = result_df6.withColumn("fiscal_year", 
                                     when(col("month_number") >= 4, col("year"))
                                     .otherwise(col("year") - 1))

# COMMAND ----------

# Row 97: is_weekend - Calculate weekend flag (Saturday=7, Sunday=1)
result_df8 = result_df7.withColumn("is_weekend", 
                                     when(col("day_of_week").isin([1, 7]), lit("Y"))
                                     .otherwise(lit("N")))

# COMMAND ----------

# Row 98: is_holiday - JOIN with holiday calendar
result_df9 = result_df8.withColumn("month_day", date_format(col("full_date"), "MM-dd"))
result_df9 = result_df9.withColumn("is_holiday",
        when(col("month_day").isin(["01-26", "08-15", "10-02", "12-25", "03-08", "10-24", "11-12"]), lit("Y"))
        .otherwise(lit("N"))
    ).drop("month_day")
    

# COMMAND ----------

 # Row 99: business_day - Calculate business day flag
result_df10 = result_df9.withColumn("business_day", 
                                     when((col("is_weekend") == "N") & (col("is_holiday") == "N"), lit("Y"))
                                     .otherwise(lit("N")))

# COMMAND ----------

dim_time_df= result_df10
dim_time_df = dim_time_df.drop("relative_day")


# COMMAND ----------

dim_time_df = dim_time_df.withColumn("holiday_name",when(dim_time_df.holiday_name.isNull(),lit("No Holiday")).otherwise(dim_time_df.holiday_name))
dim_time_df.display()

# COMMAND ----------

# Add process_date and audit columns consistently

process_date = "2025-10-30"  # Define process_date
table_name = "time_table"  # Define table_name

dim_time_df = dim_time_df \
        .withColumn("process_date", lit(process_date).cast(DateType())) \
        .withColumn("process_timestamp", current_timestamp()) \
        .withColumn("process_year", year(lit(process_date))) \
        .withColumn("process_month", month(lit(process_date))) \
        .withColumn("process_day", dayofmonth(lit(process_date))) \
        .withColumn("inserted_by", lit("ETL_SYSTEM")) \
        .withColumn("source_system", lit(table_name))

dim_time_df.display()

# COMMAND ----------

# DBTITLE 1,WRITE
dim_time_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_year", "process_month", "process_day").csv("abfss://finalproject@shrek369.dfs.core.windows.net/silver/time")