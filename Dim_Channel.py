# Databricks notebook source
# MAGIC %md
# MAGIC ## DIM_CHANNEL TRANSFORMATIONS AND CLEANING (Rows 78-87)

# COMMAND ----------

# DBTITLE 1,IMPORT
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,READ
channel_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/bronze/dim_channel_95k.csv")
channel_df.display()

# COMMAND ----------

# DBTITLE 1,ARCHIVE CODE
#select column with atleat one null value and form new df 
null_columns = [c for c in channel_df.columns if channel_df.filter(col(c).isNull()).count() > 0]
archive_channel_df = channel_df.select(*null_columns)
archive_channel_df.display()



# COMMAND ----------

# DBTITLE 1,AUDIT BRONZE
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [channel_df.filter(col(c).isNull()).count() for c in channel_df.columns]
row_count = channel_df.count()
col_count = len(channel_df.columns)

# single audit record

adudit_data = [{
    "audit_id": 1,
    "table_name": "channel",
    "row_count": row_count,
    "col_count": col_count,
    "null_count": str(null_count)
}]
#create dataframe
audit_channel_df = spark.createDataFrame(adudit_data)
audit_channel_df.display()

#write the df 
audit_channel_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/channel/bronze")

# COMMAND ----------

# DBTITLE 1,TRANSFORMATIONS
# Row 78: channel_id - Primary key
result_df1 = channel_df.withColumn("channel_id", col("channel_id").cast(IntegerType()))

# COMMAND ----------

 # Row 79: channel_code - Generate format CH{ID}
result_df2 = result_df1.withColumn("channel_code", 
                                     concat(lit("CH"), lpad(col("channel_id"), 6, "0")))

# COMMAND ----------

# Row 80: channel_name - Normalize channel name
result_df3 = result_df2.withColumn("channel_name", upper(trim(col("channel_name"))))

# COMMAND ----------

# Row 81: channel_type - Validate type values
result_df4 = result_df3.withColumn("channel_type", 
                                     when(col("channel_type").isin(["Physical", "Digital", "Assisted"]), col("channel_type"))
                                     .otherwise(lit("Digital")))

# COMMAND ----------

 # Row 82: channel_category - Validate category list
result_df5 = result_df4.withColumn("channel_category", 
                                     when(col("channel_category").isin(["ATM", "Branch", "Mobile Banking", "Internet Banking"]), 
                                          col("channel_category"))
                                     .otherwise(lit("Other")))

# COMMAND ----------

# Row 83: availability_hours - Standardize format
result_df6 = result_df5.withColumn("availability_hours", trim(col("availability_hours")))

# COMMAND ----------

# Row 84: daily_limit - Default 0 if null
result_df7 = result_df6.withColumn("daily_limit", 
                                     when(col("daily_limit").isNotNull(), 
                                          round(col("daily_limit").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

# Row 85: security_level - Validate security levels
result_df8 = result_df7.withColumn("security_level", 
                                     when(col("security_level").isin(["High", "Medium", "Low"]), col("security_level"))
                                     .otherwise(lit("Medium")))

# COMMAND ----------

    # Row 86: channel_status - Validate status values
result_df9 = result_df8.withColumn("channel_status", 
                                     when(col("channel_status").isin(["Active", "Inactive", "Maintenance"]), 
                                          col("channel_status"))
                                     .otherwise(lit("Active")))

# COMMAND ----------

# Row 87: success_rate - Validate percentage 0-100
result_df10 = result_df9.withColumn("success_rate", 
                                     when((col("success_rate") >= 0) & (col("success_rate") <= 100), 
                                          round(col("success_rate").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

dim_channel_df = result_df10
display(dim_channel_df)

# COMMAND ----------

# Add process_date and audit columns consistently

process_date = "2025-10-30"  # Define process_date
table_name = "channel_table"  # Define table_name

dim_channel_df = dim_channel_df \
        .withColumn("process_date", lit(process_date).cast(DateType())) \
        .withColumn("process_timestamp", current_timestamp()) \
        .withColumn("process_year", year(lit(process_date))) \
        .withColumn("process_month", month(lit(process_date))) \
        .withColumn("process_day", dayofmonth(lit(process_date))) \
        .withColumn("inserted_by", lit("ETL_SYSTEM")) \
        .withColumn("source_system", lit(table_name))

dim_channel_df.display()

# COMMAND ----------

# DBTITLE 1,WRITE
dim_channel_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_year", "process_month", "process_day").csv("abfss://finalproject@shrek369.dfs.core.windows.net/silver/channel")