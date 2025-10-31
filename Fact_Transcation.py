# Databricks notebook source
# MAGIC %md
# MAGIC ## FACT_TRANSACTION TRANSFORMATIONS (Rows 107-122)

# COMMAND ----------

# DBTITLE 1,IMPORT
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,READ
fact_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/bronze/fact_transaction_100k.csv")
fact_df.display()

# COMMAND ----------

# DBTITLE 1,ARCHIVE CODE
#select column with atleat one null value and form new df 
null_columns = [c for c in fact_df.columns if fact_df.filter(col(c).isNull()).count() > 0]
archive_fact_df = fact_df.select(*null_columns)
fact_account_df.display()



# COMMAND ----------

# DBTITLE 1,AUDIT BRONZE
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [fact_df.filter(col(c).isNull()).count() for c in fact_df.columns]
row_count = fact_df.count()
col_count = len(fact_df.columns)

# single audit record

adudit_data = [{
    "audit_id": 1,
    "table_name": "fact",
    "row_count": row_count,
    "col_count": col_count,
    "null_count": str(null_count)
}]
#create dataframe
audit_fact_df = spark.createDataFrame(adudit_data)
audit_fact_df.display()

#write the df 
audit_fact_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/fcat/bronze")

# COMMAND ----------

# DBTITLE 1,TRANSFORMATIONS
# Row 107: transaction_id - Primary key
result_df1 = fact_df.withColumn("transaction_id", col("transaction_id").cast(LongType()))

# COMMAND ----------

# Row 116: transaction_amount - Validate positive amount
result_df2 = result_df1.withColumn("transaction_amount", 
                                     when(col("transaction_amount") > 0, 
                                          round(col("transaction_amount").cast(DoubleType()), 2))
                                     .otherwise(lit(None)))

# COMMAND ----------

# Row 117: fee_amount - Default 0 if null
result_df3 = result_df2.withColumn("fee_amount", 
                                     when(col("fee_amount").isNotNull(), 
                                          round(col("fee_amount").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))
                                    

# COMMAND ----------

# Row 119: transcationamount - Validate non-negative
result_df4 = result_df3.withColumn("transaction_amountt", 
                                     when(col("transaction_amount") >= 0, 
                                          round(col("transaction_amount").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------


    # Row 120: balance_after - Validate non-negative
result_df5 = result_df4.withColumn("balance_after", 
                                     when(col("balance_after") >= 0, 
                                          round(col("balance_after").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

  # Row 121: risk_score - Normalize 0-10 scale
result_df6 = result_df5.withColumn("risk_score", 
                                     when((col("risk_score") >= 0) & (col("risk_score") <= 10), 
                                          round(col("risk_score").cast(DoubleType()), 2))
                                     .otherwise(lit(5.0)))
    

# COMMAND ----------

 # Row 122: fraud_indicator - Validate Y/N values
result_df7 = result_df6.withColumn("fraud_flag", 
                                     when(col("fraud_flag") == True,"Y")
                                     .when(col("fraud_flag") == False,"N")
                                     .otherwise(lit(None)))

# COMMAND ----------

fact_transcation_df=result_df7
display(fact_transcation_df)


# COMMAND ----------

# Add process_date and audit columns consistently

process_date = "2025-10-30"  # Define process_date
table_name = "fact_table"  # Define table_name

fact_transcation_df = fact_transcation_df \
        .withColumn("process_date", lit(process_date).cast(DateType())) \
        .withColumn("process_timestamp", current_timestamp()) \
        .withColumn("process_year", year(lit(process_date))) \
        .withColumn("process_month", month(lit(process_date))) \
        .withColumn("process_day", dayofmonth(lit(process_date))) \
        .withColumn("inserted_by", lit("ETL_SYSTEM")) \
        .withColumn("source_system", lit(table_name))

fact_transcation_df.display()

# COMMAND ----------

# DBTITLE 1,WRITE
fact_transcation_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_year", "process_month", "process_day").csv("abfss://finalproject@shrek369.dfs.core.windows.net/silver/fact_transcation")