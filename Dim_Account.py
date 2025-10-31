# Databricks notebook source
# MAGIC %md
# MAGIC ### Dim_Account Transformations and Cleaning(Row 21 to 35)

# COMMAND ----------

# DBTITLE 1,IMPORT
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,READ
account_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/bronze/dim_account_90k.csv")
account_df.display()

# COMMAND ----------

# DBTITLE 1,ARCHIVE CODE
#select column with atleat one null value and form new df 
null_columns = [c for c in account_df.columns if account_df.filter(col(c).isNull()).count() > 0]
archive_account_df = account_df.select(*null_columns)
archive_account_df.display()

#write the df
archive_account_df.coalesce(1).write.mode("append").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/archive/account")

# COMMAND ----------

# DBTITLE 1,AUDIT BRONZE
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [account_df.filter(col(c).isNull()).count() for c in account_df.columns]
row_count = account_df.count()
col_count = len(account_df.columns)

# single audit record

adudit_data = [{
    "audit_id": 1,
    "table_name": "account",
    "row_count": row_count,
    "col_count": col_count,
    "null_count": str(null_count)
}]
#create dataframe
audit_account_df = spark.createDataFrame(adudit_data)
audit_account_df.display()

#write the df 
audit_account_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/account/bronze")

# COMMAND ----------

# DBTITLE 1,TRANSFORMATIONS
 # Row 21: account_id - Primary key (LONG to INT)
result_df1 = account_df.withColumn("account_id", col("account_id").cast(IntegerType()))


# COMMAND ----------

 # Row 23: account_type - Map to standard types
type_mapping = {"SAV": "Savings", "CUR": "Current", "FD": "Fixed Deposit", "RD": "Recurring Deposit"}
result_df2 = result_df1.replace(type_mapping, subset=["account_type"])
result_df2.display()

# COMMAND ----------

 # Row 24: account_subtype - Validate subtype list
result_df3 = result_df2.withColumn("account_subtype", 
                                     when(col("account_subtype").isin(["Regular", "Premium", "Senior Citizen", "Student"]), 
                                          col("account_subtype"))
                                     .otherwise(lit("Regular")))

# COMMAND ----------

# Row 25: customer_id - FK to DIM_CUSTOMER
result_df4 = result_df3.withColumn("customer_id", col("customer_id").cast(IntegerType()))
    


# COMMAND ----------


# Row 26: branch_id - FK to DIM_BRANCH
result_df5 = result_df4.withColumn("branch_id", col("branch_id").cast(IntegerType()))

# COMMAND ----------

# Row 27: currency_code - Default to INR
result_df6 = result_df5.withColumn("currency_code", 
                                     when(col("currency_code").isNotNull(), col("currency_code"))
                                     .otherwise(lit("INR")))

# COMMAND ----------

 # Row 28: account_status - Validate status values
result_df7 = result_df6.withColumn("account_status", 
                                     when(col("account_status").isin(["Active", "Closed", "Dormant", "Frozen"]), 
                                          col("account_status"))
                                     .otherwise(lit("Active")))

# COMMAND ----------

# Row 29: opening_date - Convert to date
result_df8 = result_df7.withColumn("opening_date", 
                                     to_date(col("opening_date"), "yyyy-MM-dd"))
                                    

# COMMAND ----------

# Row 30: current_balance - Validate non-negative (DOUBLE to DECIMAL)
result_df9 = result_df8.withColumn("current_balance", 
                                     when(col("current_balance") >= 0, 
                                          round(col("current_balance").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

# Row 31: available_balance - Validate <= current_balance
result_df10 = result_df9.withColumn("available_balance", 
                                     when((col("available_balance") >= 0) & (col("available_balance") <= col("current_balance")), 
                                          round(col("available_balance").cast(DoubleType()), 2))
                                     .otherwise(col("current_balance")))

# COMMAND ----------

# Row 32: minimum_balance - Validate based on type
result_df11 = result_df10.withColumn("minimum_balance", 
                                     when(col("account_type") == "Savings", lit(1000.0))
                                     .when(col("account_type") == "Current", lit(5000.0))
                                     .otherwise(col("minimum_balance").cast(DoubleType())))

# COMMAND ----------

 
# Row 33: overdraft_limit - Default 0 if null
result_df12 = result_df11.withColumn("overdraft_limit", 
                                     when(col("overdraft_limit").isNotNull(), 
                                          round(col("overdraft_limit").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

# Row 34: interest_rate - Validate percentage range
result_df13 = result_df12.withColumn("interest_rate", 
                                     when((col("interest_rate") >= 0) & (col("interest_rate") <= 50), 
                                          round(col("interest_rate").cast(DoubleType()), 4))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

# Row 35: online_banking_enabled - Validate Y/N values
result_df14 = result_df13.withColumn("online_banking_enabled", 
                                     when(col("online_banking_enabled").isin(["Y", "N"]), col("online_banking_enabled"))
                                     .otherwise(lit("N")))
result_df14.display()                                    

# COMMAND ----------

#Row 22: replace null with 'unknown' in date columns
result_df=result_df14.withColumn("maturity_date", 
                                    when(col("maturity_date").isNull(),"unknown")
                                                             .otherwise(col("maturity_date").cast("date"))).withColumn("closing_date", 
                                    when(col("closing_date").isNull(),"unknown")
                                                             .otherwise(col("closing_date").cast("date"))).withColumn("dormancy_date",
                                    when(col("dormancy_date").isNull(),"unknown")
                                                             .otherwise(col("dormancy_date").cast("date"))) 
                                          


# COMMAND ----------

dim_account_df=result_df
dim_account_df.display()

# COMMAND ----------

# DBTITLE 1,PROCESS DATE
# Add process_date and audit columns consistently

process_date = "2025-10-30"  # Define process_date
table_name = "account_table"  # Define table_name

dim_account_df = dim_account_df \
        .withColumn("process_date", lit(process_date).cast(DateType())) \
        .withColumn("process_timestamp", current_timestamp()) \
        .withColumn("process_year", year(lit(process_date))) \
        .withColumn("process_month", month(lit(process_date))) \
        .withColumn("process_day", dayofmonth(lit(process_date))) \
        .withColumn("inserted_by", lit("ETL_SYSTEM")) \
        .withColumn("source_system", lit(table_name))

dim_account_df.display()

# COMMAND ----------

# DBTITLE 1,WRITE
dim_account_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_year", "process_month", "process_day").csv("abfss://finalproject@shrek369.dfs.core.windows.net/silver/account")