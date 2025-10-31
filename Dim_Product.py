# Databricks notebook source
# MAGIC %md
# MAGIC ## DIM_PRODUCT TRANSFORMATIONSAND CLEANING (Rows 66-77)

# COMMAND ----------

# DBTITLE 1,IMPORT
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,READ
product_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/bronze/dim_product_65k.csv")
product_df.display()

# COMMAND ----------

# DBTITLE 1,ARCHIVE CODE
#select column with atleat one null value and form new df 
null_columns = [c for c in product_df.columns if product_df.filter(col(c).isNull()).count() > 0]
archive_product_df = product_df.select(*null_columns)
archive_product_df.display()

#write the df
archive_product_df.coalesce(1).write.mode("append").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/archive/product")

# COMMAND ----------

# DBTITLE 1,AUDIT BRONZE
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [product_df.filter(col(c).isNull()).count() for c in product_df.columns]
row_count = product_df.count()
col_count = len(product_df.columns)

# single audit record

adudit_data = [{
    "audit_id": 1,
    "table_name": "product",
    "row_count": row_count,
    "col_count": col_count,
    "null_count": str(null_count)
}]
#create dataframe
audit_product_df = spark.createDataFrame(adudit_data)
audit_product_df.display()

#write the df 
audit_product_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/product/bronze")

# COMMAND ----------

# DBTITLE 1,TRANSFORMATIONS
 # Row 66: product_id - Primary key
result_df1 = product_df.withColumn("product_id", col("product_id").cast(IntegerType()))

# COMMAND ----------

 # Row 67: product_code - Generate format PRD{ID}
result_df2 = result_df1.withColumn("product_code", 
                                     concat(lit("PRD"), lpad(col("product_id"), 6, "0")))

# COMMAND ----------

# Row 68: product_name - Standardize naming
result_df3 = result_df2.withColumn("product_name", initcap(trim(col("product_name"))))

# COMMAND ----------

 # Row 69: product_category - Map to standard categories
result_df4 = result_df3.withColumn("product_category", 
                                     when(col("product_category").isin(["Deposits", "Loans", "Cards", "Insurance"]), 
                                          col("product_category"))
                                     .otherwise(lit("General")))

# COMMAND ----------

 # Row 70: product_subcategory - Validate subcategory
result_df5 = result_df4.withColumn("product_subcategory", initcap(trim(col("product_subcategory"))))

# COMMAND ----------


    # Row 71: minimum_balance - Validate non-negative
result_df6 = result_df5.withColumn("minimum_balance", 
                                     when(col("minimum_balance") >= 0, 
                                          round(col("minimum_balance").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

# Row 72: interest_rate - Validate percentage range
result_df7 = result_df6.withColumn("interest_rate", 
                                     when((col("interest_rate") >= 0) & (col("interest_rate") <= 100), 
                                          round(col("interest_rate").cast(DoubleType()), 4))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

 # Row 73: processing_fee - Validate non-negative
result_df8 = result_df7.withColumn("processing_fee", 
                                     when(col("processing_fee") >= 0, 
                                          round(col("processing_fee").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

# Row 74: annual_fee - Validate non-negative
result_df9 = result_df8.withColumn("annual_fee", 
                                     when(col("annual_fee") >= 0, 
                                          round(col("annual_fee").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

    # Row 75: product_status - Validate status values
result_df10 = result_df9.withColumn("product_status", 
                                     when(col("product_status").isin(["Active", "Discontinued"]), col("product_status"))
                                     .otherwise(lit("Active")))

# COMMAND ----------

# Row 76: risk_category - Validate risk levels
result_df11 = result_df10.withColumn("risk_category", 
                                     when(col("risk_category").isin(["Low", "Medium", "High"]), col("risk_category"))
                                     .otherwise(lit("Medium")))

# COMMAND ----------

# Row 77: regulatory_approval - Prefix RBI/ if missing
result_df12 = result_df11.withColumn("regulatory_approval", 
                                     when(col("regulatory_approval").startswith("RBI/"), col("regulatory_approval"))
                                     .otherwise(concat(lit("RBI/"), col("regulatory_approval"))))

# COMMAND ----------

dim_product_df=result_df12
dim_product_df = dim_product_df.drop(col("discontinue_date"))
display(dim_product_df)

# COMMAND ----------

# DBTITLE 1,process date
# Add process_date and audit columns consistently

process_date = "2025-10-30"  # Define process_date
table_name = "product_table"  # Define table_name

dim_product_df = dim_product_df \
        .withColumn("process_date", lit(process_date).cast(DateType())) \
        .withColumn("process_timestamp", current_timestamp()) \
        .withColumn("process_year", year(lit(process_date))) \
        .withColumn("process_month", month(lit(process_date))) \
        .withColumn("process_day", dayofmonth(lit(process_date))) \
        .withColumn("inserted_by", lit("ETL_SYSTEM")) \
        .withColumn("source_system", lit(table_name))

dim_product_df.display()

# COMMAND ----------

# DBTITLE 1,WRITE
dim_product_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_year", "process_month", "process_day").csv("abfss://finalproject@shrek369.dfs.core.windows.net/silver/product")

# COMMAND ----------

# MAGIC %md
# MAGIC