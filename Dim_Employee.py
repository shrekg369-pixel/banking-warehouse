# Databricks notebook source
# MAGIC %md
# MAGIC ### Dim_Employee Transformations and Cleaning (Row 36 to 50)

# COMMAND ----------

# DBTITLE 1,IMPORT
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,READ
employee_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/bronze/dim_employee_80k.csv")
employee_df.display()

# COMMAND ----------

# DBTITLE 1,ARCHIVE CODE
#select column with atleat one null value and form new df 
null_columns = [c for c in employee_df.columns if employee_df.filter(col(c).isNull()).count() > 0]
archive_employee_df = employee_df.select(*null_columns)
archive_employee_df.display()

#write the df
archive_employee_df.coalesce(1).write.mode("append").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/archive/employee")

# COMMAND ----------

# DBTITLE 1,AUDIT BRONZE
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [employee_df.filter(col(c).isNull()).count() for c in employee_df.columns]
row_count = employee_df.count()
col_count = len(employee_df.columns)

# single audit record

adudit_data = [{
    "audit_id": 1,
    "table_name": "employee",
    "row_count": row_count,
    "col_count": col_count,
    "null_count": str(null_count)
}]
#create dataframe
audit_employee_df = spark.createDataFrame(adudit_data)
audit_employee_df.display()

#write the df 
audit_employee_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/employee/bronze")

# COMMAND ----------

# DBTITLE 1,TRANSFORMATIONS
# Row 36: employee_id - Primary key
result_df1 = employee_df.withColumn("employee_id", col("employee_id").cast(IntegerType()))

# COMMAND ----------

 # Row 37: employee_code - Generate format EMP{ID}
result_df2 = result_df1.withColumn("employee_code", 
                                     concat(lit("EMP"), lpad(col("employee_id"), 6, "0")))

# COMMAND ----------

# Row 38: first_name - Trim and proper case
result_df3 = result_df2.withColumn("first_name", initcap(trim(col("first_name"))))

# COMMAND ----------

# Row 39: middle_name - Validate male names
male_names = ["Kumar", "Singh", "Raj", "Dev", "Mohan", "Lal"]
result_df4 = result_df3.withColumn("middle_name", 
                                     when(col("middle_name").isin(male_names), col("middle_name"))
                                     .otherwise(lit("Kumar")))

# COMMAND ----------

# Row 40: last_name - Trim and proper case
result_df5 = result_df4.withColumn("last_name", initcap(trim(col("last_name"))))

# COMMAND ----------

 # Row 41: full_name - Concatenate three fields
result_df6 = result_df5.withColumn("full_name", 
                                     concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")))

# COMMAND ----------

 # Row 42: designation - Validate designation list
designations = ["Manager", "Assistant Manager", "Clerk", "Officer", "Executive"]
result_df7 = result_df6.withColumn("designation", 
                                     when(col("designation").isin(designations), col("designation"))
                                     .otherwise(lit("Officer")))

# COMMAND ----------

  # Row 43: department - Validate department list
departments = ["Finance", "Operations", "IT", "HR", "Risk", "Credit"]
result_df8 = result_df7.withColumn("department", 
                                     when(col("department").isin(departments), col("department"))
                                     .otherwise(lit("Operations")))

# COMMAND ----------

# Row 44: branch_id - FK to DIM_BRANCH
result_df9 = result_df8.withColumn("branch_id", col("branch_id").cast(IntegerType()))

# COMMAND ----------

 # Row 46: hire_date - Convert to date
result_df10 = result_df9.withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))

# COMMAND ----------

# Row 47: employee_status - Validate status values
result_df11 = result_df10.withColumn("employee_status", 
                                     when(col("employee_status").isin(["Active", "Inactive", "On Leave"]), 
                                          col("employee_status"))
                                     .otherwise(lit("Active")))

# COMMAND ----------

 # Row 48: current_salary - Validate non-negative
result_df12 = result_df11.withColumn("current_salary", 
                                     when(col("current_salary") >= 0, 
                                          round(col("current_salary").cast(DoubleType()), 2))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

 # Row 50: performance_rating - Validate 1-5 scale
result_df13 = result_df12.withColumn("performance_rating", 
                                     when((col("performance_rating") >= 1.0) & (col("performance_rating") <= 5.0), 
                                          round(col("performance_rating").cast(DoubleType()), 2))
                                     .otherwise(lit(3.0)))

# COMMAND ----------

#Row : replace null with 'unknown' in date columns
result_df14 = result_df13.withColumn("termination_date", 
                                     when(col("termination_date").isNull(),"unknown")
                                     .otherwise(col("termination_date").cast('date')))
result_df14.display()                                     

                                           

# COMMAND ----------

dim_employee_df = result_df14
display(dim_employee_df)

# COMMAND ----------

# Add process_date and audit columns consistently

process_date = "2025-10-30"  # Define process_date
table_name = "employee_table"  # Define table_name

dim_employee_df = dim_employee_df \
        .withColumn("process_date", lit(process_date).cast(DateType())) \
        .withColumn("process_timestamp", current_timestamp()) \
        .withColumn("process_year", year(lit(process_date))) \
        .withColumn("process_month", month(lit(process_date))) \
        .withColumn("process_day", dayofmonth(lit(process_date))) \
        .withColumn("inserted_by", lit("ETL_SYSTEM")) \
        .withColumn("source_system", lit(table_name))

dim_employee_df.display()

# COMMAND ----------

# DBTITLE 1,WRITE
dim_employee_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_year", "process_month", "process_day").csv("abfss://finalproject@shrek369.dfs.core.windows.net/silver/employee")