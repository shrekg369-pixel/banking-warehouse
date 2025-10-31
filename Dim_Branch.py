# Databricks notebook source
# MAGIC %md
# MAGIC ## DIM_BRANCH TRANSFORMATIONS AND CLEANING (Rows 51-65)

# COMMAND ----------

# DBTITLE 1,IMPORT
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,READ
branch_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/bronze/dim_branch_40k.csv")
branch_df.display()

# COMMAND ----------

# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [branch_df.filter(col(c).isNull()).count() for c in branch_df.columns]
row_count = branch_df.count()
col_count = len(branch_df.columns)

# single audit record

adudit_data = [{
    "audit_id": 1,
    "table_name": "branch",
    "row_count": row_count,
    "col_count": col_count,
    "null_count": str(null_count)
}]
#create dataframe
audit_branch_df = spark.createDataFrame(adudit_data)
audit_branch_df.display()

#write the df 
audit_branch_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/branch/bronze")

# COMMAND ----------

# DBTITLE 1,ARCHIVE CODE
#select column with atleat one null value and form new df 
null_columns = [c for c in branch_df.columns if branch_df.filter(col(c).isNull()).count() > 0]
archive_branch_df = branch_df.select(*null_columns)
archive_branch_df.display()



# COMMAND ----------

# DBTITLE 1,TRANSFORMATIONS
# Row 51: branch_id - Primary key
result_df1 = branch_df.withColumn("branch_id", col("branch_id").cast(IntegerType()))

# COMMAND ----------

   # Row 52: branch_code - Generate format BR{ID}
result_df2 = result_df1.withColumn("branch_code", 
                                     concat(lit("BR"), lpad(col("branch_id"), 6, "0")))

# COMMAND ----------

 # Row 53: branch_name - Standardize naming
result_df3 = result_df2.withColumn("branch_name", initcap(trim(col("branch_name"))))
    

# COMMAND ----------

# Row 54: branch_type - Validate type list
result_df4 = result_df3.withColumn("branch_type", 
                                     when(col("branch_type").isin(["Main", "Sub", "Extension"]), col("branch_type"))
                                     .otherwise(lit("Sub")))

# COMMAND ----------

# Row 55: region - Map state to region using CASE WHEN (NO lookup table)
result_df5 = result_df4.withColumn("region",
        when(col("state").isin(["Maharashtra", "Gujarat", "Goa"]), lit("West"))
        .when(col("state").isin(["Karnataka", "Tamil Nadu", "Kerala", "Andhra Pradesh", "Telangana"]), lit("South"))
        .when(col("state").isin(["Delhi", "Punjab", "Haryana", "Uttar Pradesh", "Rajasthan", "Uttarakhand"]), lit("North"))
        .when(col("state").isin(["West Bengal", "Odisha", "Bihar", "Jharkhand"]), lit("East"))
        .when(col("state").isin(["Madhya Pradesh", "Chhattisgarh"]), lit("Central"))
        .when(col("state").isin(["Assam", "Meghalaya", "Manipur", "Tripura"]), lit("North East"))
        .otherwise(lit("Other"))
    )

# COMMAND ----------

# Row 56: zone - Validate zone values
result_df6 = result_df5.withColumn("zone", 
                                     when(col("zone").isin(["North", "South", "East", "West"]), col("zone"))
                                     .otherwise(lit("Central")))

# COMMAND ----------

 # Row 57: city - Standardize city names
result_df7 = result_df6.withColumn("city", initcap(trim(col("city"))))
result_df7.display()

# COMMAND ----------

# Row 58: state - Validate Indian states
indian_states = ["Maharashtra", "Karnataka", "Tamil Nadu", "Delhi", "Gujarat"]
result_df8 = result_df7.withColumn("state", 
                                     when(col("state").isin(indian_states), col("state"))
                                     .otherwise(lit(None)))
                                     

# COMMAND ----------

 # Row 59: postal_code - Map city to postal using CASE WHEN (NO lookup table)
result_df9 = result_df8.withColumn("postal_code",
        when(col("city") == "Mumbai", lit("400001"))
        .when(col("city") == "Pune", lit("411001"))
        .when(col("city") == "Bangalore", lit("560001"))
        .when(col("city") == "Chennai", lit("600001"))
        .when(col("city") == "Delhi", lit("110001"))
        .when(col("city") == "New Delhi", lit("110001"))
        .when(col("city") == "Ahmedabad", lit("380001"))
        .when(col("city") == "Kolkata", lit("700001"))
        .when(col("city") == "Hyderabad", lit("500001"))
        .when(col("city") == "Jaipur", lit("302001"))
        .otherwise(col("postal_code"))  # Keep existing if not in mapping
    )

# COMMAND ----------

# Row 60: replace null with unknown in state column
result_df10 = result_df9.withColumn("state", 
                                     when(col("state").isNull(), lit("Unknown"))
                                     .otherwise(col("state")))

                                     

# COMMAND ----------

# DBTITLE 1,READ Dim_Employee
employee_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/silver/employee/process_year=2025/process_month=10/process_day=30/part-00000-tid-9216412811537965721-2086679b-a25a-49c2-b974-304d2f3d4b8f-46-1.c000.csv")
employee_df.display()    

# COMMAND ----------

 # Row 61: manager_name - JOIN with employee table
result_df11 = result_df10.join(
        employee_df.select("employee_id", "full_name"),
        result_df10["branch_id"] == employee_df["employee_id"],
        "left"
    ).withColumn("manager_name", col("full_name"))
result_df11.display()    

# COMMAND ----------

# Row 62: branch_status - Validate status values
result_df12 = result_df11.withColumn("branch_status", 
                                     when(col("branch_status").isin(["Active", "Closed", "Under Maintenance"]), 
                                          col("branch_status"))
                                     .otherwise(lit("Active")))

# COMMAND ----------


    # Row 65: compliance_rating - Validate rating scale
result_df13 = result_df12.withColumn("compliance_rating", 
                                     when(col("compliance_rating").isin(["A", "B", "C"]), col("compliance_rating"))
                                     .otherwise(lit("B")))

# COMMAND ----------

dim_branch_df = result_df13
dim_branch_df = dim_branch_df.drop("full_name")
display(dim_branch_df)


# COMMAND ----------

# Add process_date and audit columns consistently

process_date = "2025-10-30"  # Define process_date
table_name = "branch_table"  # Define table_name

dim_branch_df = dim_branch_df \
        .withColumn("process_date", lit(process_date).cast(DateType())) \
        .withColumn("process_timestamp", current_timestamp()) \
        .withColumn("process_year", year(lit(process_date))) \
        .withColumn("process_month", month(lit(process_date))) \
        .withColumn("process_day", dayofmonth(lit(process_date))) \
        .withColumn("inserted_by", lit("ETL_SYSTEM")) \
        .withColumn("source_system", lit(table_name))

dim_branch_df.display()

# COMMAND ----------

# DBTITLE 1,WRITE
dim_branch_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_year", "process_month", "process_day").csv("abfss://finalproject@shrek369.dfs.core.windows.net/silver/branch")