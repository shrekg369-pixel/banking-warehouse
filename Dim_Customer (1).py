# Databricks notebook source
# MAGIC %md
# MAGIC ###Dim_Customer Transformations and **Cleaning**( Row 1 to 20)

# COMMAND ----------

# DBTITLE 1,IMPORT
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,READ FILE
customer_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/bronze/dim_customer_80k.csv")
customer_df.display()

# COMMAND ----------

# DBTITLE 1,ARCHIVE CODE
#select column with atleat one null value and form new df 
null_columns = [c for c in customer_df.columns if customer_df.filter(col(c).isNull()).count() > 0]
archive_customer_df = customer_df.select(*null_columns)
archive_customer_df.display




# COMMAND ----------

# DBTITLE 1,AUDIT BRONZE
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [customer_df.filter(col(c).isNull()).count() for c in customer_df.columns]
row_count = customer_df.count()
col_count = len(customer_df.columns)

# single audit record

adudit_data = [{
    "audit_id": 1,
    "table_name": "customer",
    "row_count": row_count,
    "col_count": col_count,
    "null_count": str(null_count)
}]
#create dataframe
audit_customer_df = spark.createDataFrame(adudit_data)
audit_customer_df.display()

#write the df 
audit_customer_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/customer/bronze")

# COMMAND ----------

# DBTITLE 1,TRANSFORMATIONS
#  Row 1: customer_id - Direct mapping (LONG to INT)
result_df = customer_df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
result_df.display()



# COMMAND ----------

 # Row 2: customer_code - Generate unique code format
result_df1 = result_df.withColumn("customer_code", 
                                     concat(lit("CUST"), lpad(col("customer_id"), 6, "0")))

# COMMAND ----------

  # Row 3: first_name - Trim and proper case
result_df2 = result_df1.withColumn("first_name", 
                                     initcap(trim(col("first_name"))))


# COMMAND ----------

 # Row 4: middle_name - Validate male names list
male_names = [
    "Kumar", "Singh", "Raj", "Dev", "Mohan", "Lal", "Shankar", "Ravi", "Suresh", "Gopal"
]

result_df3 = result_df2.withColumn(
    "middle_name",
    when(
        col("middle_name").isin(male_names),
        col("middle_name")
    ).otherwise(lit("Kumar"))
)
display(result_df3)



# COMMAND ----------

 # Row 5: last_name - Trim and proper case
result_df4 = result_df3.withColumn("last_name", 
                                     initcap(trim(col("last_name"))))

# COMMAND ----------

   # Row 6: full_name - Concatenate three fields
result_df5 = result_df4.withColumn("full_name", 
                                     concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")))
                                  

# COMMAND ----------

 # Row 7: gender - Validate M/F/O values
result_df6 = result_df5.withColumn("gender", 
                                     when(col("gender").isin(["M", "F", "O"]), col("gender"))
                                     .otherwise(lit("U")))

# COMMAND ----------

 # Row 9: email_address - Validate email format using regex
 email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
 result_df7 = result_df6.withColumn("email_address", 
                                     when(col("email_address").rlike(email_pattern), col("email_address"))
                                     .otherwise(lit(None)))
 result_df7.display()

# COMMAND ----------

  # Row 8: date_of_birth - Convert string to date format
result_df8 = result_df7.withColumn("date_of_birth", 
                                     to_date(col("date_of_birth"), "yyyy-MM-dd"))
result_df8.display()                                     

# COMMAND ----------

# Row 10: phone_number - Validate Indian format +91
result_df9 = result_df8.withColumn("phone_number", 
                                     when(col("phone_number").rlike(r"^\+91[7-9][0-9]{9}$"), col("phone_number"))
                                     .otherwise(lit(None)))

# COMMAND ----------

  # Row 11: address_line1 - Standardize address format
result_df10 = result_df9.withColumn("address", 
                                     regexp_replace(upper(trim(col("address_line1"))), r"\s+", " "))
result_df10.display()                                    

# COMMAND ----------

 # Row 12: city - Standardize city names
result_df11 = result_df10.withColumn("city", 
                                     initcap(trim(col("city"))))

# COMMAND ----------

 # Row 13: state - Validate Indian states list
indian_states = ["Maharashtra", "Karnataka", "Tamil Nadu", "Delhi", "Gujarat", "West Bengal", 
                    "Uttar Pradesh", "Punjab", "Rajasthan", "Telangana"]
result_df12 = result_df11.withColumn("state", 
                                     when(col("state").isin(indian_states), col("state"))
                                     .otherwise(lit(None)))

# COMMAND ----------

  # Row 14: postal_code - Validate 6-digit format
result_df13 = result_df12.withColumn("postal_code", 
                                     when(col("postal_code").rlike(r"^[0-9]{6}$"), col("postal_code"))
                                     .otherwise(lit(None)))

# COMMAND ----------

    # Row 15: country - Default to India
result_df14 = result_df13.withColumn("country", 
                                     when(col("country").isNotNull(), col("country"))
                                     .otherwise(lit("India")))

# COMMAND ----------

    # Row 17: marital_status - Validate status list
result_df15 = result_df14.withColumn("marital_status", 
                                     when(col("marital_status").isin(["Single", "Married", "Divorced", "Widowed"]), 
                                          col("marital_status"))
                                     .otherwise(lit(None)))

# COMMAND ----------

# Row 18: occupation - Standardize occupation
result_df16 = result_df15.withColumn("occupation", 
                                     initcap(trim(col("occupation"))))

# COMMAND ----------

 # Row 19: annual_income - Validate non-negative (LONG to DOUBLE)
result_df17 = result_df16.withColumn("annual_income", 
                                     when(col("annual_income") >= 0, col("annual_income").cast(DoubleType()))
                                     .otherwise(lit(0.0)))

# COMMAND ----------

result_df18 = result_df17.withColumn("languages_known",
    when(col("state") == "Maharashtra", lit("Marathi, Hindi, English"))
    .when(col("state") == "Karnataka", lit("Kannada, Hindi, English"))
    .when(col("state") == "Tamil Nadu", lit("Tamil, English, Hindi"))
    .when(col("state") == "Delhi", lit("Hindi, English, Punjabi"))
    # ... etc
    .otherwise(lit("Hindi, English"))
)

# COMMAND ----------

# Row 20: customer_status - Validate status values
dim_customer_df = result_df18.withColumn("customer_status", 
                                     when(col("customer_status").isin(["Active", "Inactive", "Suspended"]), 
                                          col("customer_status"))
                                     .otherwise(lit("Active")))
dim_customer_df.display()                                     

# COMMAND ----------

dim_customer_df=dim_customer_df.drop("phone_number")
display(dim_customer_df)

# COMMAND ----------

# DBTITLE 1,PROCESS DATE
# Add process_date and audit columns consistently

process_date = "2025-10-30"  # Define process_date
table_name = "customer_table"  # Define table_name

dim_customer_df = dim_customer_df \
        .withColumn("process_date", lit(process_date).cast(DateType())) \
        .withColumn("process_timestamp", current_timestamp()) \
        .withColumn("process_year", year(lit(process_date))) \
        .withColumn("process_month", month(lit(process_date))) \
        .withColumn("process_day", dayofmonth(lit(process_date))) \
        .withColumn("inserted_by", lit("ETL_SYSTEM")) \
        .withColumn("source_system", lit(table_name))

dim_customer_df.display()

# COMMAND ----------

# DBTITLE 1,WRITE
dim_customer_df.coalesce(1).write.mode("append").option("header",True).partitionBy("process_year", "process_month", "process_day").csv("abfss://finalproject@shrek369.dfs.core.windows.net/silver/customer")