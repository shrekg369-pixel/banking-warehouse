# Databricks notebook source
# MAGIC %md
# MAGIC ### STAR SCHEMA AND BUSINESS METRICS

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,read fact transcation
fact_transcation_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/silver/fact_transcation/process_year=2025/process_month=10/process_day=30/part-00000-tid-3832904819965358702-fcda8136-2f76-4840-af95-90ecb1cba803-132-1.c000.csv")
fact_transcation_df.display()

# COMMAND ----------

# DBTITLE 1,AUDIT FACT SILVER AND GOLD
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [fact_transcation_df.filter(col(c).isNull()).count() for c in fact_transcation_df.columns]
row_count = fact_transcation_df.count()
col_count = len(fact_transcation_df.columns)

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
audit_fact_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/fcat/silver")


from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [fact_transcation_df.filter(col(c).isNull()).count() for c in fact_transcation_df.columns]
row_count = fact_transcation_df.count()
col_count = len(fact_transcation_df.columns)

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
audit_fact_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/fcat/gold")

# COMMAND ----------

# DBTITLE 1,read customer
dim_customer_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/silver/customer/process_year=2025/process_month=10/process_day=30/part-00000-tid-4516466968061125911-bb8bf90b-2ee8-4f42-9718-675d9a918931-810-1.c000.csv")
dim_customer_df.display()

# COMMAND ----------

# DBTITLE 1,AUDIT CUSTOMER SILVER
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_customer_df.filter(col(c).isNull()).count() for c in dim_customer_df.columns]
row_count = dim_customer_df.count()
col_count = len(dim_customer_df.columns)

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
audit_customer_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/customer/silver")

# COMMAND ----------

# DBTITLE 1,read account
dim_account_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/silver/account/process_year=2025/process_month=10/process_day=30/part-00000-tid-7158138777439367345-99690649-cb0c-4f7f-821a-dc48426c7d21-33-1.c000.csv")
dim_account_df.display()

# COMMAND ----------

# DBTITLE 1,read employee
dim_employee_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/silver/employee/process_year=2025/process_month=10/process_day=30/part-00000-tid-9216412811537965721-2086679b-a25a-49c2-b974-304d2f3d4b8f-46-1.c000.csv")
dim_employee_df.display()

# COMMAND ----------

# DBTITLE 1,read branch
dim_branch_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/silver/branch/process_year=2025/process_month=10/process_day=30/part-00000-tid-5850654049586640989-c148c129-c59e-4a09-ac57-0124b05f1ba9-85-1.c000.csv")
dim_branch_df.display()

# COMMAND ----------

# DBTITLE 1,read product
dim_product_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/silver/product/process_year=2025/process_month=10/process_day=30/")
dim_product_df.display()

# COMMAND ----------

# DBTITLE 1,AUDIT PRODUCT SILVER AND GOLD
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_product_df.filter(col(c).isNull()).count() for c in dim_product_df.columns]
row_count = dim_product_df.count()
col_count = len(dim_product_df.columns)

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
audit_product_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/product/silver")

# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_product_df.filter(col(c).isNull()).count() for c in dim_product_df.columns]
row_count = dim_product_df.count()
col_count = len(dim_product_df.columns)

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
audit_product_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/product/gold")

# COMMAND ----------

# DBTITLE 1,read channel
dim_channel_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/silver/channel/process_year=2025/process_month=10/process_day=30/part-00000-tid-5080706236582010766-6d5973cf-c6d9-4c65-baaf-0749bf3998b5-109-1.c000.csv")
dim_channel_df.display()

# COMMAND ----------

# DBTITLE 1,AUDIT CHANNEL SILVER AND GOLD
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_channel_df.filter(col(c).isNull()).count() for c in dim_channel_df.columns]
row_count = dim_channel_df.count()
col_count = len(dim_channel_df.columns)

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
audit_channel_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/channel/silver")

# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_channel_df.filter(col(c).isNull()).count() for c in dim_channel_df.columns]
row_count = dim_channel_df.count()
col_count = len(dim_channel_df.columns)

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
audit_channel_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/channel/gold")

# COMMAND ----------

# DBTITLE 1,read time
dim_time_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
    "abfss://finalproject@shrek369.dfs.core.windows.net/silver/time/process_year=2025/process_month=10/process_day=30/part-00000-tid-4589852669767154039-830a9bf9-ef5a-4a08-b0b0-dc1e916ee2ac-120-1.c000.csv")
dim_time_df.display()

# COMMAND ----------

# DBTITLE 1,AUDIT TIME SILVER AND GOLD
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_time_df.filter(col(c).isNull()).count() for c in dim_time_df.columns]
row_count = dim_time_df.count()
col_count = len(dim_time_df.columns)

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
audit_time_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/time/silver")

# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_time_df.filter(col(c).isNull()).count() for c in dim_time_df.columns]
row_count = dim_time_df.count()
col_count = len(dim_time_df.columns)

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
audit_time_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/time/gold")

# COMMAND ----------

# DBTITLE 1,GOLD CUSTOMER 
from pyspark.sql.functions import col, year, current_date, when, lit


gold_dim_customer_df = dim_customer_df.withColumn(
    "customer_age",
    year(current_date()) - year(col("date_of_birth"))
)

gold_dim_customer_df = gold_dim_customer_df.withColumn(
    "age_group",
    when(col("customer_age") < 25, lit("Young Adult"))
    .when(col("customer_age") < 40, lit("Adult"))
    .when(col("customer_age") < 60, lit("Middle Aged"))
    .otherwise(lit("Senior Citizen"))
)

gold_dim_customer_df = gold_dim_customer_df.withColumn(
    "income_segment",
    when(col("annual_income") < 300000, lit("Low"))
    .when(col("annual_income") < 1000000, lit("Medium"))
    .when(col("annual_income") < 5000000, lit("High"))
    .otherwise(lit("Very High"))
)

gold_dim_customer_df = gold_dim_customer_df.select(
    "customer_id",
    "customer_code",
    "full_name",
    "gender",
    "date_of_birth",
    "customer_age",
    "age_group",
    "email_address",
    "city",
    "state",
    "postal_code",
    "country",
    "languages_known",
    "marital_status",
    "occupation",
    "annual_income",
    "income_segment",
    "process_date"
)
gold_dim_customer_df.display()


# COMMAND ----------

# DBTITLE 1,gold customer audit
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [gold_dim_customer_df.filter(col(c).isNull()).count() for c in gold_dim_customer_df.columns]
row_count = gold_dim_customer_df.count()
col_count = len(gold_dim_customer_df.columns)

# single audit record

audit_data = [{
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
audit_customer_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/customer/gold")

# COMMAND ----------

# DBTITLE 1,GOLD  ACCOUNT
from pyspark.sql.functions import col, datediff, current_date, when, lit
from pyspark.sql.types import IntegerType

gold_dim_account_df = dim_account_df.select(
    col("account_id"),
    col("account_number"),
    col("account_type"),
    col("account_subtype"),
    col("customer_id"),
    col("branch_id"),
    col("currency_code"),
    col("account_status"),
    col("opening_date"),
    datediff(current_date(), col("opening_date")).alias("account_age_days"),
    (datediff(current_date(), col("opening_date")) / 365).cast(IntegerType()).alias("account_age_years"),
    col("current_balance"),
    col("available_balance"),
    col("minimum_balance"),
    when(col("current_balance") >= col("minimum_balance"), lit("Healthy"))
        .otherwise(lit("Below Minimum")).alias("balance_status"),
    col("overdraft_limit"),
    col("interest_rate"),
    col("online_banking_enabled"),
    col("process_date")
)

gold_dim_account_df.display()

# COMMAND ----------

# DBTITLE 1,AUDIT ACCOUNT SILVER AND GOLD
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_account_df.filter(col(c).isNull()).count() for c in dim_account_df.columns]
row_count = dim_account_df.count()
col_count = len(dim_account_df.columns)

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
audit_account_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/account/silver")


# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [gold_dim_account_df.filter(col(c).isNull()).count() for c in gold_dim_account_df.columns]
row_count = gold_dim_account_df.count()
col_count = len(gold_dim_account_df.columns)

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
audit_account_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/account/gold")

# COMMAND ----------

# DBTITLE 1,GOLD BRANCH
gold_dim_branch_df = dim_branch_df.select(
        col("branch_id"),
        col("branch_code"),
        col("branch_name"),
        col("branch_type"),
        col("region"),
        col("zone"),
        col("city"),
        col("state"),
        col("postal_code"),
        col("phone_number"),
        col("manager_name"),
        col("branch_status"),
        col("employee_count"),
        col("customer_count"),
        # Customers per employee ratio
        (col("customer_count") / col("employee_count")).alias("customers_per_employee"),
        # Branch size category
        when(col("employee_count") < 20, lit("Small"))
            .when(col("employee_count") < 50, lit("Medium"))
            .otherwise(lit("Large")).alias("branch_size"),
        col("compliance_rating"),
        col("process_date")
    )
    
gold_dim_branch_df.display()

# COMMAND ----------

# DBTITLE 1,AUDIT BRANCH SILVER AND GOLD
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_branch_df.filter(col(c).isNull()).count() for c in dim_branch_df.columns]
row_count = dim_branch_df.count()
col_count = len(dim_branch_df.columns)

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
audit_branch_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/branch/silver")

# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [gold_dim_branch_df.filter(col(c).isNull()).count() for c in gold_dim_branch_df.columns]
row_count = gold_dim_branch_df.count()
col_count = len(gold_dim_branch_df.columns)

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
audit_branch_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/branch/gold")

# COMMAND ----------

# DBTITLE 1,GOLD EMPLOYEE
from pyspark.sql.functions import col, datediff, current_date, when, lit
from pyspark.sql.types import IntegerType

gold_dim_employee_df = dim_employee_df.select(
    "employee_id",
    "employee_code",
    "full_name",
    "gender",
    "date_of_birth",
    "designation",
    "department",
    "branch_id",
    "current_salary",
    "email_address",
    "phone_number",
    "education_qualification",
    "experience_years",
    "languages_known",
    "performance_rating",
    "customer_rating",
    "process_date",
   
)
gold_dim_employee_df.display()

# COMMAND ----------

# DBTITLE 1,AUDIT EMPLOYEE SILVER AND GOLD
# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [dim_employee_df.filter(col(c).isNull()).count() for c in dim_employee_df.columns]
row_count = dim_employee_df.count()
col_count = len(dim_employee_df.columns)

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
audit_employee_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/employee/silver")

# SIMPLE AUDIT CLASS

from pyspark.sql.functions import current_date, current_timestamp, sum as spark_sum, length, to_json, struct

null_count = [gold_dim_employee_df.filter(col(c).isNull()).count() for c in gold_dim_employee_df.columns]
row_count = gold_dim_employee_df.count()
col_count = len(gold_dim_employee_df.columns)

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
audit_employee_df.coalesce(1).write.mode("append").option("header",True).format("csv").save("abfss://finalproject@shrek369.dfs.core.windows.net/silver/audit_file/employee/gold")

# COMMAND ----------

# DBTITLE 1,WRITE GOLD TIME
dim_time_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_date").format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/starschema/time")

# COMMAND ----------

# DBTITLE 1,WRITE GOLD CUSTOMER
gold_dim_customer_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_date").format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/starschema/customer")

# COMMAND ----------

# DBTITLE 1,WRITE GOLD BRANCH
gold_dim_branch_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_date").format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/starschema/branch")

# COMMAND ----------

# DBTITLE 1,WRITE GOLD PRODUCT
dim_product_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_date").format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/starschema/product")

# COMMAND ----------

# DBTITLE 1,WRITE GOLD ACCOUNT
gold_dim_account_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_date").format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/starschema/account")

# COMMAND ----------

# DBTITLE 1,WRITE  GOLD EMPLOYEE
gold_dim_employee_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_date").format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/starschema/employee")

# COMMAND ----------

# DBTITLE 1,WRITE GOLD CHANNEL
dim_channel_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_date").format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/starschema/channel")

# COMMAND ----------

# DBTITLE 1,WRITE GOLD BRANCH
gold_dim_branch_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_date").format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/starschema/branch")

# COMMAND ----------

# DBTITLE 1,WRITE GOLD FACT TRANSACTION
fact_transcation_df.coalesce(1).write.mode("overwrite").option("header",True).partitionBy("process_date").format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/starschema/fact_transaction")

# COMMAND ----------

# DBTITLE 1,COMBINE JOIN
enriched_fact_df = fact_transcation_df \
        .join(gold_dim_customer_df.select("customer_id", "full_name", "age_group", "income_segment", "state"), 
              ["customer_id"], "left") \
        .join(gold_dim_account_df.select("account_id", "account_type", "account_status"), 
              ["account_id"], "left") \
        .join(gold_dim_branch_df.select("branch_id", "branch_name", "region", "zone", "city"), 
              ["branch_id"], "left") \
        .join(dim_product_df.select("product_id", "product_name", "product_category", "product_subcategory"), 
              ["product_id"], "left") \
        .join(dim_channel_df.select("channel_id", "channel_name", "channel_type", "channel_category"), 
              ["channel_id"], "left") \
        .join(gold_dim_employee_df.select("employee_id", col("full_name").alias("employee_name"), "department"), 
              ["employee_id"], "left") 
         
enriched_fact_df.display()        

# COMMAND ----------

enriched_fact_df1 =  enriched_fact_df.alias("ef").join(dim_time_df.select("time_id", "full_date", "day_name", "month_name", "quarter", "year", "fiscal_year", "is_weekend", "is_holiday", "business_day","holiday_name").alias("dt"), col("ef.transaction_date") == col("dt.full_date"), "left")
enriched_fact_df1.display()              

# COMMAND ----------

# DBTITLE 1,ENRICHED FACT TABLE
# Add calculated metrics
enriched_fact_df2 = enriched_fact_df1 \
        .withColumn("net_amount", col("transaction_amount") - col("fee_amount") ) \
        .withColumn("is_high_value", when(col("transaction_amount") >= 100000, lit("Y")).otherwise(lit("N"))) \
        .withColumn("is_risky", when(col("risk_score") >= 7, lit("Y")).otherwise(lit("N"))) \
        .withColumn("transaction_hour", hour(col("process_timestamp"))) \
        .withColumn("transaction_day_of_week", dayofweek(col("full_date")))
    
enriched_fact_df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##AGGREGATED VIEWS & METRICS (for dashboards)

# COMMAND ----------

# DBTITLE 1,1. Daily Transaction Summary
# VIEW 1: Daily Transaction Summary


daily_summary_df = enriched_fact_df2.orderBy("full_date").groupBy(
    "full_date", "year", "month_name", "day_name", "is_weekend", "is_holiday", "business_day"
).agg(
    count("transaction_id").alias("total_transactions"),
    countDistinct("customer_id").alias("unique_customers"),
    round(sum("transaction_amount"),2).alias("total_amount"),
    round(avg("transaction_amount"),2).alias("avg_transaction_amount"),
    round(sum("fee_amount"),2).alias("total_fees"),
    round(sum("net_amount"),2).alias("total_net_amount"),
    sum(when(col("fraud_flag") == "Y", 1).otherwise(0)).alias("fraud_count"),
    round(avg("risk_score"),2).alias("avg_risk_score"),
   
)
daily_summary_df.display()

daily_summary_df.coalesce(1).write.mode("overwrite").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/buisnessmetrics/dailysummary")

# COMMAND ----------

# DBTITLE 1,2.Branch Performance Metrics
#VIEW 2: Branch Performance Metrics


branch_performance_df = enriched_fact_df2.orderBy("branch_id").groupBy(
    "branch_id", "branch_name", "region", "zone", "city", "year", "month_name"
).agg(
    count("transaction_id").alias("total_transactions"),
    countDistinct("customer_id").alias("unique_customers"),
    round(sum("transaction_amount"),2).alias("total_amount"),
    round(avg("transaction_amount"),2).alias("avg_transaction_amount"),
    round(sum("fee_amount"),2).alias("total_fees_collected"),
    (sum("transaction_amount") /sum("fee_amount") * 100).alias("total_revenue"),
    round(avg("risk_score"),2).alias("avg_risk_score"),
    sum(when(col("fraud_flag") == "Y", 1).otherwise(0)).alias("fraud_incidents")
).withColumn("revenue_per_transaction", col("total_revenue") / (col("total_transactions") * 100))
branch_performance_df = branch_performance_df.dropna()
branch_performance_df.display()

branch_performance_df.coalesce(1).write.mode("overwrite").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/buisnessmetrics/branchperformance")

# COMMAND ----------

# DBTITLE 1,3.Product Performance Analysis
#VIEW 3: Product Performance Analysis


product_analysis_df = enriched_fact_df2.orderBy("product_id").groupBy(
    "product_id", "product_name", "product_category", "product_subcategory", "year", "month_name"
).agg(
    count("transaction_id").alias("transaction_count"),
    countDistinct("customer_id").alias("unique_customers"),
    sum("transaction_amount").alias("total_volume"),
    avg("transaction_amount").alias("avg_transaction_size"),
    sum("fee_amount").alias("total_fees"),
    min("transaction_amount").alias("min_transaction"),
    max("transaction_amount").alias("max_transaction"),
    avg("risk_score").alias("avg_risk")
)
product_analysis_df = product_analysis_df.dropna()

product_analysis_df.display()

product_analysis_df.coalesce(1).write.mode("overwrite").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/buisnessmetrics/productanalysis")

# COMMAND ----------

# DBTITLE 1,4.Customer Behavior
# VIEW 4: Customer Behavior

customer_behavior_df = enriched_fact_df2.orderBy("risk_score").groupBy(
    "customer_id", col("full_name").alias("customer_name"), "age_group", 
    "income_segment", "state", "year","risk_score"
).agg(
    count("transaction_id").alias("transaction_count"),
    round(sum("transaction_amount"),2).alias("total_spent"),
    round(avg("transaction_amount"),2).alias("avg_transaction"),
    countDistinct("product_id").alias("products_used"),
    countDistinct("channel_id").alias("channels_used"),
    countDistinct("branch_id").alias("branches_visited"),
    sum("fee_amount").alias("total_fees_paid"),
    round(avg("risk_score"),2).alias("avg_risk_score"),
    max("full_date").alias("last_transaction_date")
).withColumn("customer_value_tier", 
    when(col("total_spent") >= 5000000, lit("Platinum"))
    .when(col("total_spent") >= 1000000, lit("Gold"))
    .when(col("total_spent") >= 500000, lit("Silver"))
    .otherwise(lit("Bronze"))
)
customer_behavior_df = customer_behavior_df.dropna()
customer_behavior_df.display()

customer_behavior_df.coalesce(1).write.mode("overwrite").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/buisnessmetrics/customerbehavior")

# COMMAND ----------

# DBTITLE 1,5.Employee Performance
# VIEW 5: Employee Performance Dashboard

employee_performance_df = enriched_fact_df2.groupBy(
    "employee_id", col("employee_name"), "department", "branch_name", "year", "month_name"
).agg(
    sum("transaction_amount").alias("total_volume_processed"),
    avg("risk_score").alias("avg_risk_score"),
    sum(when(col("fraud_flag") == "Y", 1).otherwise(0)).alias("fraud_incidents_handled")
)
employee_performance_df = employee_performance_df.dropna()
employee_performance_df.display()

employee_performance_df.coalesce(1).write.mode("overwrite").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/buisnessmetrics/employeeperformance")

# COMMAND ----------

# DBTITLE 1,6.Regional Performance Overview
#  VIEW 6: Regional Performance Overview

regional_summary_df = enriched_fact_df2.orderBy("year").groupBy(
    "region", "year", "quarter", "month_name"
).agg(
    count("transaction_id").alias("total_transactions"),
    countDistinct("branch_id").alias("active_branches"),
    countDistinct("customer_id").alias("unique_customers"),
    round(sum("transaction_amount"),2).alias("total_volume"),
    round(avg("transaction_amount"),2).alias("avg_transaction"),
    round(sum("fee_amount"),2).alias("total_fees"),
)
regional_summary_df = regional_summary_df.dropna()
regional_summary_df = regional_summary_df.orderBy("total_volume")
regional_summary_df.display()

regional_summary_df.coalesce(1).write.mode("overwrite").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/buisnessmetrics/regional_performance")

# COMMAND ----------

# DBTITLE 1,7.Fraud Detection Summary
# VIEW 7: Fraud Detection Summary

fraud_summary_df = enriched_fact_df2.filter(col("fraud_flag") == "Y").groupBy(
    "full_date", "channel_name", "product_category", "region", "year", "month_name"
).agg(
    count("transaction_id").alias("fraud_count"),
    sum("transaction_amount").alias("fraud_amount"),
    avg("risk_score").alias("avg_risk_score"),
    countDistinct("customer_id").alias("affected_customers"),
    countDistinct("branch_id").alias("affected_branches")
).orderBy(desc(sum("transaction_amount")))

fraud_summary_df = fraud_summary_df.dropna()
fraud_summary_df.display()

fraud_summary_df.coalesce(1).write.mode("overwrite").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/buisnessmetrics/fraud_summary")

# COMMAND ----------

# DBTITLE 1,8.Time-based Transaction Patterns
# VIEW 8: Time-based Transaction Patterns

time_patterns_df = enriched_fact_df2.groupBy(
    "transaction_day_of_week", "day_name", "is_weekend", "is_holiday", "month_name", "year","holiday_name"
).agg(
    count("transaction_id").alias("transaction_count"),
    round(sum("transaction_amount"),2).alias("total_amount"),
    round(avg("transaction_amount"),2).alias("avg_amount"),
    countDistinct("customer_id").alias("unique_customers"),
).orderBy(desc(sum("transaction_amount")))

time_patterns_df = time_patterns_df.dropna()
time_patterns_df.display()

time_patterns_df.coalesce(1).write.mode("overwrite").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/buisnessmetrics/timebased_patterns")

# COMMAND ----------

# DBTITLE 1,9.Customer Lifetime Value (CLV)
# VIEW 9: Customer Lifetime Value (CLV)

customer_ltv_df = enriched_fact_df2.groupBy("customer_id", col("full_name").alias("customer_name")).agg(
    count("transaction_id").alias("total_transactions"),
    round(sum("transaction_amount"),2).alias("lifetime_value"),
    round(avg("transaction_amount"),2).alias("avg_transaction"),
    datediff(max("full_date"), min("full_date")).alias("customer_tenure_days"),
    min("full_date").alias("first_transaction_date"),
    max("full_date").alias("last_transaction_date"),
    countDistinct("product_id").alias("products_used"),
    round(sum("fee_amount"),2).alias("total_fees_paid")
).withColumn("avg_monthly_value", 
    col("lifetime_value") / (col("customer_tenure_days") / 30)
).withColumn("customer_tier",
    when(col("lifetime_value") >= 100000, lit("VIP"))
    .when(col("lifetime_value") >= 50000, lit("Premium"))
    .when(col("lifetime_value") >= 10000, lit("Gold"))
    .otherwise(lit("Standard"))
).orderBy(desc("customer_tier"))

customer_ltv_df = customer_ltv_df.dropna()
customer_ltv_df.display()

customer_ltv_df.coalesce(1).write.mode("overwrite").option("header",True).format("delta").save("abfss://finalproject@shrek369.dfs.core.windows.net/gold/buisnessmetrics/customer_lifetimevalue")
