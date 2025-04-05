# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Initiating Spark Session
spark = SparkSession.builder.appName("Salarycsv_to_parquet").getOrCreate()

# COMMAND ----------

dbutils.fs.unmount('/mnt/blobstorage1')

# COMMAND ----------

# mount azure blob storage to DBFS using account key
dbutils.fs.mount(source='wasbs://raw@blob1store.blob.core.windows.net',
                 mount_point='/mnt/blobstorage1',
                 extra_configs={'fs.azure.account.key.blob1store.blob.core.windows.net':'ZvdH/fgxasRYI5k5jZEckbFmBZEYUCMSlbiLkOSOOm6kd+UYDHK5JwRB6+o8kYbhfERDo3a0d91R+AStDDYQYQ=='})

# COMMAND ----------

# mount azure blob storage to DBFS using SAS key
# dbutils.fs.mount(source='wasbs://raw@blob1store.blob.core.windows.net',
#                  mount_point='/mnt/blobstorage',
#                  extra_configs={'fs.azure.sas.raw.blob1store.blob.core.windows.net':'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-04-03T17:24:00Z&st=2025-04-03T09:24:00Z&spr=https&sig=xwaIiH590GxM8qeDHkMqFN04cYig09C252ujKe9NJCQ%3D'})

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Listing the files in blobstorage

# COMMAND ----------

# Lists of files and folders located in DBFS 
dbutils.fs.ls('dbfs:/mnt/blobstorage1')

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading csv file from raw folder in Blob Storage

# COMMAND ----------

# reading salary csv file from raw folder in Blob storage
salary_csv = spark.read.format('csv')\
    .option("header",True)\
        .option("inferschema",True)\
            .load("dbfs:/mnt/blobstorage1/salary_Data.csv")
display(salary_csv)

# COMMAND ----------

# reading employee csv file from raw folder in Blob storage
employee_csv = spark.read.format('csv')\
    .option("header",True)\
        .option("inferschema",True)\
            .load("dbfs:/mnt/blobstorage1/employee_Data.csv")

employee_csv.display()

# COMMAND ----------

# reading department csv file from raw folder in Blob storage
department_csv = spark.read.format('csv')\
    .option("header",True)\
        .option("inferschema",True)\
            .load("dbfs:/mnt/blobstorage1/department_Data.csv")

department_csv.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Number of rows in every csv file

# COMMAND ----------

# Number of rows in salary_csv
sal_row_count = salary_csv.count()
print(f'The salary data has {sal_row_count} rows.')

# COMMAND ----------

# Number of rows in employee_csv
emp_row_count = employee_csv.count()
print(f'The employee data has {emp_row_count} rows.')

# COMMAND ----------

# Number of rows in department_csv
dept_row_count = department_csv.count()
print(f'The departmet data has {dept_row_count} rows.')

# COMMAND ----------

sal_csv_transform = salary_csv.withColumn("ingestion_date",current_date())\
    .withColumn("source_file",lit("salary.csv"))

sal_csv_transform.display()

# COMMAND ----------

emp_csv_transform = employee_csv.withColumn("ingestion_date",current_date())\
    .withColumn("source_file",lit("employee.csv"))

emp_csv_transform.display()

# COMMAND ----------

dept_csv_transform = department_csv.withColumn("ingestion_date",current_date())\
    .withColumn("source_file",lit("department.csv"))

dept_csv_transform.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #### mounting adls(bronze) to DBFS using account key

# COMMAND ----------

dbutils.fs.unmount('/mnt/bronze')

# COMMAND ----------


dbutils.fs.mount(source='wasbs://bronze@lake1gen.blob.core.windows.net',
                 mount_point='/mnt/bronze',
                 extra_configs={'fs.azure.account.key.lake1gen.blob.core.windows.net':'tW8Jk2tSYu1Q9+Y3u2z3FDTF3NIr5jHZf/y94ZCClToohZ1nlTSqZKDH6sYdON8faF1+NS/aNMk3+AStBfPL9g=='})

# COMMAND ----------

# converting csv file into parquet
sal_csv_transform.write.mode("overwrite").parquet(f'/mnt/bronze/salary_bronze')

# COMMAND ----------

emp_csv_transform.write.mode("overwrite").parquet(f'/mnt/bronze/employee_bronze')

# COMMAND ----------

dept_csv_transform.write.mode("overwrite").parquet(f'/mnt/bronze/department_bronze')

# COMMAND ----------

