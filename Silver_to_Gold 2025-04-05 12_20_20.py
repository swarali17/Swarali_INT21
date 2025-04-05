# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold

# COMMAND ----------

# MAGIC %md
# MAGIC #### mounting adls(gold) to DBFS using access key

# COMMAND ----------

dbutils.fs.unmount('/mnt/gold1')

# COMMAND ----------

dbutils.fs.mount(source='wasbs://gold@lake1gen.blob.core.windows.net',
                 mount_point='/mnt/gold1',
                 extra_configs={'fs.azure.account.key.lake1gen.blob.core.windows.net':'tM1bfsezLJ9Q7k7RQQEgOtndvCv/lBSZiXJQlkss94+USxP6wikPb5zKZIg3EO+bmb42u/G4ALS0+AStfXH5kQ=='})

# COMMAND ----------


connection_properties = {
    "user": "swarali17",
    "password": "swaramun@17",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

salary_gold = spark.read.jdbc(
    url = "jdbc:sqlserver://swarali17.database.windows.net:1433;database=DB12345",
    table = "gold_db.salary_gold",
    properties=connection_properties,
)

# COMMAND ----------

salary_gold.write.mode("overwrite").parquet("/mnt/gold/salary_gold")

# COMMAND ----------

salary_gold = spark.read.parquet("/mnt/gold/salary_gold")
salary_gold.display()

# COMMAND ----------

