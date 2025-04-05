# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC # Silver

# COMMAND ----------

# MAGIC %md
# MAGIC #### mounting adls(bronze) to DBFS using account key

# COMMAND ----------

dbutils.fs.unmount('/mnt/silver1')  

# COMMAND ----------

dbutils.fs.mount(source='wasbs://silver@lake1gen.blob.core.windows.net',
                 mount_point='/mnt/silver1',
                 extra_configs={'fs.azure.account.key.lake1gen.blob.core.windows.net':'tM1bfsezLJ9Q7k7RQQEgOtndvCv/lBSZiXJQlkss94+USxP6wikPb5zKZIg3EO+bmb42u/G4ALS0+AStfXH5kQ=='})

# COMMAND ----------

# MAGIC %md
# MAGIC reading bronze files in silver

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/bronze
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### working on salary.parquet

# COMMAND ----------

sal_par = spark.read.parquet('/mnt/bronze/salary_bronze')
sal_par.display()

# COMMAND ----------

# Calculating the number of null values in each row

for i in sal_par.columns:
    print(i, sal_par.filter(col(i).isNull()).count())

# COMMAND ----------

#dropping null values
sal_par_clean = sal_par.dropna()
sal_par_clean.display()

# COMMAND ----------

print(sal_par.count())
print(sal_par_clean.count()) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Working on employee.parquet

# COMMAND ----------

emp_par = spark.read.parquet('dbfs:/mnt/bronze/employee_bronze/')
display(emp_par)

# COMMAND ----------

# dropping null values
emp_par_clean = emp_par.dropna()
display(emp_par_clean)

# COMMAND ----------

print(emp_par.count())
print(emp_par_clean.count())

# COMMAND ----------

emp_par_clean.columns

# COMMAND ----------

# selecting required coulmns
emp_final = emp_par_clean.select("employee_id","employee_name","job_title","source_file")
emp_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Working on department.parquet

# COMMAND ----------

dept_par = spark.read.parquet('dbfs:/mnt/bronze/department_bronze/')
display(dept_par)   

# COMMAND ----------

dept_par.count()    

# COMMAND ----------

# dropping null values
dept_par_clean = dept_par.dropna()
display(dept_par_clean)

# COMMAND ----------

dept_par_clean.columns


# COMMAND ----------

# selecting required columns
dept_final = dept_par_clean.select("department_id",'department_name','source_file')
dept_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## merging all the tables

# COMMAND ----------

final_data = sal_par_clean.join(emp_final,on='employee_id',how = 'inner')\
    .join(dept_final,on = 'department_id',how = 'inner')\
        

final_data.display()

# COMMAND ----------

# adding year and month column

final_data = final_data.withColumn("year",year("salary_date"))\
    .withColumn("month",month("salary_date"))

final_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## computing additional fields

# COMMAND ----------

# monthly salary breakdown
monthly_salary = final_data.groupBy("employee_id", year("salary_date").alias("year"), month("salary_date").alias("month")) \
    .agg(sum("salary_amount").alias("total_monthly_salary"))

monthly_salary.display()

# COMMAND ----------

#yearly salary
yearly_salary = final_data.groupBy("employee_id",year("salary_date").alias("year")).\
    agg(sum("salary_amount").alias("yearly_salary"))

yearly_salary.display()

# COMMAND ----------

# salary categories

salary_classification = final_data.withColumn("salary_category",\
    when(final_data['salary_amount'] < 30000,'Low')\
        .when(final_data['salary_amount'].between(30000,60000),'Medium')\
            .otherwise('High')).select(['employee_id','department_id','salary_id','salary_category'])

salary_classification.display()

# COMMAND ----------

# joining the additional fields with the final table

final_data_demo = final_data.join(monthly_salary,on = ['employee_id','year','month'],how = 'left')\
    .join(yearly_salary,on = ['employee_id','year'],how = 'left')\
        .join(salary_classification,on = ['employee_id','department_id','salary_id'],how = 'left')

final_data_demo.display()

# COMMAND ----------

# adding processed date column (the date when data was transformed)
final_data_demo = final_data_demo.withColumn("processed_date",current_date())
final_data_demo.display()

# COMMAND ----------

final_data_demo.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### selecting required columns that are to be stored in silver

# COMMAND ----------

#creating salary_silver 
salary_silver = final_data_demo.select("salary_id","employee_name",'job_title','department_name','salary_amount','total_monthly_salary','yearly_salary','salary_category','salary_date','processed_date')
salary_silver.display()

# COMMAND ----------

employee_silver = emp_final.select("employee_name",'job_title')
employee_silver.display()

# COMMAND ----------

department_silver = dept_final.select("department_name")
department_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### storing tranformed data into silver container

# COMMAND ----------

salary_silver.write.mode("overwrite").parquet(f'/mnt/silver/salary_silver')

# COMMAND ----------

employee_silver.write.mode("overwrite").parquet(f'/mnt/silver/employee_silver')

# COMMAND ----------

department_silver.write.mode("overwrite").parquet(f'/mnt/silver/department_silver') 

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/silver

# COMMAND ----------

# MAGIC %md
# MAGIC #### reading parquet files from silver container

# COMMAND ----------

sal_silver_par = spark.read.parquet(f'/mnt/silver/salary_silver')
sal_silver_par.display()

# COMMAND ----------

sal_silver_par.columns

# COMMAND ----------

emp_silver_par = spark.read.parquet(f'/mnt/silver/employee_silver')
emp_silver_par.display()

# COMMAND ----------

dept_silver_par = spark.read.parquet(f'/mnt/silver/department_silver')
dept_silver_par.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storing data in SQL server

# COMMAND ----------


connection_properties = {
    "user": "swarali17",
    "password": "swaramun@17",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

salary_silver.write.jdbc(
    url="jdbc:sqlserver://swarali17.database.windows.net:1433;database=DB12345",
    table="silver_db.salary_silver",
    mode="overwrite", 
    properties=connection_properties,
)

# COMMAND ----------

employee_silver.write.jdbc(
    url='jdbc:sqlserver://swarali17.database.windows.net:1433;database=DB12345',
    table = "silver_db.employee_silver",
    mode = "overwrite",
    properties=connection_properties,
)

# COMMAND ----------

department_silver.write.jdbc(
    url='jdbc:sqlserver://swarali17.database.windows.net:1433;database=DB12345',
    table = "silver_db.department_silver",
    mode = "overwrite",
    properties=connection_properties,
)

# COMMAND ----------

