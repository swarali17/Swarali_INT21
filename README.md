📘 Salary Data Processing and Analysis — Project Documentation

📌 1. Project Overview
A comprehensive data engineering and BI project to process, transform, and visualize salary-related data. Leveraging the Medallion Architecture, this pipeline uses PySpark, Azure Data Factory, Azure Databricks, SQL Server, and Power BI for an end-to-end data solution.

🛠️ 2. Tools & Technologies Used


Tool	Purpose

Azure Databricks	Data transformation (PySpark scripts)

SQL Server	Final data storage (Silver & Gold layers)

Azure Data Factory	Orchestration of the data pipeline

Power BI	Interactive dashboard & visualizations



🧱 3. Medallion Architecture Implementation

🟫 Bronze Layer (Raw Data)
Source: CSV files (uploaded via Databricks)

Files: salary_Data.csv, employee_Data.csv, department_Data.csv

Stored As: Parquet format

Includes: Raw data with ingestion_date and source_file for audit tracking

⚪ Silver Layer (Cleaned & Enriched)

Performed via PySpark scripts in Azure Databricks, results pushed to SQL Server:

Joins with employee and department data

Null handling for critical fields

Additional fields computed like total_salary_value

Stored as Parquet + written to silver_db in SQL Server



SQL Scripts Used:

salary_silver.sql

employee_silver.sql

department_silver.sql

🟨 Gold Layer (Aggregated for Reporting)
Aggregation operations done in SQL Server:

SUM and AVG salary metrics

Department-wise and employee-wise aggregations

Time-based salary trends

Final table: gold_db.salary_gold



🔄 4. Data Pipeline (Automation)
Orchestrated via Azure Data Factory:

Pipeline: SOWS_pipeline

Ingests CSV → Bronze → Silver (Databricks) → Gold (SQL Server)

Automates transformations and loads


ADF Pipeline Link: 🔗 https://adf.azure.com/en/authoring/pipeline/SOWS_pipeline?factory=%2Fsubscriptions%2F90ef827e-b903-43e8-89b3-410fa7f14557%2FresourceGroups%2Fbiztrain25%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2FswaraDF1

📊 5. Power BI Dashboard
Developed to provide insightful, dynamic visuals on top of the Gold Layer:

Bar Charts:

Top 10 Highest Paid Employees

Department-wise Total Salary

Line Charts:

Monthly Salary Trends

Pie Charts:

Employee Count by Department

File: SOWS_Salary_Data.pbix

🧾 6. Key Metrics & Business Insights
Total Salary Paid per department

Average Salary by department/job title

Top Earners across the organization

Trends over time (salary distribution and totals)

🧑‍💻 7. GIT Repository
All code and scripts (SQL + PySpark + ADF JSON definitions) are stored in a GitHub repository.
https://github.com/swarali17/Swarali_INT21


📑 8. Best Practices Followed

✔ Medallion Architecture compliance

✔ Modular PySpark scripts

✔ SQL naming conventions (snake_case)

✔ Fully automated pipeline via ADF

✔ Documentation and version control with Git
