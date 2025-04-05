create schema silver_db;

create table silver_db.salary_silver (
salary_id int unique,
employee_name varchar(50),
job_title varchar(50),
department_name varchar(50),
salary_amount decimal(10,2),
total_monthly_salary decimal(10,2),
yearly_salary decimal(10,2),
salary_category decimal(10,2),
salary_date date,
processed_date date
);

select * from silver_db.salary_silver