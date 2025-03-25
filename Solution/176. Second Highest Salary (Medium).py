# Databricks notebook source
# MAGIC %md
# MAGIC ## Importing Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC **176. Second Highest Salary (Medium)**
# MAGIC
# MAGIC **Table: Employee**
# MAGIC
# MAGIC | Column Name | Type |
# MAGIC |-------------|------|
# MAGIC | id          | int  |
# MAGIC | salary      | int  |
# MAGIC
# MAGIC id is the primary key (column with unique values) for this table.
# MAGIC Each row of this table contains information about the salary of an employee.
# MAGIC  
# MAGIC **Write a solution to find the second highest distinct salary from the Employee table. If there is no second highest salary, return null (return None in Pandas).**
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC **Example 1:**
# MAGIC
# MAGIC **Input:**
# MAGIC **Employee table:**
# MAGIC | id | salary |
# MAGIC |----|--------|
# MAGIC | 1  | 100    |
# MAGIC | 2  | 200    |
# MAGIC | 3  | 300    |
# MAGIC
# MAGIC **Output:** 
# MAGIC | SecondHighestSalary |
# MAGIC |---------------------|
# MAGIC | 200                 |
# MAGIC
# MAGIC **Example 2:**
# MAGIC
# MAGIC **Input:**
# MAGIC **Employee table:**
# MAGIC | id | salary |
# MAGIC |----|--------|
# MAGIC | 1  | 100    |
# MAGIC
# MAGIC **Output:**
# MAGIC
# MAGIC | SecondHighestSalary |
# MAGIC |---------------------|
# MAGIC | null                |
# MAGIC

# COMMAND ----------

employee_176 = [(1, 100), (2, 200), (3, 300)]

employee_columns_176 = ["id", "salary"]
employee_df_176 = spark.createDataFrame(employee_176, employee_columns_176)
employee_df_176.show()
