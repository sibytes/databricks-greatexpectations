# Databricks notebook source
# %run ./ge_utils

# COMMAND ----------

# MAGIC %run ./workflow

# COMMAND ----------

db = "jaffle_shop"
task_root = "./"

# this can be list of files locations also, doesn't have to be tables
tables = spark.sql(f"SHOW TABLES IN {db}").select("database", "tableName").collect()

notebooks = [Notebook(f"{task_root}ge_profile", 2600, {"database": r["database"], "table": r["tableName"]}, 0, True) for r in tables]



# COMMAND ----------

# execute the notebooks in parallel
results = execute_notebooks(notebooks, 4, dbutils)

# COMMAND ----------

results
