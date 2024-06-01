# Databricks notebook source
# MAGIC %pip install great-expectations==0.14.12

# COMMAND ----------

# dbutils.widgets.removeAll()
dbutils.widgets.text("database", "jaffle_shop", "database")
dbutils.widgets.text("table", "orders", "table")
dbutils.widgets.text("notebook", "./ge_profile", "notebook")

# COMMAND ----------

db = dbutils.widgets.get("database")
table = dbutils.widgets.get("table")
notebook = dbutils.widgets.get("notebook")

# COMMAND ----------

import great_expectations as ge
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import *
from great_expectations.render.view import DefaultJinjaPageView
import json

# COMMAND ----------

df = spark.sql(f"select * from {db}.{table}")

# COMMAND ----------

expectation_suite, validation_result = BasicDatasetProfiler.profile(SparkDFDataset(df))


# COMMAND ----------



# COMMAND ----------

validation_result

# COMMAND ----------

document_model = ProfilingResultsPageRenderer().render(validation_result)
displayHTML(DefaultJinjaPageView().render(document_model))

# COMMAND ----------

out = json.dumps({
  "status" : "succeeded",
  "notebook" : notebook,
  "profiled": f"{db}.{table}"})

dbutils.notebook.exit(out)
