# Databricks notebook source
# MAGIC %run ./test_utils

# COMMAND ----------

# MAGIC %run ./workflow

# COMMAND ----------


# build a list of notebooks to run
task_root = "./"
notebooks = [
  Notebook(f"{task_root}test_1", 3600, {}, 0, True),
  Notebook(f"{task_root}test_2", 3600, {}, 0, True)
]

# execute the notebooks in parallel
results = execute_notebooks(notebooks, 4, dbutils)

# print(results)

# COMMAND ----------


test_results = testsuites_union(results)


# COMMAND ----------

df = get_test_results(test_results)
display(df)

# COMMAND ----------

display_pie(df)

# COMMAND ----------

display_bar(df)


# COMMAND ----------


# only do this to trap and inpspect here. If you want to pass backinto a devops pipeline
# to handle then remove this line so that the results pass back below and can be handled
# by the called.
# raise_error_onfails(df)


# COMMAND ----------


dbutils.notebook.exit(ET.tostring(test_results, encoding='unicode', xml_declaration = True))
