# Databricks notebook source
# MAGIC %run ./test_utils

# COMMAND ----------

import unittest

# COMMAND ----------

class Test1SampleTests1(unittest.TestCase):
  def tests_always_succeeds(self):
    self.assertTrue(True)
  
  def test_always_fails(self):
    self.assertTrue(False)

# COMMAND ----------

class Test1SampleTests2(unittest.TestCase):
  def tests_always_succeeds(self):
    self.assertTrue(True)
  
  def test_always_fails(self):
    self.assertTrue(False)

# COMMAND ----------

test_results = run_tests()
dbutils.notebook.exit(test_results)
