# Databricks notebook source
dfResults = (spark
             .read
             .table("uc_discovery.metastore")
            )

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# managed table allocation across metastore

(
  dfResults
  .groupBy("tableType")
  .count()
  .display()
)

# COMMAND ----------

(
  dfResults
  .withColumn("database", split(col("tableName"), "\.")[0])
  .groupBy(["database", "tableType"])
  .count()
  .display()
)

# COMMAND ----------

(
  dfResults
  .withColumn("dbfs", when(col("tableLocation").contains("dbfs:"), True).otherwise(False))
  .groupBy("dbfs")
  .count()
  .display()
)

# COMMAND ----------

(
  dfResults
  .withColumn("database", split(col("tableName"), "\.")[0])
  .withColumn("dbfs", when(col("tableLocation").contains("dbfs:"), True).otherwise(False))
  .groupBy(["database", "dbfs"])
  .count()
  .display()
)

# COMMAND ----------

(
  dfResults
  .groupBy("tableProvider")
  .count()
  .display()
)

# COMMAND ----------

(
  dfResults
  .withColumn("database", split(col("tableName"), "\.")[0])
  .groupBy(["database", "tableProvider"])
  .count()
  .selectExpr("count AS value", "database", "tableProvider")
  .display()
)

# COMMAND ----------

(
  dfResults
  .groupBy("tableSize")
  .count()
  .display()
)

# COMMAND ----------

(
  dfResults
  .withColumn("database", split(col("tableName"), "\.")[0])
  .groupBy("database")
  .sum()
  .selectExpr("`sum(tableSize)`/1000000000000 AS value", "database")
  .display()
)

# COMMAND ----------

(
  dfResults
  .withColumn("database", split(col("tableName"), "\.")[0])
  .groupBy(["database", "tableVersion"])
  .count()
  .selectExpr("count AS value", "database", "tableVersion")
  .display()
)

# COMMAND ----------


