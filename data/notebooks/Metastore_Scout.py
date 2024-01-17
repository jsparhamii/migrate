# Databricks notebook source
!pip install tqdm

# COMMAND ----------

from pyspark.sql.functions import *
from tqdm import tqdm

# COMMAND ----------

dbutils.widgets.text("database_list", "all")
database_list = dbutils.widgets.get("database_list").split(",")

dbutils.widgets.text("get_ddl", "false")
getDDL = dbutils.widgets.get("get_ddl") == "true"

dbutils.widgets.text("calculate_size", "false")
calculateSize = dbutils.widgets.get("calculate_size") == "true"

# COMMAND ----------

def getAllDatabases():
  databaseList = spark.sql(f"""SHOW DATABASES""").select("databaseName").rdd.flatMap(lambda x:x).collect()
  return databaseList

def getAllTables(database):
  tableList = spark.sql(f"""SHOW TABLES IN {database}""").select("tableName").rdd.flatMap(lambda x:x).collect()
  databaseAndTableList = [f"{database}.{t}" for t in tableList]
  return databaseAndTableList

def getTableDetail(table, detail):
  try:
    tableDetail = spark.sql(f"""DESC EXTENDED {table}""").filter(f"col_name == '{detail}'").select("data_type").rdd.flatMap(lambda x:x).collect()[0]
  except Exception as e:
    tableDetail = "N/A"
  return tableDetail

def getTableSize(table, calculateSize):
  if calculateSize:
    spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS NOSCAN")
    try:
      tableSize = (spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]['sizeInBytes'])
      if (tableSize == None):
        tableSize = int(spark.sql(f"""DESC EXTENDED {table}""").filter(f"col_name == 'Statistics'").select("data_type").rdd.flatMap(lambda x:x).collect()[0].split(' ')[0])
    except Exception as e:
      tableSize = -1
  else:
      tableSize = -1
  return tableSize

def getTableDDL(table, getDDL):
  if getDDL:
    tableDDL = spark.sql(f"""SHOW CREATE TABLE {table}""").collect()[0][0]
  else:
    tableDDL = "N/A"
  return tableDDL

# COMMAND ----------

def main_scout(database_list):
  
  if database_list == ['all']:
    database_list = getAllDatabases()
  
  print(f"Analyzing {len(database_list)} databases.")
  fullTableList = []
    
  for database in database_list:
    tableList = getAllTables(database)
    print(f"{database}: {len(tableList)}")
    fullTableList.extend(tableList)
  
  print(f"Found {len(fullTableList)} in {len(database_list)} databases.")
  
  fullTableDetails = []
  failedTables = []
  
  for table in tqdm(fullTableList):
    try:
      tableType = getTableDetail(table, "Type")
      tableLocation = getTableDetail(table, "Location")
      tableProvider = getTableDetail(table, "Provider")
      tableVersion = getTableDetail(table, "Created By")
      tableSize = getTableSize(table, calculateSize)
      tableDDL = getTableDDL(table, getDDL)
      fullTableDetails.append((table, tableType, tableLocation, tableProvider, tableVersion, tableSize, tableDDL))
    except Exception as e:
      failedTables.append((table, str(e)))
      continue
    
  columns = ["tableName", "tableType", "tableLocation", "tableProvider", "tableVersion", "tableSize", "tableDDL"]
  spark.createDataFrame(data=fullTableDetails, schema = columns).write.mode("overwrite").saveAsTable("e2_migration_testing_to_delete.metastore_scan")
  
  failedTableSchema = StructType([
    StructField("table", StringType(),True),
    StructField("error", StringType(),True)
  ])

  spark.createDataFrame(data = failedTables, schema = failedTableSchema).write.mode("overwrite").saveAsTable("e2_migration_testing_to_delete.metastore_scan_errors")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS e2_migration_testing_to_delete

# COMMAND ----------

main_scout(database_list)

# COMMAND ----------


