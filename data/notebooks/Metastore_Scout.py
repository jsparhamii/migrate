# Databricks notebook source
!pip install tqdm

# COMMAND ----------

from pyspark.sql.functions import *
from tqdm import tqdm

# COMMAND ----------

dbutils.widgets.text("database_list", "")
database_list = dbutils.widgets.get("database_list").split(",")

# COMMAND ----------

def getAllDatabases():
  databaseList = spark.sql(f"SHOW DATABASES").select("databaseName").rdd.flatMap(lambda x:x).collect()
  return databaseList

def getAllTables(database):
  tableList = spark.sql(f"SHOW TABLES IN {database}").select("tableName").rdd.flatMap(lambda x:x).collect()
  databaseAndTableList = [f"{database}.{t}" for t in tableList]
  return databaseAndTableList

def getTableDetail(table, detail):
  try:
    tableDetail = spark.sql(f"""DESC EXTENDED {table}""").filter(f"col_name == '{detail}'").select("data_type").rdd.flatMap(lambda x:x).collect()[0]
  except Exception as e:
    tableDetail = "N/A"
  return tableDetail

def getTableSize(table):
  spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS NOSCAN")
  try:
    tableSize = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]['sizeInBytes']
  except Exception as e:
    tableSize = -1
  return tableSize

def getTableDDL(table):
  tableDDL = spark.sql(f"""SHOW CREATE TABLE {table}""").collect()[0][0]
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
  for table in tqdm(fullTableList):
    try:
      tableType = getTableDetail(table, "Type")
      tableLocation = getTableDetail(table, "Location")
      tableProvider = getTableDetail(table, "Provider")
      tableVersion = getTableDetail(table, "Created By")
      tableSize = getTableSize(table)
      tableDDL = getTableDDL(table)
      fullTableDetails.append((table, tableType, tableLocation, tableProvider, tableVersion, tableSize, tableDDL))
    except Exception as e:
      print(str(e))
      continue
    
  columns = ["tableName", "tableType", "tableLocation", "tableProvider", "tableVersion", "tableSize", "tableDDL"]
  spark.createDataFrame(data=fullTableDetails, schema = columns).write.mode("overwrite").saveAsTable("uc_discovery.metastore")

# COMMAND ----------

main_scout(database_list)

# COMMAND ----------

spark.read.table("uc_discovery.metastore").display()

# COMMAND ----------


