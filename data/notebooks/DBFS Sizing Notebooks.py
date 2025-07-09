# Databricks notebook source
def recursiveDirSize(path):
  total = 0
  dir_files = dbutils.fs.ls(path)
  for file in dir_files:
    if file.isDir():
      total += recursiveDirSize(file.path)
    else:
      total += file.size
  return total


# COMMAND ----------

dbfs_paths = dbutils.fs.ls("dbfs:/")

paths = []
sizes = []

skip_paths = ["dbfs:/mnt/", "dbfs:/databricks/", "dbfs:/databricks-datasets/","dbfs:/databricks-results/"]

for p in dbfs_paths: 
  try: 
    print("Working on", p.path)
    if p.path in skip_paths:
      continue
    p_size = recursiveDirSize(p.path)
    paths.append(p.path)
    sizes.append(p_size)
    print("Completed", p.path)
  except:
    print(f"Could not find size for path {p}")

# COMMAND ----------

spark.createDataFrame([(i, j/1e6) for i, j in zip(paths, sizes)], schema = ["Path", "Size in MB"]).display()

# COMMAND ----------

spark.createDataFrame([(i, j/1e9) for i, j in zip(paths, sizes)], schema = ["Path", "Size in GB"]).display()

# COMMAND ----------

dbutils.fs.ls("dbfs:/")
