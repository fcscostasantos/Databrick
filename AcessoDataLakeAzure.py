# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

dbutils.fs.ls("/mnt")

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/dadosmktplace")

# COMMAND ----------

dbutils.fs.unmount("/mnt/dadosmktplace") 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "177ed4c5-d855-4297-bc48-c80f17ad7eb6",
       "fs.azure.account.oauth2.client.secret": "8Yd7Q~dl37J1bXudJnfIcp0Azlv~jO-vcuvNI",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/e1444647-46a5-426a-b2bc-fa8cf278117c/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://ctmktplace@stmktplace.dfs.core.windows.net/",
mount_point = "/mnt/dadosmktplace",
extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/dadosmktplace")

# COMMAND ----------

cl = spark.read.format('csv').options(header='true', inferschema='true').load('/mnt/dadosmktplace/TABELA_CLIENTES.csv')
cl.display()

# COMMAND ----------

cl.groupBy("order_status").agg(count("*").alias("qtde")).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists gold_mktplace
# MAGIC location '/mnt/dadosmktplace/gold_mktplace'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create table if not exists gold_mktplace.tb_clientes_gold(
# MAGIC   customer_id string 
# MAGIC   ,customer_unique_id string
# MAGIC   ,customer_zip_code_prefix integer
# MAGIC   ,customer_city string
# MAGIC   ,customer_state string 
# MAGIC )using delta
# MAGIC location '/mnt/dadosmktplace/gold_mktplace/tb_clientes_gold'

# COMMAND ----------

cl.write.mode('overwrite').saveAsTable("gold_mktplace.tb_clientes_gold")

# COMMAND ----------

# DBTITLE 1,Camada conexão
#Azure Storage Account for Polybase

storage_account_name = "stmktplace"
storage_account_key = "BPxNWw2GP8qsfwU6nccXxQSMOKdQ2YSUq6kmqJDguxpD09LXzVV/Y8s6ypZ2FWmtEBY4k7ATMw3oAS3eeBvt8A=="
storage_container_name = "ctsnpmktplace"

temp_dir_url = "wasbs://{}@{}.blob.core.windows.net/".format(storage_container_name, storage_account_name)

spark_config_key = "fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name)
spark_config_value = storage_account_key

spark.conf.set(spark_config_key, spark_config_value)

#Azure Synapse parameters

servername = "snpmktplace"
databasename = "snpdedicated"
username = "sqladminuser"
password = "S@np735#gdb!"

sql_dw_connection_string = "jdbc:sqlserver://{}.database.windows.net:1433;database={};user={}@{};password={};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(servername, databasename, username, servername, password)




# COMMAND ----------

# DBTITLE 1,Escreve no Synapse
#Writing Dataframe to Table on Azure Synapse

new_table_name = "bi.tb_clientes_gold"

cl.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_connection_string) \
  .option("forward_spark_azure_storage_credentials", "true") \
  .option("dbtable", new_table_name) \
  .option("tempdir", temp_dir_url) \
  .save()
