# Databricks notebook source
# DBTITLE 1,Importação das bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# DBTITLE 1,Cria Database
# MAGIC %sql
# MAGIC create database if not exists mkt_olist;

# COMMAND ----------

# DBTITLE 1,Importa tabela de pedidos
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_orders_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_orders'
)

#df_orders.display()

# COMMAND ----------

# DBTITLE 1,Importa tabela de items
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_order_items_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_items'
)

# COMMAND ----------

# DBTITLE 1,Importa tabela de pagamentos
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_order_payments_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_pagamentos'
)

# COMMAND ----------

# DBTITLE 1,Importa tabela de produtos
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_products_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_produtos'
)

# COMMAND ----------

# DBTITLE 1,Importa tabela de clientes
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_customers_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_clientes'
)

# COMMAND ----------

# DBTITLE 1,Importa tabela com revisão de pedidos
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_order_reviews_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_revisao_pedido'
)

# COMMAND ----------

# DBTITLE 1,Importa tabela de vendedores
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_sellers_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_vendedores'
)

# COMMAND ----------

# DBTITLE 1,Importa tabela de categorias
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/product_category_name_translation.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_categorias'
)

# COMMAND ----------

# DBTITLE 1,Importa base com a geolocalização
spark.read.format(
  'csv'
).options(
  header='true', inferschema='true'
).load(
  '/FileStore/tables/olist_geolocation_dataset.csv'
).write.mode(
  'overwrite'
).saveAsTable(
  'mkt_olist.mkt_olist_geolocalizacao'
)

# COMMAND ----------


