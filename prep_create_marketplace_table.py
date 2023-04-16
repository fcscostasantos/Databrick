# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src ='https://i.imgur.com/HRhd2Y0.png'>
# MAGIC 
# MAGIC <h4>Qual foi o faturamento mês a mês?</h4>
# MAGIC <h4>Qual o mês com maior número de pedidos?</h4>
# MAGIC <h4>Qual o ticket médio dos pedidos?</h4>
# MAGIC <h4>Qual o dia da semana com maior número de pedidos?</h4>
# MAGIC <h4>Quais são os Top 10 produtos mais bem avaliados?</h4>
# MAGIC <h4>Qual o faturamento mensal por método de pagamento?</h4>
# MAGIC <h4>Qual a categoria mais vendida na empresa?</h4>
# MAGIC <h4>Qual vendedor teve a melhor performance mês a mês?</h4>

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# DBTITLE 1,Read Tables
# Read Orders Table
df_orders = spark.table(
  'mkt_olist.mkt_olist_orders'
)

# Read Payment Table
df_payment = spark.table(
  'mkt_olist.mkt_olist_pagamentos'
)

# COMMAND ----------

# DBTITLE 1,Qual foi o faturamento mês a mês?
df_orders_vs_payment = df_orders.join(
  df_payment, ['order_id'], 'left'
).filter(
  col('order_status')=='invoiced'
).withColumn(
  'Mes', lpad(month(to_date(col('order_approved_at'))),2,'0')
).groupBy(
  'Mes'
).agg(
  sum('payment_value').cast('decimal(32,2)').alias('Faturamento')
).orderBy(
  col('Mes').desc()
)

df_orders_vs_payment.display()

# COMMAND ----------

df_orders.join(
  df_payment, ['order_id'], 'left'
).filter(
  col('order_status')=='invoiced'
).withColumn(
  'Mes', lpad(month(to_date(col('order_approved_at'))),2,'0')
).display()
