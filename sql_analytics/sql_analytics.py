# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # SQL Analytical Queries
# MAGIC
# MAGIC 4. SQL Analytical Queries
# MAGIC Write SQL queries (any DB engine allowed):
# MAGIC
# MAGIC Query ->	Expected Output
# MAGIC
# MAGIC Top 10 customers by revenue	-> user_id, total_spent
# MAGIC
# MAGIC Monthly revenue trend for last 12 months	-> month, total_revenue
# MAGIC
# MAGIC Cities with highest failed orders	-> city, failed_count
# MAGIC
# MAGIC Retention metric – users with repeat purchases	-> user_id, order_count
# MAGIC

# COMMAND ----------

# DBTITLE 1,Setup: SQL Views
# Read the fact_orders and dim_users
fact_orders = spark.read.format("parquet").load("/Volumes/assignment_1/silver/warehouse/fact_orders")
dim_users = spark.read.format("parquet").load("/Volumes/assignment_1/silver/warehouse/dim_users").dropDuplicates(["user_id"])

fact_orders.createOrReplaceTempView("fact_orders")
dim_users.createOrReplaceTempView("dim_users")

print("fact_orders rows:", fact_orders.count())
print("dim_users rows :", dim_users.count())

# COMMAND ----------

# DBTITLE 1,Query 1
# MAGIC %md
# MAGIC ### Query 1 -- Top 10 customers by revenue
# MAGIC
# MAGIC Sum the price per user, biggest first, keep the top 10. Saved as the temp view `v_top_customers_by_revenue`.

# COMMAND ----------

# DBTITLE 1,Q1: Top Customers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_top_customers_by_revenue AS
# MAGIC SELECT user_id, SUM(price) AS total_spent
# MAGIC FROM fact_orders
# MAGIC GROUP BY user_id
# MAGIC ORDER BY total_spent DESC;
# MAGIC
# MAGIC SELECT * FROM v_top_customers_by_revenue;

# COMMAND ----------

# DBTITLE 1,Query 2
# MAGIC %md
# MAGIC ### Query 2 -- Monthly revenue trend (last 12 months)
# MAGIC
# MAGIC Group the revenue by `order_month` and keep the 12 most recent months. The 7 orders with an unparsable date (`Not Available`) are excluded. Saved as the temp view `v_monthly_revenue_trend`.

# COMMAND ----------

# DBTITLE 1,Q2: Monthly Revenue
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_monthly_revenue_trend AS
# MAGIC SELECT order_month AS month, SUM(price) AS total_revenue
# MAGIC FROM fact_orders
# MAGIC WHERE order_month <> 'Not Available'
# MAGIC GROUP BY order_month
# MAGIC ORDER BY month DESC;
# MAGIC
# MAGIC SELECT * FROM v_monthly_revenue_trend;

# COMMAND ----------

# DBTITLE 1,Query 3
# MAGIC %md
# MAGIC ### Query 3 -- Cities with the highest failed orders
# MAGIC
# MAGIC A failed order is `Cancelled` or `Returned`. The fact table is joined to `dim_users` to get the city. Saved as the temp view `v_failed_orders_by_city`.

# COMMAND ----------

# DBTITLE 1,Q3: Failed Orders by City
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_failed_orders_by_city AS
# MAGIC SELECT u.city, COUNT(*) AS failed_count
# MAGIC FROM fact_orders f
# MAGIC JOIN dim_users u ON f.user_id = u.user_id
# MAGIC WHERE f.status IN ('Cancelled', 'Returned')
# MAGIC GROUP BY u.city
# MAGIC ORDER BY failed_count DESC;
# MAGIC
# MAGIC SELECT * FROM v_failed_orders_by_city;

# COMMAND ----------

# DBTITLE 1,Query 4
# MAGIC %md
# MAGIC ### Query 4 -- Retention: users with repeat purchases
# MAGIC
# MAGIC A retained user bought more than once (`HAVING COUNT(*) > 1`). In this dataset only user 14 has 2 orders, so the result is one row. Saved as the temp view `v_repeat_purchase_users`.

# COMMAND ----------

# DBTITLE 1,Q4: Repeat Purchases
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_repeat_purchase_users AS
# MAGIC SELECT user_id, COUNT(*) AS order_count
# MAGIC FROM fact_orders
# MAGIC GROUP BY user_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY order_count DESC;
# MAGIC
# MAGIC SELECT * FROM v_repeat_purchase_users;