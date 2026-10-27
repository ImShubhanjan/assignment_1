# Databricks notebook source
# DBTITLE 1,Pipeline Overview
# MAGIC %md
# MAGIC ##2. Data Cleaning & Transformations
# MAGIC Perform the following using PySpark OR Pandas 
# MAGIC Cleaning Task	-> Description
# MAGIC
# MAGIC Null handling	-> Replace NULL values appropriately
# MAGIC
# MAGIC Date parsing	-> Convert signup_date and order_date to datetime
# MAGIC
# MAGIC Standardization	-> Trim strings, standardize email to lowercase
# MAGIC
# MAGIC Filters	-> Remove orders marked with invalid price or null users
# MAGIC
# MAGIC Additional Transformation Requirements
# MAGIC •	Add a column order_month to aggregate monthly revenue.
# MAGIC •	Compute Lifetime Value (LTV) per customer.
# MAGIC •	Derive field account_age_days.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##3. Data Modelling (Star Schema)
# MAGIC
# MAGIC Design a warehouse model with:
# MAGIC Fact Table:
# MAGIC
# MAGIC fact_orders(order_id, user_id, price, status, order_date, order_month)
# MAGIC Dimension Tables:
# MAGIC
# MAGIC dim_users(user_id, name, email, city, signup_date)
# MAGIC dim_date(date_key, date, month, year, day_of_week)
# MAGIC
# MAGIC Use Parquet format + partitioning by order_month.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Setup & Config
import logging
from pyspark.sql import functions as F

logger = logging.getLogger("clean_and_transform")
logger.setLevel(logging.INFO)
for _h in logger.handlers[:]:
    logger.removeHandler(_h)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(_handler)

ORDERS_BRONZE = "assignment_1.bronze.orders"
USERS_BRONZE = "assignment_1.bronze.users"

df_orders = spark.table(ORDERS_BRONZE)
df_users = spark.table(USERS_BRONZE)

logger.info("bronze orders: %d rows", df_orders.count())
logger.info("bronze users : %d rows", df_users.count())

# COMMAND ----------

# DBTITLE 1,Null Handling Code
# A missing name is taken from the email: abc.xyz89@example.com -> "abc xyz"
name_from_email = F.regexp_replace(
    F.regexp_replace(F.split(F.col("email"), "@").getItem(0), "[._+-]", " "),
    "[0-9]",
    "",
)
no_name = F.trim(F.col("name")).isNull() | (F.trim(F.col("name")) == "")
df_users = df_users.withColumn(
    "name",
    F.when(no_name & F.col("email").contains("@"), name_from_email).otherwise(F.col("name")),
)

# Fill the remaining nulls with "Not Available"
df_orders = df_orders.fillna(
    {"product": "Not Available", "status": "Not Available"}
)
df_users = df_users.fillna(
    {"city": "Not Available", "email": "Not Available"}
)

logger.info("null handling done: nulls filled, missing names taken from email")

# display(df_orders)
display(df_users)

# COMMAND ----------

# DBTITLE 1,Date Parsing Code
#date parsing -- coalesce over the formats found in the data
DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MM/dd", "dd-MM-yyyy"]

#Method to parse dates if their are dates columns
def parse_date(col_name):
    """Return the first format that parses; NULL when none do."""
    return F.coalesce(*[F.expr(f"try_to_date({col_name}, '{fmt}')") for fmt in DATE_FORMATS])

df_orders = df_orders.withColumn("order_date", parse_date("order_date"))
df_users = df_users.withColumn("signup_date", parse_date("signup_date"))

logger.info("unparseable order_dates : %d", df_orders.filter(F.col("order_date").isNull()).count())
logger.info("unparseable signup_dates: %d", df_users.filter(F.col("signup_date").isNull()).count())

# COMMAND ----------

# DBTITLE 1,Standardization Code
#Standardization -- trim strings, lowercase emails, fix join key types
df_orders = df_orders.withColumns(
    {
        "product": F.trim(F.col("product")),
        "status": F.trim(F.col("status")),
        "price": F.trim(F.col("price")),
        "user_id": F.col("user_id").cast("int"),
    }
)
df_users = df_users.withColumns(
    {
        "name": F.initcap(F.trim(F.col("name"))),
        "city": F.trim(F.col("city")),
        "email": F.lower(F.trim(F.col("email"))),
        "user_id": F.col("user_id").cast("int"),
    }
)

logger.info("standardization done: strings trimmed, emails lowercased, ids cast to int")

# COMMAND ----------

# Remove currency symbols and invalid characters, then convert price to DECIMAL(10,2)
df_orders = df_orders.withColumn(
    "price",
    F.expr("try_cast(regexp_replace(price, '[^0-9.-]', '') AS DECIMAL(10,2))"),
)

# COMMAND ----------

# DBTITLE 1,Good vs Bad Split Code

# Identify orders with missing user_id, invalid price, or non-positive price
is_bad_order = F.col("user_id").isNull() | F.col("price").isNull() | (F.col("price") <= 0)

# Filter invalid orders and add a reason explaining why each record is rejected
df_bad_records_orders = df_orders.filter(is_bad_order).withColumn(
    "bad_record_reason",
    F.when(F.col("user_id").isNull(), "null user_id")
    .when(F.col("price").isNull(), "invalid price")
    .otherwise("non-positive price"),
)

display(df_bad_records_orders)
df_orders = df_orders.filter(~is_bad_order)

# Identify users with a missing user_id or a blank/missing name
is_bad_user = (
    F.col("user_id").isNull()
    | F.trim(F.col("name")).isNull()
    | (F.trim(F.col("name")) == "")
)

# Filter invalid users and add a reason explaining why each record is rejected
df_bad_records_users = df_users.filter(is_bad_user).withColumn(
    "bad_record_reason",
    F.when(F.col("user_id").isNull(), "null user_id")
    .otherwise("no name and no email to fill it from"),
)

df_users = df_users.filter(~is_bad_user)

logger.info("orders: %d bad, %d clean", df_bad_records_orders.count(), df_orders.count())
logger.info("users : %d bad, %d clean", df_bad_records_users.count(), df_users.count())

# COMMAND ----------

# DBTITLE 1,order_month Code
#order_month column + monthly revenue
df_orders = df_orders.withColumn("order_month", F.date_trunc("month", F.col("order_date")))

# dates that could not be parsed show as "Not Available" instead of NULL
df_orders = df_orders.withColumn("order_date", F.coalesce(F.col("order_date").cast("string"), F.lit("Not Available")))
df_orders = df_orders.withColumn("order_month", F.coalesce(F.col("order_month").cast("string"), F.lit("Not Available")))

monthly_revenue = (
    df_orders.groupBy("order_month")
    .agg(F.sum("price").alias("revenue"), F.count("order_id").alias("orders"))
    .orderBy("order_month")
)
display(monthly_revenue)

# COMMAND ----------

# DBTITLE 1,LTV & Account Age Code
#account_age_days + LTV per customer
df_users = df_users.withColumn(
    "account_age_days", F.datediff(F.current_date(), F.col("signup_date"))
)

# dates / ages that could not be parsed show as "Not Available" instead of NULL
df_users = df_users.withColumn("signup_date", F.coalesce(F.col("signup_date").cast("string"), F.lit("Not Available")))
df_users = df_users.withColumn("account_age_days", F.coalesce(F.col("account_age_days").cast("string"), F.lit("Not Available")))

# LTV per customer (first aggregate then join)
ltv = (
    df_orders.groupBy("user_id")
    .agg(
        F.sum("price").alias("lifetime_value"),
        F.count("order_id").alias("total_orders"),
    )
)
df_customer_ltv = (
    df_users.join(ltv, "user_id", "left")
    .withColumn("lifetime_value", F.coalesce(F.col("lifetime_value"), F.lit(0)))
    .withColumn("total_orders", F.coalesce(F.col("total_orders"), F.lit(0)))
    .select(
        "user_id", "name", "email", "city",
        "signup_date", "account_age_days", "total_orders", "lifetime_value",
    )
)


# COMMAND ----------

display(df_customer_ltv.orderBy(F.col("lifetime_value").desc()).limit(100))

# COMMAND ----------

display(df_orders)
display(df_users)

# COMMAND ----------

#Capitalization first letter for better clarity for all the string related columns
df_users = df_users.withColumn("city", F.initcap(F.trim(F.col("city"))))\
        .withColumn("name", F.initcap(F.trim(F.col("name"))))\
        .withColumn("email", F.when(
            F.col("email").contains("@"),
            F.lower(F.trim(F.col("email")))
        ).otherwise(F.lit("Not Available")))

# COMMAND ----------

#Capitalization first letter for better clarity for all the string related columns
df_orders = df_orders.withColumn("product", F.initcap(F.trim(F.col("product"))))\
         .withColumn("status", F.initcap(F.trim(F.col("status"))))

# COMMAND ----------

# DBTITLE 1,Star Schema Code
#Star schema -- 1 fact table + 2 dimension tables, saved as tables in silver
SILVER = "assignment_1.silver"

#fact_orders -- table, partitioned by order_month ("yyyy-MM" = one partition per month)
fact_orders = df_orders.select(
    "order_id", "user_id", "price", "status", "order_date"
).withColumn(
    "order_month", F.coalesce(F.date_format(F.expr("try_cast(order_date AS DATE)"), "yyyy-MM"), F.lit("Not Available"))
)
fact_orders.write.mode("overwrite").partitionBy("order_month").saveAsTable(f"{SILVER}.fact_orders")

# 2. dim_users
dim_users = df_users.select("user_id", "name", "email", "city", "signup_date")
dim_users.write.mode("overwrite").saveAsTable(f"{SILVER}.dim_users")

# dim_date -- one row per calendar date between the first and last order date
dim_date = (
    df_orders.select(
        F.min(F.expr("try_cast(order_date AS DATE)")).alias("mn"),
        F.max(F.expr("try_cast(order_date AS DATE)")).alias("mx"),
    )
    .select(F.explode(F.expr("sequence(mn, mx, interval 1 day)")).alias("date"))
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.date_format("date", "MMMM").alias("month"),
        F.year("date").alias("year"),
        F.date_format("date", "EEEE").alias("day_of_week"),
    )
)
dim_date.write.mode("overwrite").saveAsTable(f"{SILVER}.dim_date")


# COMMAND ----------

for name in ["fact_orders", "dim_users", "dim_date"]:
    logger.info("%s: %d rows", name, spark.table(f"{SILVER}.{name}").count())

display(spark.table(f"{SILVER}.fact_orders").limit(10))