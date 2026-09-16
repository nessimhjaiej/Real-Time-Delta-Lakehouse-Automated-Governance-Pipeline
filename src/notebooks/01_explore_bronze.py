# %% Step 1: Fix Python Path & Initialize Spark
import sys
from pathlib import Path

# Locate the repository root regardless of whether Jupyter was started from the
# project root, ``src``, or this notebook directory.
project_root = next(
    (
        directory
        for directory in (Path.cwd().resolve(), *Path.cwd().resolve().parents)
        if (directory / "config" / "spark_config.py").is_file()
    ),
    None,
)
if project_root is None:
    raise RuntimeError(
        "Could not find the project root. Start Jupyter from the "
        "realtime-governance-engine directory."
    )

# Put the local project before installed packages with similarly named modules.
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.spark_config import get_spark_session

# Initialize Spark session
spark = get_spark_session("InteractiveSQLQuerying")

# Register Bronze Delta tables as SQL views
spark.read.format("delta").load(
    "s3a://lakehouse/bronze/orders_batch"
).createOrReplaceTempView("bronze_orders_batch")
spark.read.format("delta").load(
    "s3a://lakehouse/bronze/orders_stream"
).createOrReplaceTempView("bronze_orders_stream")
spark.read.format("delta").load(
    "s3a://lakehouse/bronze/products_catalog"
).createOrReplaceTempView("bronze_products_catalog")
spark.read.format("delta").load(
    "s3a://lakehouse/bronze/customers"
).createOrReplaceTempView("bronze_customers")
print("Spark initialized and Bronze views registered successfully!")

# %% Step 2: Confirm MinIO Tables are Queryable
spark.sql(
    """
    SELECT 'orders_batch' AS table_name, COUNT(*) AS row_count FROM bronze_orders_batch
    UNION ALL
    SELECT 'orders_stream' AS table_name, COUNT(*) AS row_count FROM bronze_orders_stream
    UNION ALL
    SELECT 'products_catalog' AS table_name, COUNT(*) AS row_count FROM bronze_products_catalog
"""
).show()

# %% Step 3: Explore Bronze Tables (Fixed Syntax & Added .show())
spark.sql(
    """
    SELECT * 
    FROM bronze_orders_batch 
    LIMIT 5
"""
).show()
# %%
spark.sql(
    """
select * from bronze_orders_stream 
limit 5"""
).show()
# %%
spark.sql(
    """
select * from bronze_products_catalog
limit 5"""
).show()
# %% checking for duplicate order_id in bronze_orders_batch
spark.sql(
    """
SELECT invoice_id, COUNT(*) AS count
FROM bronze_orders_batch
GROUP BY invoice_id  
HAVING count > 1
"""
).show()
# %% Inspect full rows for a specific repeated invoice
spark.sql(
    """
    SELECT invoice_id, product_id, quantity, unit_price, customer_id
    FROM bronze_orders_batch
    WHERE invoice_id = 'INV-1104'
"""
).show()
## there exist duplicates for invoice_id

# %%
spark.sql(
    """
SELECT invoice_id, COUNT(*) AS count
FROM bronze_orders_stream
GROUP BY invoice_id
HAVING count > 1
"""
).show()

## there exist duplicates for invoice_id

# %%
spark.sql(
    """
    select id , count(*) as count
    from bronze_products_catalog
    group by id
    having count > 1
"""
).show()
# no duplicates for products table

# %% Check for nulls dynamically across all columns in PySpark
from pyspark.sql.functions import col, when, count

df = spark.table("bronze_orders_batch")

# Build a dynamic count of nulls per column
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])

null_counts.show()
# %% Check for nulls dynamically across all columns in PySpark
from pyspark.sql.functions import col, when, count

df = spark.table("bronze_orders_stream")

# Build a dynamic count of nulls per column
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])

null_counts.show()

# %% Check for nulls dynamically across all columns in PySpark
from pyspark.sql.functions import col, when, count

df = spark.table("bronze_products_catalog")

# Build a dynamic count of nulls per column
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])

null_counts.show()

# %% checking for negative values in three tables all in once
spark.sql(
    """
SELECT *
FROM bronze_orders_batch as a , bronze_orders_stream as b , bronze_products_catalog as c
WHERE a.quantity < 0 OR a.unit_price < 0 OR b.quantity < 0 OR b.unit_price < 0 OR c.price < 0
"""
).show()

# %% column types for all three tables
spark.sql("DESCRIBE bronze_orders_batch").show(truncate=False)
spark.sql("DESCRIBE bronze_orders_stream").show(truncate=False)
spark.sql("DESCRIBE bronze_products_catalog").show(truncate=False)
# needs Type casting for some columns

# %% veryfing that primary keys are unique
# bronze_orders_batch
spark.sql(
    """
SELECT COUNT(DISTINCT invoice_id) AS unique_invoice_ids, COUNT(*) AS total_rows
FROM bronze_orders_batch
"""
).show()
# might need a new column added as primary key

# %% veryfing that primary keys are unique
# bronze_orders_stream
spark.sql(
    """
SELECT COUNT(DISTINCT invoice_id) AS unique_invoice_ids, COUNT(*) AS total_rows
FROM bronze_orders_stream
"""
).show()
# might need a new column added as primary key
# %% veryfing that primary keys are unique
# bronze_products_catalog
spark.sql(
    """
SELECT COUNT(DISTINCT id) AS unique_ids, COUNT(*) AS total_rows
FROM bronze_products_catalog
"""
).show()
# ALL GOOD
# %%

""" 
for silver transformations we need to remove duplicates  type casting for some columns
for silver transformations we need to deal with primary key issues 
for silver transformations we need to do PII for email , and customer ids 
for silver transformations we might rename some columns ot have better names 
for silver transformations we might need to add new columns such as Total_amount 
"""

# %%
spark.sql(
    """select invoice_id , count(*) as occurence 
from bronze_orders_batch
group by invoice_id
having count(*) > 1
"""
).show()
# %%
##writing checks for the newly added customer table
spark.sql("""select * from bronze_customers limit 5""").show()
# %%
## checking for nulls
spark.sql(
    """select count(*) as null_count from bronze_customers where customer_id is null or email is null or ip_address is null
"""
).show()
# %% checking for duplicate rows /duplicate primary key
spark.sql(
    """select customer_id , count(*) as occurence
from bronze_customers
group by customer_id
having count(*) > 1
"""
).show()
# %%
"""hashing needed for customer email and ip address and customer id for PII protection"""
# %% checking for negative values in the customer table
spark.sql(
    """select * from bronze_customers where customer_id < 0 or email < 0 or ip_address < 0"""
).show()
# %% checking for email inconsistency
spark.sql("""select * from bronze_customers where email not like '%@%.%'""").show()
# %% checking for ip address inconsistency
spark.sql(
    """select * from bronze_customers where ip_address not rlike '^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'"""
).show()
# %% checking for sign up inconsistency Date Range Boundaries: Check min(first_sign_up) and max(first_sign_up)
spark.sql(
    """select min(first_sign_up) as min_signup , max(first_sign_up) as max_signup from bronze_customers"""
).show()
# %% checking
spark.sql(
    """WITH numeric_dates AS (
    SELECT 
        -- Days since a fixed baseline, for variance and median calculation
        DATEDIFF(first_sign_up, DATE'1970-01-01') AS signup_days
    FROM bronze_customers
    WHERE first_sign_up >= CURRENT_DATE - INTERVAL '2 years'
)
SELECT 
    VARIANCE(signup_days) AS variance_days,
    STDDEV(signup_days) AS stddev_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY signup_days) AS median_day_numeric,
    DATE_ADD(DATE'1970-01-01', CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY signup_days) AS INT)) AS median_signup_date

FROM numeric_dates;"""
).show()
# %%

spark.sql(
    """
    SELECT _source_system 
    FROM bronze_customers 
    WHERE _source_system <> 'uci_batch_csv'
"""
).show()
# %%
