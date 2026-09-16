# %% Initialize Session & Load Silver Delta Tables
import sys
from pathlib import Path

project_root = Path.cwd().resolve()
while not (project_root / "config" / "spark_config.py").is_file():
    project_root = project_root.parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.spark_config import get_spark_session

spark = get_spark_session("SilverLayerValidation")

# Load Silver Tables from MinIO
silver_batch = spark.read.format("delta").load("s3a://lakehouse/silver/orders_batch")
silver_stream = spark.read.format("delta").load("s3a://lakehouse/silver/orders_stream")
silver_products = spark.read.format("delta").load(
    "s3a://lakehouse/silver/products_catalog"
)
silver_customers = spark.read.format("delta").load("s3a://lakehouse/silver/customers")

# Register Temp Views for SQL querying
silver_batch.createOrReplaceTempView("silver_orders_batch")
silver_stream.createOrReplaceTempView("silver_orders_stream")
silver_products.createOrReplaceTempView("silver_products")
silver_customers.createOrReplaceTempView("silver_customers")

print("✅ Silver Delta views successfully registered!")
# %%
# selecting some data from silver_orders_batch
spark.sql(
    """
    SELECT *
    FROM silver_orders_batch
    LIMIT 5
"""
).show()
# order_line id should be first column in the table
# %%
# checking if order_line_id is unique in silver_orders_batch
spark.sql(
    """
    SELECT count(distinct(order_line_id)) as unique_ids, COUNT(*) as total_rows
    FROM silver_orders_batch
"""
).show()
# there is still 1 duplicate because of the timestamp

# %% checking the duplicate in silver_orders_batch up close
spark.sql(
    """
    SELECT
    invoice_id,
    COUNT(*) AS occurrence_count
FROM silver_orders_batch
GROUP BY invoice_id
HAVING COUNT(*) > 1
"""
).show()


# %% checking the invoide_id INV-1003
spark.sql(
    """
    SELECT *
    FROM silver_orders_batch
    WHERE invoice_id = 'INV-1003'
"""
).show()
# turns out the data is corrupted from the bronze layer and the invoice _id is duplicated for no reason !

# %% checking the silver_orders_stream in general
spark.sql(
    """
    SELECT *
    from silver_orders_stream
    limit 5
"""
).show()

# %% checking for duplicate order_line_id
spark.sql(
    """
    SELECT order_line_id, COUNT(*) AS occurrence_count
    FROM silver_orders_stream
    GROUP BY order_line_id
    HAVING COUNT(*) > 1
"""
).show()
# turns out there is many duplicates
# %% checking 1 of the duplicates
spark.sql(
    """
    SELECT *
    FROM silver_orders_stream
    WHERE order_line_id = 'INV-STREAM-5028_1'
"""
).show()
# two different customers buy the same product problem  .
# checking another id to see if the problem is consistent
spark.sql(
    """
    SELECT *
    from silver_orders_stream
    WHERE order_line_id = 'INV-STREAM-5079_2'
"""
).show()
# %% checking how many times this occurs
# writing a window function to count the number of times duplicate  order_line_id appears in the silver_orders_stream table
# %% Count total duplicates and identify duplicate instances
spark.sql(
    """
    WITH ranked_records AS (
        SELECT
            order_line_id,
            COUNT(*) OVER(PARTITION BY order_line_id) AS line_id_occurrence_count
        FROM silver_orders_stream
    )
    SELECT
        COUNT(DISTINCT order_line_id) AS total_affected_keys,
        SUM(CASE WHEN line_id_occurrence_count > 1 THEN 1 ELSE 0 END) AS total_duplicate_rows,
        COUNT(*) AS total_table_rows
    FROM ranked_records;
"""
).show()
## 33 duplicate order_line_id in the silver_orders_stream table
## same fix should be applied to the silver_orders_stream table as well

# %% checking the silver_products_catalog table
spark.sql(
    """
    SELECT *
    FROM silver_products_catalog
    LIMIT 5
"""
).show()

# %% checking for duplicate product_id in silver_products_catalog
spark.sql(
    """
    SELECT id, COUNT(*) AS occurrence_count
    FROM silver_products_catalog
    GROUP BY id
    HAVING COUNT(*) > 1
"""
).show()
# ALL GOOD
# %%
spark.sql(""" select * from silver_customers  limit 5""").show()

# %% DEDUCTIONS
"""
Include customer_id (or customer_name before hashing) in your dropDuplicates() list.
Incorporate customer_id into the order_line_id composite key to guarantee 100% key uniqueness.
this unique key should be in the first row of the silver_orders_batch table.
(the order_line_id should be the first column in the silver_orders_batch table)
this process should be applied for both silver_orders_batch and silver_orders_stream tables.
"""

# %% exploring bits of data
spark.sql(""" select * from silver_products limit 5""").show()
# %%
