# %% Initialize Session & Load Gold Delta Tables
import sys
from pathlib import Path

project_root = Path.cwd().resolve()
while not (project_root / "config" / "spark_config.py").is_file():
    project_root = project_root.parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.spark_config import get_spark_session

spark = get_spark_session("GoldLayerValidation")

# Load Gold Star Schema Tables from MinIO
dim_customers = spark.read.format("delta").load("s3a://lakehouse/gold/dim_customers")
dim_products = spark.read.format("delta").load("s3a://lakehouse/gold/dim_products")
fact_orders = spark.read.format("delta").load("s3a://lakehouse/gold/fact_orders")

# Register Temp Views for SQL querying & validation
dim_customers.createOrReplaceTempView("dim_customers")
dim_products.createOrReplaceTempView("dim_products")
fact_orders.createOrReplaceTempView("fact_orders")

print("✅ Gold Star Schema views successfully registered and ready for action!")
# %%
spark.sql("""select * from dim_customers limit 5""").show()
# %%
spark.sql("""select * from dim_products limit 5""").show()

# %%
spark.sql("""select * from fact_orders limit 5""").show()

# %%
