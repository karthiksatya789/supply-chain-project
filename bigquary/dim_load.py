from google.cloud import bigquery
import os

# GCP authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "supply-chain-data-integration-e58b2d6792ca.json"

project_id = "supply-chain-data-integration"
dataset_id = "supply_chain"




client = bigquery.Client(project=project_id)

# --------- Helper Function ---------
def execute_query(sql, description):
    try:
        print(f"🔄 {description}")
        query_job = client.query(sql)
        query_job.result()
        print(f"✅ {description} executed successfully.\n")
    except Exception as e:
        print(f"❌ Failed: {description}\n{e}")

# --------- 1. Preview Orders Table (Optional) ---------
preview_sql = f"""
SELECT * 
FROM `{project_id}.{dataset_id}.orders`
LIMIT 5
"""
print("📄 Preview Orders Table:")
for row in client.query(preview_sql).result():
    print(dict(row))

# --------- 2. Create fact_orders ---------
execute_query(f"""
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.fact_orders` AS
SELECT
  `Order ID` AS order_id,
  `Order Date` AS order_date,
  `Ship Date` AS ship_date,
  `Product ID` AS product_id,
  Sales
FROM `{project_id}.{dataset_id}.orders`
""", "Create fact_orders")

# --------- 3. Create dim_product ---------
execute_query(f"""
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_product` AS
SELECT DISTINCT
  `Product ID` AS product_id,
  `Product Name` AS product_name,
  Category,
  `Sub-Category` AS sub_category
FROM `{project_id}.{dataset_id}.orders`
""", "Create dim_product")

# --------- 4. Create dim_customer ---------
execute_query(f"""
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_customer` AS
SELECT DISTINCT
  `Customer ID` AS customer_id,
  `Customer Name` AS customer_name,
  Segment,
  Country,
  City,
  State,
  `Postal Code` AS postal_code,
  Region
FROM `{project_id}.{dataset_id}.orders`
""", "Create dim_customer")

# --------- 5. Create dim_date ---------
execute_query(f"""
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_date` AS
WITH dates AS (
  SELECT 
    DATE_ADD(DATE '2016-01-01', INTERVAL day DAY) AS date
  FROM UNNEST(GENERATE_ARRAY(0, 1460)) AS day
)
SELECT 
  date,
  EXTRACT(DAY FROM date) AS day,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(YEAR FROM date) AS year,
  FORMAT_DATE('%B', date) AS month_name,
  FORMAT_DATE('%A', date) AS day_name
FROM dates
""", "Create dim_date")

# --------- 6. Create sales_by_category View ---------
execute_query(f"""
CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.sales_by_category` AS
SELECT 
  p.category,
  SUM(f.sales) AS total_sales
FROM `{project_id}.{dataset_id}.fact_orders` f
JOIN `{project_id}.{dataset_id}.dim_product` p
  ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC
""", "Create view sales_by_category")

# --------- 7. Preview the view ---------
print("📊 Top 10 Sales by Category:")
query = f"SELECT * FROM `{project_id}.{dataset_id}.sales_by_category` LIMIT 10"
for row in client.query(query).result():
    print(dict(row))
