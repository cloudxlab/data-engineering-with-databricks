# Summary - Chapter 4
--
## Select 
```
  SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/001.json`
  SELECT * FROM text.`${da.paths.datasets}/raw/events-kafka/`
  SELECT * FROM binaryFile.`${da.paths.datasets}/raw/events-kafka/`
  SELECT * FROM csv.`${da.paths.working_dir}/sales-csv`
```
------------------------------
## CREATE TABLE/VIEW AS
```
  CREATE OR REPLACE TEMP VIEW events_temp_view
  AS SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/`;

  CREATE OR REPLACE TABLE sales AS
      SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`;

  CREATE OR REPLACE TABLE sales_unparsed AS
      SELECT * FROM csv.`${da.paths.datasets}/raw/sales-csv/`;

  CREATE TABLE sales_delta AS
    SELECT * FROM sales_tmp_vw; 

  CREATE OR REPLACE TABLE purchases AS
      SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
  FROM sales;

  CREATE OR REPLACE VIEW purchases_vw AS
  SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
  FROM sales;

  CREATE OR REPLACE TABLE users_pii
  COMMENT "Contains PII"
  LOCATION "${da.paths.working_dir}/tmp/users_pii"
  PARTITIONED BY (first_touch_date)
  AS
    SELECT *, 
      cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
      current_timestamp() updated,
      input_file_name() source_file
    FROM parquet.`${da.paths.datasets}/raw/users-historical/`;

```

----------------------
## Create Table USING

```
CREATE TABLE sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${da.paths.working_dir}/sales-csv"

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:/${da.username}_ecommerce.db",
  dbtable = "users"
)

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")
```
 
-----
## OTHER - ALTER/CLONE
```
    ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

    CREATE OR REPLACE TABLE purchases_clone
    DEEP CLONE purchases

    CREATE OR REPLACE TABLE purchases_shallow_clone
    SHALLOW CLONE purchases
```
----
## INSERT / MERGE / Copy
```
  INSERT OVERWRITE sales
    SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`
  
  INSERT INTO sales
    SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-30m`
  
  MERGE INTO users a
    USING users_update b
    ON a.user_id = b.user_id
    WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
      UPDATE SET email = b.email, updated = b.updated
    WHEN NOT MATCHED THEN INSERT *
  
  COPY INTO sales
    FROM "${da.paths.datasets}/raw/sales-30m"
    FILEFORMAT = PARQUET
```
