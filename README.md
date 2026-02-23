# Snowflake Data Pipeline Project

<img width="1063" height="232" alt="Screenshot 2026-02-23 122109" src="https://github.com/user-attachments/assets/a49d69c3-d6a0-4816-b85d-dc0179416c74" />


## Product Description
Dynamic tables are new declarative way of defining data pipeline in Snowflake. It's a new kind of Snowflake table which is defined as a query to continuously and automathiclly materialize the result of that query as a table. A DAG using Dynamic Tables was created. It runs whenever there is data in the raw base tables and infers the lag based on the downstream tables LAG parameter as "DOWNSTREAM".


## Architecture

1. Data ingestion with Python
2. Storage in Snowflake
3. Transformation with SQL
4. Analytics dashboard
