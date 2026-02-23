
## For this the sales information will be extracted from the salesdata table and join with the customer information to build the customer_sales_data_history.
USE SCHEMA DEMO.DT_DEMO;

CREATE OR REPLACE DYNAMIC TABLE customer_sales_data_history
    LAG='DOWNSTREAM'
    WAREHOUSE=COMPUTE_WH
AS
select 
    s.custid as customer_id,
    c.cname as customer_name,
    s.purchase:"prodid"::number(5) as product_id,
    s.purchase:"purchase_amount"::number(10) as saleprice,
    s.purchase:"quantity"::number(5) as quantity,
    s.purchase:"purchase_date"::date as salesdate
from
    cust_info c inner join salesdata s on c.custid = s.custid
;

-- quick sanity check
select * from customer_sales_data_history limit 10;
select count(*) from customer_sales_data_history;

## The above code creates a dynamic table named `customer_sales_data_history` that combines customer information from the `cust_info` table with sales data from the `salesdata` table. The dynamic table is set to lag downstream, meaning it will capture changes from the source tables as they occur. The warehouse specified for this operation is `COMPUTE_WH`. After creating the dynamic table, a quick sanity check is performed by selecting a few records and counting the total number of records in the dynamic table.

CREATE OR REPLACE DYNAMIC TABLE salesreport
    LAG = '1 MINUTE'
    WAREHOUSE=XSMALL_WH
AS
    Select
        t1.customer_id,
        t1.customer_name, 
        t1.product_id,
        p.pname as product_name,
        t1.saleprice,
        t1.quantity,
        (t1.saleprice/t1.quantity) as unitsalesprice,
        t1.salesdate as CreationTime,
        customer_id || '-' || t1.product_id  || '-' || t1.salesdate AS CUSTOMER_SK,
        LEAD(CreationTime) OVER (PARTITION BY t1.customer_id ORDER BY CreationTime ASC) AS END_TIME
    from 
        customer_sales_data_history t1 inner join prod_stock_inv p 
        on t1.product_id = p.pid
       
;

-- quick sanity check
select * from salesreport limit 10;
select count(*) from salesreport;

## The above code creates another dynamic table named `salesreport` that aggregates data from the `customer_sales_data_history` dynamic table and the `prod_stock_inv` table. The dynamic table is set to lag by 1 minute, meaning it will capture changes from the source tables every minute. The warehouse specified for this operation is `XSMALL_WH`. The query selects various fields including customer ID, customer name, product ID, product name, sale price, quantity, unit sales price, sales date (as CreationTime), a composite key (CUSTOMER_SK), and calculates the end time using the LEAD function. After creating the dynamic table, a quick sanity check is performed by selecting a few records and counting the total number of records in the `salesreport` dynamic table.


-- Testing the DAG by adding some raw data in the base tables
-- Add new records
insert into salesdata select * from table(gen_cust_purchase(10000,2));

-- Check raw base table
select count(*) from salesdata;

-- Check Dynamic Tables after a minute
select count(*) from customer_sales_data_history;
select count(*) from salesreport;

