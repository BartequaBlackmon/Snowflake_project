
USE SCHEMA DEMO.DT_DEMO;

CREATE OR REPLACE DYNAMIC TABLE PROD_INV_ALERT
    LAG = '1 MINUTE'
    WAREHOUSE=COMPUTE_WH
AS
    SELECT 
        S.PRODUCT_ID, 
        S.PRODUCT_NAME,CREATIONTIME AS LATEST_SALES_DATE,
        STOCK AS BEGINING_STOCK,
        SUM(S.QUANTITY) OVER (PARTITION BY S.PRODUCT_ID ORDER BY CREATIONTIME) TOTALUNITSOLD, 
        (STOCK - TOTALUNITSOLD) AS UNITSLEFT,
        ROUND(((STOCK-TOTALUNITSOLD)/STOCK) *100,2) PERCENT_UNITLEFT,
        CURRENT_TIMESTAMP() AS ROWCREATIONTIME
    FROM SALESREPORT S JOIN PROD_STOCK_INV ON PRODUCT_ID = PID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY CREATIONTIME DESC) = 1
;


-- check products with low inventory and alert
select * from prod_inv_alert where percent_unitleft < 10;


## This can help you send email alerts to your product procurement and inventory team to restock the required products.


CREATE NOTIFICATION INTEGRATION IF NOT EXISTS
    notification_emailer
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('first.last@company.com')
    COMMENT = 'email integration to update on low product inventory levels'
;

CREATE OR REPLACE ALERT alert_low_inv
  WAREHOUSE = XSMALL_WH
  SCHEDULE = '30 MINUTE'
  IF (EXISTS (
      SELECT *
      FROM prod_inv_alert
      WHERE percent_unitleft < 10 and ROWCREATIONTIME > SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
                'notification_emailer', -- notification integration to use
                'first.last@company.com', -- Email
                'Email Alert: Low Inventory of products', -- Subject
                'Inventory running low for certain products. Please check the inventory report in Snowflake table prod_inv_alert' -- Body of email
);

-- Alerts are pause by default, so let's resume it first
ALTER ALERT alert_low_inv RESUME;

-- Add new records
insert into salesdata select * from table(gen_cust_purchase(10000,2));



-- Monitor alerts in detail
SHOW ALERTS;

SELECT *
FROM
  TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START
      =>dateadd('hour',-1,current_timestamp())))
WHERE
    NAME = 'ALERT_LOW_INV'
ORDER BY SCHEDULED_TIME DESC;

-- Suspend Alerts 
-- Important step to suspend alert and stop consuming the warehouse credit
ALTER ALERT alert_low_inv SUSPEND;


-- Check Dynamic Table Refresh History to see when the dynamic tables were refreshed last and what was the action performed

SELECT * 
FROM 
    TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE 
    NAME IN ('SALESREPORT','CUSTOMER_SALES_DATA_HISTORY','PROD_INV_ALERT','CUMULATIVE_PURCHASE')
    -- AND REFRESH_ACTION != 'NO_DATA'
ORDER BY 
    DATA_TIMESTAMP DESC, REFRESH_END_TIME DESC LIMIT 10;
    


-- Resume the data pipeline
alter dynamic table customer_sales_data_history RESUME;
alter dynamic table salesreport RESUME;
alter dynamic table prod_inv_alert RESUME;

-- Suspend the data pipeline
alter dynamic table customer_sales_data_history SUSPEND;
alter dynamic table salesreport SUSPEND;
alter dynamic table prod_inv_alert SUSPEND;

