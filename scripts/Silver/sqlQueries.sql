-- fix crm_cust_info


INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
select 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE
 WHEN Upper(TRIM(cst_marital_status)) = 'S' then 'Single'
 WHEN Upper(TRIM(cst_marital_status)) = 'M' then 'Married'
 else 'n/a'
 end as cst_marital_status,
 CASE
 WHEN Upper(TRIM(cst_gndr)) = 'M' then 'Male'
 WHEN Upper(TRIM(cst_gndr)) = 'F' then 'Female'
 else 'n/a'
 end as cst_gndr,
 cst_create_date
from
(
SELECT *,
ROW_NUMBER() OVER(PARTITION by cst_id ORDER BY cst_create_date DESC) as rank_by_create_date
from bronze.crm_cust_info
) as t
where rank_by_create_date = 1 and cst_id is not null;


-- fix crm_prd_info


IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);






INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as Cat_id,
SUBSTRING(prd_key,7, LEN(prd_key) ) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST( LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1  AS DATE) AS prd_end_dt -- Calculate end date as one day before the next start date
from bronze.crm_prd_info









-- fix crm sales_details 



-- We need to change the data type of date as they are int type so before inserting the data we are going to change the data types in the table 


IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

	

-- insert into crm_sales_details 


INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt ,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
	
	

	



-- insert into silver  erp_cust_az12



INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
SELECT 
 CASE
  when cid LIKE 'NAS%' THEN SUBSTRING(cid,4,len(cid))
  else CID
  END CID,
  CASE 
  WHEN bdate > GETDATE()  or  bdate < '1925-10-17' then  NULL
  else bdate
  end as bdate,
  CASE
  when upper(trim(gen)) IN ('M','MALE') THEN 'MALE'
  when upper(trim(gen)) IN ('F','FEMALE') THEN 'FEMALE'
  ELSE 'n/a'
  end as gen
  FROM bronze.erp_cust_az12;







-- INSERT into silver.erp_loc_a101;





INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
SELECT 
REPLACE(cid,'-','') as cid,
CASE 
 WHEN TRIM(cntry) = 'DE' then 'Germany'
 WHEN TRIM(cntry) IN ('US','USA') then 'United States'
 when trim(cntry) = '' or cntry IS NULL  then 'n/a'
 else trim(cntry)
 end as cntry
FROM bronze.erp_loc_a101;




-- fixing silver.erp_px_cat_g1v2 



INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
