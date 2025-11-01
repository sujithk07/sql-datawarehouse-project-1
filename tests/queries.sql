--Bronze Layer
--1st table
--Before inserting quality check

select cst_id,
Count(*)
from bronze.crm_cust_info
group by cst_id
Having Count(*) > 1 or cst_id is null;


Select cst_firstname from bronze.crm_cust_info
where cst_firstname!=trim(cst_firstname);


Select cst_lastname from bronze.crm_cust_info
where cst_lastname!=trim(cst_lastname);

Select cst_gndr from bronze.crm_cust_info
where cst_gndr!=trim(cst_gndr);

select DISTINCT cst_gndr
from bronze.crm_cust_info;

select DISTINCT cst_marital_status
from bronze.crm_cust_info;

--After inserting quality check

select * from silver.crm_cust_info;
select cst_id,
Count(*)
from silver.crm_cust_info
group by cst_id
Having Count(*) > 1 or cst_id is null;


Select cst_firstname from silver.crm_cust_info
where cst_firstname!=trim(cst_firstname);


Select cst_lastname from silver.crm_cust_info
where cst_lastname!=trim(cst_lastname);

Select cst_gndr from silver.crm_cust_info
where cst_gndr!=trim(cst_gndr);

SELECT DISTINCT cst_gndr from silver.crm_cust_info;

SELECT DISTINCT cst_marital_status from silver.crm_cust_info;


--2nd table
--before quality check

select prd_id,
Count(*)
from bronze.crm_prd_info
group by prd_id
Having Count(*) > 1 or prd_id is null;

select prd_nm from bronze.crm_prd_info
where prd_nm!=trim(prd_nm);

select prd_cost from bronze.crm_prd_info
where prd_cost is null or prd_cost <=0;

select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;

--After quality check 

select prd_id,
Count(*)
from silver.crm_prd_info
group by prd_id
Having Count(*) > 1 or prd_id is null;

select prd_nm from silver.crm_prd_info
where prd_nm!=trim(prd_nm);

select prd_cost from silver.crm_prd_info
where prd_cost is null or prd_cost <=0;

select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt;


--3rd table
--Before Quality check

SELECT sls_ord_num from bronze.crm_sales_details
where sls_ord_num!=trim(sls_ord_num);

select * from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

select * from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

select sls_order_dt from bronze.crm_sales_details
where LEN(sls_order_dt)!=8 OR sls_order_dt IS NULL;

select sls_ship_dt from bronze.crm_sales_details
where LEN(sls_ship_dt)!=8 OR sls_ship_dt IS NULL;

select sls_due_dt from bronze.crm_sales_details
where LEN(sls_due_dt)!=8 OR sls_due_dt IS NULL;

select * from bronze.crm_sales_details
where sls_sales!=sls_quantity * sls_price
OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price <=0;

select * from bronze.crm_sales_details
where sls_quantity!=sls_sales / sls_price
OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price <=0;


select * from bronze.crm_sales_details
where sls_price!=sls_sales / sls_price
OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price <=0;

--After Quality check

SELECT sls_ord_num from silver.crm_sales_details
where sls_ord_num!=trim(sls_ord_num);

select * from silver.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

select * from silver.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

select sls_order_dt from silver.crm_sales_details
where LEN(sls_order_dt)!=8 OR sls_order_dt IS NULL;

select sls_ship_dt from silver.crm_sales_details
where LEN(sls_ship_dt)!=8 OR sls_ship_dt IS NULL;

select sls_due_dt from silver.crm_sales_details
where LEN(sls_due_dt)!=8 OR sls_due_dt IS NULL;

select * from silver.crm_sales_details
where sls_sales!=sls_quantity * sls_price
OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price <=0;

select * from silver.crm_sales_details
where sls_quantity!=sls_sales / sls_price
OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price <=0;


select * from silver.crm_sales_details
where sls_price!=sls_sales / sls_price
OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price <=0;

--silver Layer
--4th table
--Before Quality Check

select DISTINCT bdate from bronze.erp_cust_az12
where bdate > GETDATE() OR bdate < '1925-10-17';

select DISTINCT gen from bronze.erp_cust_az12;

--After Quality Check

select DISTINCT bdate from silver.erp_cust_az12
where bdate > GETDATE() OR bdate < '1925-10-17';

select DISTINCT gen from silver.erp_cust_az12;

--5th table
--Before Quality Check
select DISTINCT cntry
FROM bronze.erp_loc_a101;

--After Quality Check
select DISTINCT cntry
FROM silver.erp_loc_a101;

--6th table
--before quality check
select DISTINCT cat From bronze.erp_px_cat_g1v2;
select DISTINCT subcat From bronze.erp_px_cat_g1v2;
select DISTINCT maintenance From bronze.erp_px_cat_g1v2;

--After quality check
select DISTINCT cat From silver.erp_px_cat_g1v2;
select DISTINCT subcat From silver.erp_px_cat_g1v2;
select DISTINCT maintenance From silver.erp_px_cat_g1v2;

--Gold Layer
-- Checking 'gold.dim_customers'
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


-- Checking 'gold.dim_products'
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;


--Checking gold.fact_sales

SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
