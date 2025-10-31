CREATE DATABASE datawarehouse;

CREATE Schema bronze;

CREATE Schema silver;

CREATE Schema gold;

CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date
);

BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\SUJITH\OneDrive\Desktop\White Scholar\Sql Project\datasets\source_crm\cust_info.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);


CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\SUJITH\OneDrive\Desktop\White Scholar\Sql Project\datasets\source_crm\prd_info.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);


CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\SUJITH\OneDrive\Desktop\White Scholar\Sql Project\datasets\source_crm\sales_details.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);


CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);

BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\SUJITH\OneDrive\Desktop\White Scholar\Sql Project\datasets\source_erp\cust_az12.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);


CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);

BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\SUJITH\OneDrive\Desktop\White Scholar\Sql Project\datasets\source_erp\loc_a101.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);


CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);

BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\SUJITH\OneDrive\Desktop\White Scholar\Sql Project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK
);
