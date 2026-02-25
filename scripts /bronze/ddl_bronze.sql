USE Datawarehouse;
GO

-- CRM Customer Info
DROP TABLE IF EXISTS bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gender NVARCHAR(50),
    cst_create_date DATE
);
GO

-- CRM Product Info
DROP TABLE IF EXISTS bronze.crm_prd_info;
GO
CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);
GO

-- CRM Sales Details
DROP TABLE IF EXISTS bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
GO

-- ERP Location
DROP TABLE IF EXISTS bronze.erp_loc_a101;
GO
CREATE TABLE bronze.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);
GO

-- ERP Customer
DROP TABLE IF EXISTS bronze.erp_cust_az12;
GO
CREATE TABLE bronze.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);
GO

-- ERP Product Category
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
GO
CREATE TABLE bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintaince NVARCHAR(50)
);
GO

