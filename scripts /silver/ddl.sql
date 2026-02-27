USE Datawarehouse;
GO

-- CRM Customer Info
DROP TABLE IF EXISTS silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gender NVARCHAR(50),
    cst_create_date DATE,
	dwh_create_date datetime2 default getdate()

);
GO

-- CRM Product Info
DROP TABLE IF EXISTS silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info(
    prd_id INT,
	cat_id  NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt date,
    prd_end_dt date,
	dwh_create_date datetime2 default getdate()
);
GO

-- CRM Sales Details
DROP TABLE IF EXISTS silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
	dwh_create_date datetime2 default getdate()
);
GO

-- ERP Location
DROP TABLE IF EXISTS silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
	dwh_create_date datetime2 default getdate()
);
GO

-- ERP Customer
DROP TABLE IF EXISTS silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
	dwh_create_date datetime2 default getdate()
);
GO

-- ERP Product Category
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
GO
CREATE TABLE silver.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintaince NVARCHAR(50),
	dwh_create_date datetime2 default getdate()
);
GO
