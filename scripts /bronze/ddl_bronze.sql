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
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME,
            @end_time   DATETIME,
			@batch_start_time DATETIME,
            @batch_end_time DATETIME;


    BEGIN TRY
	    SET @batch_start_time = GETDATE();
        /* ===================== CRM CUSTOMER ===================== */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\LOQ\Documents\Data warehouse project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'crm_cust_info load time (sec): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------------------------------';


        /* ===================== CRM PRODUCT ===================== */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\LOQ\Documents\Data warehouse project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'crm_prd_info load time (sec): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------------------------------';


        /* ===================== CRM SALES ===================== */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\LOQ\Documents\Data warehouse project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'crm_sales_details load time (sec): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------------------------------';


        /* ===================== ERP CUSTOMER ===================== */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\LOQ\Documents\Data warehouse project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'erp_cust_az12 load time (sec): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------------------------------';


        /* ===================== ERP LOCATION ===================== */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\LOQ\Documents\Data warehouse project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'erp_loc_a101 load time (sec): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------------------------------';


        /* ===================== ERP PRODUCT CATEGORY ===================== */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\LOQ\Documents\Data warehouse project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'erp_px_cat_g1v2 load time (sec): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '--------------------------------------------------';
    SET @batch_end_time = GETDATE();

	PRINT '==================================================';
	PRINT 'TOTAL BRONZE LOAD TIME (sec): '
		  + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR);
	PRINT '==================================================';
    END TRY
    BEGIN CATCH
        PRINT '-----------------------';
        PRINT 'ERROR during loading bronze layer';
        PRINT ERROR_MESSAGE();
        PRINT CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '-----------------------';
    END CATCH
END;
