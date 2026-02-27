USE Datawarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        /* =========================
           TRUNCATE SILVER TABLES
        ========================== */
        TRUNCATE TABLE silver.crm_cust_info;
        TRUNCATE TABLE silver.crm_prd_info;
        TRUNCATE TABLE silver.crm_sales_details;
        TRUNCATE TABLE silver.erp_cust_az12;
        TRUNCATE TABLE silver.erp_loc_a101;
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        /* =========================
           1️⃣ CRM CUSTOMER INFO
        ========================== */
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gender,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            cst_material_status,
            CASE
                WHEN UPPER(LTRIM(RTRIM(cst_gender))) IN ('M','MALE') THEN 'Male'
                WHEN UPPER(LTRIM(RTRIM(cst_gender))) IN ('F','FEMALE') THEN 'Female'
                ELSE 'N/A'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE rn = 1;

        /* =========================
           2️⃣ CRM PRODUCT INFO (SCD2)
        ========================== */
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
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'mountain'
                WHEN 'R' THEN 'road'
                WHEN 'S' THEN 'other sales'
                WHEN 'T' THEN 'touring'
                ELSE 'na'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1 AS DATE
            )
        FROM bronze.crm_prd_info;

        /* =========================
           3️⃣ CRM SALES DETAILS
        ========================== */
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
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CASE
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,
            CASE
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,
            CASE
                WHEN sls_sales IS NULL
                  OR sls_sales <= 0
                  OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price >= 0
                THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        /* =========================
           4️⃣ ERP CUSTOMER
        ========================== */
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
			gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
                ELSE cid
            END,
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END,
            CASE
                WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE') THEN 'Male'
                ELSE 'N/A'
            END
        FROM bronze.erp_cust_az12;

        /* =========================
           5️⃣ ERP LOCATION
        ========================== */
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid,'-',''),
            CASE
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        /* =========================
           6️⃣ ERP PRODUCT CATEGORY
        ========================== */
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintaince
        )
        SELECT
            TRIM(id),
            TRIM(cat),
            TRIM(subcat),
            TRIM(maintaince)
        FROM bronze.erp_px_cat_g1v2;

        PRINT 'Silver layer loaded successfully ✅';

    END TRY
    BEGIN CATCH
        PRINT '❌ Error loading Silver layer';
        PRINT ERROR_MESSAGE();
    END CATCH
END;
GO
exec silver.load_silver
SELECT COUNT(*) FROM silver.crm_cust_info;
SELECT COUNT(*) FROM silver.crm_prd_info;
SELECT COUNT(*) FROM silver.crm_sales_details;
SELECT COUNT(*) FROM silver.erp_cust_az12;
SELECT COUNT(*) FROM silver.erp_loc_a101;
SELECT COUNT(*) FROM silver.erp_px_cat_g1v2;
