create view gold.fact_sales as 
SELECT 
    sd.sls_ord_num AS order_number,
    pr.product_key,
    sd.sls_prd_key AS product_code,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS unit_price
FROM silver.crm_sales_details sd 
LEFT JOIN gold.dim_product pr 
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu 
    ON sd.sls_cust_id = cu.customer_id;
create view gold.dim_product as
select 
ROW_NUMBER() over (order by pn.prd_start_dt , pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.cat_id category_id,
pn.prd_nm as product_name,

pc.cat prodcut_category,
pc.subcat as sub_category,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pc.maintaince,
pn.prd_start_dt
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc on pn.cat_id = pc.id
where prd_end_dt is null -- filtring to have only current data
create view gold.dim_customers as 
SELECT  
	ROW_NUMBER() over (order by cst_id) as customer_key,
    ci.cst_id AS Customer_id,
    ci.cst_key as customer_number, 
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    ci.cst_material_status as marital_status,
	case when ci.cst_gender != 'n/a' then ci.cst_gender --crm = master in gender info 
	else coalesce(ca.gen,  'n/a') 
	end as gender,
    ci.cst_create_date as create_date, 
    ca.bdate as birth_date,

    la.cntry as country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
    ON ci.cst_key = la.cid;
