🏗️ Data Warehouse Project — Medallion Architecture (Bronze → Silver → Gold)
📌 Overview

This project implements a modern Data Warehouse using SQL Server following the Medallion Architecture (Bronze, Silver, Gold) pattern.

Raw operational data from CRM and ERP systems is ingested, cleaned, transformed, and modeled into analytics-ready Star Schema tables for reporting and business intelligence.

The focus is on real-world data engineering practices: data quality, transformations, SCD handling, and production-style SQL workflows.

🧱 Architecture
CSV Source Files
        ↓
Bronze Layer (Raw Data)
        ↓
Silver Layer (Cleaned & Conformed)
        ↓
Gold Layer (Star Schema / Analytics)
🥉 Bronze Layer — Raw Ingestion

Purpose:
Store source data exactly as received for traceability and historical backup.

Key Characteristics

Loaded using BULK INSERT

No transformations or business logic

Acts as a raw data archive

Tables

bronze.crm_cust_info

bronze.crm_prd_info

bronze.crm_sales_details

bronze.erp_cust_az12

bronze.erp_loc_a101

bronze.erp_px_cat_g1v2

🥈 Silver Layer — Data Cleaning & Transformation

Purpose:
Clean, standardize, and conform data across CRM and ERP systems.

Key Transformations

Deduplication using ROW_NUMBER()

Standardizing gender, marital status, and country codes

Handling invalid and future dates

Fixing inconsistent numeric values (sales, prices)

Removing hidden characters and whitespace

SCD Type 2 implementation for product history

Implementation

Fully automated via stored procedure:
silver.load_silver

Includes:

Table truncation

Insert-based transformations

Execution time logging (per table & per batch)

Error handling using TRY...CATCH

Tables

silver.crm_cust_info

silver.crm_prd_info

silver.crm_sales_details

silver.erp_cust_az12

silver.erp_loc_a101

silver.erp_px_cat_g1v2

🥇 Gold Layer — Analytics & Business Model

Purpose:
Expose clean, business-friendly data optimized for analytics and reporting.

Design

Star Schema modeling

Fact & Dimension separation

Human-readable column names

Filtered to current records where applicable

Gold Objects

gold.fact_sales

gold.dim_customer

gold.dim_product

gold.dim_date

gold.dim_location

Use Cases

Business Intelligence tools (Power BI, Tableau)

Analytical queries

Reporting & dashboards

⭐ Star Schema

Fact Table

Sales transactions

Measures: quantity, sales amount, price

Dimension Tables

Customer

Product

Date

Location

Benefits

Fast aggregations

Simple joins

Scalable analytics

🛠️ Technologies Used

SQL Server

SQL Server Management Studio (SSMS)

T-SQL

CSV Data Sources

Draw.io (Star Schema diagram)

Git & GitHub

✅ Key Features
<img width="1260" height="818" alt="Screenshot 2026-02-28 042702" src="https://github.com/user-attachments/assets/e8d502b1-8026-4186-ba65-54c67d6f2e51" />

Medallion Architecture (Bronze / Silver / Gold)

Robust data cleansing & standardization

SCD Type 2 implementation

Execution time monitoring

Production-style stored procedures

Analytics-ready Star Schema


📈 Future Enhancements
Audit & load history tables

Power BI integration

Orchestration via SQL Agent or Airflow

👤 Author

Youssef Hassan
Computer Science Student | Aspiring Data Engineer
📍 Cairo, Egypt

GitHub: https://github.com/2311-ops

LinkedIn: https://www.linkedin.com/in/youssef-hassan-5b86b4323/

