/*
==============================================================================
Quality Checks
==============================================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schemas. It includes checks for:

    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
- Run these checks after loading data into Silver Layer.
- Investigate and resolve any issues found during the checks.
==============================================================================
*/

-- =============================================================================
-- Checking 'silver.crm_cust_info'
-- =============================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
        SELECT cst_id,
        COUNT(*) FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
        SELECT * FROM silver.crm_cust_info
        WHERE cst_firstname <> trim(cst_firstname) or cst_lastname <> trim(cst_lastname);

-- Data Standardization & Consistency
        SELECT DISTINCT(cst_gndr) from silver.crm_cust_info;
        SELECT DISTINCT(cst_marital_status) from silver.crm_cust_info;

-- =============================================================================
-- Checking 'silver.crm_prd_info'
-- =============================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
        SELECT COUNT(*) FROM silver.crm_prd_info
        GROUP BY prd_id
        HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Invalid date ranges and orders.
        SELECT * FROM silver.crm_prd_info
        WHERE prd_start_dt > prd_end_dt;

-- Data Standardization & Consistency
        SELECT DISTINCT prd_line
        FROM silver.crm_prd_info;

        select * from silver.crm_prd_info 
        where prd_cost < 0 or prd_cost is null;

-- Check for Unwanted Spaces
-- Expectation: No Results
        SELECT * FROM silver.crm_cust_info
        WHERE prd_nm <> trim(prd_nm);

-- =============================================================================
-- Checking 'silver.crm_cust_info'
-- =============================================================================
-- Invalid date ranges and orders.
        SELECT * FROM silver.crm_sales_details
        where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- Data Standardization & Consistency
        SELECT * from silver.crm_sales_details 
        where sls_sales != sls_price * sls_quantity;

-- =============================================================================
-- Checking 'silver.silver.erp_cust_az12'
-- =============================================================================
-- Data Standardization & Consistency
        SELECT DISTINCT gen
        FROM silver.erp_cust_az12;

-- Invalid date ranges and orders
        SELECT * FROM silver.erp_cust_az12
        WHERE bdate > CURRENT_DATE() OR bdate < '1925-01-01';

-- =============================================================================
-- Checking 'silver.erp_loc_a101'
-- =============================================================================

-- Data Standardization & Consistency
        SELECT DISTINCT cntry
        FROM silver.erp_loc_a101
        ORDER BY cntry;

-- ===============================================
-- Checking 'silver.erp_px_cat_glv2'
-- ===============================================
-- Check for Unwanted Spaces
-- Expectation: No Results

        SELECT * FROM silver.erp_px_cat_glv2
        WHERE
            cat != TRIM(cat)
            OR subcat != TRIM(subcat)
            OR maintenance != TRIM(maintenance);


-- Data Standardization & Consistency
        SELECT DISTINCT maintenance
        FROM silver.erp_px_cat_glv2;
