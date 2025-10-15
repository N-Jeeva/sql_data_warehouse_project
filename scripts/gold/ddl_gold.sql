
=====================================================
DDL Script: Create Gold Views
=====================================================
Script Purpose:
This script creates views for the Gold Layer in the data warehouse.
The Gold Layer represents the final dimension and fact tables (Star Schema)

Each view performs transformations and combines data from the Silver layer
to produce a clean, enriched, and business-ready dataset.

Usage:
- These views can be queried directly for analytics and reporting.

=====================================================
-- Create Dimension: gold.dim_customers
=====================================================
drop view if exists gold.dim_customers;
create view gold.dim_customers as
select
	row_number() over (order by cst_id) as customer_key,
	a.cst_id as customer_id, 
	a.cst_key as customer_number, 
	a.cst_firstname as first_name, 
	a.cst_lastname as last_name, 
	case when a.cst_gndr != 'n/a' then a.cst_gndr
		else coalesce(b.gen, 'n/a')
	end as gender,
    b.bdate as birthdate,
	a.cst_marital_status as marital_status,
    c.cntry as country,
    a.cst_create_date as create_date
from silver.crm_cust_info a
left join silver.erp_cust_az12 b
on a.cst_key = b.cid
left join silver.erp_loc_a101 c
on a.cst_key = c.cid;


=====================================================
-- Create Dimension: gold.dim_products
=====================================================

drop view if exists gold.dim_products;
create view gold.dim_products as
select 
		row_number() over (order by d.prd_key, d.prd_start_dt) as product_key,
		d.prd_id as product_id, 
		d.prd_key as product_number,
		d.prd_nm as product_name,
		d.cat_id as category_id,
		e.cat as category,
		e.subcat as subcategory,
		e.maintenance,
		d.prd_cost as product_cost, 
		d.prd_line as product_line, 
		d.prd_start_dt as start_date
from silver.crm_prd_info d
left join silver.erp_px_cat_g1v2 e
on d.cat_id = e.id
where prd_end_dt is null;

=====================================================
-- Create Dimension: gold.fact_sales
=====================================================

drop view if exists gold.fact_sales;
create view gold.fact_sales as
select 
	f.sls_ord_num as order_number, 
    g.product_key,
    h.customer_key,
    f.sls_order_dt as order_date,
	f.sls_ship_dt as shipping_date, 
	f.sls_due_dt as due_date, 
	f.sls_sales as sales_amount, 
	f.sls_quantity as quantity, 
	f.sls_price as price
from silver.crm_sales_details f
left join gold.dim_products g
on f.sls_prd_key = g.product_number
left join gold.dim_customers h
on f.sls_cust_id = h.customer_id ;
select * from gold.dim_products;
