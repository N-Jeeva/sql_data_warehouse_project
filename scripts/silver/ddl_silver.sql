/*
------------------------------------------------------------
DDL SCRIPT : CREATE SILVER TABLES
------------------------------------------------------------
Script Purpose:
      This cript will create tables inside 'silver' schema. It will drop the table if it already exists.
      Run this script to redefine the DDL Structure of DDL Tables.
*/

drop table if exists silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id int,
	prd_key varchar(50),
    cat_id varchar(50),
	prd_nm varchar(50),
	prd_cost int,
	prd_line varchar(50),
	prd_start_dt date,
	prd_end_dt date,
	dwh_create_date datetime default(current_timestamp())
);


drop table if exists silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime default(current_timestamp())
);


drop table if exists silver.erp_cust_az12;
create table silver.erp_cust_az12(
 cid varchar(50),
 bdate date,
 gen varchar(50),
  dwh_create_date datetime default(current_timestamp())
 );


drop table if exists silver.erp_loc_a101;
create table silver.erp_loc_a101(
	cid varchar(50),
	cntry varchar(50),
	dwh_create_date datetime default(current_timestamp())
 );
 

drop table if exists silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	id varchar(50),
	cat varchar(50),
	subcat varchar(50),
	maintenance varchar(50),
	dwh_create_date datetime default(current_timestamp())
 );
