







delimiter $$

create procedure silver.load_silver()
begin

set @starttime = now();

		set @starttime = now();
				Truncate table silver.crm_cust_info;
				select ' >> Truncating table : silver.crm_cust_info' as msg;
				select ' >> Inserting data into table : silver.crm_cust_info' as msg;
				INSERT INTO silver.crm_cust_info (
					cst_id,
					cst_key,
					cst_firstname,
					cst_lastname,
					cst_marital_status,
					cst_gndr,
					cst_create_date
				)
				SELECT
					TRIM(cst_id) AS cst_id,
					TRIM(cst_key) AS cst_key,
					TRIM(cst_firstname) AS cst_firstname,
					TRIM(cst_lastname) AS cst_lastname,
					CASE 
						WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
						WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
						ELSE 'n/a'
					END AS cst_marital_status,
					CASE 
						WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						ELSE 'n/a'
					END AS cst_gndr,
					CASE
						WHEN cst_create_date IS NULL 
							 OR CAST(cst_create_date AS CHAR) IN ('0000-00-00', '0000-00-00 00:00:00')
						THEN NULL
						ELSE cst_create_date
					END AS cst_create_date
				FROM (
					SELECT *,
						   ROW_NUMBER() OVER (
							 PARTITION BY TRIM(cst_id)
							 ORDER BY CASE
										WHEN cst_create_date IS NULL 
											 OR CAST(cst_create_date AS CHAR) IN ('0000-00-00', '0000-00-00 00:00:00')
										THEN NULL
										ELSE cst_create_date
									  END DESC
						   ) AS flag_last
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL AND TRIM(cst_id) <> ''
				) t
				WHERE flag_last = 1;
		set @endtime = now();
		select 'silver.crm_cust_info' as table_name, time_to_sec(timediff(@starttime, @endtime)) as loading_time;


set @starttime = now();
		Truncate table silver.crm_prd_info;
		select ' >> Truncating table : silver.crm_prd_info' as msg;
		select ' >> Inserting data into table : silver.crm_prd_info' as msg;
		INSERT INTO silver.crm_prd_info (
			prd_id, 
			prd_key,
			cat_id,
			prd_nm, 
			prd_cost, 
			prd_line, 
			prd_start_dt, 
			prd_end_dt
		)
		SELECT 
		prd_id,
		substring(prd_key, 7, length(prd_key)) as prd_key,           					# To extract required product key
		replace(substring(prd_key, 1, 5), '-', '_') as cat_id,       					# To extract required category id
		prd_nm, 
		prd_cost, 
		CASE UPPER(TRIM(prd_line))
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'M' THEN 'Mountain'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
		end as prd_line,                                            					# Expanding the symbols for better understanding 
		cast(prd_start_dt as date) as prd_start_dt,
		cast(date_sub(lead (prd_start_dt) 
		over(partition by prd_key order by prd_start_dt), interval 1 day) as date)
		as prd_end_dt																	# avoid overlapping of dates by making the prev end date one day less than the next start date
		FROM bronze.crm_prd_info;

set @endtime = now();
select 'silver.crm_cust_info' as table_name, time_to_sec(timediff(@starttime, @endtime)) as loading_time;
        

set @starttime = now();
		Truncate table silver.crm_sales_details;
		select ' >> Truncating table : silver.crm_sales_details' as msg;
		select ' >> Inserting data into table : silver.crm_sales_details' as msg;
		insert into silver.crm_sales_details(
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
		select 
		sls_ord_num, 
		sls_prd_key, 
		sls_cust_id,
		case when sls_order_dt <= 0 or length(cast(sls_order_dt as char)) !=8 then null
			else str_to_date(cast(sls_order_dt as char), '%Y%m%d') 
		end as sls_order_dt,
		case when sls_ship_dt <= 0 or length(cast(sls_ship_dt as char)) != 8 then null
			else str_to_date(cast(sls_ship_dt as char), '%Y%m%d') 
		end as sls_ship_dt,
		case when sls_due_dt <= 0 or length(cast(sls_due_dt as char)) != 8 then null
			else str_to_date(cast(sls_due_dt as char), '%Y%m%d')
		end as sls_due_dt,
			CASE
				WHEN (sls_sales IS NULL OR sls_sales <= 0) 
					 AND (sls_quantity IS NOT NULL AND sls_quantity <> 0) 
					 AND (sls_price IS NOT NULL AND sls_price <> 0)
					 AND (sls_sales <> sls_quantity * sls_price)
					THEN abs(sls_quantity) * abs(sls_price)
				ELSE sls_sales
			END AS sls_sales,

			CASE
				WHEN (sls_quantity IS NULL OR sls_quantity <= 0) 
					 AND (sls_sales IS NOT NULL AND sls_sales <> 0) 
					 AND (sls_price IS NOT NULL AND sls_price <> 0)
					 AND (sls_quantity <> sls_sales / sls_price)
					THEN abs(sls_sales) / abs(sls_price)
				ELSE sls_quantity
			END AS sls_quantity,

			CASE
				WHEN (sls_price IS NULL OR sls_price <= 0) 
					 AND (sls_sales IS NOT NULL AND sls_sales <> 0) 
					 AND (sls_quantity IS NOT NULL AND sls_quantity <> 0)
					 AND (sls_price <> sls_sales / sls_quantity)
					THEN abs(sls_sales) / abs(sls_quantity)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
set @endtime = now();
select 'silver.crm_sales_details' as table_name, time_to_sec(timediff(@starttime, @endtime)) as loading_time;



set @starttime = now();
		Truncate table silver.erp_cust_az12;
		select ' >> Truncating table : silver.erp_cust_az12' as msg;
		select ' >> Inserting data into table : silver.erp_cust_az12' as msg;
		insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen
			)
		select
		case when cid like 'NAS%' then substring(cid, 4, length(cid))
				else cid
		end as cid,
		case when bdate > current_date() then null
				else bdate
		end as bdate,
		case 
				when upper(replace(replace(replace(trim(gen), '\r', ''), '\n', ''), ' ', '')) in ('M', 'MALE') then 'Male'
				when upper(replace(replace(replace(trim(gen), '\r', ''), '\n', ''), ' ', '')) in ('F', 'FEMALE') then 'Female'
				else 'n/a'
			end as gen
		from bronze.erp_cust_az12;
set @endtime = now();
select 'silver.erp_cust_az12' as table_name, time_to_sec(timediff(@starttime, @endtime)) as loading_time;


set @starttime = now();
		Truncate table silver.erp_loc_a101;
		select ' >> Truncating table : silver.erp_loc_a101' as msg;
		select ' >> Inserting data into table : silver.erp_loc_a101' as msg;
		insert into silver.erp_loc_a101(
			cid,
			cntry
			)
		select 
			replace(cid, '-', '') as cid,
			case 
				when upper(replace(replace(replace(trim(cntry), '\r', ''), '\n', ''), ' ', '')) = 'DE' 
					then 'Germany'
				when upper(replace(replace(replace(trim(cntry), '\r', ''), '\n', ''), ' ', '')) in ('US', 'USA') 
					then 'United states'
				when trim(replace(replace(replace(cntry, '\r', ''), '\n', ''), ' ', '')) = '' or cntry is null 
					then 'n/a'
				else replace(replace(replace(trim(cntry), '\r', ''), '\n', ''), ' ', '')
			end as cntry
		from bronze.erp_loc_a101;
set @endtime = now();
select 'silver.erp_loc_a101' as table_name, time_to_sec(timediff(@starttime, @endtime)) as loading_time;


set @starttime = now();
		Truncate table silver.erp_px_cat_g1v2;
		select ' >> Truncating table : silver.erp_px_cat_g1v2' as msg;
		select ' >> Inserting data into table : silver.erp_px_cat_g1v2' as msg;
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
			)
		select
			 id,
			 cat,
			 subcat,
			 (replace(replace(replace((maintenance), '\r', ''), '\n', ''), ' ', '')) as maintenance 
		 from bronze.erp_px_cat_g1v2;
set @endtime = now();
select 'silver.erp_px_cat_g1v2' as table_name, time_to_sec(timediff(@starttime, @endtime)) as loading_time;


set @endtime = now();
	select 'Loading silver layer is completed' as step;
    select time_to_sec(timediff(@starttime, @endtime)) as silverlayer_completion_time;
 end $$
 delimiter ;
