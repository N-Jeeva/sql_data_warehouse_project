/*	-- CREATE DATABASE AND SCHEMAS --

-- String purpose:
		-- This script creates a new database named "datawarehouse" checking if it already exists.
		-- If it already exists, it will be deleted and the new one will be recreated.
		-- This script also sets up three layers that is gold, silver and bronze each with a different function.
-- Warning: 
		-- The entire database will be deleted if it already exists. All data in the database will be permanently deleted.
        -- Proceed with caution and ensure that you have proper backups before running this script.
*/

use master;

	-- Drop and recreate the datawarehouse database --
drop database if exists datawarehouse;

-- Create the datawarehouse database --

create database datawarehouse;
 use datawarehouse;
 
 -- Create schemas or separate databases in MySQL --
 create schema bronze;
 create schema silver;
 create schema gold;
