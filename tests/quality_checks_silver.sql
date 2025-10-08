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
