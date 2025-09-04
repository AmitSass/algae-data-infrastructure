-- BarAlgae Data Infrastructure - SQL Documentation
-- This file contains SQL examples and documentation

/*
BarAlgae Data Infrastructure SQL Examples
==========================================

This repository contains SQL queries and data models for the BarAlgae 
algae cultivation data infrastructure.

Key SQL Files:
- sql_examples.sql: Main analytical queries
- data_quality_checks.sql: Data validation queries
- transform/dbt/models/: dbt SQL models for data transformation

Database Schema:
- Staging tables: stg_flowcam__*, stg_scada__*, stg_weather__*
- Intermediate tables: int_flowcam__*, int_scada__*
- Mart tables: fact_flowcam__*, fact_weather__*, fact_harvest__*

Usage:
1. Connect to your PostgreSQL database
2. Run the queries in sql_examples.sql for analysis
3. Use data_quality_checks.sql for data validation
4. Execute dbt models for data transformation

For more information, see the main README.md file.
*/

-- Quick start query
SELECT 'BarAlgae SQL Infrastructure Ready!' as status;
