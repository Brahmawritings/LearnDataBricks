# Databricks Practice CSV Files

This folder contains sample CSV files used for hands-on practice with the book *Learn Databricks in $10: A Hands-On Guide for Data Engineers* by Team UnlockTheNxt.

Each file contains 50 records and is used across various chapters for practicing PySpark, SQL, Delta Lake, ML, and more.

## File Descriptions

- employees.csv — Sample employee data for file format and join practice
- sales_data.csv — Sales transactions for PySpark transformation exercises
- products.csv — Product master data for joins
- inventory.csv — Inventory levels for stock management practice
- orders.csv — Basic order data used in Delta table examples
- orders_updates.csv — Updated records to demonstrate Delta MERGE
- daily_sales_summary.csv — Daily summary records for workflow jobs
- large_orders.csv — Used for performance tuning and file size optimizations
- orders_with_issues.csv — Contains nulls or issues for data quality validation
- orders_delta.csv — Status tracking for schema evolution practice
- trips.csv — Ride data for ML regression models
- sales_orders.csv — Used in the real-world sales analytics pipeline
- flights.csv — Airline schedule and delay prediction practice
- sensor_readings.csv — IoT-like sensor data for anomaly detection
- transactions.csv — Used for fraud detection and credit scoring
- customers.csv — Customer profiles for enrichment and feature engineering

## How to Use

1. Upload the CSVs into your Databricks workspace (e.g., `/FileStore/tables/`)
2. Read them using PySpark or Spark SQL
3. Follow the exercises in each chapter for applied learning

Happy Learning!  
Team UnlockTheNxt