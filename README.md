# Assignment 1 — Data Engineering Project

## Project Overview

This repository contains a data engineering project organized around data ingestion, data cleaning and transformation, data modelling, SQL analytics, pipeline orchestration, and Power BI reporting. The folders separate the main stages of the workflow so that ingestion, preparation, analysis, and reporting can be developed and maintained independently.


## Repository Structure

```text
assignment_1/
├── Data Pipeline/
├── PowerBI/
├── clean_and_transformation/
├── data_ingestion_script/
├── data_modelling/
└── sql_analytics/
```

## Folder and File Descriptions

### `Data Pipeline/`

This folder is intended to contain the pipeline workflow or supporting files used to coordinate the project's data-processing stages. It represents the movement of data through the project, from ingestion and preparation to downstream modelling or analytics. The exact files and their individual responsibilities should be confirmed against the folder contents.

### `PowerBI/`

This folder is intended to hold Power BI reporting assets for presenting the results of the data pipeline. Such assets may include a Power BI report or related files that connect to curated data and display business metrics. The exact report pages, measures, visuals, and source connections should be documented after inspecting the files.

### `clean_and_transformation/`

This folder contains the data-cleaning and transformation logic. The confirmed script, `clean_and_transformation.py`, is a Databricks notebook-style Python script that uses PySpark to read the Bronze tables `assignment_1.bronze.orders` and `assignment_1.bronze.users`.

#### `clean_and_transformation.py`

This script prepares order and user data for analytics. It configures logging, reads the Bronze tables, and reports their row counts. For user records, it attempts to derive a missing name from the email address; it also fills selected missing product, status, city, and email values with `Not Available`. It parses order and signup dates using several supported formats, trims text fields, converts email addresses to lowercase, and casts user IDs to integers. Order prices are cleaned by removing non-numeric currency characters and converting the result to a decimal value. The script separates invalid orders—those with a missing user ID, an invalid price, or a non-positive price—and flags invalid users with a missing ID or unusable name. It then derives an order-month field, calculates monthly revenue and order counts, calculates each customer's account age in days and lifetime value, and standardizes the capitalization of names, cities, products, and statuses. Finally, it creates a star-schema model in the `assignment_1.silver` schema: `fact_orders` stores order-level measures and attributes, `dim_users` stores customer attributes, and `dim_date` contains calendar attributes such as date key, month, year, and day of week. The fact table is partitioned by `order_month`, and the script logs row counts and displays sample results for validation.

### `data_ingestion_script/`

This folder is intended to contain the script or notebook that brings source data into the project. In a typical workflow, ingestion reads source files, applies basic structural checks, and writes the raw data to a Bronze or staging location for downstream processing. The exact input locations, file formats, destination tables, and error-handling behaviour should be confirmed by inspecting the actual file or files.

### `data_modelling/`

This folder is intended to contain the data-model design or implementation used to organize cleaned data for analytics. The confirmed cleaning script creates a star schema with `fact_orders`, `dim_users`, and `dim_date` in the `assignment_1.silver` schema. If this folder contains separate modelling files, their exact purpose and relationship to those tables should be added after reviewing the files.

### `sql_analytics/`

This folder is intended to contain SQL queries or notebooks used to analyze curated data. SQL analytics commonly supports business questions such as monthly revenue, order volumes, customer activity, and customer lifetime value. The specific queries, source tables, calculations, and outputs should be documented after inspecting the folder's contents.

## Data Processing Flow

1. **Ingestion:** Load source order and user data into a raw or Bronze layer.
2. **Cleaning and transformation:** Handle missing values, parse dates, standardize text, validate prices and IDs, and separate invalid records.
3. **Data modelling:** Build the order fact table and user/date dimensions for analytics.
4. **SQL analytics:** Query the curated tables to derive business insights.
5. **Power BI reporting:** Present the resulting metrics in reports and visualizations.
6. **Pipeline coordination:** Use the pipeline assets to organize or automate the stages, according to the implementation in the repository.

