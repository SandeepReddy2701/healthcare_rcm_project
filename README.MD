# 🏥 Healthcare Revenue Cycle Management (RCM) – Project Development Guide

This project demonstrates how to build a complete end-to-end data pipeline for analyzing hospital revenue cycle data using Python, MySQL, and Google BigQuery.

---

## 📌 Project Objective

To unify and analyze healthcare data from two hospitals to produce actionable financial insights such as revenue, denial rates, and provider performance.

---

## 🛠️ Project Development Steps

### 🔹 Step 1: Requirement Understanding
- Identify the key business KPIs: revenue, denial rate, claim performance
- Understand source data: patients, transactions, encounters, departments, providers

---

### 🔹 Step 2: Data Source Preparation
- Set up two MySQL databases: `hospital_a_db` and `hospital_b_db`
- Load the provided SQL files into respective databases
- Verify schema differences between Hospital A and B

---

### 🔹 Step 3: Project Structure Design
- Create modular folders: `etl/`, `config/`, `main.py`
- Plan ETL stages: Extract, Transform, Load
- Define the target schema for BigQuery

---

### 🔹 Step 4: Extraction Planning
- Connect to both MySQL databases using a connector (e.g., SQLAlchemy)
- Select required tables from each hospital
- Add logic to handle differences in table structure (e.g., column names)

---

### 🔹 Step 5: Data Transformation Design
- Merge patient data across both hospitals with unified IDs
- Clean and standardize missing fields (e.g., null PaidAmounts)
- Format dates and unify schema for consistent analytics

---

### 🔹 Step 6: Data Warehouse Configuration
- Set up a Google Cloud project
- Enable BigQuery API and create a dataset (e.g., `healthcare_rcm`)
- Authenticate using `gcloud` or service accounts

---

### 🔹 Step 7: Load Phase Execution
- Create target tables in BigQuery: `dim_patients`, `fact_transactions`, `fact_encounters`, `dim_providers`, `dim_departments`
- Load data using Python BigQuery client
- Validate row counts and schema integrity

---

### 🔹 Step 8: Analytics Query Planning
- Define core queries to extract KPIs:
  - Total Revenue
  - Denial Rate
  - Revenue by Department / Provider
- Run and test queries inside BigQuery UI

---

### 🔹 Step 9: Reporting & Validation
- Perform data quality checks
- Document each data flow and transformation logic
- Prepare analytics-ready tables for dashboards or BI tools

---

### 🔹 Step 10: Documentation & Delivery
- Write a clear README (like this)
- Include visuals (e.g., pipeline flowchart)
- Create a summary of KPIs and insights
- Optionally prepare a slide deck or report

---

## ✅ Output Summary

- 10K+ patients processed
- 20K+ transactions analyzed
- Unified 5 tables from 2 hospitals
- Fully loaded into BigQuery for analytics

---

## 📂 Deliverables

- ETL pipeline scripts
- MySQL sample databases
- BigQuery dataset (with analytics tables)
- Project documentation & reports

---

## 📌 Notes

> If BigQuery is not available, the pipeline can be adjusted to load into another data warehouse or local reporting tool using the same cleaned data.

---

