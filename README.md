# Project Objectives

1. Build an automated data pipeline using Apache Airflow
2. Extract daily sales data from a PostgreSQL database
3. Calculate total daily revenue using SQL and Python
4. Store the final processed output in a CSV file for reporting and further analysis

---

# What the Pipeline Does

## 1 - Extract
- Connect to PostgreSQL using Airflow
- Run SQL queries to pull raw daily sales data

## 2 - Transform
- Calculate daily revenue using Python (pandas) or SQL aggregation
- Clean and prepare the final dataset

## 3 - Load
- Save the final daily revenue results into a CSV file inside the Airflow output directory

## 4 - Automate
- Use Apache Airflow DAG to run the whole workflow automatically on a schedule

---

# 🛠 Tools Used
- Python (Pandas, Numpy, Matplotlib, Airflow, etc.)
- SQL
- PostgreSQL
- Apache Airflow
- Oracle VirtualBox

---
# Arcihecture digram
![Arcihecture digram](Arcihecture digram./Arcihecture digram.png)
