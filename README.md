# Johnson & Johnson – Data Engineering Technical Assessment

Welcome aboard! 👋  
We’re glad to have you join the Data Analytics team.

This repository contains a **sample Data Engineering pipeline** built as part of a technical assessment. It reflects how we approach data engineering in practice: transforming raw data into clean, reliable, and analytics-ready datasets that can be consumed by Analysts, Data Scientists, and BI teams.

---

## Project Overview

As Data Engineers, our mission is to make data:
- trustworthy
- well-structured
- easy to consume
- scalable for analytics use cases

In this project, we use **PySpark and Databricks** to build a simple, end-to-end data pipeline that:
- reads data from storage
- applies cleaning and transformation logic
- validates results through unit tests
- produces an analytics-ready **fact table**
- exports the results for downstream consumption

The solution is intentionally kept **simple, readable, and maintainable**.

---

## Technology Stack

The following technologies are used in this project:

- **PySpark** (Python API for Apache Spark)
- **SQL**
- **Databricks**
- **Git & GitHub**
- **pytest** (Python testing framework)

---

## Pipeline Overview

The pipeline is organized into three main stages:

### 1. Unit Testing
Before running the data transformations, core logic is validated using `pytest`.  
These tests help ensure:
- data quality rules are enforced
- transformation logic behaves as expected
- edge cases are handled correctly

### 2. Data Processing
- Input data is read from **Databricks Volumes**
- Data is cleaned, validated, and transformed using PySpark
- An analytics-ready **fact table** is created

### 3. Data Output
The final dataset is written back to Databricks Volumes in two formats:
- **Parquet** – optimized for analytics and performance
- **CSV** – easy to inspect and share

---

## How to Run the Pipeline (Databricks)

To run the pipeline in Databricks, follow these steps:

1. Create a Databricks account
2. Integrate your Databricks workspace with this GitHub repository
3. Pull the repository into your Databricks Workspace
4. Create a new **Databricks Job**
5. Add a **Notebook task** for unit testing
   - Use `Task-2-Solution.pdf` for guidance
6. Add a **Python Script task** pointing to pipeline.py Python script found in PySpark folder in the repository
   - Use `Task-3-Solution.pdf` for guidance
7. (Optional) Add a notebook task to explore or visualize the results using Databricks display() function
8. Run the job to execute the full pipeline
