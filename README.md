# Algae Data Infrastructure Project

This repository presents a comprehensive Data Engineering and Analytics project implemented at **BarAlgae**, a leading microalgae cultivation company. The goal was to build a robust, scalable data platform integrating diverse data sources to enable advanced analytics and predictive modeling for improved operational decisions.

## 🚩 Project Overview

The project included building an automated data pipeline, cloud-based storage and data warehousing solutions, analytics dashboards, and machine learning models. It enabled real-time monitoring, efficient management, and predictive insights for algae cultivation.

## 🔗 Main Components

### 1. Data Ingestion & Pipeline Automation
- Automated daily ingestion from multiple sources:
  - Excel files and Google Sheets.
  - Internal SQL databases.
  - Real-time sensor data and various operational machinery.
- Orchestration of data pipelines using Apache Airflow.

### 2. Data Lake and Warehouse
- Centralized storage in an AWS S3 Data Lake, organized with partitions for optimized access.
- Data Warehouse created in AWS Redshift:
  - Structured layers (Staging, Intermediate, Marts).
  - Automated daily ETL transformations using DBT.

### 3. Dashboards and Visualizations
- Power BI dashboards for operational tracking and management:
  - KPI tracking.
  - Predictive analytics (harvest times, algae density, health indicators).
  - Daily refreshed, interactive reports.

### 4. Machine Learning Models
- Predictive models (Random Forest, XGBoost, Gradient Boosting):
  - Predict algae density and optimal harvesting time.
  - Achieved high accuracy (R² > 0.8).
  - Models fully integrated into the daily analytics platform.

### 5. AI-Based Chatbot
- Developed a chatbot powered by AI (Claude AI).
- Enabled users to query data naturally and obtain immediate insights.

## 🛠 Technologies Used
- **Cloud:** AWS (S3, Redshift)
- **ETL/ELT:** Apache Airflow, DBT
- **Analytics and Visualization:** Power BI
- **Programming:** Python (Pandas, NumPy, scikit-learn, XGBoost)
- **Version Control:** Git & GitHub

## 📚 Project Structure
1.
![1](https://github.com/user-attachments/assets/1c20392d-a589-4bde-ab38-24a285b191ef)
2.
![Mobile Data Infrastructure Architecture](https://github.com/user-attachments/assets/4567802f-e1a7-482d-8a8b-0fb80708668b)


AI-Powered Chatbot Providing Data-Driven Insights and Recommendations for Algae Cultivation:
![77](https://github.com/user-attachments/assets/d352a00c-545e-4703-8428-fc59560aa567)

Weekly Algae Density Forecasting: Predictive vs Optimal Density Comparison:
Weekly Actionable Insights: AI-Driven Recommendations Integrated with Operational Dashboards
![WhatsApp Image 2025-06-03 at 15 13 04_4f1b46af](https://github.com/user-attachments/assets/1b22ad0a-d856-40df-a04c-9619fbf6242b)
![6](https://github.com/user-attachments/assets/875e67b6-9308-4d5b-9c87-e0089689b3d7)
![3](https://github.com/user-attachments/assets/7cd3f714-d4dc-4311-994e-3bf8721906d8)
![2](https://github.com/user-attachments/assets/084f4a59-8ace-4eb2-853c-b24726e77708)






