# 📦 Supply Chain and Ecommerce Orderfulment Pipeline

## Problem Definition

Supply Chain and Ecommerce operations generate large volumes of data from multiple systems, including orders, shipments, warehouse operations, delivery times and customer feedback.

However, this data is often fragmented, inconsistent and stored in different formats. As a result:

* Reporting is delayed
* Real-time visibility into order fulfilment is limited
* Data quality issues reduce trust in analytics
* Identifying supply chain bottlenecks becomes difficult
* Manual data handling increases operational risk

Without an integrated data pipeline and structured analytical model, transforming raw logistics data into reliable, decision-ready insights is challenging.


## Stakerholder Objectives
* **Sales Performance:** onitor and improve overall sales across products and regions.
* **Profitability:** Analyze profit margins to maximize business profitability.
* **Customer Insights:** Understand customer behavior to improve targeting and retention.
* **Logistics Efficiency:** Reduce late deliveries and improve shipping performance.
* **Regional Analysis:** Identify high-performing and underperforming sales regions.
* **Product Performance:** Evaluate product demand to optimize product offerings.
* **Discount Strategy:** Assess the impact of discounts on sales and profit.

# ✅ Solution
This project implements a **cloud-based data pipeline** and builds an **OLAP-ready data warehouse** to enable efficient logistics analytics.

The solution:

* Integrates data from multiple operational systems
* Cleans and standardizes data for quality and consistency
* Transforms raw data into a dimensional (star schema) model
* Enables fast, multidimensional OLAP analysis
* Supports data-driven decision-making through BI tools

The result is a scalable analytical system that improves visibility, performance monitoring, and strategic decision-making in e-commerce logistics.



# Architecture Diagram
![AWS architecture](documentation/data/architecture.png)



# 🛠️ Tech Stack
![Apache Airflw](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=style=flat\&logo=apache-airflow\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=style=flat\&logo=docker\&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=style=flat\&logo=python\&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-025E8C?style=style=flat\&logo=postgresql\&logoColor=white)
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=style=flat\&logo=amazon-s3\&logoColor=white)
![Amazon Redshift](https://img.shields.io/badge/Amazon%20Redshift-8C4FFF?style=style=flat\&logo=amazon-redshift\&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-FF9900?style=style=flat\&logo=amazonaws\&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=style=flat\&logo=prometheus\&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=style=flat\&logo=grafana\&logoColor=white)
![Loki](https://img.shields.io/badge/Grafana%20Loki-F46800?style=style=flat\&logo=grafana\&logoColor=white)
![Tempo](https://img.shields.io/badge/Grafana%20Tempo-F46800?style=style=flat\&logo=grafana\&logoColor=white)
![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-000000?style=style=flat\&logo=opentelemetry\&logoColor=white)
![Pytest](https://img.shields.io/badge/Pytest-0A9EDC?style=style=flat\&logo=pytest\&logoColor=white)
![Linting](https://img.shields.io/badge/Linting-Flake8-4B8BBE?style=for-the-badge)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=style=flat\&logo=github)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=style=flat\&logo=github-actions)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=style=flat\&logo=linux\&logoColor=black)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=style=flat\&logo=power-bi\&logoColor=black)

### Executive Summary

The  **Supply Chain and Sales Performance dashboard** provides a clear, up-to-date view of the company’s sales, profit, orders and delivery performance. It helps teams track overall business performance while ensuring operations remain efficient as the business grows.

At the same time, the dashboard highlights **top-performing products, key regions, and customer segments** that drive most of the revenue. It also reveals areas where **discounting and pricing strategies may be affecting profitability**.

By showing where sales are strongest and where improvements are needed, the dashboard gives **operations, sales and management teams** the insights they need to make faster, data-driven decisions and improve overall business performance. 


### Key Insights

* Generated **$36.78M in sales** and **$3.97M profit** from **66K orders**, with an overall **11% profit margin**.
* **Sales and profit remained stable from 2015–2017**, followed by a **sharp decline in 2018**, indicating potential operational or data issues.
* **Sports and outdoor products** (e.g., tents, bikes, running shoes) are the **top revenue drivers**.
* The **Consumer segment contributes the highest share of sales**, highlighting a strong **B2C focus**.
* The supply chain is **highly efficient**, with a **late delivery rate of only 1%**.

![Executive Dashboard](documentation/data/dashboard.png)

### Operational and Business Outcomes

* **Improved Supply Chain Visibility:** The dashboard provides a clear view of sales, profit, orders and delivery performance, enabling better operational monitoring.
* **Better Decision-Making:** Identifies top-performing products, regions and customer segments to support data-driven business strategies.
* **Profit Optimization Opportunities:** Analysis of discounts vs. profit highlights areas where pricing and discount strategies can be improved.
* **Market Expansion Insights:** Regional sales analysis helps identify high-performing markets and underperforming regions with growth potential.
* **Operational Efficiency Monitoring:** Delivery metrics, including a **1% late delivery rate**, help track logistics performance and maintain service quality.

# Data Warehouse Overview
The analytics schema in edshift employs a star-schema centered on one fact table that capture both granular line-level delivery performance and higher-level order-metrics and surrounded by four conformed dimensions (orders, customers, products and dates).

This design lets you slice & dice daily service outcomes across the:

* `who` (customer and city)
* `what` (product and category)
* `when` (order, agreed-delivery and actual-delivery dates)
* `where` (customer geography)

All in **sub-second, ad-hoc queries**.

![Supply Chain dashboard](documentation/data/star_schema.png)



# Technology Choices

# 1. Data Storage – Amazon S3 Data Lake

**Amazon S3** was used as the primary storage layer for the project.

Instead of multiple buckets, a **single S3 bucket was used with a layered folder structure** following the **Bronze–Silver–Gold data lake architecture**:

### Bronze Layer

Stores raw data exactly as ingested from the source system. No transformations are applied at this stage to preserve the original dataset.

### Silver Layer

Contains cleaned and standardized datasets after basic transformations such as:

* data type corrections
* handling missing values
* schema alignment

### Gold Layer

Stores curated and analytics-ready datasets that are optimized for reporting, dashboards and downstream consumption.

This layered structure helps maintain:

* **data lineage**
* **data quality management**
* clear separation between raw data and business-ready datasets.

In addition, a **separate S3 bucket was created for testing and development purposes**, allowing experimentation and validation of ETL processes without affecting production data.



# 2. Data Processing & ETL – Python, Pandas, PyArrow, Delta-rs

**Python** was used to implement the ETL (Extract, Transform, Load) pipeline.

The pipeline reads raw datasets from the **Bronze layer in Amazon S3**, performs a series of transformations and writes the processed data back to the **Silver and Gold layers**.

Key transformation steps include:

* Data cleaning
* Handling missing values
* Data type conversions
* Data enrichment and feature preparation

The implementation relies on several Python libraries:

* **Pandas** – for data manipulation and transformation
* **PyArrow** – for efficient columnar data processing and Parquet support
* **Delta-rs** – for managing Delta Lake tables and enabling reliable data versioning and ACID-compliant writes

Using Python with these libraries allows efficient processing of datasets while maintaining compatibility with modern **data lake table formats**.



# 3. Data Warehousing – Amazon Redshift

**Amazon Redshift** was used as the analytical data warehouse.

Data from the **processed S3 layer** is loaded into Redshift where it is structured into a **Star Schema** for analytical queries.

The warehouse includes:

### Fact Table

* `Fact_Sales`

### Dimension Tables

* `Dim_Customer`
* `Dim_Product`
* `Dim_Category`
* `Dim_Location`
* `Dim_Time`
* `Dim_Shipping`

This schema enables **fast analytical queries, aggregations, and reporting** for business intelligence.



# 4. Data Modeling – SQL

**SQL** was used to transform the warehouse data into **analytics-ready models** within Amazon Redshift.

Using SQL for transformations allowed the project to:

* Build **structured transformation logic** for cleaning and preparing data
* Implement the **Star Schema data model**, including fact and dimension tables
* Create reusable **SQL scripts** for loading and transforming data
* Ensure **data integrity and consistency** through validation queries

SQL was used to create the following warehouse models:

### Dimension Tables

* `dim_customer`
* `dim_product`
* `dim_date`
* `dim_order`
* `dim_shipping`
* `dim_delivery`

### Fact Table

* `fact_sales`

These SQL transformations convert the **Silver layer datasets** into a **Gold layer star schema** optimized for analytical queries and business intelligence tools such as **Power BI**.



# 5. Workflow Orchestration – Apache Airflow

**Apache Airflow** was used to orchestrate the data pipeline.

Airflow schedules and manages the workflow using **DAGs (Directed Acyclic Graphs)** that coordinate:

1. Data ingestion to S3
2. Data Transformation
3. Data loading into Redshift
4. Data quality checks

Airflow ensures tasks run in the correct order and provides monitoring for pipeline execution.



# 6. Business Intelligence & Visualization

A **dashboarding tool (Power BI)** was used to visualize insights from the Redshift warehouse.

The dashboard presents key business metrics including:

* Total Sales
* Profit Margins
* Customer Segments
* Regional Sales Performance
* Product Performance
* Delivery Performance

This allows stakeholders to monitor **sales trends, profitability, logistics performance, and customer insights in one unified dashboard.**



# 7. Version Control & Collaboration – GitHub

All project code including:

* Python scripts
* Airflow DAGs
* data models
* Infrastructure configuration

was stored in **GitHub**.

Using Git enables:

* Version tracking
* Team collaboration
* Code review
* Reproducible pipelines

# 8. Monitoring & Logging

Monitoring mechanisms were implemented to ensure pipeline reliability.

* **Airflow logs** track task execution and failures.
* Alerts can be configured to notify teams if pipeline steps fail.

This ensures quick troubleshooting and reliable data processing.

## GitHub Actions

Orchestrates the above steps, then stages scripts to S3 for Glue to pick up.

![github actions img](documentation/data/cicd.png)

## 10. Monitoring & Alerting

Prometheus scrapes Airflow, with Grafana dashboards visualizing system health.

### Airflow dag Dashboard

![grafana img](documentation/data/dag_performance.png)

### Airflow cluster dashboard

![grafana img](documentation/data/airflow_health.png)

### Operational management

![grafana img](documentation/data/operational_grafana.png)

### Slack Notifications
![slack img](documentation/data/slack_alerts.png)
# Future Improvements

While the current pipeline is functional and production-ready, several improvements can further enhance scalability, maintainability and governance.

## Areas for Future Improvement

Several enhancements can further strengthen the platform:

* Implement **dbt for Transformations**
* Implement **data quality validation frameworks**
* Implement **Infrastructure as Code (Terraform)**
* Implement **data cataloging and metadata management**
* Introduce **cost monitoring for cloud resources**
* Implement **data security and access control policies**
