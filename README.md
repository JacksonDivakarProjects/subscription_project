
# Data Engineering Pipeline Documentation

**Project Author:** Jackson Divakar

  

## 1. Architecture Overview

This project implements a robust **Medallion Architecture** (Bronze, Silver, Gold) designed to ingest, process, and analyze subscription and customer data.

  

**Infrastructure & Tooling Decisions:**

* **Storage (Data Lake):** **Azure Blob Storage** is utilized as the highly scalable, cost-effective external location for storing all raw and processed data.

* **Compute (Processing):** **Databricks** serves as the primary execution engine. Its distributed computing capabilities efficiently process large datasets using PySpark.

* **Data Format:** **Delta Lake** (via Databricks) is the underlying table format, providing ACID transactions, scalable metadata handling, and advanced optimization features like Z-Ordering.

* **Configuration Management:** Environment variables (via `dotenv`) are used to manage schema paths (`BRONZE_SCHEMA_PATH`, `SILVER_SCHEMA_PATH`, `GOLD_SCHEMA_PATH`), ensuring the code is portable across development, testing, and production environments.

  

---

  

## 2. Bronze Layer: Raw Data Ingestion (`nb_ingestion_bronze`)

The Bronze layer acts as the landing zone for raw data arriving in Azure Blob Storage.

  

**Design Decisions:**

* **Decoupled Configuration:** Path configurations are injected dynamically using `.env` files. This prevents hardcoding sensitive or environment-specific blob URIs directly into the notebooks.

* **Full Refresh Loading:** DataFrames (Customer, Employee, Product, Opportunity, FX Rates) are saved using `write.mode("overwrite")`.

    * *Why this decision was made:* Overwriting ensures that the Bronze layer accurately reflects the latest full-snapshot state of the source files. (Note: For strictly incremental source files, this would be shifted to an `append` pattern).

* **Schema Registration:** Data is registered explicitly as tables (e.g., `bronze_customer`, `bronze_opportunity`) in the Databricks catalog to allow easy querying and downstream dependency tracking.

  

---

  

## 3. Silver Layer: Cleansing & Standardization (`nb_transformation_silver`)

The Silver layer cleanses and conforms the raw Bronze data into a trusted, unified format.

  

**Design Decisions:**

* **Dynamic Date Handling:** Real-world source data often contains messy, heterogeneous date formats. A custom, reusable PySpark function (`parse_mixed_date`) was built using `rlike` and regex pattern matching.

    * *Why this decision was made:* Instead of failing on bad records, the pipeline dynamically detects patterns (`dd-MM-yyyy`, `yyyy/MM/dd`, `dd/MM/yyyy`) and casts them to standard PySpark `DateType`. A similar function (`parse_mixed_datetime`) handles timestamps.

* **String Normalization:** `trim()` and `regexp_replace` functions are heavily utilized to strip whitespace and normalize categorical string values, ensuring downstream joins do not fail due to invisible characters.

* **Write Strategy:** Cleaned DataFrames are written back to the Silver schema, acting as the validated "Source of Truth" for the enterprise.

  

---

  

## 4. Gold Layer: Dimensional Modeling (`nb_modelling_gold`)

The primary goal of this layer is to reorganize the cleansed data into a format optimized for analytical reporting and BI tools (like Tableau or Power BI).

  

**Design Decisions:**

* **Star Schema Implementation:** The data is segregated into descriptive Dimensions (`dim_customer`, `dim_product`, `dim_event`) and quantitative Facts (`fact_subscription`).

    * *Why this decision was made:* This classic Kimball methodology simplifies query logic for analysts and drastically improves aggregation performance.

* **Pre-Aggregated Analytics Cube:** A wide, denormalized view named `subscription_analytics` is generated. This prevents BI tools from having to perform expensive joins repeatedly on the fly.

* **Advanced File Management (Z-Ordering):** * *Decision:* The `OPTIMIZE` command paired with `ZORDER BY (customer_id, product_id)` is applied directly to the `subscription_analytics` table.

    * *Why this decision was made:* Z-Ordering co-locates related information in the same set of Delta Parquet files on Azure Blob Storage. When analysts filter by specific customers or products, Databricks can skip reading irrelevant files entirely, reducing query times from minutes to seconds.

  

---

  

## 5. Gold Layer: Business Analytics & KPIs (`nb_kpi_gold`)

This notebook extracts direct business value from the modeled Gold tables, utilizing PySpark aggregations and SQL window functions.

  

**Design Decisions With Some KPIs:**

* **KPI 1: Customer Gain/Loss:** Uses a dynamic `current_day` variable (e.g., `'2024-06-01'`) to filter the analytics table for the current year. It groups by `close_status` ("Won" vs "Lost") to provide a net customer retention figure.

* **KPI 2: YoY Churn Trends (Customer Loss %):** Uses PySpark `year()` functions to dynamically establish `current_year` and `prev_year`. It calculates the precise percentage change in lost customers (e.g., -31.64% reduction in churn) to track the health of the business.

* **KPI 3: Revenue Leaders (High-Value Customers):** Aggregates the `recurring_revenue` at the `customer_id` level, sorting descending to instantly identify the most critical accounts for the sales and retention teams.

* **Optimization:** Aggregations rely heavily on `countDistinct()` and PySpark's lazy evaluation, allowing Databricks' Catalyst Optimizer to generate the most efficient physical execution plan against the Z-Ordered files.


## 6. Slowly Changing Dimension (SCD) & Incremental Loading Strategy

The data pipeline employs a multi-tiered approach to handling data mutations (inserts, updates, deletes) across the Medallion Architecture. This ensures a highly performant **Current State** in the Silver layer and an accurate **Historical Record** in the Gold layer.

#### 1. Silver Layer: SCD Type 1 (Current State)
The Silver layer is designed to be the "Source of Truth" for the current state of all business entities. 

* **Strategy applied:** **SCD Type 1 (Incremental Merge / Upsert)**
* **Target Tables:** `silver_customer`, `silver_product`, `silver_opportunity`
* **Implementation:** Instead of overwriting tables or keeping historical duplicates, a Delta `MERGE` statement is executed matching on primary keys (e.g., `customer_id`, `opportunity_id`). 
* **Business Value:** This deduplicates late-arriving records from the Bronze layer and ensures that analysts querying the Silver layer always see the single most up-to-date version of a record. It drastically reduces processing time compared to full table overwrites.

#### 2. Gold Layer: SCD Type 2 (Historical Tracking)
The Gold layer tracks how attributes change over time to support accurate "point-in-time" analytical reporting.

* **Strategy applied:** **SCD Type 2**
* **Target Tables:** `dim_customer` (and other critical dimensions like `dim_employee`).
* **Implementation:** When an entity's attribute (such as a customer's `country` or `is_active` status) changes in the Silver layer, the Gold layer captures this via a Delta `MERGE`. 
    * The existing record is "expired" by setting an **`is_current = false`** flag and logging the **`valid_to`** timestamp.
    * A brand new record is inserted containing the updated attributes, marked with **`is_current = true`** and a **`valid_from`** timestamp.
* **Business Value:** If a customer moves from India to the USA, historical sales made while they lived in India remain attributed to the India region.

#### 3. Gold Layer: Fact Table Loading
Fact tables require a different loading pattern to avoid massive data duplication.

* **Strategy applied:** **Incremental Merge** (Type 1 Logic)
* **Target Table:** `fact_subscription`
* **Implementation:** A Delta `MERGE` on `opportunity_id`.
* **Business Value:** This updates existing transactional records (e.g., if a deal's `close_status` changes from "Pending" to "Won") and appends new transactions. It explicitly avoids SCD Type 2 logic to prevent the fact table from artificially inflating in size, relying instead on the historical surrogate keys provided by the dimension tables. 