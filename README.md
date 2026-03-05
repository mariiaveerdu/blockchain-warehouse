# Resilient Crypto Intelligence Platform
### End-to-End Modern Data Stack Implementation

## 📌 1. Executive Summary
In high-volatility financial markets, data latency and poor quality directly impact strategic decision-making. This project implements a fully automated, self-healing data platform designed to ingest, transform, and visualize cryptocurrency market trends with high integrity.

**The Challenge:**
* **Data Fragmentation:** Raw API outputs are inconsistent and require significant normalization.
* **Reliability Gaps:** Corrupted data often bypasses traditional pipelines, leading to inaccurate reporting ("Garbage In, Garbage Out").
* **Operational Overhead:** Manual updates are unsustainable for real-time or daily market monitoring.

---

## 🏗️ 2. System Architecture
The platform utilizes a **Medallion Architecture** (Bronze/Silver/Gold) to ensure a structured and scalable data flow, leveraging a cloud-native tech stack.



### 🛠️ Technical Specifications
* **Data Warehouse:** [MotherDuck](https://motherduck.com/) (Cloud-native DuckDB) for high-performance analytical processing.
* **Transformation Engine:** [dbt (data build tool)](https://www.getdbt.com/) for modular SQL modeling and automated testing.
* **Orchestration:** GitHub Actions for scheduled execution (Cron-based).
* **Quality Assurance:** CI/CD integration via `dbt test` to enforce strict data contracts.
* **Visualization:** Streamlit Cloud for real-time Business Intelligence.

---

## 🔄 3. Data Lifecycle & Workflow

### Phase I: Ingestion (Bronze Layer)
A high-performance Python engine handles data extraction from the **CoinCap API**. Data is streamed directly into **MotherDuck** using `duckdb` Python bindings, ensuring efficient columnar storage and minimizing local resource consumption.

### Phase II: Transformation & Modeling (Silver/Gold Layers)
The transformation logic is decoupled into specialized layers:
* **Staging (Silver):** Normalization of Unix timestamps to ISO standards, field renaming for business alignment, and schema enforcement.
* **Marts (Gold):** Analytical tables (`mart_crypto_trends`, `mart_crypto_stats`) pre-aggregated for BI consumption, optimizing dashboard load times.

### Phase III: The "Safety Net" (CI/CD)
Reliability is managed through a GitHub Actions workflow acting as a gatekeeper. Before any data reaches the Gold layer, the following occurs:
* **Schema Tests:** Validation of `unique` keys and `not_null` constraints.
* **Business Logic Tests:** Enforcement of `accepted_range` for prices to prevent negative or outlier values from corrupting reports.
* **Result:** Any test failure triggers an immediate halt of the pipeline, preventing the dashboard from updating with compromised data.



---

## 🚀 4. Technical Hurdles & Engineering Solutions
* **Schema Conflict Resolution:** Identified and resolved dbt compilation errors by centralizing model documentation within a single `schema.yml`, increasing project maintainability.
* **CI Memory Optimization:** Fine-tuned dbt execution commands within GitHub Actions to prevent process termination, ensuring stable deployment in resource-constrained environments.
* **Data Integrity Enforcement:** Implemented strict null-value filtering in the Silver layer to handle inconsistent API responses for emerging assets.

---

## 📊5.  Business Intelligence Interface (Streamlit)

The final layer of the platform is an interactive BI Dashboard designed for financial analysis. It serves as the primary interface for stakeholders to monitor market movements backed by validated data.



### 💡 Key Analytical Features:
* **Real-Time Market KPIs:** Instant visibility into Bitcoin (BTC) performance and global market volatility metrics, sourced directly from the Gold layer of the data warehouse.
* **Dynamic Performance Tracking:** An interactive "Top Gainers" visualization that identifies assets with the highest 24h appreciation, allowing for rapid trend identification.
* **Data Auditability:** A dedicated "Raw Data Inspection" module that displays the most recent ingestion timestamps, ensuring full transparency of the data lineage.
* **Responsive Design:** Optimized for both desktop and mobile viewing to ensure accessibility for on-the-go market monitoring.

### 🛡️ Data Reliability Guarantee
Every data point displayed on this dashboard has undergone a **triple-validation process**:
1.  **Ingestion Check:** Verified successful API handshake.
2.  **Schema Enforcement:** dbt-tested for null values and unique constraints.
3.  **Business Logic Guardrails:** Automated rejection of outlier or corrupted price values via `dbt-utils` ranges.

---

## 🔗 Access the Live Environment
The production environment is hosted on Streamlit Cloud and is automatically synchronized with the MotherDuck warehouse.

👉 **[Launch Live Dashboard](https://blockchain-warehouse.streamlit.app/)**

## 📊 6. Key Results
* **100% Automation:** Elimination of manual intervention for daily market updates.
* **Zero-Corruption Policy:** 100% of the visualized data is verified through automated quality gates.
* **Infrastructure-as-Code:** The entire pipeline, from ingestion to testing, is version-controlled and reproducible.

---

## 📂 Repository Structure
* `extract_data.py`: Automated ingestion script.
* `models/`: dbt transformation layers.
* `models/schema.yml`: Data contracts and test definitions.
* `.github/workflows/pipeline.yml`: Orchestration and CI/CD engine.
* `app.py`: Streamlit BI Dashboard.

---

## 🛠️ Deployment & Local Setup
1. **Clone the repository** and install dependencies: `pip install -r requirements.txt`.
2. **Configure Authentication:** - Set your `MOTHERDUCK_TOKEN` in your environment variables.
   - Ensure your `profiles.yml` is in the project root (or `~/.dbt/`) with the MotherDuck adapter configured.
3. **Execute the full pipeline:** (in bash)
   
   `dbt deps   # To install dbt packages`
   
   `dbt build  # Runs models and quality tests simultaneously`
