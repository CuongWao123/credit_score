# 💳 Credit Scoring System

An end-to-end **Credit Scoring System** designed to simulate real-world financial credit evaluation workflows — including **data streaming, risk modeling, analytics, and MLOps**.  
This project covers the entire lifecycle from **data ingestion (Debezium → Kafka → Spark)** to **model training, experiment tracking (MLflow)**, and **monitoring (Grafana + EvidentlyAI)**.

---

## 🚀 Project Overview
- Developed a **credit scoring workflow** using the [Credit Scoring Dataset (Kaggle)](https://www.kaggle.com/datasets/maksimkotenkov/credit-scoring-dataset) (~**3.17 GB**, with bureau, internal, and customer data).  
- Built a **real-time data pipeline** using **Debezium** to capture changes (CDC) from **PostgreSQL**, stream them through **Kafka**, and process with **Apache Spark**.  
- Stored data in a **MinIO data lake**, then built two downstream flows:
  - A **Data Warehouse** for credit analytics and visualization.  
  - A **training dataset** for machine learning–based risk modeling.  
- Used **DuckDB** and **Polars (LazyFrame)** for high-performance data analysis and EDA on large tables.  
- Applied **MLflow** for experiment tracking, model versioning, and reproducibility.  
- Managed data and model artifacts using **DVC (Data Version Control)**.  
- Deployed **Grafana** and **EvidentlyAI** dashboards for real-time performance and data drift monitoring.  
- Designed the architecture to mirror **real-world credit risk assessment** systems (bureau + internal scoring).

---

## ⚙️ Tech Stack
**Languages & Tools:**  
`Python`, `Apache Kafka`, `Apache Spark`, `Debezium`, `MinIO`, `DuckDB`, `Polars`, `MLflow`, `DVC`, `Grafana`, `EvidentlyAI`

**Key Concepts:**  
- Real-time data pipelines (CDC, Kafka Streams, Spark Structured Streaming)  
- Credit risk modeling and scoring  
- Data lake & data warehouse design (ETL & analytics)  
- Model training, tuning, and tracking (MLflow)  
- Monitoring & observability (Grafana, EvidentlyAI)  
- Data versioning & reproducibility (DVC)

---

## 🧱 System Architecture

![System Architecture](MLsystem.png)


---

## 📊 Dataset
**Dataset:** [Credit Scoring Dataset (Kaggle)](https://www.kaggle.com/datasets/maksimkotenkov/credit-scoring-dataset)  
- Size: ~**3.17 GB**  
- Includes: **Bureau data**, **Internal credit data**, and **Customer application data**  
- Provides realistic financial attributes for building and validating credit risk models.

> This dataset enables simulation of an end-to-end credit lifecycle: from application ingestion → data processing → credit scoring → monitoring.

---

## 📈 Results
- Successfully implemented a **real-time data ingestion pipeline** connecting PostgreSQL → Kafka → Spark → MinIO.  
- Created reproducible datasets and tracked experiments through **MLflow**.  
- Achieved robust, interpretable **credit risk models** with high consistency across data updates.  
- Enabled continuous **model performance monitoring** and drift detection.

---

## 🧩 Future Improvements
- Build a **FastAPI microservice** for real-time credit scoring.  
- Integrate a **feature store (Feast)** to manage reusable credit features.  
- Expand to **federated credit scoring** across multiple institutions.  
- Deploy the full stack using **Docker + Airflow** for orchestration.

---


## 📬 Author
**Nguyen Van Cuong**  
- ✉️ [cuong.nguyenvan1150@gmail.com](mailto:cuong.nguyenvan1150@gmail.com)  
- 🌐 [GitHub: CuongWao123](https://github.com/CuongWao123)  

---

⭐ *If you found this project helpful, consider giving it a star on GitHub!*


