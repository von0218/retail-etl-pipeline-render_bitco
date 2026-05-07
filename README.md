# 🛒 Global Retail ETL Pipeline: Japan & Myanmar Market Analysis

This project demonstrates a robust end-to-end ETL (Extract, Transform, Load) pipeline that consolidates retail data from disparate international markets into a cloud-hosted **PostgreSQL** database on **Render**.

## 🛠️ Tech Stack
* **Language:** Python 3.x
* **Libraries:** Pandas, SQLAlchemy, Dotenv
* **Database:** PostgreSQL (Hosted on Render)
* **Environment:** GitHub Codespaces

---

## 🧪 ETL Process & Methodology

### 1. Extraction
Raw retail data was extracted from two distinct CSV sources representing different regional markets:
* `retail_sales_dataset.csv` (Japan Market)
* `SuperMarket Analysis.csv` (Myanmar Market)

### 2. Transformation
* **Data Cleaning:** Handled type-mismatch errors by stripping non-numeric characters and forcing price columns into floats to prevent calculation failures.
* **Currency Normalization:** Standardized disparate currencies into a unified USD format. Japan's JPY values were converted using a $0.0067$ multiplier, while Myanmar values were mapped to a global standard.
* **Schema Design:** Merged both datasets into a consolidated schema, adding a `store_location` flag to maintain data lineage and a `price_usd` column for global analysis.

### 3. Loading
The cleaned and transformed data (2,000 total rows) was pushed to a **PostgreSQL** instance on **Render**. The pipeline utilizes a "Drop and Recreate" strategy to ensure schema integrity during each run.

---

## 💡 Key Business Insights

Based on the successfully processed data, the following insights were generated via SQL analysis:

| Insight Category | Finding | Data Value |
| :--- | :--- | :--- |
| **Global Revenue** | Total normalized sales across all markets. | **$8,177.96 USD** |
| **Market Leader** | Myanmar is the primary revenue driver. | **$6,972.70** (MM) vs **$1,205.26** (JP) |
| **Top Category** | **Electronics** is the highest value category. | **$416.81** Total Revenue |
| **Customer Spend** | Average spend is significantly higher in Myanmar. | **$6.97** (MM) vs **$1.21** (JP) |
| **Demographics** | Female customers represent the majority. | **54.05%** of Transactions |

### 🔍 Analysis Summary
While transaction volume was equal across both regions (1,000 each), the **Myanmar market contributed 85% of total revenue**. This disparity suggests that the Myanmar dataset reflects a higher-tier product mix or higher purchasing power compared to the Japan retail dataset. Globally, **Clothing** leads in volume, but **Electronics** provides the highest revenue per unit.

---

## 🚀 How to Run
1. Clone the repository.
2. Install dependencies: 
   ```bash
   pip install -r requirements.txt
