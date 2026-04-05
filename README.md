# py-pgsql-etl-pipeline
Python ETL pipeline: load, clean &amp; insert CSV data into PostgreSQL

# Python PostgreSQL ETL Pipeline

A fully functional **Python-based ETL (Extract, Transform, Load) pipeline** that reads CSV data, cleans it, and inserts it into a PostgreSQL database.  
This project demonstrates **data engineering fundamentals** including database interaction, environment management, and portfolio-ready coding practices.

---

## 🔹 Features

- Reads CSV data using **pandas**  
- Cleans and transforms data (handling missing values, date formatting)  
- Inserts data into **PostgreSQL** using **psycopg2**  
- Uses `.env` for **secure database credentials**  
- Project structured for **version control** (Git) and reproducibility  
- Ready for **extension into a full data pipeline** or integration with **Airflow / Cloud storage**

---

## 📂 Project Structure

```text
data_pipeline/
│── main.py          # ETL pipeline code
│── db_test.py       # Optional DB connection test script
│── requirements.txt # Project dependencies
│── .gitignore       # Ignore venv, .env, pycache, etc.
│── .env.example     # Template for database credentials
│── data/
│   └── cleaned_data_sample.csv # Sample CSV data
│── README.md        # Project documentation


