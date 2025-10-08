# Tweets Analyzer (PySpark Project)

This project is my **first experience with Apache Spark** — built to learn and explore how to process and analyze real-world datasets efficiently using **PySpark**.

I would truly appreciate **any feedback or suggestions** for improvement.  
The goal is to gain hands-on experience with data loading, cleaning, and analysis in a distributed environment.

---

## How to Run the Project

### 1. Clone the repository
```bash
git clone https://github.com/DataDany/mini-data-analyzer.git
cd mini-data-analyzer
```

### 2. Create and activate a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate     # macOS / Linux
# .venv\Scripts\activate      # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the main script
```bash
python main.py
```

## Project Overview

The project is divided into **three main modules**:

### 1. `loaders/`
Responsible for reading and combining CSV datasets (e.g. COVID-19, financial, and GRAMMYs tweets).  
Each dataset is loaded into a Spark DataFrame with unified structure and metadata columns (`load_from`, `category`).

**Key tasks:**
- Reading multi-line CSVs
- Handling headers, quotes, and escape characters
- Unioning multiple datasets with consistent schema

---

### 2. `cleaners/`
Handles **data preprocessing and normalization** before analysis.  
It ensures consistent data types and prepares columns for aggregation.

**Key operations:**
- Cleaning and splitting hashtags
- Casting columns to correct data types (dates, timestamps, numbers)
- Converting `is_retweet` strings to Boolean values

---

### 3. `analyzers/`
Contains **aggregation and search logic** for exploring tweet data.

**Examples of implemented analyses:**
- Most frequent hashtags   
- Average number of followers per location  
- Most active users  
- Tweets per source (e.g. *Twitter for iPhone*, *Web App*)  
- Keyword and location-based tweet searches

---


