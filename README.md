# 🇮🇳 Scalable PAN Card Validation Pipeline

## Project Overview

This project implements a robust and scalable data pipeline to clean and validate a dataset of Permanent Account Numbers (PANs) for Indian nationals. The objective is to ensure that each PAN number adheres to the official format and is accurately categorized as either Valid or Invalid.

The pipeline handles common data quality issues and applies a comprehensive set of validation rules, demonstrating a production-ready approach to data engineering and quality assurance.

## Features & Optimizations

This pipeline was engineered with scalability in mind, featuring a two-tiered approach to handle different data volumes and environments.

* **Tier 1: Local Development (1M Records):** Optimized for fast, efficient testing on a single machine.
* **Tier 2: Distributed Processing (1B+ Records):** Scaled to handle massive datasets on a distributed cluster, achieving significant performance improvements.

**Key Data Quality and Engineering Features:**

* **Robust Data Cleaning**: The pipeline handles missing values, removes duplicates, strips leading/trailing spaces, and normalizes all characters to uppercase.
* **Comprehensive Validation Rules**: The script correctly validates that a valid PAN adheres to all rules, including its exact 10-character length, alphanumeric format, and the absence of adjacent or sequential characters.
* **End-to-End Reporting**: Generates a final summary report detailing total processed, valid, invalid, and unprocessed records.

**Core Performance Optimizations:**

* **From Weeks to Hours**: The pipeline was optimized to reduce compute time from an estimated weeks to a matter of hours when scaling to a billion-record dataset.
* **No Python UDFs**: Replaced slow, serialization-heavy Python UDFs with highly-optimized native Spark functions for all validation logic.
* **Efficient I/O**: The pipeline uses a **CSV-to-Parquet conversion** to leverage a columnar storage format, significantly reducing I/O and improving read performance.
* **Consolidated Logic**: All cleaning, validation, and reporting are performed in a single, highly-optimized pass to minimize Spark actions and re-computation.

## How to Run the Pipeline

### Prerequisites

* Python 3.8+
* PySpark
* Java 8+ (Java 17+ recommended for best performance)
* The [generator script](PAN_generator.py) to create the 1 billion record dataset for testing at scale.

### Instructions

1.  **Clone the Repository**:
    ```bash
    git clone [https://github.com/Vinilg38/PAN_Validation_Pipeline-PySpark.git](https://github.com/Vinilg38/PAN_Validation_Pipeline-PySpark.git)
    cd PAN_Validation_Pipeline-PySpark
    ```

2.  **Generate the Dataset(Optional)**:
    Run the `PAN_generator.py` to create the dataset with 1 billion rows for your validation pipeline. If this step is skipped, the default csv with 1 million rows will be used.
    
3.  **Run the Script**:
    The script will automatically convert the CSV to a Parquet file on its first run for future efficiency. tier1.py can be used to test the script for 1 million rows.
    ```bash
    spark-submit tier2.py
    ```

## Project Structure
