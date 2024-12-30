# End-to-End Healthcare Data Pipeline with Apache Spark and Google Cloud
## Table of Contents
1. [Project Overview](#project-overview)
2. [Project Architecture](#project-architecture)
3. [Data Flow](#data-flow)
4. [Prerequisites](#prerequisites)
5. [Technologies Used](#technologies-used)
6. [Input Files](#input-files)
7. [Output Files](#output-files)
8. [Conclusion](#conclusion)


## Project Overview
The **Healthcare Data Processing Pipeline** automates the extraction, transformation, and validation of healthcare patient data using **Apache Spark** and **Google Cloud Platform.** It ingests raw data from Google Cloud Storage (GCS), validates the records, and separates **valid** and **invalid data.**
**Invalid records** are written to **GCS** for review, while the **valid records** are transformed (e.g., date format change) and saved both to **GCS (for backup)** and **BigQuery** for further analysis. This ensures **data integrity** and provides a reliable **backup** of processed data.

#### Key features of the pipeline include:
- **Data Validation**: Identifies and filters invalid records based on missing or `NULL` values.
- **Date Transformation**: Standardizes the `admission_date` field into the `YYYY-MM-DD` format.
- **Data Storage**: Saves valid and invalid records into GCS and uploads valid records to BigQuery for further analysis.

## Project Architecture
The project follows a modular architecture, leveraging Spark for data processing, GCS for storage, and BigQuery for data analysis. Below is a visual representation of the architecture:
![Project Architecture](https://github.com/malviya1908/healthcare-etl-pipeline/blob/main/architecture/architecture.png)

### <ins>Components</ins> :
1. **GCS (Google Cloud Storage)**:
     - **Description**: Cloud-based object storage for raw data, valid/invalid records, and temporary storage.
     - **Role**: Stores healthcare data and backups (valid/invalid records).
  
  2. **Dataproc**:
     - **Description**: Fully managed cloud service for running Apache Spark clusters.
     - **Role**: Manages Apache Spark clusters for data processing and transformation.

3. **Apache Spark**:
     - **Description**: Distributed computing framework for large-scale data processing.
     - **Role**: Processes data, performs validation, transformation, and writes results.

4. **BigQuery**:
     - **Description**: Fully-managed data warehouse for fast SQL-based analytics.
     - **Role**: Stores final valid records for querying and analysis.

5. **Cloud Logging**:
     - **Description**: Managed logging service for tracking and analyzing logs.
     - **Role**: Logs pipeline steps, errors, and system performance for debugging and monitoring.

## Data Flow

1. **Data Ingestion**: The pipeline starts by reading raw CSV files from GCS.
2. **Data Validation**: Invalid records (e.g., records with null values) are identified and separated.
3. **Data Transformation**: The `admission_date` field is standardized into the `YYYY-MM-DD` format.
4. **Data Output**:
   - Invalid records are written back to GCS in the `invalid_data` folder.
   - Valid records are written both to GCS (for backup) and BigQuery (for analytics).
5. **Logging**: Throughout the pipeline, logs are generated for each step using Google Cloud Logging for monitoring and debugging purposes.



## Prerequisites
Before you can run this project, you need to ensure you have the following :
- **Google Cloud Platform (GCP) Account :** You need an active GCP account for setting up Dataproc cluster and interacting with BigQuery.
- **Apache Spark :** The project uses Apache Spark for data processing. If you are not using Dataproc, you must install Spark on your local machine.
- **Python (v3.6+) :** Required for running the PySpark code.
- **Google Cloud SDK :** To interact with GCP services like BigQuery and Dataproc cluster.

## Technologies Used
1. **Programming Language**: Python
2. **Big Data Framework**: Apache Spark (PySpark)
3. **Cloud Platform**: Google Cloud Platform (GCP)
4. **GCP Technologies :**
   - **Google Cloud Storage (GCS)**: For storing input data and output files.
   - **Dataproc**: Managed Spark and Hadoop service used for data processing.
   - **BigQuery**: Fully managed data warehouse where transformed data is loaded for further analysis.
   - **Google Cloud Logging**: Cloud-native logging solution used to track the execution of pipeline steps and capture logs for debugging and monitoring.


## Input Files
The input data for this pipeline is stored in a CSV file located in a Google Cloud Storage bucket:
The CSV file contains healthcare records, including columns such as:
- `id`: A unique identifier for the patient.
- `admission_date`: The date of the patient's admission, which will be transformed.
- Other fields containing patient information (e.g., age, diagnosis, etc.).


## Output Files
The pipeline generates the following output files:
1. **Valid Data**: Processed records that passed validation are saved in a **Google Cloud Storage bucket* and also uploaded to a **BigQuery** table for further analysis.
2. **Invalid Data**: Records that contain null or missing values are also saved in the GCS bucket.


## Conclusion
The Healthcare Data Processing Pipeline effectively processes healthcare data by performing the following tasks:
- **Data Validation**: Identifying invalid records based on null or missing values.
- **Data Transformation**: Standardizing the date format for the `admission_date` column.
- **Data Storage**: Storing the valid and invalid records in Google Cloud Storage and uploading the final valid records to BigQuery for analysis.

This pipeline ensures that the healthcare data is cleaned, validated, and transformed into a structured format suitable for analytics and reporting.



  
