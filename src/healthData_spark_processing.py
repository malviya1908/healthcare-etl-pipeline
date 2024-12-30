from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import google.cloud.logging
import logging

# Initializing Google Cloud Logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
logger = logging.getLogger('healthcare-pipeline')

# Logging helper function
def log_pipeline_step(step, message, level='INFO'):
    if level == 'INFO':
        logger.info(f"Step: {step}, Message: {message}")
    elif level == 'ERROR':
        logger.error(f"Step: {step}, Error: {message}")
    elif level == 'WARNING':
        logger.warning(f"Step: {step}, Warning: {message}")
        
# Function for Creating a spark session
def create_sparkSession(app) :
    return SparkSession.builder.appName(app).getOrCreate()


# Function to read the file
def read_file_into_dataframe(spark, fileType, gcs_source_bucket, header=True, inferschema=True, mode="PERMISSIVE") :
    df = spark.read.format(fileType)\
                    .option("header", header)\
                    .option("inferschema", inferschema)\
                    .option("mode", mode)\
                    .load(gcs_source_bucket)
    return df


# Function to filter the invalid records
def invalid_records(df) :
    invalid_df = df.filter((" OR ").join([f"{column} IS NULL" for column in df.columns]))
    return invalid_df

def valid_records(spark, patients_df, df) :
    # Creating a dataframe for invalid ids (Single Column "id" Dataframe)
    invalid_ids = [row["id"] for row in df.collect()]
    invalid_ids_df = spark.createDataFrame([(id,) for id in invalid_ids], ["id"])
    
    # Getting the valid records by joining using Left-Anti join
    valid_df = patients_df.join(broadcast(invalid_ids_df), on="id", how="leftanti")
    return valid_df


# Function to change the date format of the column admission_date
def change_date_format(df) :
    admission_date_df = df.withColumn("admission_date", to_date("admission_date", "dd-MM-yyyy"))
    return admission_date_df


def process_data() :
    # variables setup
    gcs_source_bucket = "gs://dataproc_bucket_prac/patients_data/data/patients_data.csv"
    bq_table = "synthetic-nova-438808-k6.patients_dataset.patientsData"
    patients_invalid_bucket = "gs://dataproc_bucket_prac/patients_data/invalid_data/"
    patients_valid_bucket = "gs://dataproc_bucket_prac/patients_data/valid_data/"
    bq_temp_bucket = "gs://dataproc_bucket_prac/patients_data/bq_temp_bucket/"
    
    try :
        # Initializing Spark session
        log_pipeline_step("Spark Session", "Creating a Spark Session.")
        spark = create_sparkSession("healthcare-pipeline")
        
        # Reading the data
        log_pipeline_step("Data Ingestion", "Reading raw data from gcs.")
        patients_df = read_file_into_dataframe(spark, "csv", gcs_source_bucket)
        
        # Checking for invalid columns dynamically (Columns that are null is this case)
        log_pipeline_step("Data Validation", "Checking for INVALID records.")
        invalid_records_df = invalid_records(patients_df)
        if invalid_records_df.count() > 0 :
            log_pipeline_step("Data Validation for Invalid records Completed", f"Invalid Records : {invalid_records_df.count()}", level='WARNING')
        
        # Filtering the valid records only (Using Left-Anti Join)
        log_pipeline_step("Data Validation", "Checking for VALID records.")
        valid_records_df = valid_records(spark, patients_df, invalid_records_df)
        
        # Changing the date format for admission_date column in the valid_records_df
        log_pipeline_step("Data Transformation", "Changing the date format.")
        final_patients_df = change_date_format(valid_records_df)
        
        # Writing the invalid records to the patients_invalid_bucket
        if invalid_records_df.count() > 0 :
            log_pipeline_step("Invalid Data", f"Writing INVALID records to the bucket : {patients_invalid_bucket}")
            invalid_records_df.write.mode("append").option("header", "true").csv(patients_invalid_bucket)
            log_pipeline_step("Data Write.", f"Successfully wrote the INVALID records to the bucket : {patients_invalid_bucket}")
            
        # Writing the valid records to BigQuery table and a patients_valid_bucket (For backup)
        # First writing valid records to the bucket 
        log_pipeline_step("Valid Data", f"Writing Final VALID records to the bucket : {patients_valid_bucket}")
        final_patients_df.write.mode("append").option("header", "true").csv(patients_valid_bucket)
        log_pipeline_step("Data Write.", f"Successfully wrote the Final VALID records to the bucket : {patients_valid_bucket}")
    
        # Writing valid records to the BigQuery Table
        log_pipeline_step("Data Write.", f"Writing Final VALID records to the BigQuery Table : {bq_table}")
        final_patients_df.write \
            .format("bigquery") \
            .option("table", bq_table) \
            .option("temporaryGcsBucket", bq_temp_bucket) \
            .mode("append") \
            .save()
        log_pipeline_step("Data Write.", f"Successfully wrote the Final VALID records to the BigQuery Table : {bq_table}")
    except Exception as e :
        print(f"Error : {str(e)}")
        
    finally:
        # Stopping the Spark Session
        if spark:
            spark.stop()
            log_pipeline_step("End Spark Session", f"Successfully stopped the Spark Session")
            
            
# Execute the processing function
if __name__ == "__main__":
    log_pipeline_step("Pipeline Started", "Healthcare data processing pipeline initiated.")
    process_data()
    log_pipeline_step("Pipeline Ended", "Healthcare data processing pipeline completed.")