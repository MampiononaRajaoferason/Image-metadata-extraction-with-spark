# Image-metadata-extraction-with-spark

This project extracts metadata, including format, mode, size, and EXIF data, from a list of image URLs. The extracted metadata is then stored in HDFS (Hadoop Distributed File System) in json format - for convenience purpose . The project is based on a real industry case where images were originally sourced from a data lake in Amazon S3.

## Prerequisites

Before running the project, ensure you have the following installed:

- Python 3
- Apache Spark
- Hadoop (if using HDFS)
- Required Python packages: findspark, pyspark, pandas, Pillow, piexif, boto3

Install the required Python packages:

```bash
pip install findspark pyspark pandas Pillow piexif boto3
```
# Data source
This project uses the following dataset : https://www.kaggle.com/datasets/iamsouravbanerjee/indian-food-images-dataset/

# Usage

Update the TSV/CSV file path in main.py:

```bash
tsv_path = '/path/to/your/tsv_or_csv_file.csv'
```
Run the extraction script:
```bash
python main.py
```
This script will extract metadata from the images and store the results in HDFS.

# Project Structure
metadata_extraction.py: Contains the functions for extracting metadata.

main.py: The main script to run the metadata extraction and store the results in HDFS.
# HDFS Output
Metadata for successful extractions is stored in the success_output folder in HDFS.

Metadata for failed extractions (errors) is stored in the failure_output folder in HDFS.
### Additional Notes
Adjust the decoding method in metadata_extraction.py if the EXIF data contains other encodings.

json format was chosen due to the fact the EXIF data contains nested dictionaries, making querying and extracting useful information easier in this chosen format.
# Alternatives
Instead of HDFS, consider storing the metadata in a relational database for easy querying.
Utilize cloud-based services like AWS Glue for scalable and serverless ETL jobs.


# Real Industry Case
In the original industry case, images were sourced from Amazon S3 using the boto3 library to interact with AWS S3. The output metadata was saved back into S3 for further analysis and visualization. For this replica, images from Kaggle were used to demonstrate the extraction of metadata and GPS information.


