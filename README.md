# Image-metadata-extraction-with-spark

This project extracts metadata, including format, mode, size, and EXIF data, from a list of image URLs. The extracted metadata is then stored in HDFS (Hadoop Distributed File System) in Parquet format. The project is based on a real industry case where images were originally sourced from a data lake in Amazon S3.

## Prerequisites

Before running the project, ensure you have the following installed:

- Python 3
- Apache Spark
- Hadoop (if using HDFS)
- Required Python packages: findspark, pyspark, pandas, Pillow, piexif, boto3

Install the required Python packages:

```bash
pip install findspark pyspark pandas Pillow piexif boto3
