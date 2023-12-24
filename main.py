#!/usr/bin/env python3
import findspark 
findspark.init()
findspark.find()
#import pyspark
findspark.find()


import pandas as pd

#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from metadata_extraction import extract_metadata, write_metadata_success_to_json
#from gps_extraction import extract_gps_data
# creating a spark session
spark = SparkSession.builder.appName("SparkMetadataExtraction").getOrCreate()


# Set the path to the TSV file - converted it into csv but either way, still the same approach
tsv_path = '/home/rajaoferason/Downloads/open-images-dataset-train0.csv'

# Read the TSV file into a DataFrame
df = pd.read_csv(tsv_path, header=None, names=['Image URL'], skiprows=1, sep=',')

if __name__ =="__main__":
    
    # getting the output
    results = df['Image URL'].apply(extract_metadata).tolist()
    
    metadata_success_list, metadata_failure_list = zip(*results)
    
    metadata_failure_list = [data for data in metadata_failure_list if data is not None]    
    
    
    # Writing the outputs to HDFS
    output_path_specific = '/gps_data_extraction_success'
    write_metadata(spark,metadata_success_list, output_path_specific, use_specific_schema=True)
    
    # Example usage with default schema
    output_path = '/'
    write_metadata_success_to_json(metadata_success_list, output_path)
    
    
    # read from hdfs
    ## Define the schema based on the JSON structure
    custom_schema =  StructType([
                StructField("URL", StringType(), True),
                StructField("Format", StringType(), True),
                StructField("Mode", StringType(), True),
                StructField("Size", ArrayType(IntegerType()), True),
                StructField("EXIF", StructType([
                    StructField("GPS", MapType(StringType(), StringType()), True),
                #    # Add other fields as needed
                ]), True),
    ])

    ## Specify the path to the JSON file in HDFS
    json_file_path = "hdfs://localhost:50000/metadata_images_success.json" #/metadata_images_success.json"

    ## Read the JSON file into a DataFrame
    metadata_df = spark.read.schema(custom_schema).json(json_file_path)

    ## Show the DataFrame
    metadata_df.show()



    
    # extracting gps data
    hdfs_output_path = "/gps_data.json"
    ## Extract GPS data into a new DataFrame
    gps_df = metadata_df.select(
        col("URL"),
        col("EXIF.GPS.1").alias("direction1"),  # Assuming direction1 is at path EXIF.0th.1
        col("EXIF.GPS.3").alias("direction2"),  # Assuming direction2 is at path EXIF.0th.3
        col("EXIF.GPS.4").alias("latitude"),  # Assuming latitude is at path EXIF.0th.4[0][0]
        col("EXIF.GPS.2").alias("longitude")  # Assuming longitude is at path EXIF.0th.2[0][0]
    )

    ## Show the new DataFrame
    gps_df.dropna().show()


    ## Save the GPS DataFrame to HDFS in JSON format
    gps_df.write.json(hdfs_output_path, mode="overwrite")

    ## Stop the Spark session
    spark.stop()