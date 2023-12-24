#!/usr/bin/env python3

import findspark 
findspark.init()
findspark.find()
#import pyspark
findspark.find()


import requests
from PIL import Image
from io import BytesIO
import piexif

from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, MapType

from pyspark.sql.functions import col



def convert_str_to_numeric(x):
    return int(x)
    

def convert_tuple_values_to_int(v):
    #list_to_return = []
    v_list = []
    for val in v:
        print(val,v)
        if len(val)==1: 
            print('-1-val---',val)
            v_list.append([int(x) for x in list(val)][0])
        else:
            print('--biggerthan-1--')
            print('--val---',val)
            v_list.append([int(x) for x in list(val)])
    return v_list

def gps_data_to_correct_format(gps):
    converted_gps = gps
    for k,v in converted_gps.items():
    
        #num = v.isnumeric()
        #print(type(v))
        if type(v) is tuple:
            print("key-valeu===",k,v)
            #v_list =[]
            #for val in v:
            #    #list_items.append(val)
            #    print ('value---',list(val))
            #    v_list.append([int(x) for x in list(val)])
            #    print ('value_to_keep==', v_list)
            converted_gps[k] = convert_tuple_values_to_int(v)
        elif v.isnumeric():
            #print("key-value===", k,v)
            converted_gps[k] = convert_str_to_numeric(v)
            
    return converted_gps

def convert_bytes_to_strings(obj):
    if isinstance(obj, bytes):
        try:
            return obj.decode('utf-8')
        except UnicodeDecodeError:
            # If decoding fails, return the original bytes
            return obj.decode('ascii', 'ignore')
    elif isinstance(obj, dict):
        return {key: convert_bytes_to_strings(value) for key, value in obj.items()}
    elif isinstance(obj, tuple):
        return tuple(convert_bytes_to_strings(item) for item in obj)
    elif isinstance(obj, list):
        return [convert_bytes_to_strings(item) for item in obj]
    else:
        return str(obj)
    
    

def extract_metadata(image_url):
    metadata_success = None
    metadata_failure = None

    try:
        # Download the image
        response = requests.get(image_url)
        response.raise_for_status()

        # Open the image using Pillow
        image = Image.open(BytesIO(response.content))

        # Extract EXIF data using piexif
        exif_data = {}
        try:
            exif_data = piexif.load(image.info['exif'])
            
            # Convert bytes to strings for all fields
            exif_data = convert_bytes_to_strings(exif_data)
            #print('---exif_data---',exif_data)
            
            # Convert bytes to string for relevant fields
            #for key, value in exif_data.items():
            #    if isinstance(value, bytes):
            #        exif_data[key] = value.decode('utf-8')
        except (KeyError, piexif.InvalidImageDataError):
            pass
        
        
        # Convert gps_exif to correct format
        exif_data['GPS'] = gps_data_to_correct_format(exif_data['GPS'])
        # Extract metadata for success case
        metadata_success = {
            'URL': image_url,
            'Format': image.format,
            'Mode': image.mode,
            'Size': image.size,
            'EXIF': exif_data
        }
        
    except Exception as e:
        # Extract metadata for failure case
        metadata_failure = {'URL': image_url, 'Error': str(e)}

    return metadata_success, metadata_failure



def write_metadata_success_to_json(metadata_success_list, output_path):
    """
    Write metadata success list to HDFS in JSON format.

    Parameters:
    - metadata_success_list: List of metadata success dictionaries
    - output_path: Output path in HDFS
    """
    # Filter out None values (failure cases)
    metadata_success_list = [metadata for metadata in metadata_success_list if metadata is not None]
    # Convert metadata success list to JSON string
    metadata_success_json = json.dumps(metadata_success_list)
    #print(metadata_success_json)

    # Write JSON string to a local file
    local_output_path =  '/home/rajaoferason/Spark-projects/metadata_images_success.json'
    with open(local_output_path, 'w') as json_file:
        json_file.write(metadata_success_json)

    # Upload the local file to HDFS
    os.system(f'hadoop fs -put  -f {local_output_path} {output_path}')





# getting the output
#results = df['Image URL'].apply(extract_metadata).tolist()

#metadata_success_list, metadata_failure_list = zip(*results)

# save the outputs as parquets in hdfs


# creating tables for analysis

## table 1


## table 2 for the visualization and analysis of GPS data




'''
spark-submit \
  --class YourMainClass \ #ommitted in python
  --master yarn \
  --deploy-mode cluster \
  --num-executors 5 \
  --executor-cores 2 \
  --executor-memory 4G \
  --conf spark.executor.memoryOverhead=1G \
  your-application.jar # your-application.py in our case 

'''


'''
spark-submit \
  --py-files your_python_script.py \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 5 \
  --executor-cores 2 \
  --executor-memory 4G \
  --conf spark.executor.memoryOverhead=1G \
  your_python_script.py
'''

