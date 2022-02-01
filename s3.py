
import boto3
import config
import pandas as pd
import io
from smart_open import smart_open

s3_client = boto3.client(
        service_name=config.service_name,
        region_name=config.region_name,
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key)

# Get Column names, Columns count, rows count and return Dataframe from S3 object

def dataframe_s3(s3_uri):
    bucket_name=s3_uri.split('/')[2]
    s3_file_path=s3_uri.split(bucket_name)[1][1:]
    s3_str=f's3://{config.aws_access_key_id}:{config.aws_secret_access_key}@{bucket_name}/{s3_file_path}'

    print("S3 file path: ","s3://"+bucket_name+"/"+s3_file_path)

    # Get Columns name and Columns count from S3 object
    try:
            resp = s3_client.select_object_content(
                Bucket=bucket_name,
                Key=s3_file_path, 
                ExpressionType='SQL',
                Expression='''SELECT * FROM s3object limit 1''',
                #InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
                InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
                OutputSerialization={'CSV': {}},
            )
            #print("Connection stablished with s3...")
            for event in resp['Payload']:
                if 'Records' in event:
                    records = event['Records']['Payload'].decode('utf-8')
                    columns=records.split(',')
                    #print("-----------------------------------------------------------------------\n")
                    print("S3 Sink file Columns : ",tuple(columns))
                    print("S3 Sink file Columns count: ",len(columns))

    except Exception as e:
            print(e)

    # Get Rows count from S3 object
    try:
            resp = s3_client.select_object_content(
                Bucket=bucket_name,
                Key=s3_file_path, 
                ExpressionType='SQL',
                Expression='''SELECT count(*) FROM s3object''',
                InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'}, # Comment this line while reading Parquet file
                #InputSerialization={'CSV': {"FileHeaderInfo": "None"}, 'CompressionType': 'NONE'},
                OutputSerialization={'CSV': {}},
            )
            for event in resp['Payload']:
                if 'Records' in event:
                    records = event['Records']['Payload'].decode('utf-8')
                    #print("-----------------------------------------------------------------------")
                    print("S3 Sink file records count: ",records)

    except Exception as e:
            print(e)
            
    return pd.read_csv(smart_open(s3_str))#,nrows=chunks)





# def dataframe_s3(s3_uri):
#     bucket_name=s3_uri.split('/')[2]
#     s3_file_path=s3_uri.split(bucket_name)[1][1:]
#     try:
#         #'test/file2.csv'
#         resp=s3_client.get_object(Bucket=bucket_name,Key=s3_file_path)
#         status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
#         if status == 200:
#             print("Getting S3 object, Please wait... ")
#             return pd.read_csv(io.BytesIO(resp['Body'].read()))
#         else:
#             print(f"Unsuccessful S3 get_object response. Status - {status}")
#     except Exception as e:
#         print(e)
