import io
import re
import boto3
import logging
import requests
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logger = logging.getLogger("read_s3_upload_rds")


class GlobalVariables:
    bucket_region = 'eu-central-1'
    data_url = 'https://raw.githubusercontent.com/dogukannulu/datasets/master/dirty_store_transactions.csv'
    bucket_name = '<unique_bucket_name>'
    bucket_key = '<datasets_path>/dirty_store_transactions.csv'
    database_name = '<database_name>'
    database_username = '<user_name>'
    database_password = '<password>'
    database_endpoint = '<database_endpoint>'
    database_port = 3306
    s3_client = boto3.client('s3')
    database_uri = f"mysql+pymysql://{database_username}:{database_password}@{database_endpoint}:{database_port}/{database_name}"


class ModifyColumns:
    def extract_city_name(self, string):
        cleaned_string = re.sub(r'[^\w\s]', '', string)
        city_name = cleaned_string.strip()
        return city_name

    def extract_only_numbers(self, string):
        numbers = re.findall(r'\d+', string)
        return ''.join(numbers)

    def extract_floats_without_sign(self, string):
        string_without_dollar = string.replace('$', '')
        return float(string_without_dollar)


def create_s3_bucket():
    s3=GlobalVariables.s3_client
    name=GlobalVariables.bucket_name
    region=GlobalVariables.bucket_region
    try:
        s3.create_bucket(
            Bucket = name,
            CreateBucketConfiguration = {
            'LocationConstraint': region
            }
        )
        logger.info(f"Bucket '{name}' created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            logger.warning(f"Bucket '{name}' already exists and is owned by you.")
        elif e.response['Error']['Code'] == 'BucketAlreadyExists':
            logger.warning(f"Bucket '{name}' already exists and is owned by someone else.")
        else:
            logger.warning("An error occurred:", e)


def put_object_into_s3():
    s3=GlobalVariables.s3_client
    url=GlobalVariables.data_url
    name=GlobalVariables.bucket_name
    key=GlobalVariables.bucket_key
    try:
        response = requests.get(url)
        s3.put_object(Body=response.content, Bucket=name, Key=key)
        logger.info('Object uploaded into S3 successfully')
    except ClientError as e:
        logger.warning("An error ocurred while putting the object into S3:", e)
    except Exception as e:
        logger.warning("An unexpected error occurred while putting the object into S3:", e)


def create_dataframe():
    s3=GlobalVariables.s3_client
    name=GlobalVariables.bucket_name
    key=GlobalVariables.bucket_key
    try:
        get_response = s3.get_object(Bucket=name, Key=key)
        logger.info("Object retrieved from S3 bucket successfully")
    except ClientError as e:
        logger.error("S3 object cannot be retrieved:", e)
    
    file_content = get_response['Body'].read()
    df = pd.read_csv(io.BytesIO(file_content)) # necessary transformation from S3 to pandas

    return df


def modify_dataframe(df):
    modify_columns = ModifyColumns()

    df['STORE_LOCATION'] = df['STORE_LOCATION'].apply(modify_columns.extract_city_name)
    df['PRODUCT_ID'] = df['PRODUCT_ID'].apply(modify_columns.extract_only_numbers)

    column_list = ['MRP','CP','DISCOUNT','SP']
    for i in column_list:
        df[i] = df[i].apply(modify_columns.extract_floats_without_sign)

    return df


def upload_dataframe_into_rds(df):
    table_name = 'clean_transaction'
    sql_query = f"SELECT * FROM {table_name}"
    database_uri=GlobalVariables.database_uri
    try:
        engine = create_engine(database_uri)
        logger.info('Database connection successful')
    except Exception as e:
        logger.warning('Database connection unsuccessful:', e)

    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f'Dataframe uploaded into {table_name} successfully')
        uploaded_df = pd.read_sql(sql_query, engine)
        print(uploaded_df.head())
    except Exception as e:
        logger.error('Error happened while uploading dataframe into database:', e)


def main():
    create_s3_bucket()
    put_object_into_s3()
    df_unmodified = create_dataframe()
    df_modified = modify_dataframe(df_unmodified)
    upload_dataframe_into_rds(df_modified)


if __name__ == '__main__':
    main()
