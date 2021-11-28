from pyspark import SparkFiles
from os import environ, listdir, path
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, ShortType, BooleanType, \
    IntegerType, DoubleType, DateType
import json

def get_config():
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename for filename in listdir(spark_files_dir) if filename.endswith('config.json')]
    path_to_config_file = path.join(spark_files_dir, config_files[0])
    with open(path_to_config_file,'r') as f:
        file_content = f.read()
    json_content = json.loads(file_content)
    return json_content

def get_schema(table):
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename for filename in listdir(spark_files_dir) if filename.endswith('schema.json')]
    path_to_config_file = path.join(spark_files_dir, config_files[0])
    with open(path_to_config_file,'r') as f:
        file_content = f.read()

    json_content = json.loads(file_content)[table]['COLUMN_LIST']
    schema_list = []
    for k, v in json_content.items():
        if v == 'string':
            schema_list.append(StructField(k, StringType(), True))
        if v == 'boolean':
            schema_list.append(StructField(k, BooleanType(), True))
        if v == 'int':
            schema_list.append(StructField(k, IntegerType(), True))
        if v == 'double':
            schema_list.append(StructField(k, DoubleType(), True))
        if v == 'long':
            schema_list.append(StructField(k, LongType(), True))
        if v == 'timestamp':
            schema_list.append(StructField(k, TimestampType(), True))
        if v == 'date':
            schema_list.append(StructField(k, DateType(), True))

    schema = StructType(schema_list)
    # print('PrintLog: ', schema)
    return schema

