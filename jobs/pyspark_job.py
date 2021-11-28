import json
from dependencies import get_configurations
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,row_number,concat_ws,collect_list,dense_rank,count,current_timestamp,current_date,to_date
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder.appName('pyspark_job').config('spark.files','configs/schema.json,configs/config.json').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    config = get_configurations.get_config()
    users = extract_data(spark,config,'linkedin_users')
    users_company = extract_data(spark,config,'users_company_details')
    users_connection = extract_data(spark, config, 'users_x_connections')
    transform_data(users,users_company,users_connection)
    return

def extract_data(spark, config, table):
    location = config['data_location']
    location = location + table +'.csv'
    table_schema = get_configurations.get_schema(table)
    df = spark.read.csv(location, header=True, multiLine=True, schema=table_schema, escape="\"").drop_duplicates()
    return df

def transform_data(users,users_company,users_connection):
    #     Please solve following problem statements:
    # 1.	List the users who have at least one connection.
    # 2.	List the most common employer among all users.
    # 3.	For each user list the oldest connection age and connected user.
    # 4.	List employer and all the users in single column (comma separated).
    # 5.	List currently who is the oldest employee of Intuit?
    print('List the users who have at least one connection.')
    users_connection_details = users_connection.join(users,users_connection['connection_user_id'] == users['user_id']).select(users_connection['*'], users['name'].alias('connection_name'))
    users_one_connection = users_connection_details.groupBy(col('user_id')).agg(count(col('connection_user_id')).alias('num_count'))
    users_one_connection = users_one_connection.filter(users_one_connection['num_count'] >= 1)
    users_one_connection.show()

    print('List the most common employer among all users.')
    common_employer_agg = users_company.groupBy(col('company_name')).agg(count(col('user_id')).alias('emp_count'))
    window_spec = Window().orderBy(col("emp_count").desc())
    common_employer = common_employer_agg.withColumn('rn',row_number().over(window_spec)).where(col('rn')==1).select('company_name')
    common_employer.show()

    print('For each user list the oldest connection age and connected user.')
    window_spec = Window.partitionBy(col('user_id')).orderBy(col('connected_on').asc())
    oldest_connections_per_user = users_connection_details.withColumn('rn',row_number().over(window_spec)).\
        withColumn('connection_age', current_date() - to_date(users_connection_details['connected_on'])).\
        filter(col('rn')==1).select('user_id','connection_user_id','connected_on','connection_name','connection_age')
    oldest_connections_per_user.show()

    print('List employer and all the users in single column (comma separated).')
    company_emp = users_company.join(users, users_company['user_id']==users['user_id']).select(users_company['*'],users['name'])
    window_spec_2 = Window.partitionBy(col('user_id')).orderBy(col('start_date').desc())
    company_emp = company_emp.withColumn('rn',row_number().over(window_spec_2)).filter(col('rn')==1)
    company_emp_list = company_emp.groupBy('company_name').agg(concat_ws(",",collect_list(company_emp['name'])).alias('emp_list'))
    company_emp_list.show()

    print('List currently who is the oldest employee of Intuit?')
    window_spec_1 = Window.partitionBy(col('company_name')).orderBy(col('start_date').asc())
    old_emp_intuit = company_emp.withColumn('rn_old',dense_rank().over(window_spec_1)).filter(company_emp['company_name']=='Intuit').\
        filter(col('rn_old')==1).select('user_id','name','company_name','start_date','end_date')
    old_emp_intuit.show()

    return


if __name__ == '__main__':
    main()