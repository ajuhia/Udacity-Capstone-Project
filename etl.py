#import important libraries
import os
import re
import calendar
import numpy as np
import pandas as pd
import configparser
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col, upper, year, month, dayofmonth, hour, weekofyear, date_format, isnan


pd.set_option('max_colwidth',20)

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_S3_BUCKET']=config['AWS']['AWS_S3_BUCKET']


def createSparkSession():
    """
         This function creates a spark session object and returns it.
    """
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .enableHiveSupport().getOrCreate()
    
    return spark



def processUSImmigrationData(sas_input_path_prefix,sas_input_path_suffix,spark,output_path):
    '''
        This function reads SAS data,processes it and saves it parquet format to create fact_immigration and 
        dimension tables:dim_flight and dim_calendar.
        Input: prefix and suffix strings to calculate input path, spark session object, S3 path.
        Output: Print out result.
    '''
    #Create a list of columns to extracted from SAS dataset and dict to rename these columns as per our data model
    sas_cols=['cicid','arrdate','i94mon','i94yr','visatype','i94visa','i94cit','i94port','i94addr','i94mode',
              'airline','fltno','gender','biryear','depdate']
    new_sas_cols_dict = {'arrdate':'arrival_date', 'i94mon':'month','i94yr':'year','visatype':'visa_category',
                         'i94visa':'visa_type','i94cit':'country_of_departure','i94port':'port_of_entry',
                         'i94addr':'us_address_state','i94mode':'mode_of_entry','airline':'airline', 'fltno':'flight_no',
                         'biryear':'birth_year','depdate':'departure_date_from_us'}

    sas_df = pd.DataFrame(columns=sas_cols)  #create a pandas df which will hold SAS data   
    months_list = [(x[0:3]).lower() for x in calendar.month_name[1:]] #list all months in a year
    
    #Read immigration files from disk, transform to desired schema and save the files to S3 in paraquet format
    for month in months_list[3:4]:   #remove slicing([3:4]) to retrieve data for all months, this slicing returns month april only
        file_path = f"{sas_input_path_prefix}{month}{sas_input_path_suffix}"
        print(f"Reading file:{file_path}") 
        df = pd.read_sas(file_path, 'sas7bdat', encoding="ISO-8859-1")
        df =  df.loc[:,sas_cols]
        sas_df = sas_df.append(df,ignore_index= True)
        print(f'{file_path} file read sucessfully!')
    
    #Data Transformation       
    sas_df.rename(columns=new_sas_cols_dict, inplace=True) #Rename the columns as per data model  
    sas_df['arrival_date'] = pd.to_datetime(sas_df['arrival_date'], unit='D', origin='1960-01-01') #Convert SAS Date
    sas_df['departure_date_from_us'] = pd.to_datetime(sas_df['departure_date_from_us'], unit='D', origin='1960-01-01') #Convert SAS Date
    
    
    #code to extract immigration_df for fact and dimension tables
    immigration_df = sas_df.to_csv("fact_immigration")
    immigration_df = spark.read.option("header","true").csv("fact_immigration")
    
    # extract columns to create fact_immigration table and write parquet file to s3
    fact_immigration= immigration_df.select(['cicid', 'arrival_date', 'visa_type', 'port_of_entry', 'us_address_state',
                    'country_of_departure','mode_of_entry','flight_no','gender','birth_year','departure_date_from_us'])
    fact_immigration.write.parquet(os.path.join(output_path,'fact_immigration'),mode = "overwrite", partitionBy='arrival_date')

    # extract columns to create dim_flight table and write parquet file to s3
    flight_df = immigration_df.select(['flight_no','airline'])
    flight_df = flight_df.drop_duplicates(subset=['flight_no']) #drop duplicates
    flight_df = flight_df.filter(flight_df.flight_no.isNotNull()) #filter df to exclude null values
    flight_df.write.parquet(os.path.join(output_path,'dim_flight'),mode = "overwrite")


    # extract columns to create dim_calendar table and write parquet file to s3
    time_df = immigration_df.select('arrival_date','year','month',  
                 F.dayofmonth("arrival_date").alias('day'),
                 F.weekofyear("arrival_date").alias('week'), 
                 F.date_format(F.col("arrival_date"), "E").alias("weekday"))
    
    time_df = time_df.drop_duplicates(subset=['arrival_date']) #drop duplicates
    time_df.write.parquet(os.path.join(output_path,'dim_calendar'),mode = "overwrite", partitionBy=['year', 'month'])
    
    print('*****SAS data files processed!*******')


    
def processTempData(filepath):
    '''
        This function reads temperature data, filters US cities data transforms it and returns temperature dataframe.
        Input:  input path
        Output: pandas df
    '''
    print(f"Reading file:{filepath}")  
    temp_df = pd.read_csv(filepath) #read temperature data
    print(f'{filepath} file read sucessfully!')
    
    temp_df = temp_df.dropna() #drop null values
    col_dict = {temp_df.columns[1]:'avg_temperature', 'dt':'recorded_temp_date', temp_df.columns[2]:'avg_temp_uncertainity'}
    temp_df = temp_df.rename(columns = col_dict) #rename columns as per data model
    temp_df.columns = temp_df.columns.str.lower() #transform df as per data model
    temp_df['recorded_temp_date'] = pd.to_datetime(temp_df['recorded_temp_date']) #convert to datetime object
    
      
    temp_df = temp_df[temp_df.country =="United States"] #filter df for US only
    # fetch latest temp records only
    temp_df = temp_df[temp_df.groupby('city').recorded_temp_date.transform('max')==temp_df['recorded_temp_date']] 
    temp_df.reset_index(inplace=True, drop=True)
    print('*****Temperature file processed!*******')    
    return temp_df




def processUSCitiesData(temp_df,filepath,spark,output_path):
    '''
        This function reads us cities demographics data,processes it, merges it with temperature data and saves it parquet format to create fact_us_cities_demographics table.
        Input: temperature df, input path, spark session object, S3 output path.
        Output: Print out result.
    '''    
    #Read the file and transform the header names using a list of new headers
    print(f"Reading file:{filepath}")  
    names=['city', 'state_name', 'median_age', 'male_population', 'female_population',
           'total_population', 'no_of_veterans', 'foriegn_born', 'avg_household_size','state_code','race','count']
    city_df = pd.read_csv(filepath,sep = ';',names=names,header=0)
    print(f'{filepath} file read sucessfully!')
    
    city_df.dropna(inplace=True) #drop rows with NaN values
    city_df = city_df[names[0:-3]] #filter DF for required columns only as per data model 
    city_df = pd.merge(city_df, temp_df, how="left", on='city') #merge city_df with input temperature DF
    city_df = city_df[city_df.columns[0:-3]] #filter DF for required columns only as per data model
    
    #code to transform pandas df to spark df and save in parquet format on s3
    city_df = spark.createDataFrame(city_df)
    city_df = city_df.drop_duplicates(['city','state_name']) #drop duplicates
    city_df.write.mode("overwrite").parquet(os.path.join(output_path,'fact_us_cities_demographics'))
    print('*****US Cities Demographics file processed with temperature data!*******')    


def processAirportData(filepath,spark,output_path):
    '''
        This function reads us airport data, transforms data to handle null 
        values,rename cols, drops duplicates and saves it parquet format to create fact_airport table.
        Input: input path, spark session object, S3 output path.
        Output: Print out result.
    ''' 
    print(f"Reading file:{filepath}")  
    airport_df = pd.read_csv(filepath) #read airport data
    print(f'{filepath} file read sucessfully!')    

    #Fill continent as nan : North America if iso_country in 'US' ir 'CA', like other US or Canada regions data already present in df 
    convert_dict ={'continent':str,'gps_code':str, 'local_code':str,'municipality':str,'iso_country':str}
    airport_df=airport_df.astype(convert_dict)
    airport_df.rename(columns = {'ident':'airport_id'},inplace=True) #rename col as per data model
    airport_df['continent'] = np.where(airport_df.iso_country == ('US'or'CA') ,'nan',airport_df['continent'])

    #Split coloumn coordinates into two new columns lat and long 
    airport_df[['latitude','longitude']] = airport_df['coordinates'].str.split(',', expand=True)
    airport_df['latitude']= airport_df.latitude.apply(lambda x : round(float(x),2))
    airport_df['longitude']= airport_df.longitude.apply(lambda x : round(float(x),2))

    #Drop column iata_code as more than 80% records are null for this column and drop coordinates column
    airport_df.drop(labels=['iata_code','coordinates'], axis = 1, inplace=True)
    airport_df.reset_index(inplace=True, drop=True)
    
    #code to load csv file to S3
    airport_df = spark.createDataFrame(airport_df)
    airport_df = airport_df.drop_duplicates(subset=['airport_id'])
    airport_df.write.mode("overwrite").parquet(os.path.join(output_path,'fact_airport'))
    print('*****Airport codes file processed!*******')
    
    
    
def processSasDescriptionFile(filepath,spark,output_path):
    '''
        This function reads SAS Description file, extracts columns,rename cols, save it parquet format 
        to create dim_visa, dim_travelmode, dim_country, dim_state.
        Input: input path, spark session object, S3 output path.
        Output: Print out result.
    '''     
    sas_dict={}
    sas_data = []
    
    print(f"Processing file:{filepath}")  
    with open(filepath, "r") as file:
        for line in file:
            line = re.sub(r"\s+", " ", line)
            if "/*" in line and "-" in line:
                k, v = [i.strip(" ") for i in line.split("*")[1].split("-", 1)]
                k = k.replace(' & ', '_').lower()
                sas_dict[k] = {'description': v}
                
            elif '=' in line and ';' not in line:
                sas_data.append([i.strip(' ').strip("'").title() for i in line.split('=')])
            
            elif len(sas_data) > 0:
                sas_dict[k]['data'] = sas_data
                sas_data = []

    #To create dim_country           
    country_df = spark.createDataFrame(sas_dict['i94cit_i94res']['data'], schema=['country_code', 'country_name'])
    country_df.write.mode("overwrite").parquet(os.path.join(output_path,'dim_country')) #load parquet file to S3
    print("dim_country created!")

    #To create dim_state
    state_df = spark.createDataFrame(sas_dict['i94addr']['data'],schema=['state_code', 'state_name'])
    state_df = state_df.withColumn('state_code', upper(state_df.state_code))
    state_df.write.mode("overwrite").parquet(os.path.join(output_path,'dim_state')) #load parquet file to S3
    print("dim_state created!")
    
    #To create dim_visa  
    visa_df = spark.createDataFrame(sas_dict['i94visa']['data'], schema=['visa_type_id', 'visa_category'])
    visa_df.write.mode("overwrite").parquet(os.path.join(output_path,'dim_visa')) #load parquet file to S3
    print("dim_visa created!")

    #To create dim_travelmode
    travel_mode_df = spark.createDataFrame(sas_dict['i94mode']['data'], schema=['mode_type_id', 'mode_category'])
    travel_mode_df.write.mode("overwrite").parquet(os.path.join(output_path,'dim_travelmode')) #load parquet file to S3
    print("dim_travelmode created!")
    

def check_row_count(spark, output_path, table_list):
    '''
        This function counts the number of rows in all the tables in the list, if less than 0 raise error.
        Input: Spark Session object, S3 path, list of tables.
        Output: Print out result.
    '''
    print("Checking Data quality:for rowcount")
    
    for table in table_list:
        table_df = spark.read.parquet(os.path.join(output_path, table))
        cnt = table_df.count()
        if cnt == 0:
            raise ValueError(f'Quality check FAILED for {table} with zero records.')
        else:
            print(f'Rowcount quality check PASSED for {table} with {cnt} records.')

def check_for_primary_key(spark, output_path, table_dict):
    '''
        This function checks for primary key constraints: column should be unique and have no null records.
        Input: Spark Session object, S3 path, dict of tables and columns.
        Output: Print out check result.
    '''
    print("Checking Data quality:for primary key constraints")
    
    for table,column in table_dict.items():
        table_df = spark.read.parquet(os.path.join(output_path, table))        
        if table_df.count() > table_df.dropDuplicates(''.join(column).strip(',').split(',')).count():
            raise ValueError(f'Primary Key unique constraint check FAILED for table:{table} column:{column}')            
        elif (not isinstance(column,list)) and (table_df.filter((table_df[column] == "") | table_df[column].isNull() | isnan(table_df[column])).count() > 0):
            raise ValueError(f'Primary Key null constraint check FAILED for table:{table} column:{column}')            
        else:
            print(f'Primary Key constraints check PASSED for table:{table} column:{column}')
    
    
    
def main():
    """
    This function:
    1. Creates a spark session.
    2. Calls above implemented functions to fetch and transform this data to create fact and dimension tables,
    which will then be written to parquet files and load them to s3. Finally, run data quality checks on these tables.
    """
    print("ETL process started!")
    
    spark = createSparkSession() #create spark session
    output_path = 's3a://juhi-capstone/'
    sas_input_path_prefix = '../../data/18-83510-I94-Data-2016/i94_'
    sas_input_path_suffix = '16_sub.sas7bdat'
    table_dict = {'fact_us_cities_demographics': ['city,state_name'],'fact_immigration':'cicid','fact_airport':'airport_id',
             'dim_calendar':'arrival_date','dim_flight':'flight_no','dim_visa':'visa_type_id','dim_country':'country_code',
              'dim_state':'state_code','dim_travelmode':'mode_type_id'}

    #Process temperature data and US cities demographics data to create fact table: fact_us_cities_demographics
    temp_df = processTempData('../../data2/GlobalLandTemperaturesByCity.csv')
    processUSCitiesData(temp_df,'us-cities-demographics.csv',spark,output_path)
    
    #Process sas data to create fact table: fact_immigration and dimension tables : dim_calendar and dim_flight
    processUSImmigrationData(sas_input_path_prefix,sas_input_path_suffix,spark,output_path)
    
    #Process airport data to create fact table: fact_airport
    processAirportData('airport-codes_csv.csv',spark,output_path)
    
    #Process SAS Description file to create dimensions: dim_country, dim_state, dim_visa, dim_travelmode
    processSasDescriptionFile("I94_SAS_Labels_Descriptions.SAS",spark,output_path)
    
    #Data quality check: check for rowcount
    check_row_count(spark, output_path,table_dict.keys())

    #Data quality check: check for Primary key constraints: Unique Key and Null value check
    check_for_primary_key(spark, output_path, table_dict)

    print("ETL process completed!")

if __name__ == "__main__":
    main()
