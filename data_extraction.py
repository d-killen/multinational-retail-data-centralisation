#%%
import pandas as pd
import tabula
import requests
import boto3
#%%

class DataExtractor:
#This class will work as a utility class, in it you will be creating methods that help extract data
#from different data sources.
#The methods contained will be fit to extract data from a particular data source, these sources will include 
#CSV files, an API and an S3 bucket.

    def read_rds_table(self, db_connector, table_name):
        #will extract the database table to a pandas DataFrame
        
        #Use your list_db_tables method to get the name of the table containing user data.

        #Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
        db_df = pd.read_sql_table(table_name, db_connector.init_db_engine())
        db_df.set_index('index', inplace=True)
        return db_df
    
    def retrieve_pdf_data(self, link):
        read_in = tabula.read_pdf(link, pages="all")
        df = pd.concat(read_in)
        return df
    
    def list_number_of_stores(self, num_stores_endpoint, header):
        response = requests.get(num_stores_endpoint, headers=header)
        data = response.json()
        num_of_stores = data['number_stores']
        return num_of_stores
    
    def retrieve_stores_data(self, store_endpoint, store_count):
        store_list = []

        for i in range(store_count):
            store_number = i
            store_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
            
            response = requests.get(store_endpoint, headers=header)
            store_list.append(response.json())
            i += 1
                
        df_stores=pd.DataFrame(store_list)
        df_stores.set_index('index', inplace=True)
        df_stores.head()
        return df_stores
    
    def extract_from_s3(self, s3_address):
        address = s3_address.split(sep = '/')

        bucket = address[len(address)-2]
       
        file_name = address[len(address)-1]
        
        file_type = file_name.split(sep='.')[-1]
        
        s3 = boto3.client('s3') 
        
        obj = s3.get_object(Bucket= bucket, Key= file_name) 

        if file_type == 'csv':
            product_data = pd.read_csv(obj['Body'])
        elif file_type == 'json':
            product_data = pd.read_json(obj['Body'])    

        return product_data
