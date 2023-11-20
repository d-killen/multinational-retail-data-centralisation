import boto3
import pandas as pd
import requests
import tabula

class DataExtractor:
    """Class to extract company data from various sources

    Methods
        read_rds_table(db_connector, table_name):
            Return a dataframe from a RDS source
        retrieve_pdf_data(link):
            Return a dataframe from a pdf source
        list_number_of_stores(num_stores_endpoint, header):
            Returns the number of company stores
        retrieve_stores_data(store_endpoint, store_count):
            Return a dataframe from an API source
        extract_from_s3(s3_address):
            Return a dataframe from a AWS S3 source
    """
    def read_rds_table(self, db_connector, table_name):
        """Return a dataframe from a RDS source

        Parameters
            db_connector : DatabaseConnector object
                instance of DatabaseConnector class
            table_name : str
                name of table to extract from database

        Returns
            db_df : dataframe
                values held in named table in the database
        """
        db_df = pd.read_sql_table(table_name, db_connector.init_db_engine("RDS"))
        db_df.set_index('index', inplace=True)
        return db_df
    
    def retrieve_pdf_data(self, link):
        """Return a dataframe from a pdf source

        Parameters:
            link: str
                link to pdf object

        Returns:
            df: dataframe
                data contained in pdf
        """
        read_in = tabula.read_pdf(link, pages="all")
        df = pd.concat(read_in)
        return df
    
    def list_number_of_stores(self, num_stores_endpoint, header):
        """Returns the number of company stores

        Parameters
            num_stores_endpoint : str 
                endpoint for store count
            header : dict
                dict containing x-api key

        Returns
            num_of_stores : int
                count of how many stores the company has
        """
        response = requests.get(num_stores_endpoint, headers=header)
        data = response.json()
        num_of_stores = data['number_stores']
        return num_of_stores
    
    def retrieve_stores_data(self, store_endpoint, store_count):
        """Return a dataframe from an API source

        Parameters
            store_endpoint : str 
                endpoint for store data
            store_count : int
                number of stores to retrieve

        Returns
            stores_df : int
                count of how many sores the compnay has
        """
        store_list = []

        for i in range(store_count):
            store_number = i
            store_endpoint = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
            
            response = requests.get(store_endpoint, headers=header)
            store_list.append(response.json())
            i += 1
                
        stores_df=pd.DataFrame(store_list)
        stores_df.set_index('index', inplace=True)
        stores_df.head()
        return stores_df
    
    def extract_from_s3(self, s3_address):
        """Return a dataframe from a AWS S3 source

        Parameters
            s3_address : str 
                address of S3 object

        Returns
            product_df : dataframe
                dataframe of data held S3 object
        """
        address = s3_address.split(sep = '/')
        bucket = address[len(address)-2]
        file_name = address[len(address)-1]
        file_type = file_name.split(sep='.')[-1]
        
        s3 = boto3.client('s3') 
        obj = s3.get_object(Bucket= bucket, Key= file_name) 

        if file_type == 'csv':
            product_df = pd.read_csv(obj['Body'])
        elif file_type == 'json':
            product_df = pd.read_json(obj['Body'])    

        return product_df
