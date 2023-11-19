#%%
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

#%%
class DatabaseConnector:
#This class will be used to connect with and upload data to the database


    #Create a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
    #You will need to pip install PyYAML and import yaml to do this

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def init_db_engine(self):
        # initialise and return an sqlalchemy database engine using creds from yaml
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        creds = self.read_db_creds()
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self):
        #list all the tables in the database
        engine = self.init_db_engine()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def init_sd_engine(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        creds = self.read_db_creds()
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{creds['SD_USER']}:{creds['SD_PASSWORD']}@{creds['SD_HOST']}:{creds['SD_PORT']}/{creds['SD_DATABASE']}")
        return engine
    
    def upload_to_db(self, dataframe, table_name):
        #This method will take in a Pandas DataFrame and table name to upload to as an argument
        #Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users
        engine = self.init_sd_engine()
        print(f"***\nDetails of dataframe for {table_name} to be uploaded to database:\n")
        dataframe.info()
        dataframe.to_sql(table_name, engine, if_exists='replace')
        print(f"\n{table_name} uploaded!\n")
        return