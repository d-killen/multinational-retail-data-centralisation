from sqlalchemy import create_engine, inspect
import yaml

class DatabaseConnector:
    """Class to connect to databases

    Methods
        read_db_creds():
            Read credentials from a .yaml file
        init_db_engine(database):
            Return a dataframe from a pdf source
        list_db_tables():
            Returns the number of company stores
        upload_to_db(dataframe, table_name, engine):
            Uploads a dataframe to a database
    """
    def read_db_creds(self):
        """ Read credentials from db_creds.yaml file

        Returns
            creds : dict
                Credentials held in db_creds.yaml file
        """
        with open('db_creds.yaml', 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def init_db_engine(self, database):
        """Returns an instance of SQLAlchemy engine

        Parameters
            database : str
                RDS or sales_data depending on which database should be connected to

        Returns
            engine : SQLAlchemy engine
                Instance of SQLAlchemy engine
        """
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        creds = self.read_db_creds()
        if database == "RDS":
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
            return engine
        elif database == "sales_data":
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{creds['SD_USER']}:{creds['SD_PASSWORD']}@{creds['SD_HOST']}:{creds['SD_PORT']}/{creds['SD_DATABASE']}")
            return engine
        else:
            raise Exception("Invalid database")
    
    def list_db_tables(self):
        """Returns a list of table names in the RDS database

        Returns
            table_names : list
                Table names in the database
        """
        engine = self.init_db_engine('RDS')
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
       
    def upload_to_db(self, input_dataframe, table_name, engine):
        """Uploads dataframe to database table

        Parameters
            input_dataframe : dataframe
                Dataframe to be uploaded
            table_name : str
                Name of table dataframe will be uploaded to
            engine : SQLAlchemy engine
                Instance of SQLAlchemy engine connected to database
        """
        print(f"***\nDetails of dataframe for {table_name} to be uploaded to database:\n")
        input_dataframe.info()
        input_dataframe.to_sql(table_name, engine, if_exists='replace')
        print(f"\n{table_name} uploaded!\n")
        return