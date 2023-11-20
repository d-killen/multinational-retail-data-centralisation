from dateutil.parser import parse
import pandas as pd


#Disable Chained Assigment warning in Pandas
pd.options.mode.chained_assignment = None  # default='warn'

class DataCleaning:
    """Class to clean data from various sources

    Methods
        clean_user_data(user_df):
            Cleans the user data dataframe
        clean_card_data(card_df):
            Cleans the card data dataframe
        clean_store_data(store_df):
            Cleans the store data dataframe
        convert_product_weights(product_df):
            Converts all weights to kg in the product data dataframe
        clean_products_data(products_df):
            Cleans the product data dataframe
        clean_orders_data(orders_df):
            Cleans the orders data dataframe
        clean_date_time_data(date_time_df):
            Cleans the date/time data dataframe
    """   
    def clean_user_data(self, user_df):
        """Cleans the user data dataframe

        Parameters
            user_df : dataframe
                Dataframe of raw user data

        Returns
            clean_user_df : dataframe
                Dataframe of clean user data
        """       
        allowed_countries = ['Germany', 'United Kingdom', 'United States']
        country_mask = user_df['country'].isin(allowed_countries)
        user_df = user_df[country_mask]

        allowed_country_code = ['DE', 'GB', 'US', 'GGB']
        country_code_mask = user_df['country_code'].isin(allowed_country_code)
        user_df = user_df[country_code_mask]
        user_df['country_code'] = user_df['country_code'].str.replace('GGB', 'GB', regex=False)

        #remove NULLs
        user_df.dropna(inplace=True)

        user_df['date_of_birth'] = user_df['date_of_birth'].apply(parse)
        user_df.date_of_birth = pd.to_datetime(user_df.date_of_birth, errors='coerce')

        user_df['join_date'] = user_df['join_date'].apply(parse)
        user_df.join_date = pd.to_datetime(user_df.join_date, errors='coerce')

        #correct index
        user_df.reset_index(drop=True, inplace=True)

        clean_user_df = user_df
        return clean_user_df
    
    def clean_card_data(self, card_df):
        """Cleans the card data dataframe

        Parameters
            card_df : dataframe
                Dataframe of raw card data

        Returns
            clean_card_df : dataframe
                Dataframe of clean card data
        """   

        # cardnumbers should only have numbers
        card_df['card_number'] = card_df['card_number'].astype(str)
        card_df['card_number'] = card_df['card_number'].str.replace('?', '', regex=False)
        
        # expiry_date should be XX/XX
        expiry_mask=card_df.expiry_date.str.contains('/')
        card_df=card_df[expiry_mask]

        # remove NULLs
        card_df.dropna(inplace=True)
        
        # format dates
        card_df['date_payment_confirmed'] = card_df['date_payment_confirmed'].apply(parse)
        card_df.date_payment_confirmed = pd.to_datetime(card_df.date_payment_confirmed, errors='coerce')

        # correct index
        card_df.reset_index(drop=True, inplace=True)

        clean_card_df = card_df
        return clean_card_df
    
    def clean_store_data(self, store_df):
        """Cleans the store data dataframe

        Parameters
            store_df : dataframe
                Dataframe of raw store data

        Returns
            clean_store_df : dataframe
                Dataframe of clean store data
        """ 
        #correct country codes
        allowed_country_code = ['DE', 'GB', 'US']
        country_code_mask = store_df['country_code'].isin(allowed_country_code)
        store_df = store_df[country_code_mask]

        #format dates
        store_df['opening_date'] = store_df['opening_date'].apply(parse)
        store_df.opening_date = pd.to_datetime(store_df.opening_date, errors='coerce')

        #correct head count
        store_df['staff_numbers'] = store_df['staff_numbers'].astype(str)
        regex_hc = "[^0-9]"
        store_df['staff_numbers'] = store_df['staff_numbers'].str.replace(regex_hc, '', regex=True)
        store_df['staff_numbers'] = pd.to_numeric(store_df.staff_numbers, errors='coerce')

        #correct continent
        store_df['continent']=store_df['continent'].str.replace('ee', '', regex=False)

        #correct index
        store_df.reset_index(drop=True, inplace=True)

        clean_store_df = store_df
        return clean_store_df
    
    def convert_product_weights(self, product_df):
        """Converts all weights to kg in the product data dataframe

        Parameters
            product_df : dataframe
                Dataframe of raw product data

        Returns
            converted_product_df : dataframe
                Dataframe of product data with weights all in kg
        """
        def multipack_string_to_weight(string):
            """Converts weights of multipack items

            Parameters
                string : string
                    String of multipack item count and weight eg "5 x 15g"

            Returns
                value : float
                    Weight of multipack item eg "75"
            """
            list=string.split(sep=' ')
            value = float(list[0])*float(list[2])
            return value

        product_df['weight']=product_df['weight'].astype(str)

        #values with kg
        kg_mask = product_df.weight.str.endswith('kg')
        kg_df = product_df[kg_mask]
        #remove kg
        kg_df['weight'] = kg_df['weight'].str.strip('kg')
        kg_df['weight'] = kg_df['weight'].astype(float)

        #values with g
        g_mask = (product_df.weight.str.endswith('g')) & (product_df['weight'].str[-2] != 'k') & (~product_df.weight.str.contains('x'))
        g_df = product_df[g_mask]
        #remove g and convert to kg
        g_df['weight'] = g_df['weight'].str.strip('g')
        g_df['weight'] = g_df['weight'].astype(float)
        g_df['weight'] = g_df['weight']/1000

        #multipack items
        multi_mask = product_df.weight.str.contains('x')
        multi_df = product_df[multi_mask]
        #remove g and multiply, convert to kg
        multi_df['weight'] = multi_df['weight'].str.strip('g')
        multi_df['weight'] = multi_df['weight'].apply(multipack_string_to_weight)
        multi_df['weight'] = multi_df['weight'].astype(float)
        multi_df['weight'] = multi_df['weight']/1000
        
        #values with ml
        ml_mask = product_df.weight.str.endswith('ml')
        ml_df = product_df[ml_mask]
        #remove 'ml' and convert to kg
        ml_df['weight'] = ml_df['weight'].str.strip('ml')
        ml_df['weight'] = ml_df['weight'].astype(float)
        ml_df['weight'] = ml_df['weight']/1000

        #values with oz
        oz_mask = (product_df.weight.str.endswith('oz'))
        oz_df = product_df[oz_mask]
        #remove 'oz' and convert to kg
        oz_df['weight'] = oz_df['weight'].str.strip('oz')
        oz_df['weight'] = oz_df['weight'].astype(float)
        oz_df['weight'] = oz_df['weight']*0.0283495

        #incorrect g values
        ig_mask = (product_df.weight.str.endswith('g .'))
        ig_df = product_df[ig_mask]
        #remove 'oz' and convert to kg
        ig_df['weight'] = ig_df['weight'].str.strip('g .')
        ig_df['weight'] = ig_df['weight'].astype(float)
        ig_df['weight'] = ig_df['weight']/1000
        
        #concat frames
        frames = [kg_df, g_df, multi_df, ml_df, oz_df, ig_df]
        product_df = pd.concat(frames)

        converted_product_df = product_df
        return converted_product_df
    
    def clean_products_data(self, products_df):
        """Cleans the product data dataframe

        Parameters
            products_df : dataframe
                Dataframe of raw product data

        Returns
            clean_products_df : dataframe
                Dataframe of clean product data
        """ 
        #correct removed column
        products_df['removed']=products_df['removed'].str.replace('Still_avaliable', 'Still_available', regex=False)
        allowed_removed = ['Still_available', 'Removed']
        removed_mask = products_df['removed'].isin(allowed_removed)
        products_df = products_df[removed_mask]

        #convert price to float
        products_df['product_price']=products_df['product_price'].str.replace('£', '', regex=False)
        products_df['product_price'] = pd.to_numeric(products_df.product_price, errors='coerce')

        #correct dates
        products_df['date_added'] = products_df['date_added'].apply(parse)
        products_df.date_added = pd.to_datetime(products_df.date_added, errors='coerce')

        #correct ean case
        products_df.rename(columns={"EAN":"ean"}, inplace=True)

        #correct index
        products_df.reset_index(drop=True, inplace=True)

        clean_products_df = products_df
        return products_df
    
    def clean_orders_data(self, orders_df):
        """Cleans the order data dataframe

        Parameters
            orders_df : dataframe
                Dataframe of raw order data

        Returns
            clean_orders_df : dataframe
                Dataframe of clean order data
        """
        #drop coloumn "1"
        orders_df.drop('1', axis=1, inplace=True)
        orders_df.drop('first_name', axis=1, inplace=True)
        orders_df.drop('last_name', axis=1, inplace=True)

        orders_df['card_number'] = orders_df['card_number'].astype(str)

        #correct index
        orders_df.reset_index(drop=True, inplace=True)
        
        clean_orders_df = orders_df
        return clean_orders_df
    
    def clean_date_time_data(self, date_time_df):
        """Cleans the date/time data dataframe

        Parameters
            date_time_df : dataframe
                Dataframe of raw date/time data

        Returns
            clean_date_time_df : dataframe
                Dataframe of clean date/time data
        """
        #remove NULLs and Errors
        allowed_time_period = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        time_period_mask = date_time_df['time_period'].isin(allowed_time_period)
        date_time_df = date_time_df[time_period_mask]

        #correct index
        date_time_df.reset_index(drop=True, inplace=True)

        clean_date_time_df = date_time_df
        return clean_date_time_df