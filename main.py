#%%
# Main file to control project flow

# Milestone 1: Create Git Repository

# Milestone 2 Task 1: sales_data created in pgadmin4

# Milestone 2 Task 2: Project classes initialised and imported, SQL engine initialised:
from sqlalchemy import inspect, text, MetaData, Table, Column, String, update, func
import re

import database_utils
import data_extraction
import data_cleaning

dbc = database_utils.DatabaseConnector()
de = data_extraction.DataExtractor()
dc = data_cleaning.DataCleaning()

engine = dbc.init_sd_engine()

# SQL Functions
def sql_col_info(engine, table_name, column_name):
    # Set metadata
    metadata = MetaData()

    # Reflect the existing table with the engine passed
    metadata.reflect(bind=engine, only=[table_name])

    # Access the reflected table and column
    table = metadata.tables[table_name]
    column = table.columns[column_name]

    # Print the details of the column
    print(f"Column '{column_name}' in table '{table_name}':")
    print(f" - Type: {column.type}")
    print(f" - Nullable: {column.nullable}")
    print(f" - Default: {column.default}")

    # Print the full table schema
    print("Full Table Schema:")
    print(repr(table))
    return

def sql_col_cast(engine, table_name, column_name, new_data_type):
    # Set metadata
    metadata = MetaData()

    # Reflect the existing table with the engine passed
    metadata.reflect(bind=engine, only=[table_name])

    # Access the reflected table & column, print current datatype
    table = metadata.tables[table_name]
    column = table.columns[column_name]
    old_datatype = column.type
    print(f"Old datatype for {column_name}: {old_datatype}")

    # Create statements to add a new column with the desired data type using the text construct
    alter_statements = [
        text(f"ALTER TABLE {table_name} ADD COLUMN {column_name}_new {new_data_type}"),
        text(f"UPDATE {table_name} SET {column_name}_new = {column_name}::{new_data_type}"),
        text(f"ALTER TABLE {table_name} DROP COLUMN {column_name}"),
        text(f"ALTER TABLE {table_name} RENAME COLUMN {column_name}_new TO {column_name}")
        ]

    #  Connect and run statements
    with engine.connect() as connection:
        for statement in alter_statements:
            connection.execute(statement)
        connection.commit()

    # Refresh the metadata to reflect the changes & print the new type
    metadata.clear()
    metadata.reflect(bind=engine, only=[table_name])
    table = metadata.tables[table_name]
    column = table.columns[column_name]
    new_datatype = column.type
    print(f"New datatype for {column_name}: {new_datatype}")
    return

def sql_col_max_length(engine, table_name, column_name):
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT MAX(LENGTH({column_name})) AS max_length FROM {table_name}"))
        length = result.first().max_length
    return length

# Milestone 2 Task 3: Extract and clean user data
dbc.list_db_tables()
user_data = de.read_rds_table(dbc, 'legacy_users')
clean_user_data = dc.clean_user_data(user_data)
dbc.upload_to_db(clean_user_data, 'dim_users')

# Milestone 2 Task 4: Extract and clean card data
card_data_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
card_data = de.retrieve_pdf_data(card_data_link)
clean_card_data = dc.clean_card_data(card_data)
dbc.upload_to_db(clean_card_data, 'dim_card_details')

# Milestone 2 Task 5: Extract and clean store data
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
store_count = de.list_number_of_stores(num_stores_endpoint, header)
store_data = de.retrieve_stores_data(store_endpoint, store_count)
clean_store_data = dc.clean_store_data(store_data)
dbc.upload_to_db(clean_store_data, 'dim_store_details')

# Milestone 2 Task 6: Extract and clean product data
s3_address = 's3://data-handling-public/products.csv'
product_data = de.extract_from_s3(s3_address)
conv_product_data = dc.convert_product_weights(product_data)
clean_product_data = dc.clean_products_data(conv_product_data)
dbc.upload_to_db(clean_product_data, 'dim_products')

# Milestone 2 Task 7: Extract and clean order data
dbc.list_db_tables()
order_data = de.read_rds_table(dbc, 'orders_table')
clean_order_data = dc.clean_orders_data(order_data)
dbc.upload_to_db(clean_order_data, 'orders_table')

# Milestone 2 Task 8: Extract and clean date events data
s3_address = 's3://data-handling-public/date_details.json'
date_time_data = de.extract_from_s3(s3_address)
clean_date_time_data = dc.clean_date_time_data(date_time_data)
dbc.upload_to_db(clean_date_time_data, 'dim_date_times')

# Confirm all tables are present
inspector = inspect(engine)
print(f"***\nTables present in the database:\n{inspector.get_table_names()}\n")

# Milestone 2 Task 9 & 10: Recursion and Commit: Done

#%%
# Milestone 3 requires modifications to the sales_data database:

# Milestone 3 Task 1: Cast columns of orders_table to correct data types
# +------------------+--------------------+--------------------+
# |   orders_table   | current data type  | required data type |
# +------------------+--------------------+--------------------+
# | date_uuid        | TEXT               | UUID               |
# | user_uuid        | TEXT               | UUID               |
# | card_number      | TEXT               | VARCHAR(?)         |
# | store_code       | TEXT               | VARCHAR(?)         |
# | product_code     | TEXT               | VARCHAR(?)         |
# | product_quantity | BIGINT             | SMALLINT           |
# +------------------+--------------------+--------------------+
# The ? in VARCHAR should be replaced with an integer representing the maximum length of 
# the values in that column.

table_name = 'orders_table'
print(f"***\nUpdating details for {table_name}:")

card_number_length = sql_col_max_length(engine, table_name, 'card_number')
store_code_length = sql_col_max_length(engine, table_name, 'store_code')
product_code_length = sql_col_max_length(engine, table_name, 'product_code')
 
sql_col_cast(engine, table_name, 'date_uuid', 'UUID')
sql_col_cast(engine, table_name, 'user_uuid', 'UUID')
sql_col_cast(engine, table_name, 'card_number', f'VARCHAR({card_number_length})')
sql_col_cast(engine, table_name, 'store_code', f'VARCHAR({store_code_length})')
sql_col_cast(engine, table_name, 'product_code', f'VARCHAR({product_code_length})')
sql_col_cast(engine, table_name, 'product_quantity', 'SMALLINT')
    
# Milestone 3 Task 2: Cast columns of dim_users table to correct data types
# The column required to be changed in the users table are as follows:
# +----------------+--------------------+--------------------+
# | dim_user_table | current data type  | required data type |
# +----------------+--------------------+--------------------+
# | first_name     | TEXT               | VARCHAR(255)       |
# | last_name      | TEXT               | VARCHAR(255)       |
# | date_of_birth  | TEXT               | DATE               |
# | country_code   | TEXT               | VARCHAR(?)         |
# | user_uuid      | TEXT               | UUID               |
# | join_date      | TEXT               | DATE               |
# +----------------+--------------------+--------------------+

table_name = 'dim_users'
print(f"***\nUpdating details for {table_name}:")

country_code_length = sql_col_max_length(engine, table_name, 'country_code')
 
sql_col_cast(engine, table_name, 'first_name', 'VARCHAR(255)')
sql_col_cast(engine, table_name, 'last_name', 'VARCHAR(255)')
sql_col_cast(engine, table_name, 'date_of_birth', 'DATE')
sql_col_cast(engine, table_name, 'country_code', f'VARCHAR({country_code_length})')
sql_col_cast(engine, table_name, 'user_uuid', 'UUID')
sql_col_cast(engine, table_name, 'join_date', 'DATE')

# Milestone 3 Task 3: Update dim_store_details table
# There are two latitude columns in the store details table. Using SQL, merge one of the columns 
# into the other so you have one latitude column.
# Then set the data types for each column as shown below:
# +---------------------+-------------------+------------------------+
# | store_details_table | current data type |   required data type   |
# +---------------------+-------------------+------------------------+
# | longitude           | TEXT              | FLOAT                  |
# | locality            | TEXT              | VARCHAR(255)           |
# | store_code          | TEXT              | VARCHAR(?)             |
# | staff_numbers       | TEXT              | SMALLINT               |
# | opening_date        | TEXT              | DATE                   |
# | store_type          | TEXT              | VARCHAR(255) NULLABLE  |
# | latitude            | TEXT              | FLOAT                  |
# | country_code        | TEXT              | VARCHAR(?)             |
# | continent           | TEXT              | VARCHAR(255)           |
# +---------------------+-------------------+------------------------+
# There is a row that represents the business's website change the location column values where 
# they're null to N/A.

table_name = 'dim_store_details'
print(f"***\nUpdating details for {table_name}:")

# Merge lat with latitude
# Set metadata & create table instance
metadata = MetaData()
dim_store_details = Table(table_name, metadata, autoload_with=engine)

# Create a statement to merge "lat" with "latitude"
merge_columns_query = update(dim_store_details).values(
    latitude =  func.coalesce(dim_store_details.c.latitude, '') + \
                func.coalesce(dim_store_details.c.lat, '')
)

# Create a statement to drop "lat"
drop_lat_query = text(f"ALTER TABLE dim_store_details DROP COLUMN lat")

# Create a statement to set N/A to null
update_latitude_query = update(dim_store_details).values(latitude=None).where(
                                dim_store_details.c.latitude == 'N/A')
update_longitude_query = update(dim_store_details).values(longitude=None).where(
                                dim_store_details.c.longitude == 'N/A')

# Execute the update
with engine.connect() as connection:
    connection.execute(merge_columns_query)
    connection.execute(drop_lat_query)
    connection.execute(update_latitude_query)
    connection.execute(update_longitude_query)
    connection.commit()
    print('Columns "lat" and "latitude mergd into "latitude"')

# Set NULLABLE=True for store_type
# Set metadata & create table instance
metadata = MetaData()
dim_store_details = Table(table_name, metadata, autoload_with=engine)
# Define the new nullable "store_type" column
new_store_type_column = Column('store_type', String, nullable=True)
# Update statements
update_statements = [
    text(f"ALTER TABLE {table_name} ADD COLUMN store_type_new VARCHAR(255)"),
    text(f"UPDATE {table_name} SET store_type_new = store_type::VARCHAR(255)"),
    text(f"ALTER TABLE {table_name} DROP COLUMN store_type"),
    text(f"ALTER TABLE {table_name} RENAME COLUMN store_type_new TO store_type")
    ]
# Execute the update
with engine.connect() as connection:
    for statement in update_statements:
        connection.execute(statement)
    connection.commit()

# Confirm VARCHAR(255) & Nullable=True
# Refresh metadata
metadata = MetaData()
metadata.reflect(bind=engine, only=[table_name])
# Access the reflected table and column
table = metadata.tables[table_name]
column = table.columns['store_type']
# Print the results
print(f"Column store_type, Nullable set to {column.nullable}")

# Correct data types
store_code_length = sql_col_max_length(engine, table_name, 'store_code')
country_code_length = sql_col_max_length(engine, table_name, 'country_code')

sql_col_cast(engine, table_name, 'longitude', 'FLOAT')
sql_col_cast(engine, table_name, 'locality', 'VARCHAR(255)')
sql_col_cast(engine, table_name, 'store_code', f'VARCHAR({store_code_length})')
sql_col_cast(engine, table_name, 'staff_numbers', 'SMALLINT')
sql_col_cast(engine, table_name, 'opening_date', 'DATE')
sql_col_cast(engine, table_name, 'store_type', 'VARCHAR(255)')
sql_col_cast(engine, table_name, 'latitude', 'FLOAT')
sql_col_cast(engine, table_name, 'country_code', f'VARCHAR({country_code_length})')
sql_col_cast(engine, table_name, 'continent', 'VARCHAR(255)')

# # Create a statement to set null to N/A
# update_latitude_query = text("ALTER TABLE dim_store_details ALTER COLUMN latitude SET DEFAULT 'N/A'")
# update_longitude_query = text("ALTER TABLE dim_store_details ALTER COLUMN longitude SET DEFAULT 'N/A'")

# # Execute the update
# with engine.connect() as connection:
#     connection.execute(update_latitude_query)
#     connection.execute(update_longitude_query)
#     connection.commit()

# Milestone 3 Task 4: Update dim_products table for weights
# You will need to do some work on the products table before casting the data types correctly.
# The product_price column has a £ character which you need to remove using SQL.
# The team that handles the deliveries would like a new human-readable column added for the 
# weight so they can quickly make decisions on delivery weights.
# Add a new column weight_class which will contain human-readable values based on the weight 
# range of the product.
# +--------------------------+-------------------+
# | weight_class VARCHAR(?)  | weight range(kg)  |
# +--------------------------+-------------------+
# | Light                    | < 2               |
# | Mid_Sized                | >= 2 - < 40       |
# | Heavy                    | >= 40 - < 140     |
# | Truck_Required           | => 140            |
# +----------------------------+-----------------+

table_name = 'dim_products'
print(f"***\nUpdating details for {table_name}:")

# Set metadata & create table instance
metadata = MetaData()
dim_products = Table(table_name, metadata, autoload_with=engine)
# Update statements
update_statements = [
    text(f"ALTER TABLE {table_name} ADD COLUMN weight_class VARCHAR(20)"),
    text(f"UPDATE {table_name} SET weight_class = CASE \
        WHEN weight < 2 THEN 'Light' \
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized' \
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy' \
        WHEN weight >= 140 THEN 'Truck_Required' \
         END")
    ]
# Execute statements
with engine.connect() as connection:
    for statement in update_statements:
        connection.execute(statement)
    connection.commit()

# Milestone 3 Task 5: Update dim_products table
# After all the columns are created and cleaned, change the data types of the products table.
# You will want to rename the removed column to still_available before changing its data type.
# Make the changes to the columns to cast them to the following data types:
# +-----------------+--------------------+--------------------+
# |  dim_products   | current data type  | required data type |
# +-----------------+--------------------+--------------------+
# | product_price   | TEXT               | FLOAT              |
# | weight          | TEXT               | FLOAT              |
# | EAN             | TEXT               | VARCHAR(?)         |
# | product_code    | TEXT               | VARCHAR(?)         |
# | date_added      | TEXT               | DATE               |
# | uuid            | TEXT               | UUID               |
# | still_available | TEXT               | BOOL               |
# | weight_class    | TEXT               | VARCHAR(?)         |
# +-----------------+--------------------+--------------------+

table_name = 'dim_products'
print(f"***\nUpdating details for {table_name}:")

#Rename and correct "removed" column
# Set metadata & create table instance
metadata = MetaData()
dim_products = Table(table_name, metadata, autoload_with=engine)
# Update statements
update_statements = [
    text(f"ALTER TABLE {table_name} RENAME COLUMN removed TO still_available"),
    text(f"UPDATE {table_name} SET still_available = CASE \
        WHEN still_available = 'Still_available' THEN 'True' \
        WHEN still_available = 'Removed' THEN 'False'  \
        END")
    ]
# Execute statements
with engine.connect() as connection:
    for statement in update_statements:
        connection.execute(statement)
    connection.commit()
    print('Column "removed" renamed to "still_available"')

# Correct data types
ean_length = sql_col_max_length(engine, table_name, 'ean')
product_code_length = sql_col_max_length(engine, table_name, 'product_code')
weight_class_length = sql_col_max_length(engine, table_name, 'weight_class')

sql_col_cast(engine, table_name, 'product_price', 'FLOAT')
sql_col_cast(engine, table_name, 'weight', 'FLOAT')
sql_col_cast(engine, table_name, 'ean', f'VARCHAR({ean_length})')
sql_col_cast(engine, table_name, 'product_code', f'VARCHAR({product_code_length})')
sql_col_cast(engine, table_name, 'date_added', 'DATE')
sql_col_cast(engine, table_name, 'uuid', 'UUID')
sql_col_cast(engine, table_name, 'still_available', 'BOOL')
sql_col_cast(engine, table_name, 'weight_class', f'VARCHAR({weight_class_length})')

# Milestone 3 Task 6: Update dim_date_times table
# Now update the date table with the correct types:
# +-----------------+-------------------+--------------------+
# | dim_date_times  | current data type | required data type |
# +-----------------+-------------------+--------------------+
# | month           | TEXT              | VARCHAR(?)         |
# | year            | TEXT              | VARCHAR(?)         |
# | day             | TEXT              | VARCHAR(?)         |
# | time_period     | TEXT              | VARCHAR(?)         |
# | date_uuid       | TEXT              | UUID               |
# +-----------------+-------------------+--------------------+

table_name = 'dim_date_times'
print(f"***\nUpdating details for {table_name}:")

# Correct data types
month_length = sql_col_max_length(engine, table_name, 'month')
year_length = sql_col_max_length(engine, table_name, 'year')
day_length = sql_col_max_length(engine, table_name, 'day')
time_period_length = sql_col_max_length(engine, table_name, 'time_period')

sql_col_cast(engine, table_name, 'month', f'VARCHAR({month_length})')
sql_col_cast(engine, table_name, 'year', f'VARCHAR({year_length})')
sql_col_cast(engine, table_name, 'day', f'VARCHAR({day_length})')
sql_col_cast(engine, table_name, 'time_period', f'VARCHAR({time_period_length})')
sql_col_cast(engine, table_name, 'date_uuid', 'UUID')

# Milestone 3 Task 7:Update dim_card_details table
# Now we need to update the last table for the card details.
# Make the associated changes after finding out what the lengths of each variable should be:
# +------------------------+-------------------+--------------------+
# |    dim_card_details    | current data type | required data type |
# +------------------------+-------------------+--------------------+
# | card_number            | TEXT              | VARCHAR(?)         |
# | expiry_date            | TEXT              | VARCHAR(?)         |
# | date_payment_confirmed | TEXT              | DATE               |
# +------------------------+-------------------+--------------------+

table_name = 'dim_card_details'
print(f"***\nUpdating details for {table_name}:")

# Correct data types
card_number_length = sql_col_max_length(engine, table_name, 'card_number')
expiry_date_length = sql_col_max_length(engine, table_name, 'expiry_date')

sql_col_cast(engine, table_name, 'card_number', f'VARCHAR({card_number_length})')
sql_col_cast(engine, table_name, 'expiry_date', f'VARCHAR({expiry_date_length})')
sql_col_cast(engine, table_name, 'date_payment_confirmed', 'DATE')

# Milestone 3 Task 8: Create primary keys
# Now that the tables have the appropriate data types we can begin adding the primary keys 
# to each of the tables prefixed with dim.
# Each table will serve the orders_table which will be the single source of truth for our 
# orders.
# Check the column header of the orders_table you will see all but one of the columns exist 
# in one of our tables prefixed with dim.
# We need to update the columns in the dim tables with a primary key that matches the same 
# column in the orders_table.
# Using SQL, update the respective columns as primary key columns.

# Update statements
update_statements = [
    text(f"ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid)"),
    text(f"ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code)"),
    text(f"ALTER TABLE dim_products ADD PRIMARY KEY (product_code)"),
    text(f"ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid)"),
    text(f"ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number)"),
    ]
# Execute statements
with engine.connect() as connection:
    for statement in update_statements:
        connection.execute(statement)
    connection.commit()
    print("***\nPrimary Keys set")

# Milestone 3 Task 9: Create foreign keys
# With the primary keys created in the tables prefixed with dim we can now create the foreign 
# keys in the orders_table to reference the primary keys in the other tables.
# Use SQL to create those foreign key constraints that reference the primary keys of the other 
# table.
# This makes the star-based database schema complete.

# Update statements
update_statements = [
    text(f"ALTER TABLE orders_table ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid)"),
    text(f"ALTER TABLE orders_table ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code)"),
    text(f"ALTER TABLE orders_table ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code)"),
    text(f"ALTER TABLE orders_table ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid)"),
    text(f"ALTER TABLE orders_table ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number)"),
    ]
# Execute statements
with engine.connect() as connection:
    for statement in update_statements:
        connection.execute(statement)
    connection.commit()
    print("***\nForeign Keys set\n***")

# Milestone 3 Task 10: Upload changes to GitHub
#------------------------------------------------------------------------------------------------------------

#%%
# Milestone 4 Task 1:
# The Operations team would like to know which countries we currently operate in and which 
# country now has the most stores. Perform a query on the database to get the information, 
# it should return the following information:
# +----------+-----------------+
# | country  | total_no_stores |
# +----------+-----------------+
# | GB       |             265 |
# | DE       |             141 |
# | US       |              34 |
# +----------+-----------------+
# Note: DE is short for Deutschland(Germany)

# Query statements
query_statement = text("""
    SELECT country_code, COUNT(country_code) 
    FROM dim_store_details 
    GROUP BY country_code 
    ORDER BY count DESC
    """)
    
# Execute statements
with engine.connect() as connection:
        result = connection.execute(query_statement)
        print("\nMilestone 4 Task 1:")
        print("-----------------------------------------")
        print('|\tcountry\t|\ttotal_no_stores\t|')
        print("-----------------------------------------")
        for row in result:
            print(f"|\t{row.country_code}\t|\t{row.count}\t\t|")
        print("-----------------------------------------")

# Milestone 4 Task 2:
# The business stakeholders would like to know which locations currently have the most stores.
# They would like to close some stores before opening more in other locations.
# Find out which locations have the most stores currently. The query should return the following:
# +-------------------+-----------------+
# |     locality      | total_no_stores |
# +-------------------+-----------------+
# | Chapletown        |              14 |
# | Belper            |              13 |
# | Bushley           |              12 |
# | Exeter            |              11 |
# | High Wycombe      |              10 |
# | Arbroath          |              10 |
# | Rutherglen        |              10 |
# +-------------------+-----------------+

# Query statements
query_statement = text("""
    SELECT locality, COUNT(locality)
    FROM dim_store_details 
    GROUP BY locality 
    ORDER BY count DESC 
    LIMIT 7;
    """)
    
# Execute statements
with engine.connect() as connection:
        result = connection.execute(query_statement)
        print("\nMilestone 4 Task 2:")
        print("-------------------------------------------------")
        print('|\tlocality\t|\ttotal_no_stores\t|')
        print("-------------------------------------------------")
        for row in result:
            print(f"| {row.locality}\t\t|\t{row.count}\t\t|")
        print("-------------------------------------------------")

# Milestone 4 Task 3:
# Query the database to find out which months have produced the most sales. 
# The query should return the following information:
# +-------------+-------+
# | total_sales | month |
# +-------------+-------+
# |   673295.68 |     8 |
# |   668041.45 |     1 |
# |   657335.84 |    10 |
# |   650321.43 |     5 |
# |   645741.70 |     7 |
# |   645463.00 |     3 |
# +-------------+-------+

# Query statements
query_statement = text("""
    WITH MonthlyTotals AS ( 
    SELECT 
        dim_date_times.month, 
        SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales 
    FROM  
        orders_table 
    JOIN 
        dim_products ON orders_table.product_code = dim_products.product_code 
    JOIN  
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid 
    GROUP BY  
        dim_date_times.month 
) 
SELECT DISTINCT ON (dim_date_times.month, MonthlyTotals.total_sales) 
    orders_table.product_quantity,
    orders_table.product_code, 
    orders_table.date_uuid,
    dim_date_times.month,
    dim_date_times.date_uuid,
    dim_products.product_price,
    dim_products.product_code,
    dim_products.product_price * orders_table.product_quantity AS total_price,
    MonthlyTotals.total_sales
FROM 
    orders_table
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN
    MonthlyTotals ON dim_date_times.month = MonthlyTotals.month
ORDER BY
    MonthlyTotals.total_sales DESC, dim_date_times.month, orders_table.product_code, orders_table.date_uuid;
""")
    
# Execute statements
with engine.connect() as connection:
        result = connection.execute(query_statement)
        print("\nMilestone 4 Task 3:")
        print("-----------------------------------------")
        print('|\ttotal_sales\t|\tmonth\t|')
        print("-----------------------------------------")
        for row in result:
            print(f"|\t{row.total_sales:.2f}\t|\t{row.month}\t|")
        print("-----------------------------------------")

# Milestone 4 Task 4:
# The company is looking to increase its online sales.
# They want to know how many sales are happening online vs offline.
# Calculate how many products were sold and the amount of sales made for online and offline purchases.
# You should get the following information:
# +------------------+-------------------------+----------+
# | numbers_of_sales | product_quantity_count  | location |
# +------------------+-------------------------+----------+
# |            26957 |                  107739 | Web      |
# |            93166 |                  374047 | Offline  |
# +------------------+-------------------------+----------+

query_statement = text("""
    SELECT
    CASE 
        WHEN dim_store_details.store_type IN ('Web Portal') THEN 'Web' 
        ELSE 'Offline' 
    END AS location, 
    COUNT(DISTINCT orders_table.date_uuid) AS numbers_of_sales, 
    SUM(orders_table.product_quantity) AS product_quantity_count 
    FROM
        orders_table
    JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
    GROUP BY 
        location 
    ORDER BY 
        location DESC;
    """)

# Execute statements
with engine.connect() as connection:
        result = connection.execute(query_statement)
        print("\nMilestone 4 Task 4:")
        print("-----------------------------------------------------------------------------------------")
        print('|\tnumbers_of_sales\t|\tproduct_quantity_count\t|\tlocation\t|')
        print("-----------------------------------------------------------------------------------------")
        for row in result:
            print(f"|\t{row.numbers_of_sales}\t\t\t|\t{row.product_quantity_count}\t\t\t|\t{row.location}\t\t|")
        print("-----------------------------------------------------------------------------------------")

# Milestone 4 Task 5:
# The sales team wants to know which of the different store types is generated the most revenue 
# so they know where to focus.
# Find out the total and percentage of sales coming from each of the different store types.
# The query should return:
# +-------------+-------------+---------------------+
# | store_type  | total_sales | percentage_total(%) |
# +-------------+-------------+---------------------+
# | Local       |  3440896.52 |               44.87 |
# | Web portal  |  1726547.05 |               22.44 |
# | Super Store |  1224293.65 |               15.63 |
# | Mall Kiosk  |   698791.61 |                8.96 |
# | Outlet      |   631804.81 |                8.10 |
# +-------------+-------------+---------------------+

# Query statements
query_statement = text("""
    WITH StoreTypeSales AS ( 
        SELECT 
            dim_store_details.store_type, 
            SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales 
        FROM 
            orders_table 
        JOIN 
            dim_store_details ON orders_table.store_code = dim_store_details.store_code 
        JOIN 
            dim_products ON orders_table.product_code = dim_products.product_code 
        GROUP BY 
            dim_store_details.store_type 
    ), 
    TotalSales AS ( 
        SELECT 
            SUM(total_sales) AS overall_total_sales 
        FROM 
            StoreTypeSales 
    ) 
    SELECT 
        StoreTypeSales.store_type, 
        StoreTypeSales.total_sales, 
        (StoreTypeSales.total_sales / TotalSales.overall_total_sales) * 100 AS sales_percentage 
    FROM 
        StoreTypeSales 
    CROSS JOIN 
        TotalSales 
    ORDER BY 
        StoreTypeSales.total_sales DESC;
    """)

# Execute statements
with engine.connect() as connection:
        result = connection.execute(query_statement)
        print("\nMilestone 4 Task 5:")
        print("-------------------------------------------------------------------------")
        print('|\tstore_type\t|\ttotal_sales\t| percentage_total(%)\t|')
        print("-------------------------------------------------------------------------")
        for row in result:
            print(f"| {row.store_type} \t\t|\t{row.total_sales:.2f}\t|\t{row.sales_percentage:.2f}\t\t|")
        print("-------------------------------------------------------------------------")

# Milestone 4 Task 6:
# The company stakeholders want assurances that the company has been doing well recently.
# Find which months in which years have had the most sales historically.
# The query should return the following information:
# +-------------+------+-------+
# | total_sales | year | month |
# +-------------+------+-------+
# |    27936.77 | 1994 |     3 |
# |    27356.14 | 2019 |     1 |
# |    27091.67 | 2009 |     8 |
# |    26679.98 | 1997 |    11 |
# |    26310.97 | 2018 |    12 |
# |    26277.72 | 2019 |     8 |
# |    26236.67 | 2017 |     9 |
# |    25798.12 | 2010 |     5 |
# |    25648.29 | 1996 |     8 |
# |    25614.54 | 2000 |     1 |
# +-------------+------+-------+

# Query statements
query_statement = text(""" 
WITH MonthlySales AS ( 
SELECT 
    dim_date_times.month, 
    dim_date_times.year, 
    SUM(dim_products.product_price * orders_table.product_quantity) AS sales_value 
FROM 
    orders_table 
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code 
JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid 
GROUP BY 
    dim_date_times.year, dim_date_times.month 
) 
SELECT 
    year, 
    month, 
    SUM(sales_value) AS total_sales 
FROM 
    MonthlySales 
GROUP BY 
    year, month 
ORDER BY 
    total_sales DESC 
LIMIT 10;
""")

# Execute statements
with engine.connect() as connection:
        result = connection.execute(query_statement)
        print("\nMilestone 4 Task 6:")
        print("---------------------------------------------------------")
        print('|\ttotal_sales\t|\tyear\t|\tmonth\t|')
        print("---------------------------------------------------------")
        for row in result:
            print(f"|\t{row.total_sales:.2f} \t|\t{row.year}\t|\t{row.month}\t|")
        print("---------------------------------------------------------")

# Milestone 4 Task 7:
# The operations team would like to know the overall staff numbers in each location around 
# the world. Perform a query to determine the staff numbers in each of the countries the company 
# sells in.
# The query should return the values:
# +---------------------+--------------+
# | total_staff_numbers | country_code |
# +---------------------+--------------+
# |               13307 | GB           |
# |                6123 | DE           |
# |                1384 | US           |
# +---------------------+--------------+

# Query statements
query_statement = text("""
    SELECT
        country_code,
        SUM(staff_numbers) AS total_staff_numbers
    FROM
        dim_store_details
    GROUP BY
        country_code
    ORDER BY
        total_staff_numbers DESC;
    """)

# Execute statements
with engine.connect() as connection:
        result = connection.execute(query_statement)
        print("\nMilestone 4 Task 7:")
        print("-----------------------------------------")
        print('| total_staff_numbers \t| country_code\t|')
        print("-----------------------------------------")
        for row in result:
            print(f"|\t{row.total_staff_numbers}\t\t| \t{row.country_code} \t|")
        print("-----------------------------------------")

# Milestone 4 Task 8:
# The sales team is looking to expand their territory in Germany. Determine which type of 
# store is generating the most sales in Germany.
# The query will return:
# +--------------+-------------+--------------+
# | total_sales  | store_type  | country_code |
# +--------------+-------------+--------------+
# |   198373.57  | Outlet      | DE           |
# |   247634.20  | Mall Kiosk  | DE           |
# |   384625.03  | Super Store | DE           |
# |  1109909.59  | Local       | DE           |
# +--------------+-------------+--------------+

# Query statements
query_statement = text("""
    SELECT
        dim_store_details.store_type,
        dim_store_details.country_code,
        SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
    FROM
        orders_table
    JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
    JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    WHERE
        dim_store_details.country_code = 'DE'
    GROUP BY
        dim_store_details.store_type, dim_store_details.country_code 
    ORDER BY
    total_sales;
    """)

# Execute statements
with engine.connect() as connection:
        result = connection.execute(query_statement)
        print("\nMilestone 4 Task 8:")
        print("-----------------------------------------------------------------")
        print('|\ttotal_sales\t|\tstore_type\t|  country_code\t|')
        print("-----------------------------------------------------------------")
        for row in result:
            print(f"|\t{row.total_sales:.2f}\t| \t{row.store_type:^12}\t|\t{row.country_code}\t|")
        print("-----------------------------------------------------------------")

# Milestone 4 Task 9:
# Sales would like the get an accurate metric for how quickly the company is making sales.
# Determine the average time taken between each sale grouped by year, the query should return 
# the following information:
#  +------+-------------------------------------------------------+
#  | year |                           actual_time_taken           |
#  +------+-------------------------------------------------------+
#  | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
#  | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
#  | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
#  | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
#  | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
#  +------+-------------------------------------------------------+
# Hint: You will need the SQL command LEAD.

# Query statements
query_statement = text("""
WITH TimeBetweenSales AS (
    SELECT
        EXTRACT
            (YEAR FROM (dim_date_times.year || '-' || dim_date_times.month || '-' || dim_date_times.day || ' ' || dim_date_times.timestamp)::timestamp) AS sale_year,
            (dim_date_times.year || '-' || dim_date_times.month || '-' || dim_date_times.day || ' ' || dim_date_times.timestamp)::timestamp AS sale_time,
        LEAD
            ((dim_date_times.year || '-' || dim_date_times.month || '-' || dim_date_times.day || ' ' || dim_date_times.timestamp)::timestamp) 
        OVER 
            (PARTITION BY dim_date_times.year 
        ORDER BY (dim_date_times.year || '-' || dim_date_times.month || '-' || dim_date_times.day || ' ' || dim_date_times.timestamp)::timestamp) AS next_sale_time,
        EXTRACT
            (EPOCH FROM (LEAD((dim_date_times.year || '-' || dim_date_times.month || '-' || dim_date_times.day || ' ' || dim_date_times.timestamp)::timestamp) 
        OVER 
            (PARTITION BY dim_date_times.year ORDER BY (dim_date_times.year || '-' || dim_date_times.month || '-' || dim_date_times.day || ' ' || dim_date_times.timestamp)::timestamp) - (dim_date_times.year || '-' || dim_date_times.month || '-' || dim_date_times.day || ' ' || dim_date_times.timestamp)::timestamp)) AS time_between_sales
    FROM
        orders_table
    JOIN
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
)

SELECT
    sale_year,
    TO_CHAR(INTERVAL '1 second' * AVG(time_between_sales), 'HH24:MI:SS.MS') AS actual_time_taken
FROM
    TimeBetweenSales
GROUP BY
    sale_year
ORDER BY
    actual_time_taken DESC
LIMIT 5;
""")

# Execute statements
with engine.connect() as connection:
        result = connection.execute(query_statement)
        print("\nMilestone 4 Task 9:")
        print("------------------------------------------")        
        print('| total_sales\t| actual_time_taken \t |')
        print("------------------------------------------")
    
        for row in result:
            time = row.actual_time_taken
            time_split = re.split(r'[:.]', time)
            print(f"| {row.sale_year}\t\t|h:{time_split[0]}, m:{time_split[1]}, s:{time_split[2]}, ms:{time_split[3]}|")
        print("------------------------------------------")

#Milestone 4 Task 10: Update GitHub & Clean up code