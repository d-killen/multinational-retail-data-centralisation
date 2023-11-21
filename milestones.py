# Main file to control project flow through the milestones
from sqlalchemy import Column, func, inspect, MetaData, String, Table, text, update 
import database_utils
import data_extraction
import data_cleaning
import re


# Function Declarations
# Milestone 2 objectives grouped within a function:
def milestone_2():
    # Milestone 2 Task 1: sales_data created in pgadmin4

    # Milestone 2 Task 2: Project classes imported and initialised; SQL engine initialised

    # Milestone 2 Task 3: Extract, clean and upload user data
    dbc.list_db_tables()
    user_data = de.read_rds_table(dbc, 'legacy_users')
    clean_user_data = dc.clean_user_data(user_data)
    dbc.upload_to_db(clean_user_data, 'dim_users', engine)

    # Milestone 2 Task 4: Extract, clean and upload card data
    card_data_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_data = de.retrieve_pdf_data(card_data_link)
    clean_card_data = dc.clean_card_data(card_data)
    dbc.upload_to_db(clean_card_data, 'dim_card_details', engine)

    # Milestone 2 Task 5: Extract, clean and upload store data
    store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    store_count = de.list_number_of_stores(num_stores_endpoint, header)
    store_data = de.retrieve_stores_data(store_endpoint, store_count)
    clean_store_data = dc.clean_store_data(store_data)
    dbc.upload_to_db(clean_store_data, 'dim_store_details', engine)

    # Milestone 2 Task 6: Extract, clean and upload product data
    s3_address = 's3://data-handling-public/products.csv'
    product_data = de.extract_from_s3(s3_address)
    conv_product_data = dc.convert_product_weights(product_data)
    clean_product_data = dc.clean_products_data(conv_product_data)
    dbc.upload_to_db(clean_product_data, 'dim_products', engine)

    # Milestone 2 Task 7: Extract, clean and upload order data
    dbc.list_db_tables()
    order_data = de.read_rds_table(dbc, 'orders_table')
    clean_order_data = dc.clean_orders_data(order_data)
    dbc.upload_to_db(clean_order_data, 'orders_table', engine)

    # Milestone 2 Task 8: Extract, clean and upload date events data
    s3_address = 's3://data-handling-public/date_details.json'
    date_time_data = de.extract_from_s3(s3_address)
    clean_date_time_data = dc.clean_date_time_data(date_time_data)
    dbc.upload_to_db(clean_date_time_data, 'dim_date_times', engine)

    # Confirm all tables are present
    inspector = inspect(engine)
    print(f"***\nTables present in the database:\n{inspector.get_table_names()}\n")

    # Milestone 2 Task 9 & 10: Recursion and Commit

    return
# Milestone 3 objectives grouped within a function:
def milestone_3():   # TODO: Consider use of functions to streamline
    # Milestone 3 requires modifications to the sales_data database:
    # Milestone 3 Task 1: Cast columns of orders_table to correct data types
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
    table_name = 'dim_store_details'
    print(f"***\nUpdating details for {table_name}:")

    # Merge lat with latitude
    # Set metadata & create table instance
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

    # Milestone 3 Task 4: Update dim_products table for weights
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

    # Milestone 3 Task 7: Update dim_card_details table
    table_name = 'dim_card_details'
    print(f"***\nUpdating details for {table_name}:")

    # Correct data types
    card_number_length = sql_col_max_length(engine, table_name, 'card_number')
    expiry_date_length = sql_col_max_length(engine, table_name, 'expiry_date')

    sql_col_cast(engine, table_name, 'card_number', f'VARCHAR({card_number_length})')
    sql_col_cast(engine, table_name, 'expiry_date', f'VARCHAR({expiry_date_length})')
    sql_col_cast(engine, table_name, 'date_payment_confirmed', 'DATE')

    # Milestone 3 Task 8: Create primary keys

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
# Milestone 4 objectives grouped within a function:
def milestone_4():   # TODO: Use SQLAlchemy ORM to improve code specifically query structure
     
    # Milestone 4 Task 1:
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
    # Query statements
    query_statement = text("""
        WITH monthly_totals AS ( 
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
    SELECT DISTINCT ON (dim_date_times.month, monthly_totals.total_sales) 
        orders_table.product_quantity,
        orders_table.product_code, 
        orders_table.date_uuid,
        dim_date_times.month,
        dim_date_times.date_uuid,
        dim_products.product_price,
        dim_products.product_code,
        dim_products.product_price * orders_table.product_quantity AS total_price,
        monthly_totals.total_sales
    FROM 
        orders_table
    JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    JOIN 
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
    JOIN
        monthly_totals ON dim_date_times.month = monthly_totals.month
    ORDER BY
        monthly_totals.total_sales DESC, dim_date_times.month, orders_table.product_code, orders_table.date_uuid;
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
    # Query statements
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
    # Query statements
    query_statement = text("""
        WITH store_type_sales AS ( 
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
        sales_totals AS ( 
            SELECT 
                SUM(total_sales) AS overall_total_sales 
            FROM 
                store_type_sales 
        ) 
        SELECT 
            store_type_sales.store_type, 
            store_type_sales.total_sales, 
            (store_type_sales.total_sales / sales_totals.overall_total_sales) * 100 AS sales_percentage 
        FROM 
            store_type_sales 
        CROSS JOIN 
            sales_totals 
        ORDER BY 
            store_type_sales.total_sales DESC;
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
    # Query statements
    query_statement = text(""" 
    WITH monthly_sales AS ( 
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
        monthly_sales 
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
    # Query statements
    query_statement = text("""
    WITH time_taken_between_sales AS (
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
        time_taken_between_sales
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

    # Milestone 4 Task 10: Update GitHub & Clean up code
    return
# SQL Functions
def sql_col_cast(engine, table_name, column_name, new_data_type):
    """Casts a table column to a specific datatype

    Parameters
        engine : SQLAlchemy engine
            Instance of SQLAlchemy engine connected to database
        table_name : str
            Name of table to access in database
        column_name : str
            Name of column to access in database
        new_data_type : str
            Name of desired datatype
    """
    # Reflect the existing table with the engine passed
    metadata.reflect(bind=engine, only=[table_name])

    # Access the reflected table & column, print the current datatype
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
    """Returns length of longest value in a column

    Parameters
        engine : SQLAlchemy engine
            Instance of SQLAlchemy engine connected to database
        table_name : str
            Name of table to access in database
        column_name : str
            Name of column to access in database

    Returns
        length : int
            Length of longest value in column
    """
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT MAX(LENGTH({column_name})) AS max_length FROM {table_name}"))
        length = result.first().max_length
    return length

dbc = database_utils.DatabaseConnector()
de = data_extraction.DataExtractor()
dc = data_cleaning.DataCleaning()

# Create the SQL connection engine
engine = dbc.init_db_engine("sales_data")

# Create metadata object
metadata = MetaData()

# Milestone 1: Create Git Repository
# Milestone 2: Extract clean and upload data to sales_data:
milestone_2()
# Milestone 3: Correct datatypes and data in the database:
milestone_3()
# Milestone 4: Conduct the necessary queries:
milestone_4()