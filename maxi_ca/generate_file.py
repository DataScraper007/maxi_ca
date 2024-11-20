from datetime import datetime

import pymysql
import pandas as pd
import db_config

today_date = datetime.today().strftime('%Y%m%d')
# Database connection details
conn = pymysql.connect(
    host=db_config.db_host,
    user=db_config.db_user,
    password=db_config.db_password,
    database=db_config.db_name
)
cur = conn.cursor()

# SQL query to read data from the table
query = f"SELECT * FROM maxi_ca_products_{today_date}"

# Define a mapping for renaming the columns
rename_columns = {
    'id': 'Id',
    'product_url': 'Product URL',
    'categories': 'Category',
    'product_name': 'Product Name',
    'product_no': 'Product Number',
    # 'brand_name': 'Brand Name',
    # 'availability': 'Availability',
    'price': 'Price',
    'mrp': 'MRP',
    # 'discount': 'Discount',
    'currency': 'Currency',
    'serving_for_people': 'Serving For People',
    'quantity': 'Quantity',
    # 'average_weight': 'Average Weight',
    'price_per_quantity': 'Price Per Quantity',
    'description': 'Product Description',
    'ingredients': 'Ingredients',
    'product_image': 'Product Image',
    'valid_date': 'Valid Date'
}

# Connect to the database and export the data
try:
    df = pd.read_sql(query, conn)

    # Rename columns
    df.rename(columns=rename_columns, inplace=True)

    # Replace 'NA' with actual NaN values
    df['MRP'].replace('NA', pd.NA, inplace=True)

    # Fill MRP where it's null with the value from Price
    df['MRP'] = df['MRP'].fillna(df['Price'])

    # Export to Excel
    output_file = fr'C:\Nirmal\files\maxi_ca\maxi_products_{today_date}.xlsx'
    df.to_excel(output_file, index=False)

    print(f"Data exported successfully to {output_file}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if conn:
        conn.close()
