# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime

import pymysql
from maxi_ca import db_config


class MaxiCaPipeline:

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.date = datetime.today().strftime('%Y%m%d')

    def open_spider(self, spider):
        # Open database connection when spider starts
        self.connection = pymysql.connect(host=db_config.db_host, user=db_config.db_user,
                                          password=db_config.db_password,
                                          database=db_config.db_name)
        self.cursor = self.connection.cursor()

        create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS maxi_ca_products_{self.date} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    product_url VARCHAR(255),
                    categories TEXT,
                    product_name VARCHAR(255),
                    brand_name VARCHAR(255),
                    product_no VARCHAR(50) UNIQUE,
                    price VARCHAR(255),
                    mrp VARCHAR(255),
                    discount VARCHAR(255),
                    currency VARCHAR(3),
                    serving_for_people VARCHAR(255),
                    price_per_quantity VARCHAR(50),
                    availability BOOL,
                    description TEXT,
                    valid_date VARCHAR(255),
                    quantity VARCHAR(50),
                    average_weight VARCHAR(255),
                    ingredients TEXT,
                    product_image TEXT
                );
                """
        # Execute the query to create the table
        self.cursor.execute(create_table_query)
        self.connection.commit()

    def process_item(self, item, spider):
        try:
            # Create placeholders for fields and values
            id = item['index_id']
            item.pop('index_id')
            field_list = []
            value_list = []

            # for key, value in item.items():
            #     if not value or str(value).strip() == '' or value != 0:
            #         item[key] = 'NA'

            # Loop through the fields in the item and prepare the SQL insert statement
            for field, value in item.items():
                field_list.append(f"`{field}`")  # Using backticks to avoid conflicts with reserved SQL keywords
                value_list.append('%s')  # Placeholder for parameterized query

            fields = ','.join(field_list)
            values = ','.join(value_list)

            insert_query = f"INSERT INTO maxi_ca_products_{self.date} ({fields}) VALUES ({values})"

            # Execute the insert query with item values
            self.cursor.execute(insert_query, tuple(item.values()))
            self.connection.commit()

            print('Data Inserted Successfully')

            self.cursor.execute('update product_links set status="DONE" where id=%s', (id))
            self.connection.commit()

        except pymysql.MySQLError as e:
            print(f"Database Error: {e}")
        except Exception as e:
            print(f"Error: {e}")

        return item

    def close_spider(self, spider):
        # Close the connection when the spider finishes
        if self.connection:
            self.connection.close()
