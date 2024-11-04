import gzip
import json
import os
import re

import pymysql
import scrapy
import lxml.html
from datetime import datetime
from urllib.parse import urlencode
from scrapy.cmdline import execute
from maxi_ca.items import MaxiCaItem


class GetPDPSpider(scrapy.Spider):
    name = "get_pdp"

    def __init__(self, start_id, end_id):
        super().__init__()
        self.cookies = None
        self.headers = None
        self.start_id = start_id
        self.end_id = end_id
        self.date = datetime.today().strftime('%Y%m%d')
        self.page_save = rf"C:\Nirmal\page_save\maxi_ca_page_save\{self.date}"
        os.makedirs(self.page_save, exist_ok=True)
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='maxi_ca'
        )
        self.cur = self.conn.cursor()

    def start_requests(self):
        self.cur.execute(
            f'select id, url from product_links where status="pending" limit {self.start_id} offset {self.end_id}')
        results = self.cur.fetchall()

        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'fr',
            'Business-User-Agent': 'PCXWEB',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Origin': 'https://www.maxi.ca',
            'Origin_Session_Header': 'B',
            'Pragma': 'no-cache',
            'Referer': 'https://www.maxi.ca/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'Site-Banner': 'maxi',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'x-apikey': 'C1xujSegT5j3ap3yexJjqhOfELwGKYvz',
        }

        params = {
            'lang': 'en',
            'date': '01112024',
            'pickupType': 'STORE',
            # 'storeId': '8632',
            'storeId': '7222',
            'banner': 'maxi',
        }
        for result in results:
            _id, url = result
            _id = 0
            url = "21176208_EA"
            yield scrapy.Request(
                url=f"https://api.pcexpress.ca/pcx-bff/api/v1/products/{url}?" + urlencode(params),
                headers=self.headers,
                cookies=self.cookies,
                cb_kwargs={'id': _id, 'url_id': url}
            )
            break

    def parse(self, response, **kwargs):
        base_url = "https://www.maxi.ca"
        json_data = json.loads(response.text)
        with gzip.open(self.page_save + '/' + kwargs['url_id'] + '.html.gz', 'w') as file:
            file.write(json.dumps(json_data).encode('utf-8'))

        item = MaxiCaItem()
        item['index_id'] = kwargs['id']
        item['product_url'] = base_url + json_data['link']
        item['product_name'] = json_data['name']
        item['product_no'] = json_data['code']
        item['categories'] = self.extract_categories(json_data)
        item['description'] = self.extract_desc(json_data)
        item['currency'] = "$"
        item['valid_date'] = "NA"
        item['quantity'] = json_data['packageSize']
        item['ingredients'] = json_data['ingredients'] if json_data['ingredients'] else "NA"
        item['product_image'] = self.extract_images(json_data)
        item['serving_for_people'] = "NA"
        item.update(self.extract_price_data(json_data))
        yield item

    def extract_categories(self, response):
        categories = response['breadcrumbs']
        if categories:
            l = ['Welcome']
            for category in categories:
                l.append(category['name'])
            return ' | '.join(l)

    def extract_desc(self, response):
        description_html = response.get('description', '')
        text = ' '.join(lxml.html.fromstring(description_html).xpath('//text()'))
        clean_text = re.sub(r'\s+', ' ', text.replace('\r', ' ').replace('\n', ' ')).strip()
        return clean_text

    def extract_price_data(self, response):
        price_data = response['offers']
        item = {}
        for price in price_data:
            item['price'] = price['price']['value']
            item['mrp'] = price.get('wasPrice').get('value', '') if price.get('wasPrice') else "NA"
            ppq_list = []
            for comparison_price in price['comparisonPrices']:
                ppq_list.append(f"{comparison_price['value']}/{comparison_price['quantity']}{comparison_price['unit']}")
            item['price_per_quantity'] = ' | '.join(ppq_list) if ppq_list else 'NA'
            item['availability'] = True if price['stockStatus'].lower() == 'ok' else False
        return item

    def extract_images(self, response):
        images_list = []
        for images in response['imageAssets']:
            images_list.append(images['largeUrl'])
        return ' | '.join(images_list) if images_list else "NA"


if __name__ == '__main__':
    execute(f'scrapy crawl {GetPDPSpider.name} -a start_id=3793 -a end_id=0'.split())

# https://www.maxi.ca/api/pickup-locations?bannerIds=maxi
