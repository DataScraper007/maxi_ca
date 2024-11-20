import json
import re
from datetime import datetime

import pymysql
import scrapy
from scrapy.cmdline import execute


class GetLinksSpider(scrapy.Spider):
    name = "get_links"

    def __init__(self):
        super().__init__()
        self.cookies = None
        self.headers = None
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='maxi_ca'
        )
        self.cur = self.conn.cursor()
        # self.start_id = start_id
        # self.end_id = end_id

    def start_requests(self):
        self.cur.execute(
            f'select id, url from categories_links where status="pending"'
            # f'where id between {self.start_id} and {self.end_id}'
        )
        results = self.cur.fetchall()
        print("result", len(results))
        for result in results:
            _id, url = result
            match = re.search(r'/c/(\d+)$', url)
            if match:
                extracted_id = match.group(1)
                self.headers = {
                    'Accept': '*/*',
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
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                    'is-helios-account': 'false',
                    'is-iceberg-enabled': 'false',
                    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'x-apikey': 'C1xujSegT5j3ap3yexJjqhOfELwGKYvz',
                    'x-application-type': 'Web',
                    'x-channel': 'web',
                    'x-loblaw-tenant-id': 'ONLINE_GROCERIES',
                    'x-preview': 'false',
                }

                count = 1
                # while page:
                payload = {
                    'cart': {
                        'cartId': '3e5bccce-ddfe-4fe2-96bc-00a3216e35be',
                    },
                    'fulfillmentInfo': {
                        # 'storeId': '8632',
                        'storeId': '7222',
                        'pickupType': 'STORE',
                        'offerType': 'OG',
                        'date': datetime.today().strftime('%d%m%Y'),
                        'timeSlot': None,
                    },
                    'listingInfo': {
                        'filters': {},
                        'sort': {},
                        'pagination': {
                            'from': count,
                        },
                        'includeFiltersInResponse': True,
                    },
                    'banner': 'maxi'
                }

                yield scrapy.Request(
                    url=f"https://api.pcexpress.ca/pcx-bff/api/v2/listingPage/{extracted_id}",
                    headers=self.headers,
                    method='post',
                    body=json.dumps(payload),
                    dont_filter=True,
                    cb_kwargs={'id': _id, 'page': count, 'extracted_id': extracted_id, 'url': url}
                )
            else:
                self.headers_2 = {
                    'Accept': '*/*',
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
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                    'is-helios-account': 'false',
                    'is-iceberg-enabled': 'false',
                    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'x-apikey': 'C1xujSegT5j3ap3yexJjqhOfELwGKYvz',
                    'x-application-type': 'Web',
                    'x-channel': 'web',
                    'x-loblaw-tenant-id': 'ONLINE_GROCERIES',
                    'x-preview': 'false',
                }
                count = 1
                payload = {
                    'cart': {
                        'cartId': '3e5bccce-ddfe-4fe2-96bc-00a3216e35be',
                    },
                    'fulfillmentInfo': {
                        # 'storeId': '8632',
                        'storeId': '7222',
                        'pickupType': 'STORE',
                        'offerType': 'OG',
                        'date': datetime.today().strftime('%d%m%Y'),
                        'timeSlot': None,
                    },
                    'listingInfo': {
                        'filters': {},
                        'sort': {},
                        'pagination': {
                            'from': count,
                        },
                        'includeFiltersInResponse': True,
                    },
                    'banner': 'maxi'
                }

                last_endpoint = url.rstrip("/").split("/")[-1]
                yield scrapy.Request(
                    url=f"https://api.pcexpress.ca/pcx-bff/api/v3/collections/{last_endpoint}",
                    headers=self.headers_2,
                    method='post',
                    body=json.dumps(payload),
                    dont_filter=True,
                    cb_kwargs={'id': _id, 'page': count, 'url': url},
                    callback=self.parse_2
                )

    def parse(self, response, **kwargs):
        try:
            print("count", kwargs['page'])
            product_id_data = json.loads(response.text)
            print(product_id_data['layout']['sections'])
            product_data = product_id_data['layout']['sections']['productListingSection']['components']
            if product_data:
                for i in product_data:
                    page = i['data']['productGrid']['pagination']['hasMore']
                    if i['componentId'] == 'productListingComponent':
                        for j in i['data']['productGrid']['productTiles']:
                            product_id = j['productId']
                            print("product_id", product_id)
                            self.cur.execute('insert ignore into product_links (url) values (%s)', product_id)
                            self.conn.commit()
                            self.cur.execute('update categories_links set status="DONE" where url=%s', kwargs['url'])
                            self.conn.commit()
                            print('Data inserted..')
                    if page:
                        count = kwargs['page'] + 1
                        payload = {
                            'cart': {
                                'cartId': '3e5bccce-ddfe-4fe2-96bc-00a3216e35be',
                            },
                            'fulfillmentInfo': {
                                # 'storeId': '8632',
                                'storeId': '7222',
                                'pickupType': 'STORE',
                                'offerType': 'OG',
                                'date': datetime.today().strftime('%d%m%Y'),
                                'timeSlot': None,
                            },
                            'listingInfo': {
                                'filters': {},
                                'sort': {},
                                'pagination': {
                                    'from': count,
                                },
                                'includeFiltersInResponse': True,
                            },
                            'banner': 'maxi'
                        }

                        yield scrapy.Request(
                            url=f"https://api.pcexpress.ca/pcx-bff/api/v2/listingPage/{kwargs['extracted_id']}",
                            headers=self.headers,
                            method='post',
                            body=json.dumps(payload),
                            dont_filter=True,
                            cb_kwargs={'id': kwargs['id'], 'page': count, 'extracted_id': kwargs['extracted_id'],
                                       'url': kwargs['url']},
                            callback=self.parse
                        )
        except Exception as e:
            print(e)
            self.cur.execute('update categories_links set status="ERROR" where url=%s', kwargs['url'])
            self.conn.commit()

    def parse_2(self, response, **kwargs):
        try:
            product_id_data = json.loads(response.text)
            product_data = product_id_data['layout']['sections']['mainContentCollection']['components']
            if product_data:
                for i in product_data:
                    page = i['data']['pagination']['hasMore']
                    if i['componentId'] == 'productGridComponent':
                        for j in i['data']['productTiles']:
                            product_id = j['productId']
                            print("product_id", product_id)
                            self.cur.execute('insert ignore into product_links (url) values (%s)', product_id)
                            self.conn.commit()
                            self.cur.execute('update categories_links set status="DONE" where url=%s', kwargs['url'])
                            self.conn.commit()
                            print('Data inserted..')
                    if page:
                        count = kwargs['page']
                        payload = {
                            'cart': {
                                'cartId': '3e5bccce-ddfe-4fe2-96bc-00a3216e35be',
                            },
                            'fulfillmentInfo': {
                                # 'storeId': '8632',
                                'storeId': '7222',
                                'pickupType': 'STORE',
                                'offerType': 'OG',
                                'date': datetime.today().strftime('%d%m%Y'),
                                'timeSlot': None,
                            },
                            'listingInfo': {
                                'filters': {},
                                'sort': {},
                                'pagination': {
                                    'from': count,
                                },
                                'includeFiltersInResponse': True,
                            },
                            'banner': 'maxi'
                        }

                        last_endpoint = kwargs['url'].rstrip("/").split("/")[-1]
                        yield scrapy.Request(
                            url=f"https://api.pcexpress.ca/pcx-bff/api/v3/collections/{last_endpoint}",
                            headers=self.headers_2,
                            method='post',
                            body=json.dumps(payload),
                            dont_filter=True,
                            cb_kwargs={'id': kwargs['id'], 'page': count, 'url': kwargs['url']},
                            callback=self.parse_2
                        )
        # count += 1
        except Exception as e:
            print(e)
            self.cur.execute('update categories_links set status="ERROR" where url=%s', kwargs['url'])
            self.conn.commit()


if __name__ == '__main__':
    execute(f'scrapy crawl {GetLinksSpider.name}'.split())
