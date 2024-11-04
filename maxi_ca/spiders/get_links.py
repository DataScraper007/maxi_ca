import json
import re
from typing import Iterable
from urllib.parse import urlencode

import pymysql
from scrapy import Selector
import scrapy
from scrapy import Request
from scrapy.cmdline import execute


class GetLinksSpider(scrapy.Spider):
    name = "get_links"
    allowed_domains = ["abc.com"]
    start_urls = ["https://abc.com"]

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

    def start_requests(self):
        self.cookies = {
            'lcl_lang_pref': 'fr',
            'Origin_Session_Cookie': 'B',
            'PIM-SESSION-ID': 'SRwURvN8Eo5UlsE1',
            'customer_state': 'anonymous',
            '_fbp': 'fb.1.1730191344716.187502835929205516',
            '_spvid_ses.6c21': '*',
            'moe_uuid': 'f14abb0a-c8ff-48b1-bd51-80f8da2af6f3',
            'auto_store_selected': '8711',
            '_gcl_au': '1.1.1160380446.1730191359',
            '_gid': 'GA1.2.423570965.1730191360',
            'cjConsent': 'MHxOfDB8Tnww',
            'cjUser': '2ac50e6b-042d-484d-b3d2-de07e9411ead',
            '_pin_unauth': 'dWlkPU5UQTVaRGMwTXpVdFlXRTNZUzAwWVRGaExUbGhNVFl0TmpJek9ESTNPV1prWmpZeQ',
            'mdLogger': 'false',
            'kampyle_userid': 'ff6f-f729-4f0d-7147-fdf6-4604-c8e9-9e65',
            '_tt_enable_cookie': '1',
            '_ttp': 'Q1S7UodSuDVhFp8Kkj4SUFr67Ab',
            'OptanonAlertBoxClosed': '2024-10-29T08:43:28.582Z',
            'bluecoreNV': 'false',
            '__eoi': 'ID=535e89182421ef69:T=1730191706:RT=1730204376:S=AA-AfjZrLowXEYQll2aiCmMx0gmd',
            'bm_sz': '6D310D934B7DB33538C833F0C89C7722~YAAQC5YqF5or8cqSAQAASNU82BmV6lhzsA/rIYWnORcSIBe844rnbiFD/XfETeEyuC/joIzHCADQQowmEILazmUrk/FEdc2NSYFn5aq03ZuAdRtr9ul3Jl1PMjKtYROW9Ie59i86AH0CcNatjCCk1zdaPJu4Ss20qpx2ezZ9s7g9Sg3wuZX7CB3s+pLdZZhFPRm+s/Q5OWz89qMKomuShnMw3n4aGZKD/2J3XyQEDmXcUJAGkHnApbzhbXkpRUuSZNZUevVaRfbDzY79awC5LUbqc11k1WlbFOAmLrL31S+Rjd6+vaLKeu28NbKLCwiu6ht7IIqeagEazGYKpl6+NaW4taISZqvEHuUGjpK2Qw5V+zPDoBQtPOfvywdJTgVS0M10HbHwTrtptQ9ZDP9UjA04SA+cNuC7HED5iv4OrNNF2VKz7VA8oAjXz9uJmyEOov7/YCLX4vQifRWcMfKLMDIt96R2a1dP4vUEgP9LBuTs32NqYfEBGZCtjcKjfl0MwHRCaDLX580XAd/bsMVAe/lYt1oMZJFzoeyTPeaObcfn4x9Zy3Ti4Isg~3290178~3682881',
            'ak_bmsc': '9C0A482C97AD0EA3838C32CE8E027011~000000000000000000000000000000~YAAQC5YqF/5JAsuSAQAA5+Vg2BlKYdMmkDb4Gxl4cMGtO+kcbHcnqaQMFSwMk3Y/hJDfiht3IfTNXvL6/pRrynDa7p5yipYJSdMwnjcfWXYoAHiRXI1E9WlRgqEYwEi+ZqzI64ST4HlO7rTjl9TAXAb23UiFiBGbUvYO94IfjLvIP3ZWylxxvKOnphHH3jkn+4MrKgBmPFNSd0G1b4QOmzjw+ZLJxX453opwudCZlwOWDj7MrPCElQkV/7HiJ8AdyU6AVwFMk+UHLbLNMtxn91fTx/13YutOvIvzzGJLEGO5nIzICct5Dsl4Q9CBuzu4kDlhhZxET53xlqw1SEEbZX8SsyB2QvfeppuNiYHrNgfJiVz7v7axnqvBetIJ',
            'lastVisited': '1730207114156',
            'OptanonConsent': 'isGpcEnabled=0&datestamp=Tue+Oct+29+2024+18%3A35%3A18+GMT%2B0530+(India+Standard+Time)&version=202405.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=6e226c1b-1151-4c18-83e4-00415acbdcf0&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&intType=1&geolocation=CA%3BON&AwaitingReconsent=false',
            'mp_maxi_mixpanel': '%7B%22distinct_id%22%3A%20%22192d7710b1e379-077ea340bd2863-26011951-e1000-192d7710b1f7b5%22%2C%22bc_persist_updated%22%3A%201730191363014%2C%22languagePreference%22%3A%20%22fr%22%7D',
            '_ga': 'GA1.1.1204884959.1730191360',
            'bc_invalidateUrlCache_targeting': '1730207133722',
            'akavpau_vp': '1730207406~id=f74cfc40e1cbbd051ad2f0e2dd141c81',
            '_abck': '6BDCFBAC68C1CF542550599986559772~0~YAAQC5YqFxGBAsuSAQAAak1h2AyFeXGqOSepyaqlpmHXJQ3Jjh8kTakbzHQLpZ1nLEytv/Gk5K6qNOEGAm8P92DEQUFZFD7c76ttmOXXlwo/KumvVIxiu0V2Ptzk0BuaiboMo4670c4vBIIi7MTENbtm4W+VwgifHwH7PobuKNcnus8bRN4CWBXOpwH6pyGBezOhTHZsvgm8meyrH9dIqbM5RW054eRqnnqGbWFqlfG/fTuycn7I5w9D42zp0z4wZeSeoLx1RaZyepiBQ1bJtIQO0c6t8ZxS0oO845ZjuRYJ6X4gHFsEDlSqSMq74DVmZie0hq8ZL+wy/LHJq5U5ajIhC2K9fYzGDRNkj5IJH2jjw7G/jnP+QIq6pOxBYz62Aupqg90ZeePj9HDGOjKOK0+xTrUpyHXCYy2jAQ==~-1~-1~-1',
            'kampyleUserSession': '1730207140669',
            'kampyleUserSessionsCount': '21',
            'kampyleSessionPageCounter': '1',
            'kampyleUserPercentile': '24.368870057161175',
            'kampylePageLoadedTimestamp': '1730207140739',
            '_ga_B9WFKJZHQ5': 'GS1.1.1730203774.3.1.1730207144.49.0.0',
            '_spvid_id.6c21': '1dda28d9-c01d-420f-809e-03e474df3afc.1730191347.1.1730207157..992a937f-557a-4823-ae2b-a52dd034d4e7..5c924a90-8c72-40ee-97e1-ff17cd2bfbd1.1730191347349.453',
            'bm_sv': '06AFBD55BBBC0D793C7240B90C11F863~YAAQD5YqF4XVHdSSAQAAeJdh2Bn9kD0U0MOfaBWex0VvxvT3D8VONCLEXkGrWJkyLlr0bBq5fhaZHGcH5T5vktfVQMR5Pxv9n6fhpQSHPhwwC6yB56r0B83iftWbrsfbC0VxErm9YR7NoUJAFjPJ7SrBqoP7ZiJ4yfO/ud3ljG7Zjo5o0zd66JFlMSF/pcZdyYhBeg3HOknnzlJOllGpQvPdfnOO7ZnSDCOi4lef/qVEzvozJdaxCfaSg4eZ~1',
        }

        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            # 'Cookie': 'lcl_lang_pref=fr; Origin_Session_Cookie=B; PIM-SESSION-ID=SRwURvN8Eo5UlsE1; customer_state=anonymous; _fbp=fb.1.1730191344716.187502835929205516; _spvid_ses.6c21=*; moe_uuid=f14abb0a-c8ff-48b1-bd51-80f8da2af6f3; auto_store_selected=8711; _gcl_au=1.1.1160380446.1730191359; _gid=GA1.2.423570965.1730191360; cjConsent=MHxOfDB8Tnww; cjUser=2ac50e6b-042d-484d-b3d2-de07e9411ead; _pin_unauth=dWlkPU5UQTVaRGMwTXpVdFlXRTNZUzAwWVRGaExUbGhNVFl0TmpJek9ESTNPV1prWmpZeQ; mdLogger=false; kampyle_userid=ff6f-f729-4f0d-7147-fdf6-4604-c8e9-9e65; _tt_enable_cookie=1; _ttp=Q1S7UodSuDVhFp8Kkj4SUFr67Ab; OptanonAlertBoxClosed=2024-10-29T08:43:28.582Z; bluecoreNV=false; __eoi=ID=535e89182421ef69:T=1730191706:RT=1730204376:S=AA-AfjZrLowXEYQll2aiCmMx0gmd; bm_sz=6D310D934B7DB33538C833F0C89C7722~YAAQC5YqF5or8cqSAQAASNU82BmV6lhzsA/rIYWnORcSIBe844rnbiFD/XfETeEyuC/joIzHCADQQowmEILazmUrk/FEdc2NSYFn5aq03ZuAdRtr9ul3Jl1PMjKtYROW9Ie59i86AH0CcNatjCCk1zdaPJu4Ss20qpx2ezZ9s7g9Sg3wuZX7CB3s+pLdZZhFPRm+s/Q5OWz89qMKomuShnMw3n4aGZKD/2J3XyQEDmXcUJAGkHnApbzhbXkpRUuSZNZUevVaRfbDzY79awC5LUbqc11k1WlbFOAmLrL31S+Rjd6+vaLKeu28NbKLCwiu6ht7IIqeagEazGYKpl6+NaW4taISZqvEHuUGjpK2Qw5V+zPDoBQtPOfvywdJTgVS0M10HbHwTrtptQ9ZDP9UjA04SA+cNuC7HED5iv4OrNNF2VKz7VA8oAjXz9uJmyEOov7/YCLX4vQifRWcMfKLMDIt96R2a1dP4vUEgP9LBuTs32NqYfEBGZCtjcKjfl0MwHRCaDLX580XAd/bsMVAe/lYt1oMZJFzoeyTPeaObcfn4x9Zy3Ti4Isg~3290178~3682881; ak_bmsc=9C0A482C97AD0EA3838C32CE8E027011~000000000000000000000000000000~YAAQC5YqF/5JAsuSAQAA5+Vg2BlKYdMmkDb4Gxl4cMGtO+kcbHcnqaQMFSwMk3Y/hJDfiht3IfTNXvL6/pRrynDa7p5yipYJSdMwnjcfWXYoAHiRXI1E9WlRgqEYwEi+ZqzI64ST4HlO7rTjl9TAXAb23UiFiBGbUvYO94IfjLvIP3ZWylxxvKOnphHH3jkn+4MrKgBmPFNSd0G1b4QOmzjw+ZLJxX453opwudCZlwOWDj7MrPCElQkV/7HiJ8AdyU6AVwFMk+UHLbLNMtxn91fTx/13YutOvIvzzGJLEGO5nIzICct5Dsl4Q9CBuzu4kDlhhZxET53xlqw1SEEbZX8SsyB2QvfeppuNiYHrNgfJiVz7v7axnqvBetIJ; lastVisited=1730207114156; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Oct+29+2024+18%3A35%3A18+GMT%2B0530+(India+Standard+Time)&version=202405.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=6e226c1b-1151-4c18-83e4-00415acbdcf0&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&intType=1&geolocation=CA%3BON&AwaitingReconsent=false; mp_maxi_mixpanel=%7B%22distinct_id%22%3A%20%22192d7710b1e379-077ea340bd2863-26011951-e1000-192d7710b1f7b5%22%2C%22bc_persist_updated%22%3A%201730191363014%2C%22languagePreference%22%3A%20%22fr%22%7D; _ga=GA1.1.1204884959.1730191360; bc_invalidateUrlCache_targeting=1730207133722; akavpau_vp=1730207406~id=f74cfc40e1cbbd051ad2f0e2dd141c81; _abck=6BDCFBAC68C1CF542550599986559772~0~YAAQC5YqFxGBAsuSAQAAak1h2AyFeXGqOSepyaqlpmHXJQ3Jjh8kTakbzHQLpZ1nLEytv/Gk5K6qNOEGAm8P92DEQUFZFD7c76ttmOXXlwo/KumvVIxiu0V2Ptzk0BuaiboMo4670c4vBIIi7MTENbtm4W+VwgifHwH7PobuKNcnus8bRN4CWBXOpwH6pyGBezOhTHZsvgm8meyrH9dIqbM5RW054eRqnnqGbWFqlfG/fTuycn7I5w9D42zp0z4wZeSeoLx1RaZyepiBQ1bJtIQO0c6t8ZxS0oO845ZjuRYJ6X4gHFsEDlSqSMq74DVmZie0hq8ZL+wy/LHJq5U5ajIhC2K9fYzGDRNkj5IJH2jjw7G/jnP+QIq6pOxBYz62Aupqg90ZeePj9HDGOjKOK0+xTrUpyHXCYy2jAQ==~-1~-1~-1; kampyleUserSession=1730207140669; kampyleUserSessionsCount=21; kampyleSessionPageCounter=1; kampyleUserPercentile=24.368870057161175; kampylePageLoadedTimestamp=1730207140739; _ga_B9WFKJZHQ5=GS1.1.1730203774.3.1.1730207144.49.0.0; _spvid_id.6c21=1dda28d9-c01d-420f-809e-03e474df3afc.1730191347.1.1730207157..992a937f-557a-4823-ae2b-a52dd034d4e7..5c924a90-8c72-40ee-97e1-ff17cd2bfbd1.1730191347349.453; bm_sv=06AFBD55BBBC0D793C7240B90C11F863~YAAQD5YqF4XVHdSSAQAAeJdh2Bn9kD0U0MOfaBWex0VvxvT3D8VONCLEXkGrWJkyLlr0bBq5fhaZHGcH5T5vktfVQMR5Pxv9n6fhpQSHPhwwC6yB56r0B83iftWbrsfbC0VxErm9YR7NoUJAFjPJ7SrBqoP7ZiJ4yfO/ud3ljG7Zjo5o0zd66JFlMSF/pcZdyYhBeg3HOknnzlJOllGpQvPdfnOO7ZnSDCOi4lef/qVEzvozJdaxCfaSg4eZ~1',
            'Pragma': 'no-cache',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'authorization': 'Bearer JoEeGjT-gSp5ExLPhRnA1o-ARRuF3gcp2kSAGqGk7D0'
        }

        params = {
            'content_type': 'navigationList',
            'locale': 'fr-CA',
            'include': '4',
        }

        yield scrapy.Request(
            url="https://cdn.contentful.com/spaces/0dlg9rxz8nvy/environments/master/entries?" + urlencode(params),
            headers=self.headers,
            cookies=self.cookies,
            dont_filter=True
        )

    def parse(self, response, **kwargs):
        json_data = json.loads(response.text)
        json_data = json_data['includes']['Entry']
        payload = {
            'cart': {
                'cartId': '3e5bccce-ddfe-4fe2-96bc-00a3216e35be',
            },
            'fulfillmentInfo': {
                'storeId': '8632',
                'pickupType': 'STORE',
                'offerType': 'OG',
                'date': '29102024',
                'timeSlot': None,
            },
            'listingInfo': {
                'filters': {},
                'sort': {},
                'pagination': {
                    'from': 1,
                },
                'includeFiltersInResponse': True,
            },
            'banner': 'maxi'
        }
        for data in json_data:
            url = data['fields'].get('url')
            if url:
                match = re.search(r'/c/(\d+)$', url)
                if match:
                    extracted_id = match.group(1)
                    headers = {
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

                    yield scrapy.Request(
                        url=f'https://api.pcexpress.ca/pcx-bff/api/v2/listingPage/{extracted_id}',
                        method='post',
                        body=json.dumps(payload),
                        headers=headers,
                        # cookies=self.cookies,
                        callback=self.parse_links,
                        dont_filter=True,
                        cb_kwargs={'count': 1}
                    )
                else:
                    print(url)
                # self.cur.execute('insert ignore into product_links (url) values (%s)', url)
                # self.conn.commit()
                # print('Data inserted..')

    def parse_links(self,response, **kwargs):
        json_data = json.loads(response.text)
        components = json_data['layout']['sections']['productListingSection']['components']
        for component in components:
            if component['componentId'] == 'productGridComponent':
                products = component['data']['productGrid']['productTiles']
                if products:
                    for product in products:
                        product_id = product['productId']
                        self.cur.execute('insert ignore into product_links (url) values (%s)', product_id)
                        self.conn.commit()
                        print('Data inserted..')
            pagination = component['data']['productGrid']['pagination']['hasMore']
            if pagination:
                payload = {
                    'cart': {
                        'cartId': '3e5bccce-ddfe-4fe2-96bc-00a3216e35be',
                    },
                    'fulfillmentInfo': {
                        'storeId': '8632',
                        'pickupType': 'STORE',
                        'offerType': 'OG',
                        'date': '29102024',
                        'timeSlot': None,
                    },
                    'listingInfo': {
                        'filters': {},
                        'sort': {},
                        'pagination': {
                            'from': kwargs['count'] + 1,
                        },
                        'includeFiltersInResponse': True,
                    },
                    'banner': 'maxi'
                }
                yield scrapy.Request(
                    url=response.url,
                    method='post',
                    body=json.dumps(payload),
                    headers=self.headers,
                    cookies=self.cookies,
                    callback=self.parse_links,
                    dont_filter=True,
                    cb_kwargs={'count': kwargs['count'] + 1}
                )
            

if __name__ == '__main__':
    execute(f'scrapy crawl {GetLinksSpider.name}'.split())
