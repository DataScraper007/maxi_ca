import pymysql
import scrapy
from scrapy.cmdline import execute
from scrapy.spiders import SitemapSpider

class MySitemapSpider(SitemapSpider):
    name = 'my_sitemap_spider'
    sitemap_urls = ['https://www.maxi.ca/sitemap.xml']

    def __init__(self):
        super().__init__()
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='maxi_ca'
        )
        self.cur = self.conn.cursor()

    def sitemap_filter(self, entries):
        for entry in entries:
            if '/c/' in entry['loc'] or '/collection' in entry['loc']:
                yield entry
    def parse(self, response, **kwargs):
        self.logger.info(f'Extracted URL: {response.url}')
        url = response.url.replace('https://www.maxi.ca', '')
        print(url)
        self.cur.execute("insert ignore into categories_links (url) values (%s)", url)
        self.conn.commit()
        print("data inserted..")


if __name__ == '__main__':
    execute(f'scrapy crawl {MySitemapSpider.name}'.split())
