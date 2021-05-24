import traceback
import re
import csv
import json
import time
import scrapy
import requests
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess
from uszipcode import SearchEngine

# PROXY = '37.48.118.90:13042'
PROXY = "45.79.220.141:3128"


def get_proxies_from_free_proxy():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.content)
    proxies = set()
    for i in parser.xpath('//tbody/tr'):
        if i.xpath('.//td[3][text()="US"]') and\
           i.xpath('.//td[7][contains(text(),"yes")]'):
            ip = i.xpath('.//td[1]/text()')[0]
            port = i.xpath('.//td[2]/text()')[0]
            proxies.add("{}:{}".format(ip, port))
            if len(proxies) == 20:
                return proxies
    return proxies


def get_states():
    return [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
        "Connecticut", "Delaware", "District of Columbia", "Florida",
        "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
        "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico",
        "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma",
        "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island",
        "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
        "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin",
        "Wyoming"
    ]


def get_zip_codes_map():
    search = SearchEngine()
    zipcodes = list()
    for state in get_states():
    # for state in ['New York']:
        final_response = list()
        response = search.by_state(state, returns=2000)
        for r in response:
            if r.major_city not in [x.major_city for x in final_response]:
                final_response.append(r)
        for res in response:
            if res:
                zipcodes.append({
                    'zip_code': res.zipcode,
                    'latitude': res.lat,
                    'longitude': res.lng,
                    'city': res.major_city,
                    'state': res.state
                })
    return sorted(zipcodes, key=lambda k: k['state'])


class ExtractItem(scrapy.Item):
    address1 = scrapy.Field()
    amenityIDs = scrapy.Field()
    city = scrapy.Field()
    corpID = scrapy.Field()
    counter = scrapy.Field()
    flexClasses = scrapy.Field()
    genderSpecific = scrapy.Field()
    hasBoomClass = scrapy.Field()
    hasFlex = scrapy.Field()
    hasSilverSneakersClass = scrapy.Field()
    locID = scrapy.Field()
    locationType = scrapy.Field()
    mileDistance = scrapy.Field()
    name = scrapy.Field()
    phone = scrapy.Field()
    state = scrapy.Field()
    upmcPersonalTrainer = scrapy.Field()
    zipCode = scrapy.Field()


class SilverSneankerSpider(scrapy.Spider):
    name = "silver_sneakers_spider"
    allowed_domains = ["tivityhealth"]
    scraped_data = list()
    fieldnames = [
        'address1', 'amenityIDs', 'city', 'corpID', 'counter', 'flexClasses',
        'genderSpecific', 'hasBoomClass', 'hasFlex', 'hasSilverSneakersClass',
        'locID', 'locationType', 'mileDistance', 'name', 'phone', 'state',
        'upmcPersonalTrainer', 'zipCode'
    ]

    def start_requests(self):
        base_url = "https://locationsearch.tivityhealth.com/api/"\
                   "GeneralLocationsClassesByGeoPoint?"\
                   "ProductCode=101&"\
                   "APIKey=54654D75-3AEB-4C5A-80CC-53DA5F71EA18&"\
                   "CallerSystemName=SilverSneakersWebsite&"\
                   "MileRadius=200"
        zip_codes_map = get_zip_codes_map()
        for index, zip_code_map in enumerate(zip_codes_map, 1):
            url = f"{base_url}&Latitude={zip_code_map['latitude']}&"\
                  f"Longitude={zip_code_map['longitude']}"
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True
            )

    def parse(self, response):
        if not response.status == 200:
            return
        results = json.loads(response.text)
        if not results['searchResult'] == 'RecordsFound':
            return
        results = results['locations']
        for result in results:
            if result.get('locID') not in self.scraped_data:
                item = ExtractItem()
                dict_to_write = {k: result[k] for k in self.fieldnames}
                item.update(dict_to_write)
                self.scraped_data.append(result['locID'])
                yield item


def run_spider(no_of_threads, request_delay):
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess(settings)
    process.crawl(SilverSneankerSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 40
    request_delay = 0.01
    run_spider(no_of_threads, request_delay)
