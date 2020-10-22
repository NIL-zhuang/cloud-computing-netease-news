from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import NeteaseNewsItem
import time


class NewsSpider(CrawlSpider):
    name = 'news'
    allowed_domains = ['163.com']
    # 起始url地址，从这个地方开 始抓取数据
    start_urls = ['https://www.163.com']
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36',
    }
    # 匹配网易新闻的所有频道但是孤立订阅号
    rules = (Rule(LinkExtractor(allow=r"https://[\w|.]{2,10}.163.com/\d{2}/\d{4}/\d{2}/[\w]{16}.html.*"),   # 这个正则表达式用来匹配网易新闻的url，不包括订阅号
                  callback='parse_item', follow=True),)

    # def parse_news(self, response):
    #     pass

    # def parse(self, response):
    def parse_item(self, response):
        # 会在每个url完成下载后被调用，用于解析网页数据，提取结构化数据生成item
        item = NeteaseNewsItem()
        item['url'] = response.url
        item['depth'] = response.meta['depth']
        self.get_title(item, response)
        self.get_section(item)
        self.get_time(item, response)
        self.get_source(item, response)
        self.get_content(item, response)
        self.write_to_txt(item)
        return item

    def get_title(self, item: NeteaseNewsItem, response):
        # 爬取网页标题
        title = response.css('.post_content_main h1::text').extract()
        item['title'] = title[0] if title else '#'

    def get_section(self, item: NeteaseNewsItem):
        # https://sx.news.163.com/ 这样的网址表示网易山西
        # 解析url获取他对应省份和版块
        url: str = item['url'].replace('https://', '')
        domain_list = url.split('.')
        if domain_list[1] in ['news', ]:
            # item['province'] = domain_list[0]
            item['section'] = domain_list[1]
        else:
            item['section'] = domain_list[0]

    def get_time(self, item: NeteaseNewsItem, response):
        time = response.css('.post_time_source::text').extract()
        item['time'] = time[0].strip().rstrip("　来源: ") if time else '#'

    def get_source(self, item: NeteaseNewsItem, response):
        # css查询的时候id用#表示
        source = response.css('div.post_time_source a::text').extract()
        if source:
            item['source'] = source[0]
        else:
            print(source)

    def get_content(self, item: NeteaseNewsItem, response):
        # 获取新闻正文
        text = response.css('.post_text p::text').extract()

        def change_text(text):
            # 列表内数据清洗
            for index, value in enumerate(text):
                text[index] = value.strip()
            return text

        if text:
            item['content'] = change_text(text)
        else:
            print(text)

    def write_to_txt(self, item):
        timestamp = time.strftime("%Y%m%d%H", time.localtime())
        with open('news_163'+timestamp+'.txt', 'ab') as f:
            # f.seek(0)
            f.write('title: {}\n'.format(item['title']).encode())
            f.write('section: {}\n'.format(item['section']).encode())
            f.write('url: {}\n'.format(item['url']).encode())
            f.write('time: {}\n'.format(item['time']).encode())
            f.write('source: {}\n'.format(item['source']).encode())
            f.write('content: {}\n'.format(item['content']).encode())
            f.write('depth: {}\n'.format(item['depth']).encode())
            # f.write('province: {}\n'.format(item['province']).encode())
            f.write(('-'*20 + '\n').encode())
