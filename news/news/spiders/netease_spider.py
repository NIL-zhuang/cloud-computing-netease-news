from news.items import NewsItem
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class NeteaseSpider(CrawlSpider):
    name = "news"
    allowed_domains = ["news.163.com"]
    start_urls = ['http://news.163.com/']

    rules = (
        Rule(LinkExtractor(allow=r'https://news.163.com/20/1020/\d+/.*/?\.html'),
             callback='parse_item',
             follow=True),  # 是否在爬到的结果上继续往上爬
    )

    def parse_item(self, response):
        item = NewsItem()
        item['thread'] = response.url.strip().split('.')[-2].split('/')[-1]
        self.get_title(response, item)
        self.get_time(response, item)
        self.get_source(response, item)
        self.get_source_url(response, item)
        self.get_text(response, item)
        self.write_to_txt(item)
        return item

    def get_title(self, response, item):
        # extract()之后是一个列表
        title = response.css('.post_content_main h1::text').extract()
        if title:
            item['title'] = title[0]

    def get_time(self, response, item):
        GetTime = response.css('.post_time_source::text').extract()
        if GetTime:
            item['time'] = GetTime[0].strip().rstrip('　来源: ')

    def get_source(self, response, item):
        source = response.css(
            'div.post_time_source a::text').extract()  # css查询时候的id用#表示
        if source:
            item['source'] = source[0]
        else:
            print(source)

    def get_source_url(self, response, item):
        source_url = response.css(
            'div.post_time_source a::attr(href)').extract()
        if source_url:
            item['url'] = source_url[0]
        else:
            print(source_url[0])

    def get_text(self, response, item):
        '''
        类函数，用于获取新闻正文
        '''
        text = response.css('.post_text p::text').extract()

        def change_text(text):
            '''
            写一个函数专门处理列表内数据清洗
            '''
            for index, value in enumerate(text):
                text[index] = value.strip()
            return text

        if text:
            item['article'] = change_text(text)
        else:
            print(text)

    def write_to_txt(self, item):
        with open('news163_data_1.txt', 'ab') as f:
            f.seek(0)
            f.write('News Thread: {}'.format(item['thread']).encode())
            f.write('\r\n'.encode())
            f.write('News Title: {}'.format(item['title']).encode())
            f.write('\r\n'.encode())
            f.write('News Time: {}'.format(item['time']).encode())
            f.write('\r\n'.encode())
            f.write('News Source: {}'.format(item['source']).encode())
            f.write('\r\n'.encode())
            f.write('News URL: {}'.format(item['url']).encode())
            f.write('\r\n'.encode())
            f.write('News Body: {}'.format(item['article']).encode())
            f.write('\r\n'.encode())
            f.write(('_' * 20).encode())
            f.write('\r\n'.encode())
