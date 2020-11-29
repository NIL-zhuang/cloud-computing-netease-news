from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import NeteaseNewsItem
from scrapy.exporters import JsonItemExporter
import time
import json
from itemadapter import ItemAdapter
import re
import jieba


class NewsSpider(CrawlSpider):
    name = 'news'
    allowed_domains = ['163.com']
    # 起始url地址，从这个地方开始抓取数据
    start_urls = ['https://www.163.com']
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36', }
    # 匹配网易新闻的所有频道但是孤立订阅号
    # 这个正则表达式用来匹配网易新闻的url，不包括订阅号
    rules = (Rule(LinkExtractor(allow=r"https://[\w|.]{2,10}.163.com/\d{2}/\d{4}/\d{2}/[\w]{16}.html.*"),
                  callback='parse_item', follow=True),)

    pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

    with open("news.json") as f:
        string = f.read()
    url_set = set(re.findall(pattern, string))
    print("current news:"+str(len(url_set)))
    stopwords = [line.strip() for line in open('./Netease_news/stop_words.txt', encoding='UTF-8').readlines()]

    def parse_item(self, response):
        # 会在每个url完成下载后被调用，用于解析网页数据，提取结构化数据生成item
        item = NeteaseNewsItem()
        item['url'] = str(response.url.encode()).split('?')[0].replace("b", '').replace("'", '')
        item['depth'] = response.meta['depth']
        self.get_title(item, response)
        print(item['title'])
        self.get_section(item)
        self.get_time(item, response)
        self.get_source(item, response)
        self.get_content(item, response)
        self.write_to_txt(item)
        if item['url'] not in self.url_set:
            self.write_to_json(item)
            self.url_set.add(item['url'])
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

    def get_province(self, item: NeteaseNewsItem,  response):
        if ".news" not in item['url']:
            item['province'] = 'nation'
        else:
            item['province'] = item['url'].strip("https://").split('.')[0]

    def write_to_txt(self, item: NeteaseNewsItem):
        timestamp = time.strftime("%Y%m%d%H%M", time.localtime())
        with open(timestamp+'.txt', 'ab') as f:
            str = ' '.join([tmpstr for tmpstr in item['content']])
            seg = jieba.lcut(str, cut_all=False)
            # print(seg)
            outwords = self.stop_to_str(seg)
            f.write((' '.join(outwords)).encode())

    def write_to_json(self, item: NeteaseNewsItem):
        with open('news.json', 'a') as f:   # 将新闻写进同一个json
            json.dump(ItemAdapter(item).asdict(), f, ensure_ascii=False)

    def stop_to_str(self, seg):
        outlist = []
        for word in seg:
            if word not in self.stopwords and len(word) > 0:
                outlist.append(word)
        # out是分割单词后的语句，outlist是所有分割单词
        return outlist
