# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NeteaseNewsItem(scrapy.Item):
    title = scrapy.Field()      # 新闻标题
    section = scrapy.Field()    # 新闻版块，比如news, sports
    url = scrapy.Field()        # 新闻源连接
    time = scrapy.Field()       # 发布时间
    source = scrapy.Field()     # 新闻源
    content = scrapy.Field()    # 内容
    depth = scrapy.Field()      # 在www主站内的深度，也就是权重
