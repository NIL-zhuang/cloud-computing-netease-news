#!/bin/bash
cd Netease_news
rm crawl.log
scrapy crawl news -s LOG_FILE=crawl.log
