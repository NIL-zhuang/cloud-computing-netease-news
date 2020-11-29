# 云计算-网易新闻分析

2020NJUSE云计算作业，使用spark-streaming分析网易新闻的数据

## 项目结构
├── crawl.sh  
├── data  
│         ├── 2020112816.txt  
│         └── news.json  
├── display  
│         ├── attention  
│         │         ├── attention.html  
│         │         └── Demo.png  
│         ├── news  
│         │         ├── news.html  
│         │         └── news.json  
│         └── wordcount  
│             └── wordcount.html  
├── main.py  
├── move.sh  
├── Netease_news  
│         ├── crawl.log  
│         ├── Netease_news  
│         │         ├── __init__.py  
│         │         ├── items.py  
│         │         ├── middlewares.py  
│         │         ├── pipelines.py  
│         │         ├── __pycache__  
│         │         │         ├── __init__.cpython-38.pyc  
│         │         │         ├── items.cpython-38.pyc  
│         │         │         └── settings.cpython-38.pyc  
│         │         ├── settings.py  
│         │         ├── spiders  
│         │         │         ├── __init__.py  
│         │         │         ├── news.py  
│         │         │         └── __pycache__  
│         │         │             ├── __init__.cpython-38.pyc  
│         │         │             └── news.cpython-38.pyc  
│         │         └── stop_words.txt  
│         ├── news.json  
│         └── scrapy.cfg  
├── NewsAnalysis  
│         ├── stop_words.txt  
│         ├── tfidf.py  
│         └── wordcount.py  
├── out  
│         ├── out.json  
│         └── outtime.json  
├── README.md  
├── run.sh  
├── start-all.py  
├── wct  
├── wct_handle.py  
├── wordcount_history.json  
└── wordcount.sh  
## 复现步骤
### 环境准备
1. 启动hadoop和spark集群环境
本项目中使用的集群环境为spark-3.0.1-bin-hadoop3.2和hadoop-3.2.1
我们在验证阶段使用local模式(单机运行),在实际运行时采用集群模式
2. 安装python及第三方库  
   python(集群机器上均配置3.8版本)  
   jieba  
   pyspark  
   numpy  
   scrapy  
### 爬虫
运行main.py文件进行网易新闻的爬取
### 流监听与流处理
运行start-all.py文件提交spark任务，任务包含词频统计(wordcount)和热点新闻分析(tfidf)
在执行spark任务期间启动wct_handle.py对流产生的wordcount进行实时写入(wordcount_history.json)
### 前端展示
我们通过实时更新本地文件的方式对前端展示的数据源实时更新(包含wordcount_history.json,out文件夹下的json,data文件夹下的news.json)
在display文件夹下
1. 打开attention文件  
打开attention.html可以查看实时的新闻热词(由于数据量较大,渲染根据电脑性能大概需要30~60s之后加载出页面)
渲染出的大致结果见attention文件夹下的Demo截图，鼠标移到新闻标题可以显示相似新闻
2. 打开wordcount文件夹  
打开wordcount.html可以查看不同时间结点新闻关键词统计动态变化
(在本地的wordcount_history.json获取到最新的新闻词频统计之后刷新浏览器能够看到从最开始到最新一次的词频变化)
3. 打开news文件夹  
打开time.html可以查看新闻发布的时间统计情况  
打开source.html可以查看新闻来源的统计情况  