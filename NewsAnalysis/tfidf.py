import os
import copy
import json
import time
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# 导入分词jieba
import jieba
import numpy as np

# 导入spark ml库
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

# 调用HDFS中的文件
from pyspark import SparkContext

# 检查HDFS中的文件是否存在
import subprocess


class Cluster:
    # 初始化
    def __init__(self, file_name):

        # spark streaming初始化
        self.spark = SparkSession.builder.appName("NewsAnalysis").getOrCreate()

        # self.sc = self.spark.sparkContext

        # self.ssc = StreamingContext(self.sc, 20)

        # self.out_stream = self.ssc.textFileStream("hdfs://mark-pc:9000/data")

        self.len = 0

        self.nowtime = time.time()

        # word_count = self.out_stream.flatMap(lambda line: line.split(' ')).map(
        # lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).repartition(1).saveAsTextFiles("./word")

        # 存放所有新闻的所有属性
        self.all_news = []

        # 存放所有新闻的内容
        self.all_content = []

        # 存放所有新闻的单词
        self.all_words = []

        # 存放所有新闻的时间
        self.all_time = []

        # 存放tfidf值
        self.tfidfdata = []

        # 存放时间段的tfidf值
        self.timetfidfdata = []

        # 存放每篇新闻关键词
        self.everykeyword = []

        # 存放时间段模式的关键词
        self.timekeyword = []

        # 存放热度
        self.attention = []

        # 存放时间段内单词
        self.nowwords = []

        # 存放每篇文章对应其他文章的相似度
        self.similarity = []

        self.samenews = []

        # 监听文件夹名
        self.file_name = file_name

        # 停用词表
        self.stopwords = [line.strip() for line in open(
            '/home/mark/isework/cloud-computing-netease-news/NewsAnalysis/stop_words.txt',
            encoding='UTF-8').readlines()]
        # self.stopwords = [line.strip() for line in open(
        #     'stop_words.txt',
        #     encoding='UTF-8').readlines()]

    # 去除停用词
    def stopandtostr(self, seg):
        out = ''
        outlist = []
        for word in seg:
            if word not in self.stopwords:
                outlist.append(word)
                out += word
                out += ' '

        # out是分割单词后的语句，outlist是所有分割单词
        return out, outlist

    def read(self):
        # 监听json文件，并分割新闻，中文分词，去除停用词
        json_file = open(self.file_name, encoding='utf-8').read()
        objs = json_file.replace('}{', '}abc{')
        objs = objs.split('abc')

        i = 0
        for item in objs:
            data = json.loads(item)

            if 'content' in data:
                str = ''

                # 把所有语句联通
                for tmpstr in data['content']:
                    str += tmpstr

                # 所有新闻语句分词
                if str != '':
                    seg = jieba.lcut(str, cut_all=False)
                    outstr, outwords = self.stopandtostr(seg)
                    self.all_news.append(data)
                    self.all_content.append(outstr)
                    self.all_words.append(outwords)
                    self.all_time.append(int(
                        (self.nowtime - time.mktime(time.strptime(data['time'], "%Y-%m-%d %H:%M:%S"))) / (
                            60)))
                    i += 1

        self.len = len(self.all_news)

    # 求所有词语的tfidf值
    def tfidf(self):

        self.tfidfdata = []

        tmp_tfidf_data = []

        # 放入每篇文章的分隔单词后的语句
        i = 0
        for str in self.all_content:
            tmp_tfidf_data.append((i, str))
            i += 1

        # 分隔单词并求tf值
        sentenceData = self.spark.createDataFrame(tmp_tfidf_data, ["label", "sentence"])
        tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

        wordsData = tokenizer.transform(sentenceData)

        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
        featurizedData = hashingTF.transform(wordsData)

        # 求idf值
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)

        rescaledData = idfModel.transform(featurizedData)

        # row是指一篇文章中的单词的tfidf值，所以存放了每篇文章单词的tfidf值的数组的数组
        for row in rescaledData.select('features').collect():
            self.tfidfdata.append(row["features"].values)

        # 按照tfidf值取得每篇文章的关键词
        self.everykeyword = []
        self.geteverykeyword()

    # 一段时间内的新闻合并来求tfidf值
    def timetfidf(self, start, end):

        self.timetfidfdata = []
        tmp_timetfidf_data = []

        i = 0
        nowstr = ''
        for str in self.all_content:
            if i < start:
                tmp_timetfidf_data.append((i, str))
                i += 1
            elif i <= end:
                for word in self.all_words[i]:
                    self.nowwords.append(word)
                nowstr += str
                i += 1
            else:
                break

        tmp_timetfidf_data.append((start, nowstr))

        # 计算方法同tfidf()
        sentenceData = self.spark.createDataFrame(tmp_timetfidf_data, ["label", "sentence"])
        tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

        wordsData = tokenizer.transform(sentenceData)

        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
        featurizedData = hashingTF.transform(wordsData)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)

        # 仍然是放入对应tfidf值
        for row in rescaledData.select('features').collect():
            self.timetfidfdata.append(row["features"].values)

        self.spark.stop()

        # 取得时间段内关键词
        self.gettimekeyword()

    # 所有文章的相似文本聚类
    def getsame(self):

        for num in range(self.len):

            print(num)

            # tmpsame数组相似度
            tmpsame = []

            # 遍历所有新闻求相似度
            for k in range(self.len):

                # 第num篇新闻的关键词
                combine_keyword = copy.copy(self.everykeyword[num])

                # 如果是原文，相似度置为0
                if k == num:
                    tmpsame.append(0)
                else:

                    # 两篇文章的关键词合并
                    for word in self.everykeyword[k]:
                        combine_keyword.append(word)

                    # 去除重复关键词
                    combine_keyword = list(set(combine_keyword))

                    # 放入0
                    vec1 = []
                    vec2 = []
                    for j in range(len(combine_keyword)):
                        vec1.append(0)
                        vec2.append(0)

                    # 遍历新闻看关键词个数
                    for word in self.all_words[num]:
                        if word in combine_keyword:
                            vec1[combine_keyword.index(word)] += 1
                    for word in self.all_words[k]:
                        if word in combine_keyword:
                            vec2[combine_keyword.index(word)] += 1

                    # 求点积，为分子
                    up = dianji(vec1, vec2)
                    # 求分母，为分母
                    down = fanshu(vec1) * fanshu(vec2)

                    # 将相似度放入相似度数组
                    tmpsame.append(up / down)

                self.similarity.append(tmpsame)

            index = top(tmpsame)

            outnews = []

            count = 0
            for idex in index:

                tmp = tmpsame.index(idex)

                if count >= 10:
                    break
                else:
                    if self.all_news[tmp]['title'] not in outnews \
                        and self.all_news[num]['section']==self.all_news[tmp]['section']\
                            and abs(self.all_time[num]-self.all_time[tmp])<=24*60:
                        outnews.append(self.all_news[tmp]['title'])
                        count += 1

            self.samenews.append(outnews)

    # 存储json文件
    def saveall(self):

        outlist1 = []
        for i in range(self.len):
            outlist1.append({'title': self.all_news[i]['title'], 'attention': self.attention[i],
                             'keyword': self.everykeyword[i], 'samenews': self.samenews[i]})

        with open('/home/mark/isework/cloud-computing-netease-news/out/out.json', 'w', encoding='utf-8') as f:
            json.dump(outlist1, f, ensure_ascii=False)

        with open('/home/mark/isework/cloud-computing-netease-news/out/outtime.json', 'w', encoding='utf-8') as f:
            json.dump({'time':time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),'timekeyword': self.timekeyword}, f, ensure_ascii=False)

        # with open('../out/out.json', 'w', encoding='utf-8') as f:
        #     json.dump(outlist1, f, ensure_ascii=False)
        #
        # with open('../out/outtime.json', 'w', encoding='utf-8') as f:
        #     json.dump({'time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'timekeyword': self.timekeyword},
        #               f, ensure_ascii=False)

    # 利用hacker news热度算法进行计算，其中投票在这里改写为相似度超过30%的新闻数
    # 详情参见https://blog.csdn.net/ouzhuangzhuang/article/details/82467949?utm_medium=distribute.pc_relevant_t0.none-task-blog-searchFromBaidu-1.control&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-searchFromBaidu-1.control

    def get_attention(self):

        self.attention = []
        g = 9.8
        for i in range(self.len):
            points = 0
            for j in self.similarity[i]:
                if j >= 0.3:
                    points += 1
            self.attention.append((points - 1) / ((self.all_time[i] + 2) * g)*(10-int(self.all_news[i]['depth']))**2)
        maxi = max(self.attention)
        mini = min(self.attention)
        for i in range(self.len):
            self.attention[i] = (self.attention[i] - mini) / (maxi - mini) * 100

    # 取得前20个关键词，按照tfidf值
    def geteverykeyword(self):
        for i in range(self.len):
            index20 = top(self.tfidfdata[i])
            out20 = []
            count = 0
            for idex in index20:

                tmp = self.tfidfdata[i].tolist().index(idex)

                if count >= 20:
                    break
                else:
                    if self.all_words[i][tmp] not in out20:
                        out20.append(self.all_words[i][tmp])
                        count += 1
            self.everykeyword.append(out20)

    # 取得前20个时间段内关键词，按照tfidf值
    def gettimekeyword(self):
        index20 = top(self.timetfidfdata[len(self.timetfidfdata) - 1])
        out20 = []

        count = 0
        for idex in index20:

            tmp = self.timetfidfdata[len(self.timetfidfdata) - 1].tolist().index(idex)

            if count >= 20:
                break
            else:
                if self.nowwords[tmp] not in out20:
                    out20.append(self.nowwords[tmp])
                    count += 1

        self.timekeyword = out20

    def clean(self):
        self.attention = []
        self.samenews = []
        self.everykeyword = []
        self.all_words = []
        self.similarity = []
        self.timekeyword = []
        self.all_news = []
        self.all_time = []
        self.all_content = []
        self.tfidfdata = []
        self.timetfidfdata = []
        self.nowwords = []


# 求两个向量的点积
def dianji(vec1, vec2):
    return np.dot(vec1,vec2)


# 求向量的范数
def fanshu(vec):
    return np.linalg.norm(vec)


# 求列表的前n个最大值的坐标数组
def top(onearray):
    if not isinstance(onearray, list):
        onearray = onearray.tolist()

    newone = copy.copy(onearray)
    newone.sort()
    newone.reverse()

    return newone


def wordCount():
    inputFile = 'hdfs://master:9000/temp/hdin/*'  # 文档地址
    outputFile = 'hdfs://master:9000/temp/spark-out'  # 结果目录
    txtNum = 0
    inputFile = inputFile + str(txtNum)  # 这里视为文档规律命名，实际情况中需要调整
    txtNum += 1

    sc = SparkContext('local', 'wordcount')
    text_file = sc.textFile(inputFile)

    counts = text_file.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile(outputFile)


def listening_wordCount():
    sc = SparkContext(appName="")
    ssc = StreamingContext(sc, 90)
    text_file = ssc.textFileStream("")  # 文本文件目录
    outputFile = ''  # 结果存储目录

    counts = text_file.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile(outputFile)


'''
def tryHdfsPath(location):
    filexistchk = "hdfs dfs -test -e " + location + ";echo $?"
    # echo $? will print the exit code of previously execited command
    filexistchk_output = subprocess.Popen(filexistchk, shell=True, stdout=subprocess.PIPE).communicate()
    filechk = "hdfs dfs -test -d " + location + ";echo $?"
    filechk_output = subprocess.Popen(filechk, shell=True, stdout=subprocess.PIPE).communicate()
    # Check if location exists
    if '1' not in str(filexistchk_output[0]):
        # check if its a directory
        if '1' not in str(filechk_output[0]):
            print('The given URI is a directory: ' + location)
        else:
            print('The given URI is a file: ' + location)
    else:
        print(location + " does not exist. Please check the URI")
'''

if __name__ == "__main__":
    cluster = Cluster('/home/mark/isework/cloud-computing-netease-news/data/news.json')  # 文件名注意修改
    # os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.8'
    # cluster=Cluster('../Data/news.json')
    while (True):
        os.system(
            "hdfs dfs -get -f hdfs://mark-pc:9000/json/news.json /home/mark/isework/cloud-computing-netease-news/data/news.json")
        # if os.path.exists("../data/news.json"):
        cluster.nowtime = time.time()
        cluster.read()
        cluster.tfidf()
        cluster.timetfidf(cluster.len - 100, cluster.len - 1)
        cluster.geteverykeyword()
        cluster.gettimekeyword()
        cluster.getsame()
        cluster.get_attention()
        cluster.saveall()
        cluster.clean()
        os.system(
            "hdfs dfs -put -f /home/mark/isework/cloud-computing-netease-news/out/out.json hdfs://mark-pc:9000/out/out.json ")
        os.system(
            "hdfs dfs -put -f /home/mark/isework/cloud-computing-netease-news/out/outtime.json hdfs://mark-pc:9000/out/outtime.json ")
        time.sleep(1801)
