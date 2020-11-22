import copy
import json

#导入分词jieba
import jieba

#导入spark ml库
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import SparkSession

class Cluster:
    # 初始化
    def __init__(self, file_name):
        # spark初始化
        self.spark = SparkSession.builder.appName("NewsAnalysis").getOrCreate()
        self.len = 0

        #存放所有新闻
        self.all_news = []

        self.all_content = []
        self.all_words = []
        self.tfidfdata = []
        self.timetfidfdata=[]
        self.everykeyword=[]
        self.timekeyword=[]
        self.nowwords=[]
        self.file_name = file_name
        self.stopwords = [line.strip() for line in open('stop_words.txt', encoding='UTF-8').readlines()]

    # 去除停用词
    def stopandtostr(self, seg):
        out = ''
        outlist = []
        for word in seg:
            if word not in self.stopwords:
                outlist.append(word)
                out += word
                out += ' '

        #out是分割单词后的语句，outlist是所有分割单词
        return out, outlist

    def read(self):
        #读取json文件，并分割新闻
        json_file = open(self.file_name, encoding='utf-8').read()
        objs = json_file.replace('}{', '}abc{')
        objs = objs.split('abc')

        i = 0
        for item in objs:
            data = json.loads(item)

            if 'content' in data:
                str = ''

                #把所有语句联通
                for tmpstr in data['content']:
                    str += tmpstr

                #所有新闻语句分词
                if str!='':
                    seg = jieba.lcut(str, cut_all=False)
                    outstr, outwords = self.stopandtostr(seg)
                    self.all_news.append(data)
                    self.all_content.append(outstr)
                    self.all_words.append(outwords)
                    i += 1

        self.len = len(self.all_news)

    #求所有词语的tfidf值
    def tfidf(self):

        self.tfidfdata=[]

        tmp_tfidf_data=[]

        #放入每篇文章的分隔单词后的语句
        i=0
        for str in self.all_content:
            tmp_tfidf_data.append((i,str))
            i+=1

        #分隔单词并求tf值
        sentenceData = self.spark.createDataFrame(tmp_tfidf_data, ["label", "sentence"])
        tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

        wordsData = tokenizer.transform(sentenceData)

        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
        featurizedData = hashingTF.transform(wordsData)

        #求idf值
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)

        rescaledData = idfModel.transform(featurizedData)

        #row是指一篇文章中的单词的tfidf值，所以存放了每篇文章单词的tfidf值的数组的数组
        for row in rescaledData.select('features').collect():
            self.tfidfdata.append(row["features"].values)

        #停止
        self.spark.stop()

        #按照tfidf值取得每篇文章的关键词
        self.everykeyword=[]
        self.geteverykeyword()


    #一段时间内的新闻合并来求tfidf值
    def timetfidf(self,start,end):

        self.timetfidfdata=[]
        tmp_timetfidf_data=[]

        i = 0
        nowstr=''
        for str in self.all_content:
            if i<start:
                tmp_timetfidf_data.append((i, str))
                i+=1
            elif i<=end:
                for word in self.all_words[i]:
                    self.nowwords.append(word)
                nowstr+=str
                i+=1
            else:
                break

        tmp_timetfidf_data.append((start,nowstr))

        #计算方法同tfidf()
        sentenceData = self.spark.createDataFrame(tmp_timetfidf_data, ["label", "sentence"])
        tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

        wordsData = tokenizer.transform(sentenceData)

        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
        featurizedData = hashingTF.transform(wordsData)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)

        #仍然是放入对应tfidf值
        for row in rescaledData.select('features').collect():
            self.timetfidfdata.append(row["features"].values)


        self.spark.stop()

        #取得时间段内关键词
        self.gettimekeyword()



    #第num篇文章的相似文本聚类
    def getsame(self,num):

        #求tfidf，并取得关键词
        self.tfidf()

        #tmpsame数组相似度
        tmpsame=[]

        #遍历所有新闻求相似度
        for i in range(self.len):

            #第num篇新闻的关键词
            combine_keyword=copy.copy(self.everykeyword[num])

            #如果是原文，相似度置为0
            if i==num:
                tmpsame.append(0)
            else:

                #两篇文章的关键词合并
                for word in self.everykeyword[i]:
                    combine_keyword.append(word)

                #去除重复关键词
                combine_keyword=list(set(combine_keyword))

                #放入0
                vec1=[]
                vec2=[]
                for j in range(len(combine_keyword)):
                    vec1.append(0)
                    vec2.append(0)

                #遍历新闻看关键词个数
                for word in self.all_words[num]:
                    if word in combine_keyword:
                        vec1[combine_keyword.index(word)]+=1
                for word in self.all_words[i]:
                    if word in combine_keyword:
                        vec2[combine_keyword.index(word)]+=1

                #求点积，为分子
                up=dianji(vec1,vec2)
                #求分母，为分母
                down=fanshu(vec1)*fanshu(vec2)

                #将相似度放入相似度数组
                tmpsame.append(up/down)

        #打印第num篇文章标题
        print(self.all_news[num]['title'])
        index = top(tmpsame,10)

        print("-----------------------------------")

        #打印相似度最高10个的新闻标题
        for idex in index:
            print(self.all_news[idex]['title'])


    #取得前20个关键词，按照tfidf值
    def geteverykeyword(self):
        for i in range(self.len):
            index20 = top(self.tfidfdata[i],20)
            out20=[]
            for idex in index20:
                out20.append(self.all_words[i][idex])
            self.everykeyword.append(out20)

    #取得前20个时间段内关键词，按照tfidf值
    def gettimekeyword(self):
        index20 = top(self.timetfidfdata[len(self.timetfidfdata)-1],20)
        out20 = []

        for idex in index20:
            out20.append(self.nowwords[idex])

        self.timekeyword=out20

#求两个向量的点积
def dianji(vec1,vec2):
    oc=0
    for i in range(len(vec1)):
        oc+=vec1[i]*vec2[i]
    return oc

#求向量的范数
def fanshu(vec):
    oc=0
    for i in range(len(vec)):
        oc+=vec[i]*vec[i]
    return oc**0.5

#求列表的前n个最大值的坐标数组
def top(onearray,n):
    if not isinstance(onearray,list):
        onearray=onearray.tolist()

    newone=copy.copy(onearray)
    newone.sort()
    newone.reverse()

    large=[]
    for i in range(min(len(onearray),n)):
        large.append(onearray.index(newone[i]))
    return large


if __name__ == "__main__":
    cluster = Cluster('news.json')
    cluster.read()
    cluster.getsame(500)
