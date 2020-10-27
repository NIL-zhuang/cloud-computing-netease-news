
import heapq
import json
import jieba
import pandas
from pyspark import SparkConf, SparkContext
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import SparkSession


class Cluster:
    def __init__(self, file_name):
        self.spark = SparkSession.builder.appName("NewsAnalysis").getOrCreate()
        self.len = 0
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
        # 去停用词
        out = ''
        outlist = []
        for word in seg:
            if word not in self.stopwords:
                outlist.append(word)
                out += word
                out += ' '
        return out, outlist

    def read(self):
        json_file = open(self.file_name, encoding='utf-8').read()
        objs = json_file.replace('}{', '}abc{')
        objs = objs.split('abc')

        # print(len(objs))
        i = 0
        for item in objs:
            data = json.loads(item)
            # print(data)

            if 'content' in data:
                str = ''
                for tmpstr in data['content']:
                    str += tmpstr
                if str!='':
                    seg = jieba.lcut(str, cut_all=False)
                    outstr, outwords = self.stopandtostr(seg)
                    self.all_news.append(data)
                    self.all_content.append(outstr)
                    self.all_words.append(outwords)
                    i += 1

        self.len = len(self.all_news)

    def tfidf(self):
        self.tfidfdata=[]
        tmp_tfidf_data=[]
        i=0
        for str in self.all_content:
            tmp_tfidf_data.append((i,str))
            i+=1
        sentenceData = self.spark.createDataFrame(tmp_tfidf_data, ["label", "sentence"])
        tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

        wordsData = tokenizer.transform(sentenceData)

        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
        featurizedData = hashingTF.transform(wordsData)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)

        for row in rescaledData.select('features').collect():
            self.tfidfdata.append(row["features"].values)

        self.spark.stop()

        self.everykeyword=[]
        self.geteverykeyword()



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

        sentenceData = self.spark.createDataFrame(tmp_timetfidf_data, ["label", "sentence"])
        tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

        wordsData = tokenizer.transform(sentenceData)

        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
        featurizedData = hashingTF.transform(wordsData)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)

        for row in rescaledData.select('features').collect():
            self.timetfidfdata.append(row["features"].values)

        self.gettimekeyword()
        self.spark.stop()

    # def getsame(self,num):


    def geteverykeyword(self):
        for i in range(self.len):
            index20 = top20(self.tfidfdata[i])
            out20=[]
            for idex in index20:
                out20.append(self.all_words[i][idex])
            self.everykeyword.append(out20)

    def gettimekeyword(self):
        index20 = top20(self.timetfidfdata[len(self.timetfidfdata)-1])
        out20 = []

        for idex in index20:
            out20.append(self.nowwords[idex])

        self.timekeyword=out20


def top20(onearray):
    tmp = dict(zip(range(len(onearray)), onearray))
    large20 = heapq.nlargest(20, tmp)
    return large20


if __name__ == "__main__":
    cluster = Cluster('news.json')
    cluster.read()
    cluster.timetfidf(400,500)
    print(cluster.timekeyword)
