# -*- coding: utf-8 -*-

import os
import logging
import configparser
import math

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row, DataFrame

class Singleton(object):
    def __init__(self, clazz):
        self.clazz = clazz
        self.instance = {}

    def __call__(self):
        if self.clazz not in self.instance:
            self.instance[self.clazz] = self.clazz()
        return self.instance[self.clazz]

@Singleton
class EngineUtils(object):
    def __init__(self):
        conf = SparkConf().setAppName("engine").setMaster("local")
        self.sc = SparkContext(conf=conf)
        self.sqlCtx = SQLContext(self.sc)
        self.config = configparser.ConfigParser()
        try:
            self.config.read("../conf/engine.ini")
        except Exception as e:
            logging.fatal(e)
            os._exit(-1)

    def __del__(self):
        self.sc.stop()

    def getSparkContext(self):
        return self.sc

    def getSqlContext(self):
        return self.sqlCtx

    def getConfigParser(self):
        return self.config

@Singleton
class DataSet(object):
    def __init__(self):
        sqlCtx = EngineUtils().getSqlContext()
        if os.path.exists("./dataset/movie_data"):
            self.moviesDataFrame = sqlCtx.read.parquet("./dataset/movie_data")
        else:
            self.moviesDataFrame = self.loadAndSaveMovieData()
        if os.path.exists("./dataset/rating_data"):
            self.ratingsDataFrame = sqlCtx.read.parquet("./dataset/rating_data")
        else:
            self.ratingsDataFrame = self.loadAndSaveRatingData()
        if os.path.exists("./dataset/user_data"):
            self.usersDataFrame = sqlCtx.read.parquet("./dataset/user_data")
        else:
            self.usersDataFrame = self.loadAndSaveUserData()
        if os.path.exists("./dataset/rating_matrix"):
            self.ratingMatrixDataFrame = sqlCtx.read.parquet("./dataset/rating_matrix")
        else:
            self.ratingMatrixDataFrame = self.productRatingMatrix()
        if os.path.exists("./dataset/content_sim"):
            self.contentsSimDataFrame = sqlCtx.read.parquet("./dataset/content_sim")
        else:
            self.contentsSimDataFrame = self.countContentsSim()
        if os.path.exists("./dataset/user_anti_sim"):
            self.usersAntiSimDataFrame = sqlCtx.read.parquet("./dataset/user_anti_sim")
        else:
            self.usersAntiSimDataFrame = self.countUsersAntiSim()

    def getMoviesDataFrame(self):
        return self.moviesDataFrame

    def getRatingsDataFrame(self):
        return self.ratingsDataFrame

    def getUsersDataFrame(self):
        return self.usersDataFrame

    def getRatingMatrixDataFrame(self):
        return self.ratingMatrixDataFrame

    def getContentsSimDataFrame(self):
        return self.contentsSimDataFrame

    def getUsersAntiSimDataFrame(self):
        return self.usersAntiSimDataFrame

    def saveAsParquet(self, dataFrame, dirName):
        dataFrame.write.parquet(dirName)

    def loadAndSaveMovieData(self):
        config = EngineUtils().getConfigParser()
        sc = EngineUtils().getSparkContext()
        sqlCtx = EngineUtils().getSqlContext()
        filePath = config.get("data", "movies")
        if not os.path.exists(filePath):
            logging.fatal("%s not exists, Please check out path or file, Current path: %s.", filePath, os.getcwd())
            os._exit(-1)
        moviesData = sc.textFile(filePath)

        tmp = moviesData.map(lambda line: line.split("::"))
        data = tmp.map(lambda line: Row(movieId=int(line[0]), title=line[1], genres=line[2]))
        moviesDataFrame = sqlCtx.createDataFrame(data)
        self.saveAsParquet(moviesDataFrame, "./dataset/movie_data")
        return moviesDataFrame


    def loadAndSaveRatingData(self):
        config = EngineUtils().getConfigParser()
        sc = EngineUtils().getSparkContext()
        sqlCtx = EngineUtils().getSqlContext()
        filePath = config.get("data", "ratings")
        if not os.path.exists(filePath):
            logging.fatal("%s not exists, Please check out path or file, Current path: %s.", filePath, os.getcwd())
            os._exit(-1)
        ratingsData = sc.textFile(filePath)

        tmp = ratingsData.map(lambda line: line.split("::"))
        data = tmp.map(lambda line: Row(userId=int(line[0]), movieId=int(line[1]), rating=float(line[2]), timestamp=int(line[3])))
        ratingsDataFrame = sqlCtx.createDataFrame(data)
        self.saveAsParquet(ratingsDataFrame, "./dataset/rating_data")
        return ratingsDataFrame

    def loadAndSaveUserData(self):
        config = EngineUtils().getConfigParser()
        sc = EngineUtils().getSparkContext()
        sqlCtx = EngineUtils().getSqlContext()
        filePath = config.get("data", "users")
        if not os.path.exists(filePath):
            logging.fatal("%s not exists, Please check out path or file, Current path: %s.", filePath, os.getcwd())
            os._exit(-1)
        usersData = sc.textFile(filePath)

        tmp = usersData.map(lambda line: line.split("::"))
        data = tmp.map(
            lambda line: Row(userId=int(line[0]), gender=line[1], age=int(line[2]), occupation=int(line[3]), zipCode=line[4]))
        usersDataFrame = sqlCtx.createDataFrame(data)
        self.saveAsParquet(usersDataFrame, "./dataset/user_data")
        return usersDataFrame

    def countUsersAntiSim(self):
        def encodeAge(ageList):
            result = []
            for i in range(len(ageList)):
                tmpDict = {}
                if i == 0:
                    tmpDict[ageList[i]["age"]] = 1
                else:
                    tmpDict[ageList[i]["age"]] = result[i - 1][ageList[i - 1]["age"]] + ageList[i - 1]["count"]
                result.append(tmpDict)
            return result

        def countAntiSimBetweenUsers(lUser, rUser, codedList, ageList):
            lAge = 0
            rAge = 0
            for i in range(len(codedList)):
                if lAge != 0 and rAge != 0:
                    break
                tmpAge = ageList[i]["age"]
                if tmpAge == lUser["age"]:
                    lAge = codedList[i][tmpAge]
                if tmpAge == rUser["age"]:
                    rAge = codedList[i][tmpAge]
            result = abs(lAge - rAge)
            if lUser["gender"] != rUser["gender"]:
                result += 1
            if lUser["occupation"] != rUser["occupation"]:
                result += 1
            if lUser["zipCode"] != rUser["zipCode"]:
                result += 1
            return result / 4

        sqlCtx = EngineUtils().getSqlContext()
        usersDataFrame = self.usersDataFrame
        sqlCtx.registerDataFrameAsTable(usersDataFrame, "user_data")
        tmp = usersDataFrame.groupBy(usersDataFrame.age).count()
        ageList = tmp.orderBy("age").collect()
        encodedAgeList = encodeAge(ageList)

        rdd = usersDataFrame.rdd.cartesian(usersDataFrame.rdd)
        data = rdd.map(lambda line: Row(user1=line[0]['userId'], \
                                        user2=line[1]['userId'], \
                                        antiSim=countAntiSimBetweenUsers( \
                                            line[0], \
                                            line[1], \
                                            encodedAgeList, \
                                            ageList)))
        usersAntiSimDataFrame = sqlCtx.createDataFrame(data)
        self.saveAsParquet(usersAntiSimDataFrame, "./dataset/user_anti_sim")
        return usersAntiSimDataFrame

    def countContentsSim(self):
        def countSimBetweenContents(lContent, rContent):
            cnt = 0
            for item in lContent:
                for tmp in rContent:
                    if item == tmp:
                        cnt += 1
                        break
            return cnt / math.sqrt(len(lContent) * len(rContent))

        sqlCtx = EngineUtils().getSqlContext()
        moviesDataFrame = self.moviesDataFrame
        rdd = moviesDataFrame.rdd.cartesian(moviesDataFrame.rdd)

        data = rdd.map(lambda line: Row(movie1=line[0]["movieId"], \
                                        movie2=line[1]["movieId"], \
                                        sim=countSimBetweenContents( \
                                            str(line[0]["genres"].encode("utf-8")).split("|"), \
                                            str(line[1]["genres"].encode("utf-8")).split("|"))))

        contentsSimDataFrame = sqlCtx.createDataFrame(data)
        self.saveAsParquet(contentsSimDataFrame, "./dataset/content_sim")
        return contentsSimDataFrame

    def productRatingMatrix(self):
        def launch(userNum, line):
            result = [line[0]]
            userIdAndRatings = line[1]
            size = len(userIdAndRatings)
            idx = 0
            for i in range(1, userNum + 1):
                if userIdAndRatings[idx][0] != i:
                    result.append(0.0)
                else:
                    result.append(userIdAndRatings[idx][1])
                    if idx != size - 1:
                        idx += 1
            return result

        sqlCtx = EngineUtils().getSqlContext()
        ratingDataFrame = self.ratingsDataFrame.select("movieId", "userId", "rating")
        rdd = ratingDataFrame.rdd.map(lambda line: (line[0], (line[1], line[2])))
        tmp = rdd.groupByKey()
        rddLaunched = tmp.map(lambda line: (line[0], line[1].data))

        userNum = self.usersDataFrame.count()
        data = rddLaunched.map(lambda line: launch(userNum, line))
        ratingMatrixDataFrame = sqlCtx.createDataFrame(data.map(lambda line: Row(row=line)))
        self.saveAsParquet(ratingMatrixDataFrame, "./dataset/rating_matrix")
        return ratingMatrixDataFrame
    

if __name__ == "__main__":
    # DataSet().loadAndSaveMovieData()
    # DataSet().loadAndSaveRatingData()
    # DataSet().loadAndSaveUserData()
    # DataSet().productRatingMatrix()
    # DataSet().countContentsSim()
    # DataSet().countUsersAntiSim()
    DataSet()
    EngineUtils().getSparkContext().stop()
