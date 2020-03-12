# -*- coding: utf-8 -*-

import sys
sys.path.append("../")
from utils import EngineUtils, DataSet
from operator import add
from pyspark.sql import Row
from engine.ttypes import UserProfile

class PopuBasedAlgorithm(object):
    def __init__(self, antiSimThreshold=0.4, simPatch=10):
        self.antiSimThreshold = antiSimThreshold
        self.simPatch = simPatch

    def Predict(self, userId, movieId):
        usersAntiSimDataFrame = DataSet().getUsersAntiSimDataFrame()
        simUserDataFrame = usersAntiSimDataFrame.where(usersAntiSimDataFrame.user1 == userId).where( \
            usersAntiSimDataFrame.antiSim < self.antiSimThreshold).select("*")
        ratingsDataFrame = DataSet().getRatingsDataFrame()
        movieFilteredDataFrame = ratingsDataFrame.where(ratingsDataFrame.movieId == movieId).select("*")

        joinedDataFrame = simUserDataFrame.join(movieFilteredDataFrame, simUserDataFrame.user2 == movieFilteredDataFrame.userId, \
                                                "inner").select("*")
        sortedDataFrame = joinedDataFrame.orderBy("antiSim").limit(self.simPatch).select("*")
        tmpList = sortedDataFrame.collect()

        simScore = 0.0
        result = 0.0
        for i in range(len(tmpList)):
            if tmpList[i]["user2"] != userId:
                simScore += 1 - tmpList[i]["antiSim"]

        for i in range(len(tmpList)):
            if tmpList[i]["user2"] != userId:
                result += (1 - tmpList[i]["antiSim"]) / simScore * tmpList[i]["rating"]

        return result


    def Recommend(self, userProfile, topk=10):
        userId = userProfile.userId
        sqlCtx = EngineUtils().getSqlContext()
        usersAntiSimDataFrame = DataSet().getUsersAntiSimDataFrame()
        simUserDataFrame = usersAntiSimDataFrame.where(usersAntiSimDataFrame.user1 == userId).where( \
            usersAntiSimDataFrame.antiSim < self.antiSimThreshold).orderBy("antiSim").select("*")

        ratingsDataFrame = DataSet().getRatingsDataFrame()
        joinedDataFrame = ratingsDataFrame.join(simUserDataFrame, ratingsDataFrame.userId == simUserDataFrame.user2, \
                                                "inner").select("*")
        tmpList = joinedDataFrame.orderBy("antiSim").groupBy("user2").count().collect()

        cnt = 0
        for i in range(len(tmpList)):
            if i == self.simPatch:
                break
            cnt += tmpList[i]["count"]

        sortedDataFrame = joinedDataFrame.orderBy("antiSim").limit(cnt).select("*")
        rdd = joinedDataFrame.rdd.map(lambda line: Row(movie=line["movieId"], score=(1 - line["antiSim"]) * line["rating"]))
        rddReduced = rdd.reduceByKey(add).map(lambda line: Row(movie=line[0], score=line[1]))
        df = sqlCtx.createDataFrame(rddReduced).orderBy("score", ascending=0).limit(topk).select("*")
        movies = df.rdd.map(lambda line: line[0]).collect()
        scores = df.rdd.map(lambda line: line[1]).collect()

        return movies, scores