# -*- coding: utf-8 -*-

import sys
sys.path.append("../")
from utils import EngineUtils, DataSet
from operator import add
from engine.ttypes import UserProfile
from pyspark.sql import Row

class ContentBasedAlgorithm(object):
    def __init__(self, simThresold=0.8, simPatch=10):
        self.simThreshold = simThresold
        self.simPatch = 10

    def Predict(self, userId, movieId):
        sqlCtx = EngineUtils().getSqlContext()
        ratingsDataFrame = DataSet().getRatingsDataFrame()
        movieWatchedDataFrame = ratingsDataFrame.where(ratingsDataFrame.userId ==userId).select("*")

        contentsSimDataFrame = DataSet().getContentsSimDataFrame()
        simMovieDataFrame = contentsSimDataFrame.where(contentsSimDataFrame.movie1 == movieId).select("*")
        simMovieWatchedDataFrame = movieWatchedDataFrame.join(simMovieDataFrame, simMovieDataFrame.movie2 == movieWatchedDataFrame.movieId, \
                                                              "inner").where(simMovieDataFrame.sim > self.simThreshold).select("movie2", "sim", "rating")
        simList = simMovieWatchedDataFrame.orderBy("sim", ascending=0).limit(self.simPatch).select("*").collect()
        simScore = 0.0
        for i in range(len(simList)):
            if simList[i]["movie2"] != movieId:
                simScore += simList[i]["sim"]

        result = 0.0
        for i in range(len(simList)):
            if simList[i]["movie2"] != movieId:
                result += simList[i]["sim"] / simScore * simList[i]["rating"]

        return result

    def Recommend(self, userProfile, topk=10):
        userId = userProfile.userId
        movieWatchedNumRecently = userProfile.movieWatchedNumRecently
        ratingsDataFrame = DataSet().getRatingsDataFrame()
        movieWatchedRecentlyDataFrame = ratingsDataFrame.orderBy("timestamp", ascending=0).where( \
            ratingsDataFrame.userId == userId).limit(movieWatchedNumRecently).select("movieId", "rating")
        contentsSimDataFrame = DataSet().getContentsSimDataFrame()
        contenesSimFilteredDataFrame = contentsSimDataFrame.where(contentsSimDataFrame.sim > self.simThreshold).select("*")
        simMovieDataFrame = movieWatchedRecentlyDataFrame.join(contenesSimFilteredDataFrame, movieWatchedRecentlyDataFrame.movieId == contenesSimFilteredDataFrame.movie1, \
                                                               "inner").select("movieId", "movie2", "sim", "rating")
        scoreDataFrame = simMovieDataFrame.withColumn("score", simMovieDataFrame.sim * simMovieDataFrame.rating).select("movie2", "score")
        rdd = scoreDataFrame.rdd.reduceByKey(add).map(lambda line: Row(movie=line[0], score=line[1]))
        df = EngineUtils().getSqlContext().createDataFrame(rdd).orderBy("score", ascending=0).limit(topk).select("*")
        movies = df.rdd.map(lambda line: line[0]).collect()
        scores = df.rdd.map(lambda line: line[1]).collect()
        return movies, scores


if __name__ == "__main__":
    s = EngineUtils()