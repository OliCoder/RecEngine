# -*- coding: utf-8 -*-

import os
import sys
sys.path.append("../")
from utils import DataSet, EngineUtils
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import SparkSession
from engine.ttypes import UserProfile

class AlternatingLeastSquareAlgorithm(object):
    def __init__(self, modelPath="dataset/als_model", maxIter=10, regParam=0.01, implicitPrefs=False, upgradeModel = False):
        # TODO（chenjinghui）：从文件加载模型
        if os.path.exists(modelPath):
            self.model = ALSModel.load(modelPath)
            return
        data = DataSet().getRatingsDataFrame()
        self.model = ALS(maxIter=maxIter, regParam=regParam, implicitPrefs=implicitPrefs, \
                         userCol="userId", itemCol="movieId", ratingCol="rating").fit(data)
        if modelPath != "" and upgradeModel:
            self.model.save(modelPath)

    def Predict(self, userId, movieId):
        sqlCtx = EngineUtils().getSqlContext()
        tmpDataFrame = sqlCtx.createDataFrame([(userId, movieId)], schema=["userId", "movieId"])
        prediction = self.model.transform(tmpDataFrame)
        result = prediction.rdd.map(lambda line: line[2]).collect()
        return result[0]

    def Recommend(self, userProfile, topk=10):
        userId = userProfile.userId
        sqlCtx = EngineUtils().getSqlContext()
        movieNum = DataSet().getMoviesDataFrame().count()
        tmpList = [(userId, i) for i in range(1, movieNum + 1)]
        tmpDataFrame = sqlCtx.createDataFrame(tmpList, schema=["userId", "movieId"])
        prediction = self.model.transform(tmpDataFrame).dropna().orderBy("prediction", ascending=False).limit(topk).select("*")
        movies = prediction.rdd.map(lambda line: line[1]).collect()
        scores = prediction.rdd.map(lambda line: line[2]).collect()
        return movies, scores

if __name__ == "__main__":
    pass