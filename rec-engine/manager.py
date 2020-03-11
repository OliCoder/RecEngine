# -*- coding: utf-8 -*-

import json
import logging
import os
import math
import random

from utils import Singleton
from alg.alternatingLeastSquareAlgorithm import AlternatingLeastSquareAlgorithm
from alg.contentBasedAlgorithm import ContentBasedAlgorithm
from alg.popuBasedAlgorithm import PopuBasedAlgorithm
from engine.ttypes import UserProfile

@Singleton
class EngineManager(object):
    def __init__(self, jsonContent=""):
        self.Update(jsonContent)

    def Update(self, jsonContent):
        self.models = []
        if len(jsonContent) == 0:
            logging.error("Json content is empty, use default config")
            with open("../conf/defaultEngineGroup.json", "r") as file:
                Obj = json.load(file)
        try:
            Obj = json.loads(jsonContent)
            if len(Obj["models"]) == 0:
                logging.error("Json content is empty, use default config")
                with open("../conf/defaultEngineGroup.json", "r") as file:
                    Obj = json.load(file)
        except Exception as e:
            logging.error("Parser json failed, exception: %s", str(e))
            return False

        for model in Obj["models"]:
            params = model["params"]
            if model["name"] == "ALS":
                maxIter = 10; regParam = 0.01; implicitPrefs = False
                if "maxIter" in params.keys():
                    maxIter = params["maxIter"]
                if "regParam" in params.keys():
                    regParam = params["regParam"]
                if "implicitPrefs" in params.keys():
                    implicitPrefs = params["implicitPrefs"]
                self.models.append([AlternatingLeastSquareAlgorithm(maxIter=maxIter, regParam=regParam, implicitPrefs=implicitPrefs), params["weight"]])
            elif model["name"] == "CONTENT":
                simThreshold = 0.8; simPatch = 10
                if "simThreshold" in params.keys():
                    simThreshold = params["simThreshold"]
                if "simPatch" in params.keys():
                    simPatch = params["simPatch"]
                self.models.append([ContentBasedAlgorithm(simThresold=simThreshold, simPatch=simPatch), params["weight"]])
            elif model["name"] == "POPU":
                antiSimThreshold = 0.4; simPatch = 10
                if "antiSimThreshold" in params.keys():
                    antiSimThreshold = params["antiSimThreshold"]
                if "simPatch" in params.keys():
                    simPatch = params["simPatch"]
                self.models.append([PopuBasedAlgorithm(antiSimThreshold=antiSimThreshold,simPatch=simPatch), params["weight"]])
        return True

    def Predict(self, userProfile, movieId):
        score = 0.0
        for item in self.models:
            score += item[0].Predict(userProfile.userId, movieId) * item[1]
        return score

    def Recommend(self, userProfile, topk):
        movies = []
        for item in self.models:
            movies.extend(item[0].Recommend(userProfile, math.ceil(topk * item[1])))
        if len(movies) > topk:
            random.shuffle(movies)
        return movies[:topk]