# -*- coding: utf-8 -*-

import sys
sys.path.append("./")
from utils import EngineUtils
from engine.ttypes import UserProfile
from alg.alternatingLeastSquareAlgorithm import AlternatingLeastSquareAlgorithm
from alg.contentBasedAlgorithm import ContentBasedAlgorithm
from alg.popuBasedAlgorithm import PopuBasedAlgorithm

def TEST_ALS():
    user = UserProfile(1, 3)
    als = AlternatingLeastSquareAlgorithm()
    print(als.Predict(user, 1))
    print(als.Recommend(user))

def TEST_CONTENT():
    user = UserProfile(1, 3)
    alg = ContentBasedAlgorithm()
    print(alg.Predict(user, 1))
    print(alg.Recommend(user))

def TEST_POPU():
    user = UserProfile(1, 3)
    alg = PopuBasedAlgorithm()
    print(alg.Predict(user, 1))
    print(alg.Recommend(user))

if __name__ == "__main__":
    EngineUtils().getSparkContext().setLogLevel("ERROR")
    TEST_ALS()
    TEST_CONTENT()
    TEST_POPU()