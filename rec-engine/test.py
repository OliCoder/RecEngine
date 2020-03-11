# -*- coding: utf-8 -*-

import sys
sys.path.append("./")
from utils import EngineUtils
from alg.alternatingLeastSquareAlgorithm import AlternatingLeastSquareAlgorithm
from alg.contentBasedAlgorithm import ContentBasedAlgorithm
from alg.popuBasedAlgorithm import PopuBasedAlgorithm

def TEST_ALS():
    als = AlternatingLeastSquareAlgorithm()
    print(als.Predict(1, 1))
    print(als.Recommend(1))

def TEST_CONTENT():
    alg = ContentBasedAlgorithm()
    print(alg.Predict(1, 1))
    print(alg.Recommend(1, 3))

def TEST_POPU():
    alg = PopuBasedAlgorithm()
    print(alg.Predict(1, 1))
    print(alg.Recommend(1))

if __name__ == "__main__":
    EngineUtils().getSparkContext().setLogLevel("ERROR")
    TEST_ALS()
    TEST_CONTENT()
    TEST_POPU()