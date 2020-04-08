# -*- coding: utf-8 -*-

import logging
import os

from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

from manager import EngineManager
from engine import EngineService
from utils import EngineUtils
from alg.alternatingLeastSquareAlgorithm import AlternatingLeastSquareAlgorithm


class EngineServiceHandler(object):
    def Ping(self):
        return "Pong"

    def UpdateEngineGroup(self, groupConf):
        logging.info("Start update engine group.")
        return EngineManager().Update(groupConf)

    def Predict(self, userProfile, movieId):
        return EngineManager().Predict(userProfile, movieId)

    def Recommend(self, userProfile, topk):
        return EngineManager().Recommend(userProfile, topk)

def main():
    config = EngineUtils().getConfigParser()

    HOST = config.get("server", "host")
    PORT = config.get("server", "port")
    dst = config.get("log", "destination")
    if not os.path.exists(dst):
        index = dst.rfind("/", 0, len(dst))
        if index != -1:
            os.makedirs(dst[:index])

    if config.get("server", "dev") == "release":
        logging.basicConfig(filename=dst, level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    else:
        logging.basicConfig(filename=dst, level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    handler = EngineServiceHandler()
    processor = EngineService.Processor(handler)
    transport = TSocket.TServerSocket(HOST, PORT)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    logging.info("Engine server start run at %s:%s", HOST, PORT)
    server.serve()
    logging.info("Engine server exit ...")

if __name__ == "__main__":
    main()