# -*- coding: utf-8 -*-

import logging

from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

from engine import EngineService
from .utils.utils import EngineUtils


class EngineServiceHandler(object):
    def UpdateEngineGroup(self, groupConf):
        logging.info("Start update engine group.")

        logging.info("Start parse json conf.")

        return True

if __name__ == "__main__":
    config = EngineUtils().getConfigParser()

    HOST = config.get("server", "host")
    PORT = config.get("server", "port")
    dst = config.get("log", "destination")

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