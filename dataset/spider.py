# -*- coding: utf-8 -*-

import requests
from pyquery import PyQuery as pq
import pandas as pd
import threading
from tqdm import tqdm

file = open("imgs-source", "a")

class Spider(threading.Thread):
    def __init__(self, imdbId):
        super(Spider, self).__init__()
        self.baseUrl = "https://www.imdb.com/title/tt0"
        self.imdbId = str(imdbId)

    def run(self):
        url = self.baseUrl + self.imdbId + '/'
        rsp = requests.get(url, proxies={'http': 'http://localhost:12333', 'https': 'http://localhost:12333'}).text
        doc = pq(rsp)
        imgUrl = doc('.poster a img').attr.src
        if imgUrl is None:
            file.write(self.imdbId + "\n")
        else:
            file.write(self.imdbId + " " + imgUrl + '\n')

def main_multi_thread():
    linksDataFrane = pd.read_csv("./links.csv", dtype=str)

    threads = []
    for index, item in tqdm(linksDataFrane.iterrows()):
        if int(item[0]) > 3952:
            break
        th = Spider(item[1])
        threads.append(th)
        th.start()

    for t in threads:
        t.join()
    file.close()

def main():
    linksDataFrane = pd.read_csv("./links.csv", dtype=str)
    for index, item in tqdm(linksDataFrane.iterrows()):
        if int(item[0]) > 3952:
            break
        baseUrl = "https://www.imdb.com/title/tt0"
        imdbId = str(item[1])
        url = baseUrl + imdbId + '/'
        # rsp = requests.get(url, proxies={'http': 'http://localhost:12333', 'https': 'http://localhost:12333'}).text
        rsp = requests.get(url).text
        doc = pq(rsp)
        imgUrl = doc('.poster a img').attr.src
        if imgUrl is None:
            file.write(imdbId + "\n")
        else:
            file.write(imdbId + " " + imgUrl + '\n')
        if index == 3:
            break
    file.close()


if __name__ == "__main__":
    main()