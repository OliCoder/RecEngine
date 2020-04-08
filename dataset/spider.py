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
    for index, item in linksDataFrane.iterrows():
        if int(item[0]) > 3952:
            break
        print(str(index) + "/3952")
        baseUrl = "https://www.imdb.com/title/tt0"
        imdbId = str(item[1])
        url = baseUrl + imdbId + '/'
        rsp = requests.get(url).text
        doc = pq(rsp)
        imgUrl = doc('.poster a img').attr.src
        if imgUrl is None:
            file.write(imdbId + "\n")
        else:
            file.write(imdbId + " " + imgUrl + '\n')
    file.close()

def downloadImg(movieId, url):
    rsp = requests.get(url)
    img = rsp.content
    path = "./pic/" + str(movieId) + ".jpg"
    with open(path, "wb+") as f:
        f.write(img)

def downnloadImgs():
    linksDataFrane = pd.read_csv("./links.csv", dtype=str)
    movieIds = list(linksDataFrane["movieId"])
    imdbIds = list(linksDataFrane["imdbId"])
    moviesMap = {}
    for i in range(len(imdbIds)):
        moviesMap[int(imdbIds[i])] = movieIds[i]

    with open("./imgs-source") as f:
        for line in f:
            line = line.strip("\n").split()
            if int(moviesMap[int(line[0])]) <=3068:
                continue
            if len(line) == 2:
                downloadImg(moviesMap[int(line[0])], line[1])

def fixMissingUrl():
    cnt = 0
    succ = 0
    with open("./fix", "a") as nf:
        with open("./imgs-source") as f:
            for line in f:
                line = line.strip("\n").split()
                if len(line) != 2:
                    url = "https://www.imdb.com/title/tt0" + str(line[0]) + '/'
                    print(url)
                    rsp = requests.get(url).text
                    doc = pq(rsp)
                    imgUrl = doc('.poster a img').attr.src
                    if imgUrl:
                        nf.write(str(line[0]) + " " + imgUrl + "\n")
                        succ += 1
                    cnt += 1
                else:
                    nf.write(str(line[0]) + " " + str(line[1]) + "\n")

    print(cnt, succ)


if __name__ == "__main__":
    # main()
    fixMissingUrl()