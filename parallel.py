import json
import os
import re
import sys
import numpy as np
from mpi4py import MPI

firstTime = MPI.Wtime()
def bInput(mainPath):
    trackList = []
    for dir in os.listdir(mainPath):
        for file in os.listdir(os.path.join(mainPath, dir)):
            trackList.append(os.path.join(mainPath, dir, file))
    return trackList

def readData(path):
    with open(path, 'r', errors='surrogateescape') as inputFile:
        print("executing for " + inputFile.name)
        data = inputFile.read()
    return data

def writeOutput(path, data):
    with open(path, 'w') as outputText:
        outputText.write(data)

def reArrange(data):
    data = data.lower()
    data = data.replace("\n", " ")
    data = re.sub('\t+', ' ', data)
    data = re.sub(' +', ' ', data)
    return data

def normalTermFreq(marks, fileFreq):
    termFreq = {}
    count = len(marks)
    for mark in marks:
        if mark in termFreq: termFreq[mark] = termFreq[mark] + 1.0 / count
        else:
            termFreq[mark] = 1.0 / count
            if mark in fileFreq:
               fileFreq[mark] = fileFreq[mark] + 1
            else:
                fileFreq[mark] = 1
    return termFreq, fileFreq

def wordGram(marks, n):
    grams = []
    for i in range(0, len(marks)):
        nominee = marks[i:i + n]
        gram = ' '.join(nominee)
        grams.append(gram)
    return grams

def nGram(data, Range):
    marks = data.split(" ")
    gram = []
    for i in range(Range[0], Range[1] + 1):
        gram.extend(wordGram(marks, i))
    return gram

communicator = MPI.COMM_WORLD
coreNum = communicator.Get_size()
rank = communicator.Get_rank()
base = 0
dataLocation = sys.argv[1]
resultLocation = sys.argv[2]
ngramSize = int(sys.argv[3])
ngram = int(sys.argv[4])
chunk = None
trackList = None
termFreqs = []
fileFreq = {}
fileTime = MPI.Wtime()
print("file loading time " + str((fileTime - firstTime)))

if rank == base:
    execTime = MPI.Wtime()
    if not os.path.exists(resultLocation):
        os.makedirs(resultLocation)
    if not os.path.exists(os.path.join(resultLocation, "Parallel")):
        os.makedirs(os.path.join(resultLocation, "Parallel"))
    trackList = bInput(dataLocation)
    chunk = np.array_split(trackList, coreNum)
datas = communicator.scatter(chunk, base)

for file in datas:
    data = readData(file)
    data = reArrange(data)
    marks = nGram(data, (ngramSize, ngram))
    markTermFreq, fileFreq = normalTermFreq(marks, fileFreq)
    writeOutput(os.path.join(resultLocation, "Parallel",os.path.basename(file) + ".json"),json.dumps(markTermFreq, indent=2, ensure_ascii=False))
    termFreqs.append(markTermFreq)
relFileFreqs = communicator.gather(fileFreq, base)

if rank == base:
    fileFreq = {}
    vecNo = 0
    allData = len(trackList)
    for relFileFreq in relFileFreqs:
        for mark in relFileFreq:
            if mark in fileFreq:
                fileFreq[mark] = fileFreq[mark] + relFileFrequency[mark]
            else:
                fileFreq[mark] = relFileFreq[mark]
                vecNo += 1

if rank == base:
    computeTime = MPI.Wtime()
    print("Compute time " + str((computeTime - fileTime)))
    lastTime = MPI.Wtime()
    print("Total time to complete " + str((lastTime - execTime)))
