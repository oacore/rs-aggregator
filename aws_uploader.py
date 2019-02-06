# Import the os module, for the os.walk function
import os
import requests
import sys
from multiprocessing.dummy import Pool as ThreadPool

# Set the directory you want to start from
rootDir = 'target/destination'
awsurl="https://search-core-resync-tqjhs3lbpljpp76xbglvpgyrme.us-west-2.es.amazonaws.com"
start=0
count=0
pool = ThreadPool(32)
files=[]
def run(file):
    fname = file[1]
    dirName=file[0]
    if fname.endswith("json"):
        coreid = fname.split(".")[0]
        with open(dirName+"/"+fname, 'r') as f:
            payload=f.read()
        print("Uploading %s" % coreid)
        response = requests.put(awsurl+"/articles/articles/"+coreid, data=payload, headers={"Content-Type":"application/json"})
        print(response.text)


if sys.argv[1]:
    start=int(sys.argv[1])

for dirName, subdirList, fileList in os.walk(rootDir):
    print('Found directory: %s' % dirName)
    count = count+len(fileList)
    if count>start:
        for fname in fileList:
            if fname.endswith("json"):
                print(dirName)
                print (fname)
                files.append([dirName, fname])
results = pool.map(run, files)
