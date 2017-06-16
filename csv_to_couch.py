# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 23:57:48 2017

@author: paweueck

python 3

This program is supposed to run only once per a csv file! as it is creating docs in couch db.
If you run the program multiple times per file it will create a number of versions of the same document in the DB.
So be careful!

"""
# resources:
# https://pythonhosted.org/CouchDB/
# https://docs.python.org/3/library/json.html

# 1. convert csv files / line with data to json first?
# add doctype = filename to each of the json documents
# 2. write jsons to couch db

import couchdb
import pandas
import math
import numpy

def parseValue(value):
    if (isinstance(value,str)):
        return value
    elif type(value)==numpy.int64:
        value = int(value)
    elif value is None:
        value = None
    else: 
        try:
            value = str(value)
        except:
            pass 
    return value
      
couch = couchdb.Server('https://paweueck:admin@paweueck.cloudant.com/') #remote
#couch = couchdb.Server('http://admin:admin@localhost:5984/') #local

db = couch['movies'] # existing db



alldocs = dict() #this will be used as register of file_name and documents we uploaded to the DB.


#reading csv and converting each row to json i.e. python dictionary

csv_folder = 'D:\Archive-temp\web_data_m\postgres_exported\\'
file_name = "series"
file_ext = ".csv"
csv_path = csv_folder + file_name + file_ext

#df = pandas.read_csv(csv_path, sep=';', header = 0, low_memory = False )

#for series
skiprows = [318362, 415098, 433542, 611610, 921684, 1030594, 1065826, 1188800]
df = pandas.read_csv(csv_path, sep=';', header = 0, skiprows = skiprows )

# for acted_in
#skiprows = list(range(1,136000+640001)) + [2680652, 9244523, 10460573, 10794599, 12663641, 18002237, 21721094, 25082054]
#df = pandas.read_csv(csv_path, sep=';', header = 0, skiprows = skiprows )

dflen = len(df)

checkpoint = 20000 #this is how many records we will upload at once to the db

newdocs = list()
num_docs_uploaded = 0

for x in range(dflen):
    newdoc = dict()
    for column in df.columns.values:
        value = parseValue(df[column][x])
        newdoc[column] = value
    newdoc['doctype'] = file_name #adding one key value representing the doctype - will be required later in couch for querying
    newdocs.append(newdoc) # we are going to need this to keep track what we added to db - in case we want to remove what we added.
    
    if ((x!=0) & (x%checkpoint==0))|(x==(dflen-1)):
       db.update(newdocs)    
       num_docs_uploaded+=len(newdocs)
       print ("records written to couch: ", num_docs_uploaded, " / ", dflen) #for process completion indication
       newdocs = list()
    #print(newdoc)

#writing what we created to our register on the python side
alldocs[file_name] = newdocs


#get docs from DB.

#deletion of document references from our register
del alldocs[file_name]

