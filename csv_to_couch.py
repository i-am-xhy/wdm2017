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

        
#couch = couchdb.Server('https://paweueck:admin@paweueck.cloudant.com/') #remote
couch = couchdb.Server('http://admin:admin@localhost:5984/') #local

db = couch['movies'] # existing db

#reading csv and converting each row to json i.e. python dictionary

csv_folder = 'D:\Archive-temp\web_data_m\postgres_exported\\'
file_name = "genres"
file_ext = ".csv"
csv_path = csv_folder + file_name + file_ext

df = pandas.read_csv(csv_path, sep=';', header = 0 )
dflen = len(df)

checkpoint = 1000 #every n records, we will print the progress of writing to db

alldocs = dict() #this will be used as register of file_name and documents we uploaded to the DB.

newdocs = list()

for x in range(dflen):
    newdoc = dict()
    for column in df.columns.values:
        value = parseValue(df[column][x])
        newdoc[column] = value
        #newdoc = dict(newdoc)               
    newdoc['doctype'] = file_name #adding one key value representing the doctype - will be required later in couch for querying
    db.save(newdoc)
    
    newdocs.append(newdoc) # we are going to need this to keep track what we added to db - in case we want to remove what we added.
    
    if x%checkpoint==0:
       print ("records written to couch: ", x, " / ", dflen-1)
    #print(newdoc)

#writing what we created to our register on the python side
alldocs[file_name] = newdocs





# DELETION of documents - in case we want to get rid of them!
#deletion of documents from db
for item in newdocs:
    db.delete(item)
#deletion of document references from our register
del alldocs[file_name]

