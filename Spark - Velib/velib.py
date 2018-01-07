#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 16:37:00 2017

@author: Reuben+Samuel+Joel
"""

import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext()
ssc = StreamingContext(sc, 5)

def print_all_stations(stream):
    print("the stations are")
    stations = stream.map(lambda station: json.loads(station))\
        .map(lambda station: (
            station['contract_name'] + ' ' + station['name'],
            station['available_bikes']
        ))\
        .pprint()

    
def print_empty_stations(stream):
    print("The empty stations are")
    stations = stream.map(lambda station: json.loads(station))\
        .filter(lambda station:station['available_bikes']==0)\
        .map(lambda station: (
            station['contract_name'] + ' ' + station['name'],
            station['available_bikes']
        ))\
        .pprint()


# =============================================================================
#  print the Velib stations that have become empty
# =============================================================================
def getKeyValue_emptiness(station):                                                                               #ticker unique
    return(station["contract_name"]+ ' ' + station["name"],station["available_bike_stands"])

       
def updateFunction(newValues, runningCount):
    if runningCount is None:
        if len(newValues)>0:
            runningCount = int(newValues[0]>0) 
        else:
            runningCount=0
        
    
    #runningCount=0 si la valeur précédente etait nulle
    #runningCount=1 si la valeur précédente n'est pas nulle
    #runningCount=2 si la station s'est vidée avant
    
    #Si pas d'info, on considère que la station est vide
    if len(newValues)==0:
        res=0
    #Si la nouvelle valeur est 0
    elif newValues[0]==0:
        #Si l'ancienne était également nulle, res=0
        if runningCount==0:
            res=0
        #Si l'ancienne valeur était positive, res=2
        elif runningCount==1:
            res=2
        #Si la station venait de se vider, on initialise à 0
        elif runningCount==2:
            res=0
    #Si la station est non vide res=1
    else:
        res=1
    return res

def print_new_empty_stations(stream):
    stations = stream.map(lambda station: getKeyValue_emptiness(json.loads(station)))\
        .transform(lambda rdd : rdd.distinct())\
        .updateStateByKey(updateFunction)\
        .filter(lambda station : station[1]==2)\
        .pprint()  


# =============================================================================
#  print the Velib stations activities
# =============================================================================
def getKeyValue_activity(station):
    return(station["contract_name"]+ ' ' + station["name"],station["available_bike_stands"])

#L'activity est la valeur absolu de la différence de nombre de vélos disponibles entre t et t+1
def activity(station):                                      
    act=0
    bikes=station[1]
    #On classe par temps
    bikes=sorted(bikes,key=lambda x : x[1])
    for i in range(0,len(bikes)-1):
        act+=abs(bikes[i][0]-bikes[i+1][0])
    return(station[0],act)

def print_activity(stream):
    #on s'assure que les données sans dans le bon ordre avec time
    stations =stream.map(lambda station: getKeyValue_activity(json.loads(station)))\
           .transform(lambda rdd : rdd.distinct())\
           .transform(lambda time,rdd : rdd.map(lambda station :(station[0],[(station[1],time)])))\
           .reduceByKeyAndWindow(lambda x,y: x + y,None,300,60)\
           .map(lambda station : activity(station))\
           .transform(lambda rdd: rdd.sortBy(lambda station: -station[1])).pprint()                     
       

# =============================================================================
# ANSWERS
# =============================================================================


#Defining stream
stream = ssc.socketTextStream("velib.behmo.com", 9999)


#EXO1
#print_all_stations(stream)

#EXO2
#print_empty_stations(stream)

#EXO3
#print_new_empty_stations(stream)

#EXO4
print_activity(stream)

ssc.checkpoint("./checkpoint")
ssc.start()
ssc.awaitTermination()


