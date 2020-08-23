#!/usr/bin/python

##############################
#
# Sysdig mediator for IBM Predictive Insights
#
# 8/22/20 - Jason Cress (jcress@us.ibm.com)
#
###################################################

import urllib2
import json
import sys
import time
import re
import datetime
import os
import shlex
import csv
import StringIO
import logging
import calendar
import os.path
from os import path

##############
#
# Function to perform a SysDig API Query
#
########################################

aspectStats = {}

def sysDigQuery(queryData):

   method = "POST"

   request = urllib2.Request(sysdigQueryUrl, queryData)
   request.add_header("Content-Type",'application/json')
   request.add_header("Accept",'application/json')
   request.add_header("Authorization",'Bearer  ' + myToken)
   request.add_header("Cookie", myApiKey)
   request.get_method = lambda: method

   response = urllib2.urlopen(request)
   return response.read()


##############
#
# Simple function to validate json
#
##################################
def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True

#############################
#
#  Function to read configuration file
#
#######################################

def load_properties(filepath, sep='=', comment_char='#'):
    """
    Read the file passed as parameter as a properties file.
    """
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"') 
                props[key] = value 
    return props

#########################
#
# Function to validate collection aspect
#
########################################
def validateAspect(aspect):
   if aspect in configvars.keys():
      if(os.path.isfile(configvars[aspect])):
         aspectStats[aspect] = open(configvars[aspect], 'r').read()
         if(not validateJSON(aspectStats[aspect])):
           print("FATAL: " + aspect + " json is malformed. Please validate json file located at " + configvars[aspect])
           exit()
      else:
         print("FATAL: " + aspect + " json file " + configvars[aspect] + " does not exist, but is being requested")
         exit()
   else:
      logging.info("WARNING: no " + aspect + " definition in configuration file")
      logging.info("Statistics for aspect " + aspect + " not being requested.")
      return False
   return True

#################
#
#  Initial configuration items
#
##############################

mediatorBinDir = os.path.dirname(os.path.abspath(__file__))
extr = re.search("(.*)bin", mediatorBinDir)
if extr:
   mediatorHome = extr.group(1)
else:
   print "FATAL: unable to find mediator home directory. Is it installed properly? bindir = " + mediatorBinDir
   exit()

if(os.path.isdir(mediatorHome + "log")):
   logging.basicConfig(filename=mediatorHome + 'log/sysdig-mediator.log',level=logging.INFO)
else:
   print "FATAL: unable to find log directory." 
   exit()

if(os.path.isdir(mediatorHome + "sysdigcsv")):
   csvFileDir = "../sysdigcsv/"
else:
   print "FATAL: unable to find sysdigcsv directory"
   exit()

if(os.path.isfile(mediatorHome + "/config/sysdig_config.txt")):
   pass
else:
   print "FATAL: unable to find mediator config file " + mediatorHome + "/config/sysdig_config.txt"
   exit()

configvars = load_properties(mediatorHome + "/config/sysdig_config.txt")

logging.debug("Configuration variables are: " + str(configvars))

if 'apiKey' in configvars.keys():
   myApiKey = configvars['apiKey']
   #logging.info("Sysdig apiKey is " + myApiKey)
else:
   print "FATAL: Sysdig apikey not defined in config file."
   exit()

if 'hostName' in configvars.keys():
   mySysdigHost = configvars['hostName']
   logging.info("Sysdig host is " + mySysdigHost)
else:
   print "FATAL: Sysdig host name not defined in config file."
   exit()

if 'protocol' in configvars.keys():
   myProtocol = configvars['protocol']
   logging.info("Protocol to use is " + myProtocol)
else:
   print "FATAL: Protocol (http or https) not defined in config file."
   exit()

if 'token' in configvars.keys():
   myToken = configvars['token']
   #logging.debug("Token to use: " + myToken)
else:
   print "FATAL: API key not defined in config file."
   exit()

if 'port' in configvars.keys():
   mySysdigPort = configvars['port']
   logging.debug("DEBUG: TCP Port to use: " + mySysdigPort)
else:
   print "WARNING: Port number not defined in config file. Defaulting to port 80"
   mySysdigPort = "80"

if 'logginglevel' in configvars.keys():
   if (configvars['logginglevel'] in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']):
      logging.basicConfig(filename=mediatorHome + 'log/sysdig-mediator.log',level=configvars['logginglevel'])
      logging.info("Logging is" + configvars['logginglevel'])
   else:
      logging.info("Unknown log level, default to INFO")
      

if 'saveApiResponse' in configvars.keys():
   if (configvars['saveApiResponse'] == '1'):
      logging.info("saving API response under log directory...")
      saveApiResponse = 1
   else:
      saveApiResponse = 0

myTimeStamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())

# create unix timestamp from current run time and align it to previous 10 minute boundary to match SysDig response

tm = datetime.datetime.utcnow()
tm = tm - datetime.timedelta(minutes=tm.minute % 10, seconds=tm.second, microseconds = tm.microsecond)
tm = tm - datetime.timedelta(minutes=tm.minute - 10)

myUnixTimeStamp = str(calendar.timegm(tm.timetuple()))
sysdigQueryUrl = myProtocol + "://" + mySysdigHost + ":" + mySysdigPort + "/api/data" 

############################
#
#  Perform Sysdig API query and parse json responses as python object
#
#  'aspect' is the type of metrics i.e. nodeStats, clusterStats, namespaceStats, clusterCalculatedStats
#
#################################################################################################

def apiReadWriteCsv(aspectData, aspectName):

   myCsvFileName = mediatorHome + "/sysdigcsv/" + aspectName + "-" + myUnixTimeStamp + ".csv"
   logging.info("writing to csv file named " + myCsvFileName)

   #outFile = open(myCsvFileName, "w")

   logging.info("Querying data for aspect " + aspectName + ", query url: " + sysdigQueryUrl)

   # read node configuration json into python dict

   statsConfigDict = json.loads(aspectData)
   csvHeader = "timestamp,"
   for entries in statsConfigDict['metrics']:
      csvHeader = csvHeader + str(entries['id']) + ","
   csvHeader = csvHeader.strip(',')

   # do query

   statsResponse = sysDigQuery(aspectData)

   # convert response to python dictionary

   statsDict = json.loads(statsResponse)

   # iterate through entries
   numitems = 0
   for entries in statsDict['data']:
     numitems = numitems + 1
     csvLine = str(entries['t']) + ","
     
     # iterate through metrics 

     for dataentries in entries['d']:
        stritem = str(dataentries)
        csvLine = csvLine + stritem + ","
     myCsvFileName = mediatorHome + "/sysdigcsv/" + aspectName + "-" + str(entries['t']) + ".csv"
     if(path.exists(myCsvFileName)):
        outFile = open(myCsvFileName, "a") 
     else:
        outFile = open(myCsvFileName, "w") 
        outFile.write(csvHeader)
        outFile.write("\n")
     csvLine = csvLine.strip(',')
     outFile.write(csvLine)
     outFile.write("\n")

   # next, add any calculated metrics to the csv line

#   calculatedMetricsFilename = aspectName + "CalculatedMetrics.txt"
#   if(os.path.isfile(mediatorHome + "/config/" + calculatedMetricsFilename)):
#      print "Found calculated metric definitions for " + aspectName
#   else:
#      print "No calculated metrics found by file name " + calculatedMetricsFilename
#
   logging.info("Item count is: " + str(numitems))
#
#   print "API read completed"
   outFile.close

############################
#
#  Begins here...
#
############################		

logging.info("====== BEGIN RUN - TIMESTAMP IS " + myUnixTimeStamp)

aspectList = { 'nodeStats', 'clusterStats', 'namespaceStats', 'deploymentStats' }
for myAspect in aspectList:
   if(validateAspect(myAspect)):
      apiReadWriteCsv(aspectStats[myAspect], myAspect)

logging.info("====== END RUN - TIMESTAMP IS " + myUnixTimeStamp)
exit()

######
#
# uncomment the following to use a file instead of url query
#
# set saveApiResponse = 0 if doing this
#
############################################################

#with open("servicestatus_Nevada.json") as f:
#   parsedServiceStatusContents = json.load(f)
#saveApiResponse=0 

#########
#
# Write the API response to log directory if requested in config file
#
#####################################################################

if(saveApiResponse):
   serviceStatusApiOutput = open( mediatorHome + "/log/serviceStatusApiOutput.json", "w")
   serviceStatusApiOutput.write(serviceStatusContents)
   serviceStatusApiOutput.close

###########
#
#  Iterate through service status response and pull PI metrics of interest, write to files
#
##########################################################################################

exit()
recordCount = int(parsedServiceStatusContents['recordcount'])
logging.debug("number of service status records: " + str(recordCount))

recordIndex = 0
while recordIndex < recordCount:

   myHostName = parsedServiceStatusContents['servicestatus'][recordIndex]['host_name']
   myServiceName = parsedServiceStatusContents['servicestatus'][recordIndex]['name']

   logging.debug("Service name: " + myServiceName)
   if(parsedServiceStatusContents['servicestatus'][recordIndex]['performance_data']):
      myPerfData = str(parsedServiceStatusContents['servicestatus'][recordIndex]['performance_data'])
      logging.debug("performance_data=" + myPerfData)
   else:
      pass
      logging.debug("WARNING: no performance data found for Nagios monitor record: " + myServiceName)

   #########################
   # 
   #  Check to see if there are any matches (explicit or substring) for this record in the configuration dictionary
   #
   ################################################################################################################

   
   for serviceIndex in configDict:
      substringMatch = 0
      extr = re.search('match:(.*)', configDict[serviceIndex]['servicename'])
      if extr:
         checkMatch = extr.group(1)
         if(checkMatch in myServiceName):
            logging.debug("Match on substring test for monitor config record: " + myServiceName)
            logging.debug("checkMatch is: " + checkMatch + " and myServiceName is: " + myServiceName)
            substringMatch = 1 
         else:
            checkMatch = "not-substring-match"
         
      if((configDict[serviceIndex]['servicename'] == myServiceName) or (substringMatch == 1)):
         logging.debug("Found matching config entry for " + myServiceName + " and config record " + configDict[serviceIndex]['servicename'])
         logging.debug("writing to filename " + configDict[serviceIndex]['filename'] + ", csvheader " +  configDict[serviceIndex]['csvheader'] + ", data " + configDict[serviceIndex]['csvdatadef'])
         writePiCsvEntry(configDict[serviceIndex]['filename'], configDict[serviceIndex]['csvheader'], configDict[serviceIndex]['csvdatadef'], configDict[serviceIndex]['csvDict'], parsedServiceStatusContents['servicestatus'][recordIndex])
        
            
      else:
         pass
         logging.debug("No config entry for " + myServiceName)
         ##########################
         #
         #  No explicit or "match:" definitions found in the config for this monitor API record
         #
         ###################################################################################

   recordIndex = recordIndex + 1

