#!/usr/bin/env python

__author__ = "Gabriel Bassett"
"""
 AUTHOR: {0}
 DATE: <DATE>
 DEPENDENCIES: <a list of modules requiring installation>
 Copyright <YEAR> {0}

 LICENSE:
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

 DESCRIPTION:
 <ENTER DESCRIPTION>

""".format(__author__)
# PRE-USER SETUP
pass

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
PLUGIN_CONFIG_FILE = "alexa_1M.yapsy-plugin"
NAME = "Alexa Top 1M"
FEED = "http://s3.amazonaws.com/alexa-static/top-1m.csv.zip"
SLEEP_TIME = 14400  # 4 hours in seconds

########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
import networkx as nx
from datetime import datetime # timedelta imported above
import dateutil
import uuid
import ConfigParser
import inspect
import requests  # for downloading the intel list
import ipaddress  # for validating ip addresses
import time  # for sleep
import threading  # import threading so minion doesn't block the app
import imp  # Importing imp to import verum
import copy
import tldextract  # used for validating domains
import zipfile  # for unzipping alexis 1m file
from StringIO import StringIO  # for opening alexis 1m file in memory


## SETUP
loc = inspect.getfile(inspect.currentframe())
ind = loc.rfind("/")
loc = loc[:ind+1]
config = ConfigParser.SafeConfigParser()
config.readfp(open(loc + PLUGIN_CONFIG_FILE))

if config.has_section('Core'):
    if 'name' in config.options('Core'):
        NAME = config.get('Core', 'name')
if config.has_section('Log'):
    if 'level' in config.options('Log'):
        LOGLEVEL = config.get('Log', 'level')
    if 'file' in config.options('Log'):
        LOGFILE = config.get('Log', 'file')
    else:
        LOGFILE = None

if LOGFILE:
    logging.basicConfig(filename=LOGFILE, level=LOGLEVEL)
else:
    logging.basicConfig(level=LOGLEVEL)

## EXECUTION
class PluginOne(IPlugin):
    thread = None
    app = None  # The object instance
    Verum = None  # the module
    today = datetime.strptime("1970", "%Y")  # Today's date
    shutdown = False  # Used to trigger shutdown of the minion
    parent = None  # The parent instance of the verum app object

    #  CHANGEME: The init should contain anything to load modules or data files that should be variables of the  plugin object
    def __init__(self):
        """

        """
        pass

    #  CHANGEME: Configuration needs to set the values needed to identify the plugin in the plugin database as well as ensure everyhing loaded correctly
    #  CHANGEME: Current  layout is for an enrichment plugin
    #  CHANGEME: enrichment [type, successful_load, name, description, inputs to enrichment such as 'ip', cost, speed]
    #  CHANGEME: interface [type, successful_load, name]
    #  CHANGEME: score [type, successful_load, name, description, cost, speed]
    #  CHANGEME: minion [TBD]
    def configure(self, parent=None):
        """

        :param verum: The directory of the verum module 
        :return: return list of [configure success (bool), name, description, list of acceptable inputs, resource cost (1-10, 1=low), speed (1-10, 1=fast)]
        """
        global FEED

        config_options = config.options("Configuration")

        if 'cost' in config_options:
            cost = config.get('Configuration', 'cost')
        else:
            cost = 9999

        if config.has_section('Documentation') and 'description' in config.options('Documentation'):
            description = config.get('Documentation', 'description')
        else:
            logging.error("'Description not in config file.")
            return [None, False, NAME, None, cost]

        if 'type' in config_options:
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, False, NAME, description, cost]

        #  Module import success
        if parent is not None:
            self.parent = parent
        else:
            logging.info("Parent verum app instance not passed to minion.  Please rerun, passing the parent object instance to successfully configure.")
            return [plugin_type, False, NAME, description, cost]

        if self.parent.loc is not None:
            # Import the app object so that acces app features (such as the storage backend) can be used.
            fp, pathname, mod_description = imp.find_module("verum", [self.parent.loc])
            self.Verum = imp.load_module("verum", fp, pathname, mod_description)
        else:
            logging.error("'verum' location not supplied to minion configuration function.  Rerun with the location of the verum module specified.")
            return [plugin_type, False, NAME, description, cost]

        if 'feed' in config_options:
            FEED = config.get('Configuration', 'feed')
        else:
            logging.error("'Feed' not specified in config file.")
            return [plugin_type, False, NAME, description, cost]

        # Return success
        return [plugin_type, True, NAME, description, cost]


    def minion(self,  storage=None, *args, **xargs):        
        self.app = self.Verum.app(self.parent.PluginFolder, None)
        # set storage
        if storage is None:
            storage = self.parent.storage
        self.app.set_interface(storage)

        # Check until stopped
        while not self.shutdown:
            # Check to see if it's the same day, if it is, sleep for a while, otherwise run the import
            delta = datetime.utcnow() - self.today
            if delta.days <= 0:
                time.sleep(SLEEP_TIME)
            else:
                logging.info("Starting daily {0} enrichment.".format(NAME))

               # Create list of IPs for cymru enrichment
                ips = set()

                # Get the file
                r = requests.get(FEED)

                # Unzip the file
                z = zipfile.ZipFile(StringIO(r.content))

                # get the time
                dt = datetime.utcnow()

                with z.open('top-1m.csv') as f:
                    for line in f:
                        try:
                            line = line.strip().split(",")

                            # Validate data in row
                            ext = tldextract.extract(line[1])
                            if not ext.domain or not ext.suffix:
                                # domain is not legitimate
                                next

                            # classify benign and merge with current graph
                            g =  self.app.classify.run({'key': 'domain', 'value': line[1], 'classification': 'benign'}, confidence=1 - (int(line[0])-1)/float(1000000))

                            # enrich depending on type
                            try:
                                g = self.Verum.merge_graphs(g, self.app.run_enrichments(line[1], "domain", names=['TLD Enrichment']))
                                g = self.Verum.merge_graphs(g, self.app.run_enrichments(line[1], "domain", names=['DNS Enrichment']))
                                g = self.Verum.merge_graphs(g, self.app.run_enrichments(line[1], "domain", names=['IP Whois Enrichment']))
                            except Exception as e:
                                logging.info("Enrichment of {0} failed due to {1}.".format(line[1], e))
                                #print "Enrichment of {0} failed due to {1}.".format(domain, e)  # DEBUG
                                #raise
                                pass

                            # Collect IPs
                            line_ips = set()
                            for node, data in g.nodes(data=True):
                                if data['key'] == 'ip':
                                    line_ips.add(data['value']) 

                            for ip in line_ips:
                                try:
                                    g = self.Verum.merge_graphs(g, self.app.run_enrichments(ip, "ip", names=[u'Maxmind ASN Enrichment']))
                                except Exception as e:
                                    logging.info("Enrichment of {0} failed due to {1}.".format(ip, e))
                                    pass

                            try:
                                self.app.store_graph(self.Verum.remove_non_ascii_from_graph(g))
                            except:
                                print g.nodes(data=True)  # DEBUG
                                print g.edges(data=True)  # DEBUG
                                raise

                            ips = ips.union(line_ips)
                            # Do cymru enrichment
                            if len(ips) >= 50:
                                # validate IPs
                                ips2 = set()
                                for ip in ips:
                                    try:
                                        _ = ipaddress.ip_address(unicode(ip))
                                        ips2.add(ip)
                                    except:
                                        pass
                                ips = ips2
                                del(ips2)
                                try:
                                    self.app.store_graph(self.app.run_enrichments(ips, 'ip', names=[u'Cymru Enrichment']))
                                    #print "Cymru enrichment complete."
                                except Exception as e:
                                    logging.info("Cymru enrichment of {0} IPs failed due to {1}.".format(len(ips), e))
                                    #print "Cymru enrichment of {0} IPs failed due to {1}.".format(len(ips), e)  # DEBUG
                                    pass
                                ips = set()

                        except Exception as e:
                            print line
                            print e
                            raise

                # Copy today's date to today
                self.today = datetime.utcnow()

                logging.info("Daily {0} enrichment complete.".format(NAME))
                print "Daily {0} enrichment complete.".format(NAME)  # DEBUG

    def start(self, *args, **xargs):
        self.shutdown = False
        self.thread = threading.Thread(target=self.minion, *args, **xargs)
        self.thread.start()

    def isAlive(self):
        if self.thread is None:
            return False
        else:
            return self.thread.isAlive()


    def stop(self, force=True):
        if force:
            self.thread = None  # zero out thread
        else:
            self.shutdown = False  # just dont' iterate.  May take up to (SLEEP_TIME) hours
