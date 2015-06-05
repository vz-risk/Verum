#!/usr/bin/env python
"""
 AUTHOR: Gabriel Bassett
 DATE: 12-17-2013
 DEPENDENCIES: a list of modules requiring installation
 Copyright 2014 Gabriel Bassett

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
 Functions necessary to enrich the context graph

"""
# PRE-USER SETUP
from datetime import timedelta

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
CONFIG_FILE = "/tmp/verum.cfg"
# Below values will be overwritten if in the config file or specified at the command line
TITAN_HOST = "localhost"
TITAN_PORT = "8182"
TITAN_GRAPH = "vzgraph"
PluginFolder = "./plugins"
NEO4J_HOST = 'localhost'
NEO4J_PORT = '7474'



########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
import imp
import argparse
import logging
from datetime import datetime # timedelta imported above
# todo: Import with IMP and don't import the titan graph functions if they don't import
try:
    from bulbs.titan import Graph as TITAN_Graph
    from bulbs.titan import Config as TITAN_Config
    from bulbs.model import Relationship as TITAN_Relationship
    titan_import = True
except:
    titan_import = False
try:
    from py2neo import Graph as py2neoGraph
    neo_import = True
except:
    neo_import = False
try:
    from yapsy.PluginManager import PluginManager
    plugin_import = True
except:
    plugin_import = False
import ConfigParser
import sqlite3
import networkx as nx
import os
print os.getcwd()

## SETUP
__author__ = "Gabriel Bassett"
# Read Config File - Will overwrite file User Variables Section
config = ConfigParser.SafeConfigParser()
config.readfp(open(CONFIG_FILE))
if config.has_section('TITANDB'):
    if 'host' in config.options('TITANDB'):
        TITAN_HOST = config.get('TITANDB', 'host')
    if 'port' in config.options('TITANDB'):
        TITAN_PORT = config.get('TITANDB', 'port')
    if 'graph' in config.options('TITANDB'):
        TITAN_GRAPH = config.get('TITANDB', 'graph')
if config.has_section('NEO4J'):
    if 'host' in config.options('NEO4J'):
        NEO4J_HOST = config.get('NEO4J', 'host')
    if 'port' in config.options('NEO4J'):
        NEO4J_PORT = config.get('NEO4J', 'port')
if config.has_section('Core'):
    if 'plugins' in config.options('Core'):
        PluginFolder = config.get('Core', 'plugins')

# Parse Arguments - Will overwrite Config File
parser = argparse.ArgumentParser(description='This script processes a graph.')
parser.add_argument('-d', '--debug',
                    help='Print lots of debugging statements',
                    action="store_const", dest="loglevel", const=logging.DEBUG,
                    default=logging.WARNING
                   )
parser.add_argument('-v', '--verbose',
                    help='Be verbose',
                    action="store_const", dest="loglevel", const=logging.INFO
                   )
parser.add_argument('--log', help='Location of log file', default=None)
parser.add_argument('--plugins', help="Location of plugin directory", default=PluginFolder)
parser.add_argument('--titan_host', help="Host for titanDB database.", default=TITAN_HOST)
parser.add_argument('--titan_port', help="Port for titanDB database.", default=TITAN_PORT)
parser.add_argument('--titan_graph', help="Graph for titanDB database.", default=TITAN_GRAPH)
parser.add_argument('--neo4j_host', help="Host for Neo4j database.", default=NEO4J_HOST)
parser.add_argument('--neo4j_port', help="Port for Neo4j database.", default=NEO4J_PORT)

## Set up Logging
args = parser.parse_args()
if args.log is not None:
    logging.basicConfig(filename=args.log, level=args.loglevel)
else:
    logging.basicConfig(level=args.loglevel)
# <add other setup here>


## EXECUTION
#TODO: Selectively import classes based on modules that imported

class TalksTo(TITAN_Relationship):
    label = "talksto"


class DescribedBy(TITAN_Relationship):
    label = "describedBy"


class Influences(TITAN_Relationship):
    label = "influences"


class enrich():
    titandb_config = None
    neo4j_config = None
    enrichment_db = None
    plugins = None

    def __init__(self):

        # Set up the TitanDB Config
        self.set_titan_config(args.titan_host, args.titan_port, args.titan_graph)

        # Set up the Neo4j Config
        self.set_neo4j_config(args.neo4j_host, args.neo4j_port)

        # Load enrichments database
        self.enrichment_db = self.set_enrichment_db()

        # Load the plugins Directory
        self.load_plugins()
        # TODO: set up the plugin directory

    # Load the plugins from the plugin directory.
    def load_plugins(self):
        print "Configuring Plugin manager."
        self.plugins = PluginManager()
        self.plugins.setPluginPlaces([PluginFolder])
        #self.plugins.collectPlugins()
        self.plugins.locatePlugins()
        self.plugins.loadPlugins()
        print "Plugin manager configured."

        # Loop round the plugins and print their names.
        cur = self.enrichment_db.cursor()
        for plugin in self.plugins.getAllPlugins():
            print "Configuring plugin {0}.".format(plugin.name)
            config = plugin.plugin_object.configure()
            if config[0]:
                success = 1
            else:
                success = 0
            # Insert enrichment
            # TODO
            if config[6] == 'enrichment':
                cur.execute("INSERT INTO enrichments VALUES (?, ?, ?, ?, ?)", (config[1],
                                                                               success,
                                                                               config[2],
                                                                               config[4],
                                                                               config[5])

                )
                for input in config[3]:
                    # Insert into inputs table
                    cur.execute("INSERT INTO inputs VALUES (?,?)", config[1], input)
                self.enrichment_db.commit()
            elif config[6] == 'interface':
                pass
                #TODO import interfaces (titanDB and Neo4j currently)


    def set_enrichment_db(self):
        conn = sqlite3.connect(":memory:")
        c = conn.cursor()
        # Create enrichments table
        c.execute('''CREATE TABLE enrichments (name text NOT NULL PRIMARY KEY,
                                               config int,
                                               description text,
                                               cost int,
                                               speed int)''')
        # Create inputs table
        c.execute('''CREATE TABLE inputs (name text NOT NULL,
                                          input text NOT NULL,
                                          PRIMARY KEY (name, input),
                                          FOREIGN KEY (name) REFERENCES enrichments(name))''')
        conn.commit()
        return conn


    def get_enrichments(self, inputs, cost=10, speed=10, enabled=True):
        """

        :param inputs: list of input types.   (e.g. ["ip", "domain"])  All enrichments that match at least 1 input type will be returned.
        :param cost:  integer 1-10 of resource cost of running the enrichment.  (1 = cheapest)
        :param speed: integer 1-10 speed of enrichment. (1 = fastest)
        :param enabled: Plugin is correctly configured.  If false, plugin may not run correctly.
        :return: list of names of enrichments matching the criteria
        """


    def run_enrichments(self, topic, topic_type, cost=10, speed=10):
        """

        :param topic: topic to enrich (e.g. "1.1.1.1", "www.google.com")
        :param topic_type: type of topic (e.g. "ip", "domain")
        :param cost: integer 1-10 of resource cost of running the enrichment.  (1 = cheapest)
        :param speed: integer 1-10 speed of enrichment. (1 = fastest)
        :return: networkx graph representing the enrichment of the topic
        """
        enrichments = self.get_enrichments([topic_type], cost, speed, enabled=True)
        g = nx.MultiDiGraph()

        for enrichment in enrichments:
            # TODO: Run enrichment
            # TODO: Merge enrichment graph with g
            pass

        return g