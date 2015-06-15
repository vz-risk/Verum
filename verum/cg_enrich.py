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
import logging

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
CONFIG_FILE = "/tmp/verum.cfg"
LOGLEVEL = logging.INFO
LOG = None



########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
import imp
import argparse
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

## SETUP
__author__ = "Gabriel Bassett"
# Parse Arguments - Will overwrite Config File
if __name__ == "__main__":
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
    parser.add_argument('--plugins', help="Location of plugin directory", default=None)


# Read Config File - Will overwrite file User Variables Section
log = LOG
loglevel = LOGLEVEL
try:
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(CONFIG_FILE))
    config_file = True
except Exception as e:
    config_file = False
    logging.warning("Config import failed with error {0}".format(e))
# If the config file loaded...
if config_file:
    if config.has_section('Core'):
        if 'plugins' in config.options('Core'):
            PluginFolder = config.get('Core', 'plugins')
    if config.has_section('LOGGING'):
        if 'level' in config.options('LOGGING'):
            level = config.get('LOGGING', 'level')
            if level == 'debug':
                loglevel = logging.DEBUG
            elif level == 'verbose':
                loglevel = logging.INFO
            else:
                loglevel = logging.WARNING
        else:
            loglevel = logging.WARNING
        if 'log' in config.options('LOGGING'):
            log = config.get('LOGGING', 'log')
        else:
            log = None
## Set up Logging
if __name__ == "__main__":
    args = parser.parse_args()
    if args.log is not None:
        log = args.log
    if args.loglevel != logging.Warning:
        loglevel = args.loglevel
    # Get plugins folder
    if args.plugins:
        PluginFolder = args.plugins

if log:
    logging.basicConfig(filename=log, level=loglevel)
else:
    logging.basicConfig(level=loglevel)


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
    storage = None
    PluginFolder = None

    def __init__(self, PluginFolder=PluginFolder):
        #global PluginFolder
        self.PluginFolder = PluginFolder

        # Load enrichments database
        self.enrichment_db = self.set_enrichment_db()

        # Load the plugins Directory
        if self.PluginFolder:
            self.load_plugins()
        else:
            logging.warning("Plugin folder not doesn't exist.  Plugins not configured.  Please run set_plugin_folder(<PluginFolder>) to set the plugin folder and then load_plugins() to load plugins.")


    def set_plugin_folder(self, PluginFolder):
        self.PluginFolder = PluginFolder

    def get_plugin_folder(self):
        return self.PluginFolder

    # Load the plugins from the plugin directory.
    def load_plugins(self):
        print "Configuring Plugin manager."
        self.plugins = PluginManager()
        self.plugins.setPluginPlaces([self.PluginFolder])
        #self.plugins.collectPlugins()
        self.plugins.locatePlugins()
        self.plugins.loadPlugins()
        print "Plugin manager configured."

        # Loop round the plugins and print their names.
        cur = self.enrichment_db.cursor()
        for plugin in self.plugins.getAllPlugins():
            print "Configuring plugin {0}.".format(plugin.name)
            plugin_config = plugin.plugin_object.configure()
            # Insert enrichment
            if plugin_config[0] == 'enrichment': # type
                cur.execute('''INSERT INTO enrichments VALUES (?, ?, ?, ?, ?)''', (plugin_config[2], # Name
                                                                               int(plugin_config[1]), # Enabled
                                                                               plugin_config[3], # Descripton
                                                                               plugin_config[5], # Cost
                                                                               plugin_config[6]) # Speed 

                )
                for inp in plugin_config[4]: # inputs
                    # Insert into inputs table
                    cur.execute('''INSERT INTO inputs VALUES (?,?)''', (plugin_config[2], inp))
                self.enrichment_db.commit()
            elif plugin_config[0] == 'interface': # type
                cur.execute('''INSERT INTO storage VALUES (?, ?)''', (plugin_config[2], int(plugin_config[1])))


    def set_interface(self, interface):
        """

        :param interface: The name of the plugin to use for storage.
        Sets the storage backend to use.  It must have been configured through a plugin prior to setting.
        """
        cur = self.enrichment_db.cursor()
        configured_storage = list()
        for row in cur.execute('''SELECT DISTINCT name FROM storage;'''):
            configured_storage.append(row[0])
        if interface in configured_storage:
            self.storage = interface
        else:
            configured_storage = None
            raise ValueError("Requested interface {0} not configured. Options are {1}.".format(interface, configured_storage))

    def set_enrichment_db(self):
        """

        Sets up the enrichment sqlite in memory database
        """
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        # Create enrichments table
        cur.execute('''CREATE TABLE enrichments (name text NOT NULL PRIMARY KEY,
                                               configured int,
                                               description text,
                                               cost int,
                                               speed int);''')
        # Create inputs table
        cur.execute('''CREATE TABLE inputs (name text NOT NULL,
                                          input text NOT NULL,
                                          PRIMARY KEY (name, input),
                                          FOREIGN KEY (name) REFERENCES enrichments(name));''')
        # Create storage table
        cur.execute('''CREATE TABLE storage (name text NOT NULL PRIMARY KEY,
                                             configured int
                                            );''')
        conn.commit()

        return conn


    def get_inputs(self):
        """ NoneType -> list of strings
        
        :return: A list of the potential enrichment inputs (ip, domain, etc)
        """
        inputs = list()
        cur = self.enrichment_db.cursor()
        for row in cur.execute('''SELECT DISTINCT input FROM inputs;'''):
            inputs.append(row[0])
        return inputs


    def get_enrichments(self, inputs, cost=10, speed=10, enabled=True):
        """

        :param inputs: list of input types.   (e.g. ["ip", "domain"])  All enrichments that match at least 1 input type will be returned.
        :param cost:  integer 1-10 of resource cost of running the enrichment.  (1 = cheapest)
        :param speed: integer 1-10 speed of enrichment. (1 = fastest)
        :param enabled: Plugin is correctly configured.  If false, plugin may not run correctly.
        :return: list of names of enrichments matching the criteria
        """
        cur = self.enrichment_db.cursor()

        plugins = list()
        names = list()
        for row in cur.execute('''SELECT DISTINCT name FROM inputs WHERE input IN (?)''', (",".join(inputs))):
            names.append(row[0])
        for row in cur.execute('''SELECT DISTINCT name
                                  FROM enrichments
                                  WHERE cost <= ?
                                    AND speed <= ?
                                    AND configured = ?
                                    AND names IN (?)''',
                                (cost,
                                 speed,
                                 enabled,
                                 ",".join(names)
                               )):
            plugins.append(row[0])

        return plugins


    def run_enrichments(self, topic, topic_type, names=None, cost=10, speed=10, start_time=""):
        """

        :param topic: topic to enrich (e.g. "1.1.1.1", "www.google.com")
        :param topic_type: type of topic (e.g. "ip", "domain")
        :param cost: integer 1-10 of resource cost of running the enrichment.  (1 = cheapest)
        :param speed: integer 1-10 speed of enrichment. (1 = fastest)
        :param names: a name (as string) or a list of names of enrichments to use
        :return: None if storage configured (networkx graph representing the enrichment of the topic
        """
        enrichments = self.get_enrichments([topic_type], cost, speed, enabled=True)
        g = nx.MultiDiGraph()

        # IF a name(s) are given, subset to them
        if names:
            enrichments = set(enrichments).intersection(set(names))

        for enrichment in enrichments:
            # TODO: Test the beloq code
            # get the plugin
            plugin = self.plugins.getPluginByName(enrichment)
            # run the plugin
            g2 = plugin.plugin_object.run(topic, start_time)
            # merge the graphs
            for node, props in g2.nodes(data=True):
                g.add_node(node, props)
            for edge, props in g2.edges(data=True):
                g.add_edge()

        return g


    def store_graph(self, g, storage=None):
        """

        :param g: a networkx graph to merge with the set storage
        """
        if not storage:
            storage = self.storage
        if not storage:
            raise ValueError("No storage set.  run set_storage() to set or provide directly.  Storage must be a configured plugin.")
        else:
            # get the plugin
            plugin = self.plugins.getPluginByName(self.storage)
            # merge the graph
            plugin.plugin_object.enrich(g)