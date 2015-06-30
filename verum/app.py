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
try:
    from yapsy.PluginManager import PluginManager
    plugin_import = True
except:
    plugin_import = False
import ConfigParser
import sqlite3
import networkx as nx
import os
import urlparse  # For validate_url helper
import inspect

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
        if 'minions' in config.options('Core'):
            MinionFolder = config.get('Core', 'minions')
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

# setup
loc = inspect.getfile(inspect.currentframe())
ind = loc.rfind("/")
loc = loc[:ind+1]


## EXECUTION
class app():
    db = None  # the sqlite database of plugins
    plugins = None  # Configured plugins
    storage = None  # The plugin to use for storage
    PluginFolder = None  # Folder where the plugins are
    MinionFolder = None  # Folder where the minions are
    score = None  # the plugin to use for scoring
    classify = None  # the clasification plugin
    helper = None

    def __init__(self, PluginFolder=PluginFolder, MinionFolder=MinionFolder):
        #global PluginFolder
        self.PluginFolder = PluginFolder

        #global MinionsFolder
        self.MinionFolder = MinionFolder

        # Load enrichments database
        self.db = self.set_db()

        # LOAD HELPER FROM SAME DIRECTORY
        fp, pathname, description = imp.find_module("helper", [loc])
        self.helper = imp.load_module("helper", fp, pathname, description)

        # Load the plugins Directory
        if self.PluginFolder:
            self.load_plugins()
        else:
            logging.warning("Plugin folder doesn't exist.  Plugins not configured.  Please run set_plugin_folder(<PluginFolder>) to set the plugin folder and then load_plugins() to load plugins.")


    ## PLUGIN FUNCTIONS

    def set_plugin_folder(self, PluginFolder):
        self.PluginFolder = PluginFolder

    def get_plugin_folder(self):
        return self.PluginFolder

    # Load the plugins from the plugin directory.
    def load_plugins(self):
        print "Configuring Plugin manager."
        self.plugins = PluginManager()
        if self.MinionFolder is None:
            self.plugins.setPluginPlaces([self.PluginFolder])
        else:
            self.plugins.setPluginPlaces([self.PluginFolder, self.MinionFolder])
        #self.plugins.collectPlugins()
        self.plugins.locatePlugins()
        self.plugins.loadPlugins()
        print "Plugin manager configured."

        # Loop round the plugins and print their names.
        cur = self.db.cursor()

        # Clear tables
        cur.execute("""DELETE FROM enrichments""")
        cur.execute("""DELETE FROM inputs""")
        cur.execute("""DELETE FROM storage""")
        cur.execute("""DELETE FROM score""")
        cur.execute("""DELETE FROM minion""")

        for plugin in self.plugins.getAllPlugins():
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
                self.db.commit()
            elif plugin_config[0] == 'interface': # type
                cur.execute('''INSERT INTO storage VALUES (?, ?)''', (plugin_config[2], int(plugin_config[1])))
            elif plugin_config[0] == 'score':
                cur.execute('''INSERT INTO score VALUES (?, ?, ?, ?, ?)''', (plugin_config[2], # Name
                                                                               int(plugin_config[1]), # Enabled
                                                                               plugin_config[3], # Descripton
                                                                               plugin_config[4], # Cost
                                                                               plugin_config[5]) # Speed 
                )
            if plugin_config[0] == 'minion':
                plugin_config = plugin.plugin_object.configure(verum=loc[:-6], plugins=self.PluginFolder)  # -6 strips off the "verum/" from the location
                cur.execute('''INSERT INTO minion VALUES (?, ?, ?, ?)''', (plugin_config[2], # Name
                                                                           int(plugin_config[1]), # Enabled
                                                                           plugin_config[3], # Descripton
                                                                           plugin_config[4]) # Speed 
                )
                
            if plugin.name == "classify":  # Classify is a unique name.  TODO: figure out if handling multiple 'classify' plugins is necessary
                self.classify = plugin.plugin_object

            print "Configured {2} plugin {0}.  Success: {1}".format(plugin.name, plugin_config[1], plugin_config[0])


    def set_db(self):
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
        # Create interface table
        cur.execute('''CREATE TABLE storage (name text NOT NULL PRIMARY KEY,
                                             configured int
                                            );''')

        # Create score table
        cur.execute('''CREATE TABLE score (name text NOT NULL PRIMARY KEY,
                                             configured int,
                                             description text,
                                             cost int,
                                             speed int);''')

        # Create minion table
        cur.execute('''CREATE TABLE minion (name text NOT NULL PRIMARY KEY,
                                             configured int,
                                             description text,
                                             cost int);''')
        conn.commit()
        return conn


    ## ENRICHMENT FUNCTIONS

    def get_inputs(self):
        """ NoneType -> list of strings
        
        :return: A list of the potential enrichment inputs (ip, domain, etc)
        """
        inputs = list()
        cur = self.db.cursor()
        for row in cur.execute('''SELECT DISTINCT input FROM inputs;'''):
            inputs.append(row[0])
        return inputs


    def get_enrichments(self, inputs, cost=10000, speed=10000, configured=True):
        """

        :param inputs: list of input types.   (e.g. ["ip", "domain"])  All enrichments that match at least 1 input type will be returned.
        :param cost:  integer 1-10 of resource cost of running the enrichment.  (1 = cheapest)
        :param speed: integer 1-10 speed of enrichment. (1 = fastest)
        :param enabled: Plugin is correctly configured.  If false, plugin may not run correctly.
        :return: list of tuples of (names, type) of enrichments matching the criteria
        """
        cur = self.db.cursor()

        if type(inputs) == str:
            inputs = [inputs]

        plugins = list()
        names = list()
        for row in cur.execute("""  SELECT DISTINCT e.name, i.input
                                    FROM enrichments e, inputs i
                                    WHERE e.name = i.name
                                      AND e.cost <= ?
                                      AND e.speed <= ?
                                      AND configured = ?
                                      AND i.input IN ({0})""".format(("?," * len(inputs))[:-1]),
                                [cost,
                                 speed,
                                 int(configured)] +
                                 inputs
                               ):
            plugins.append(tuple(row))

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
        enrichments = self.get_enrichments([topic_type], cost, speed, configured=True)
        enrichments = [e[0] for e in enrichments]
        #print enrichments  # DEBUG
        g = nx.MultiDiGraph()

        # IF a name(s) are given, subset to them
        if names:
            enrichments = set(enrichments).intersection(set(names))

        for enrichment in enrichments:
            # get the plugin
            plugin = self.plugins.getPluginByName(enrichment)
            # run the plugin
            g2 = plugin.plugin_object.run(topic, start_time)
            # merge the graphs
            for node, props in g2.nodes(data=True):
                g.add_node(node, props)
            for edge in g2.edges(data=True):
                g.add_edge(edge[0], edge[1], attr_dict=edge[2])

        return g


    ## INTERFACE FUNCTIONS

    def get_interfaces(self, configured=None):
        """

        :return: list of strings of names of interface plugins
        """
        cur = self.db.cursor()
        interfaces = list()

        if configured is None:
            for row in cur.execute('''SELECT DISTINCT name FROM storage;'''):
                interfaces.append(row[0])
        else:
             for row in cur.execute('''SELECT DISTINCT name from storage WHERE configured=?;''', (int(configured),)):
                interfaces.append(row[0])           
        return interfaces

    def get_default_interface(self):
        return self.storage

    def set_interface(self, interface):
        """

        :param interface: The name of the plugin to use for storage.
        Sets the storage backend to use.  It must have been configured through a plugin prior to setting.
        """
        cur = self.db.cursor()
        configured_storage = list()
        for row in cur.execute('''SELECT DISTINCT name FROM storage WHERE configured=1;'''):
            configured_storage.append(row[0])
        if interface in configured_storage:
            self.storage = interface
        else:
            raise ValueError("Requested interface {0} not configured. Options are {1}.".format(interface, configured_storage))

    '''
    # I don't think I need
    def load_minions(self):
        # Loop round the plugins and print their names.
        cur = self.db.cursor()

        # Clear tables
        cur.execute("""DELETE FROM minion""")

        for plugin in self.plugins.getAllPlugins():
            if plugin_config[0] == 'minion':
                plugin_config = plugin.plugin_object.configure(self)
                cur.execute("""INSERT INTO minion VALUES (?, ?, ?, ?)""", (plugin_config[2], # Name
                                                                           int(plugin_config[1]), # Enabled
                                                                           plugin_config[3], # Descripton
                                                                           plugin_config[4]) # Speed 
                )
    '''

    def get_minions(self, cost=10000, configured=None):
        """

        :param cost: a maximum cost of running the minion
        :param configured: True, False, or None (for both).  
        :return: list of strings of tuples of (name, description) of minion plugins
        """
        cur = self.db.cursor()
        minions = list()

        if configured is None:
            for row in cur.execute('''SELECT DISTINCT name, description FROM minion WHERE cost <= ?;''', [int(cost)]):
                minions.append(tuple(row[0:2]))
        else:
             for row in cur.execute('''SELECT DISTINCT name, description FROM minion WHERE cost <= ? AND configured=?;''', [int(cost), int(configured)]):
                minions.append(tuple(row[0:2]))    
        return minions

    def start_minions(self, names=None, cost=10000):
        """

        :param names: a list of names of minions to run
        :param cost: a maximum cost for minions 
        """
        minions = self.get_minions(cost=cost, configured=True)
        minions = [m[0] for m in minions]

        # IF a name(s) are given, subset to them
        if names:
            minions = set(minions).intersection(set(names))

        for minion in minions:
            # get the plugin
            plugin = self.plugins.getPluginByName(minion)
            # start the plugin
            plugin.plugin_object.start()

    def get_running_minions(self):
        """
        
        :return: A set of names of minions which are running
        """

        minions = self.get_minions(cost=10000, configured=True)
        minions = [m[0] for m in minions]

        running_minions = set()
        # Iterate Through the minions
        for minion in minions:
            plugin = self.plugins.getPluginByName(minion)
            if plugin.plugin_object.isAlive():
                running_minions.add(minion)

        return running_minions

    def stop_minions(self, names=None):
        minions = self.get_running_minions()
        if names is not None:
            minions = set(minions).intersection(set(names))

        for minion in minions:
            # get the plugin
            plugin = self.plugins.getPluginByName(minion)
            # start the plugin
            plugin.plugin_object.stop()        

    def run_query(self, topic, max_depth=4, dont_follow=['enrichment', 'classification'], storage=None):
        """

        :param storage: the storage plugin to use
        :return: a networkx subgraph surrounded around the topic 
        """
        if not storage:
            storage = self.storage
        if not storage:
            raise ValueError("No storage set.  run set_storage() to set or provide directly.  Storage must be a configured plugin.")
        else:
            # get the plugin
            plugin = self.plugins.getPluginByName(self.storage)

        return plugin.plugin_object.query(topic, max_depth=max_depth, dont_follow=dont_follow)


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


    ## SCORE FUNCTIONS

    def get_scoring_plugins(self, cost=10000, speed=10000, names=None, configured=True):
        """

        :param cost:  integer 1-10 of resource cost of running the enrichment.  (1 = cheapest)
        :param speed: integer 1-10 speed of enrichment. (1 = fastest)
        :param enabled: Plugin is correctly configured.  If false, plugin may not run correctly.
        :return: list of names of scoring plugins matching the criteria
        """
        cur = self.db.cursor()

        plugins = list()

        if names is None:
            for row in cur.execute('''SELECT DISTINCT name
                                      FROM score
                                      WHERE cost <= ?
                                        AND speed <= ?
                                        AND configured = ?''',
                                    [cost,
                                     speed,
                                     int(configured)]
                                   ):
                plugins.append(row[0])
        else:
            for row in cur.execute('''SELECT DISTINCT name
                                      FROM score
                                      WHERE cost <= ?
                                        AND speed <= ?
                                        AND configured = ?
                                        AND name IN ({0});'''.format(("?," * len(names))[:-1]),
                                    [cost,
                                     speed,
                                     int(configured)] + 
                                     names
                                   ):
                plugins.append(row[0])

        return plugins


    def score_subgraph(self, topic, sg, plugin_name=None):
        if plugin_name is None:
            plugin_name=self.score

        score_plugin = self.plugins.getPluginByName(plugin_name)
        return score_plugin.plugin_object.score(sg, topic)


    def set_scoring_plugin(self, plugin):
        """

        :param interface: The name of the plugin to use for storage.
        Sets the storage backend to use.  It must have been configured through a plugin prior to setting.
        """
        cur = self.db.cursor()
        configured_scoring_plugins = list()
        for row in cur.execute('''SELECT DISTINCT name FROM score WHERE configured=1;'''):
            configured_scoring_plugins.append(row[0])
        if plugin in configured_scoring_plugins:
            self.score = plugin
        else:
            raise ValueError("Requested scoring plugin {0} is not configured. Options are {1}.".format(plugin, configured_scoring_plugins))


    def get_default_scoring_plugin(self):
        return self.score
