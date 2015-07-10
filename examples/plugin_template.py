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
PLUGIN_CONFIG_FILE = "plugin_template.yapsy-plugin"  # CHANGEME
NAME = "<NAME FROM CONFIG FILE AS BACKUP IF CONFIG FILE DOESN'T LOAD>"  # CHANGEME


########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
import networkx as nx
from datetime import datetime # timedelta imported above
import uuid
import ConfigParser
import inspect
import threading
"""
try:
    import <SOME UNIQUE MODULE>
    module_import_success = True
except:
    module_import_success = False
    logging.error("Module import failed.  Please install the following module: <SOME UNIQUE MODULE>.")
"""

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


## EXECUTION
class PluginOne(IPlugin):
    inputs = None
    shutdown = False  # Used to trigger shutdown of a minion

    #  CHANGEME: The init should contain anything to load modules or data files that should be variables of the  plugin object
    def __init__(self):
        pass

    #  CHANGEME: Configuration needs to set the values needed to identify the plugin in the plugin database as well as ensure everyhing loaded correctly
    #  CHANGEME: Current  layout is for an enrichment plugin
    #  CHANGEME: enrichment [type, successful_load, name, description, inputs to enrichment such as 'ip', cost, speed]
    #  CHANGEME: interface [type, successful_load, name]
    #  CHANGEME: score [type, successful_load, name, description, cost, speed]
    #  CHANGEME: minion [type, successful_load, name, description, cost]
    def configure(self):
        """

        :return: return list of configuration variables starting with [plugin_type, successful_load, name, description, <PLUGIN TYPE SPECIFIC VALUES>]
        """
        config_options = config.options("Configuration")

        # Cost and speed are not applicable to all plugin types
        """
        if 'cost' in config_options:
            cost = config.get('Configuration', 'cost')
        else:
            cost = 9999
        if 'speed' in config_options:
            speed = config.get('Configuration', 'speed')
        else:
            speed = 9999
        """

        if config.has_section('Documentation') and 'description' in config.options('Documentation'):
            description = config.get('Configuration', 'type')
        else:
            logging.error("'Description not in config file.")
            return [None, False, NAME, None, cost, speed]

        if 'type' in config_options:
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, False, NAME, description, None, cost, speed]

        # Inputs is only applicable to enrichment plugins
        """
        if 'inputs' in config_options:
            self.inputs = config.get('Configuration', 'Inputs')
            self.inputs = [l.strip().lower() for l in self.inputs.split(",")]
        else:
            logging.error("No input types specified in config file.")
            return [plugin_type, False, NAME, description, None, cost, speed]
        """

        # Module success is only applicable to plugins which import unique code
        """
        if not module_import_success:
            logging.error("Module import failure caused configuration failure.")
            return [plugin_type, False, NAME, description, self.inputs, cost, speed]
        """

        return [plugin_type, True, NAME, description, self.inputs, cost, speed]


    ############  GENERAL NOTES ############
    #  CHANGEME: All functions must implement a "configuration()" function
    #  CHANGEME: The correct type of execution function must be defined for the type of plugin
    ############  GENERAL NOTES ############


    #  CHANGEME: enrichment: "run(<thing to enrich>, inputs, start_time, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  CHANGEME: Enrichment plugin specifics:
    #  -     Created nodes/edges must follow http://blog.infosecanalytics.com/2014/11/cyber-attack-graph-schema-cags-20.html
    #  -     The enrichment should include a node for the <thing to enrich>
    #  -     The enrichment should include a node for the enrichment which is is statically defined & key of "enrichment"
    #  -     An edge should exist from <thing to enrich> to the enrichment node, created at the end after enrichment
    #  -     Each enrichment datum should have a node
    #  -     An edge should exist from <thing to enrich> to each enrichment datum
    #  -     The run function should then return a networkx directed multi-graph including the nodes and edges
    def run(self, enrichment_target, inputs=None, start_time=""):
        """

        :param enrichment_target: a string containing a target to enrich
        :return: a networkx graph representing the sections of the domain
        """


        pass  # TODO: Place enrichment in here

        return g


    #  CHANGEME: interface: enrich(graph, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  CHANGEME:            query(topic, max_depth, config, dont_follow, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  CHANGEME: Interface plugin specifics:
    #  -     In the most efficient way possible, merge nodes and edges into the storage medium
    #  -     Merger of nodes should be done based on matching key & value.
    #  -     URI should remain static for a given node.
    #  -     Start time should be updated to the sending graph
    #  -     Edges should be added w/o attempts to merge with edges in the storage back end
    #  -     When adding nodes it is highly recommended to keep a node-to-storage-id mapping with a key of the node
    #  -       URI.  This will assist in bulk-adding the edges.
    #  -     Query specifics of interface plugins:
    #  -     In the most efficient way possible retrieve and return the merged subgraph (as a networkx graph) including all nodes and 
    #  -     edges within the max_distance from any node in the topic graph from the storage backend graph.
    #  -     As a default, ['enrichment', 'classification'] should not be followed.
    #  -     The query function must add a 'topic_distance' property to all nodes.
   def enrich(self, g):
        """

        :param g: networkx graph to be merged
        :return: Nonetype
        """
        pass  # TODO: Replace this with storage into a backend storage system


    #  CHANGEME: score: score(subgraph, topic, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  CHANGEME: Score plugin specifics:
    #  -     Scoring plugins should take a topic and networkx (sub)graph and return a dictionary keyed with the node (name) and with
    #  -     values of the score assigned to the node for the given topic.
    def score(self, sg, topic):  # get_bayesian_network_probability
        """

        :param sg: egocentric subgraph around topic in networkx format
        :param topic: graph of topics
        :return: Dictionary of probabilities keyed by node
        """
        scores = dict() 

        pass  # TODO: Replace with code to score the subgraph with respect to the topic

        return scores



    #  CHANGEME: minion: minion() 
    #  CHANGEME:        start() 
    #  CHANGEME:        stop()
    #  CHANGEME:        isAlive()
    #  CHANGEME: Minion plugin specifics:
    #  -     Minions fit exist in a separate directory to prevent them importing themselves when they import their own VERUM instance
    #  -     The minion configuration function must take an argument of the parent verum object.  When not present, it shouldn't error but
    #  -      instead return with successful_load set to false and a logging.info message that the parent was not passed in.
    #  -     Must have 4 functions: minion(), start(), and stop() and isAlive()
    #  -     minion() is the function which will be threaded.  **Make sure to call create the new verum instance WITHIN this function
    #  -      to avoid SQLite errors!**
    #  -     start() creates the thread object as an attribute of the plugin class and starts it
    #  -     stop() stops the thread.  Preferably with both a normal exit by setting a shutdown variable of the plugin class as well as a 
    #  -      force stop option which removes the thread object
    #  -     isAlive() calls the thread isAlive() function and returns the status
    def minion(self,  *args, **xargs):
        self.shutdown = False
        
        pass  # TODO: Write the function which will be threaded to form the minion

    def start(self, *args, **xargs):
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

