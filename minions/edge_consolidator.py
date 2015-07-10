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
PLUGIN_CONFIG_FILE = "edge_consolidator.yapsy-plugin"  # CHANGEME
NAME = "Neo4j Edge Consolidator"  # CHANGEME
JUMP = 0.9
NEO4J_HOST = 'localhost'
NEO4J_PORT = '7474'
LOGFILE = None
USERNAME = None
PASSWORD = None
SLEEP_TIME = 5

########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
from collections import defaultdict  # used for storing duplicate edges
import networkx as nx
from datetime import datetime # timedelta imported above
import uuid
import ConfigParser
import inspect
import threading
try:
    from py2neo import Graph as py2neoGraph
    from py2neo import Node as py2neoNode
    from py2neo import Relationship as py2neoRelationship
    from py2neo import authenticate as py2neoAuthenticate
    neo_import = True
except:
    logging.error("Neo4j plugin did not load.")
    neo_import = False
import imp  # for verum import
import random  # for jumps
from time import sleep # for sleeping between iterations

## SETUP
random.seed()

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
if config.has_section('neo4j'):
    if 'host' in config.options('neo4j'):
        NEO4J_HOST = config.get('neo4j', 'host')
    if 'port' in config.options('neo4j'):
        NEO4J_PORT = config.get('neo4j', 'port')
    if 'username' in config.options('neo4j'):
        USERNAME = config.get('neo4j', 'username')
    if 'password' in config.options('neo4j'):
        PASSWORD = config.get('neo4j', 'password')

## EXECUTION
class PluginOne(IPlugin):
    storage = None
    thread = None
    app = None  # The object instance
    Verum = None  # the module
    shutdown = False  # Used to trigger shutdown of the minion
    parent = None  # The parent instance of the verum app object
    neo4j_config = None
    sleep_time = SLEEP_TIME
    jump = JUMP

    #  CHANGEME: The init should contain anything to load modules or data files that should be variables of the  plugin object
    def __init__(self):
        pass

    #  CHANGEME: Configuration needs to set the values needed to identify the plugin in the plugin database as well as ensure everyhing loaded correctly
    #  CHANGEME: Current  layout is for an enrichment plugin
    #  CHANGEME: enrichment [type, successful_load, name, description, inputs to enrichment such as 'ip', cost, speed]
    #  CHANGEME: interface [type, successful_load, name]
    #  CHANGEME: score [type, successful_load, name, description, cost, speed]
    #  CHANGEME: minion [type, successful_load, name, description, cost]
    def configure(self, parent=None):
        """

        :return: return list of configuration variables starting with [plugin_type, successful_load, name, description, <PLUGIN TYPE SPECIFIC VALUES>]
        """
        config_options = config.options("Configuration")

        # Cost and speed are not applicable to all plugin types
        if 'cost' in config_options:
            cost = config.get('Configuration', 'cost')
        else:
            cost = 9999
        if 'jump' in config_options:
            self.jump = config.get('Configuration', 'jump')
        if 'sleep_time' in config_options:
            self.sleep_time = float(config.get('Configuration', 'sleep_time'))


        if config.has_section('Documentation') and 'description' in config.options('Documentation'):
            description = config.get('Configuration', 'type')
        else:
            logging.error("'Description not in config file.")
            return [None, False, NAME, cost]

        if 'type' in config_options:
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, False, NAME, description, cost]

        # Module success is only applicable to plugins which import unique code
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

        # Ensure a neo4j storage plugin
        if not neo_import:
            logging.error("Py2neo import failed.  Ensure py2neo v2.* is installed.")
            return [plugin_type, False, NAME, description, cost]

        try:
            self.set_neo4j_config(NEO4J_HOST, NEO4J_PORT, USERNAME, PASSWORD)
        except Exception as e:
            logging.error("Neo4j configuration failed with error {0}.  Check host, port, username, and password.".format(e))
            return [plugin_type, False, NAME, description, cost]


        return [plugin_type, True, NAME, description, cost]


    ############  GENERAL NOTES ############
    #  CHANGEME: All functions must implement a "configuration()" function
    #  CHANGEME: The correct type of execution function must be defined for the type of plugin
    ############  GENERAL NOTES ############


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

        # Get graph
        neo_graph = py2neoGraph(self.neo4j_config)

        random_cypher = ''' MATCH (a)-[:describedBy]->() 
                            RETURN a, rand() as r
                            ORDER BY r
                            LIMIT 1
                        '''

        # pick a random node
        records = neo_graph.cypher.execute(random_cypher)
        node = records[0][0]

        logging.info("first node to consolidate edges for is class: {0}, key: {1}, value: {2}".format(node.properties['class'], node.properties['key'], node.properties['value']))
        print "first node to consolidate edges for is class: {0}, key: {1}, value: {2}".format(node.properties['class'], node.properties['key'], node.properties['value'])  # DEBUG

        while not self.shutdown:
            edges = defaultdict(set)
            destinations = set()

            # get edges starting with the node
            for rel in node.match_outgoing():
                if 'uri' in rel.properties:
                    edge_uri = rel.properties['uri']
                else:
                    # SRC URI
                    if 'uri' in rel.start_node.properties:
                        source_uri = rel.start_node.properties['uri']
                    else:
                        source_uri = "class={0}&key={1}&value={2}".format(rel.start_node.properties['attribute'], rel.start_node.properties['key'], rel.start_node.properties['value'])

                    # DST URI
                    if 'uri' in rel.end_node.properties:
                        dest_uri = rel.end_node.properties['uri']
                    else:
                        dest_uri = "class={0}&key={1}&value={2}".format(rel.end_node.properties['attribute'], rel.end_node.properties['key'], rel.end_node.properties['value'])

                    # Edge URI
                    source_hash = uuid.uuid3(uuid.NAMESPACE_URL, source_uri)
                    dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, dest_uri)
                    edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
                    rel_chain = "relationship"
                    while rel_chain in rel.properties:
                        edge_uri = edge_uri + "&{0}={1}".format(rel_chain,rel.properties[rel_chain])
                        rel_chain = rel.properties[rel_chain]
                    if "origin" in rel.properties:
                        edge_uri += "&{0}={1}".format("origin", rel.properties["origin"])

                # aggregate edges by dst, and uri
                edges[edge_uri].add(rel)  # WARNING: The use of URI here is vulnerable to values being out of order in the URI and edges not being removed.

                # collect destinations to pick next node
                destinations.add(rel.end_node)

            time = datetime.utcnow()

            # SRC URI
            if 'uri' in node.properties:
                source_uri = node.properties['uri']
            else:
                source_uri = "class={0}&key={1}&value={2}".format(node.properties['attribute'], node.properties['key'], node.properties['value'])

            for edge_uri in edges:
                edge_list = list(edges[edge_uri])

                # DST URI
                if 'uri' in edge_list[0].end_node.properties:
                    dest_uri = edge_list[0].end_node.properties['uri']
                else:
                    dest_uri = "class={0}&key={1}&value={2}".format(edge_list[0].end_node.properties['attribute'], edge_list[0].end_node.properties['key'], edge_list[0].end_node.properties['value'])

                logging.debug("Removing {0} edges from node {1} to {2}.".format(len(edge_list[1:]), source_uri, dest_uri))
                print "Removing {0} edges from node {1} to {2}.".format(len(edge_list[1:]), source_uri, dest_uri)  # DEBUG

                for edge in edge_list[1:]:
                    # keep earliest time as start
                    edge_time = datetime.strptime(edge.properties['start_time'], "%Y-%m-%dT%H:%M:%SZ")
                    if 'start_time' in edge.properties and time > edge_time:
                        time = edge_time

                    #  remove all but one node of each group
                    edge.delete()

                # Update time on remaining node
                if 'start_time' not in edge_list[0].properties or time < datetime.strptime(edge_list[0].properties['start_time'], "%Y-%m-%dT%H:%M:%SZ"):
                    edge_list[0].properties['start_time'] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
                    edge_list[0].push()

                logging.debug("Keeping edge {0} from node {1} to node {2}.".format(edge_list[0].uri, source_uri, dest_uri))
                print "Keeping edge {0} from node {1} to node {2}.".format(edge_list[0].uri, source_uri, dest_uri)  # DEBUG

            #  Sleep to slow it down
            sleep(self.sleep_time)

            jump = random.random()

            # do the random walk
            if len(destinations) == 0 or jump <= self.jump:
                # pick a random node
                records = neo_graph.cypher.execute(random_cypher)
                node = records[0][0]
                logging.debug("Edge consolidation random walk jumped.")
            else:
                node = random.choice(destinations)
                logging.debug("Edge consolidation random walk didn't jumped.")

            logging.info("Next node to consolidate edges for is class: {0}, key: {1}, value: {2}".format(node.properties['class'], node.properties['key'], node.properties['value']))
            print "Next to consolidate edges for node is class: {0}, key: {1}, value: {2}".format(node.properties['class'], node.properties['key'], node.properties['value'])  # DEBUG

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

    def set_neo4j_config(self, host, port, username=None, password=None):
        if username and password:
            py2neoAuthenticate("{0}:{1}".format(host, port), username, password)
            self.neo4j_config = "http://{2}:{3}@{0}:{1}/db/data/".format(host, port, username, password)
        else:
            self.neo4j_config = "http://{0}:{1}/db/data/".format(host, port)
