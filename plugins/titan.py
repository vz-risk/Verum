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
TITAN_CONFIG_FILE = "titan.yapsy-plugin"
# Below values will be overwritten if in the config file or specified at the command line
TITAN_HOST = "localhost"
TITAN_PORT = "8182"
TITAN_GRAPH = "vzgraph"
LOGFILE = None
LOGLEVEL = logging.INFO
NAME = 'TitanDB'


########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
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
    from yapsy.PluginManager import PluginManager
    plugin_import = True
except:
    plugin_import = False
import ConfigParser
import sqlite3
import networkx as nx
import os
import inspect

## SETUP
__author__ = "Gabriel Bassett"
# Read Config File - Will overwrite file User Variables Section
loc = inspect.getfile(inspect.currentframe())
i = loc.rfind("/")
loc = loc[:i+1]
config = ConfigParser.SafeConfigParser()
config.readfp(open(loc + TITAN_CONFIG_FILE))
if config.has_section('titanDB'):
    if 'host' in config.options('titanDB'):
        TITAN_HOST = config.get('titanDB', 'host')
    if 'port' in config.options('titanDB'):
        TITAN_PORT = config.get('titanDB', 'port')
    if 'graph' in config.options('titanDB'):
        TITAN_GRAPH = config.get('titanDB', 'graph')
if config.has_section('Core'):
    if 'plugins' in config.options('Core'):
        PluginFolder = config.get('Core', 'plugins')
    if 'name' in config.options('Core'):
        NAME = config.get('Core', 'name')
if config.has_section('Log'):
    if 'level' in config.options('Log'):
        LOGLEVEL = config.get('Log', 'level')
    if 'file' in config.options('Log'):
        LOGFILE = config.get('Log', 'file')


## Set up Logging
if LOGFILE is not None:
    logging.basicConfig(filename=LOGFILE, level=LOGLEVEL)
else:
    logging.basicConfig(level=LOGLEVEL)
# <add other setup here>


## EXECUTION

class TalksTo(TITAN_Relationship):
    label = "talksto"


class DescribedBy(TITAN_Relationship):
    label = "describedBy"


class Influences(TITAN_Relationship):
    label = "influences"


class PluginOne(IPlugin):
    titandb_config = None

    def __init__(self):
        pass


    def configure(self):
        """

        :return: return list of [configure success (bool), name (str)]
        """
        config_options = config.options("Configuration")

        # Create titan config
        # Import host, port, graph from config file
        try:
            self.set_titan_config(TITAN_HOST, TITAN_PORT, TITAN_GRAPH)
            config_success = True
        except:
            config_success = False
        # If config is successful, return
        if config_success and titan_import and plugin_import:
            success = True
        else:
            success = False

        # Return
        if 'type' in config_options:
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, success, NAME]
        return [plugin_type, success, NAME]
  

    def set_titan_config(self, host, port, graph):
        self.titan_config = TITAN_Config('http://{0}:{1}/graphs/{2}'.format(host, port, graph))


    def removeNonAscii(self, s): return "".join(i for i in s if ord(i)<128)


    def enrich(self, g):
        """

        :param g: graph to be merged
        :param titan: reference to titan database
        :return: Nonetype

        NOTE: Merge occurs on node name rather than attributes
        NOTE: Merge iterates through edges, finds the edge's nodes, looks for the edge & creates if it doesn't exist.
               Any nodes without edges are iterated through and created if they do not already exist.
        """
        # Get config
        titan = self.titandb_config

        # Connect to TitanDB Database
        titan_graph = TITAN_Graph(titan)

        # Add schema relationships
        titan_graph.add_proxy("talks_to", TalksTo)
        titan_graph.add_proxy("described_by", DescribedBy)
        titan_graph.add_proxy("influences", Influences)

        for edge in g.edges(data=True):
    #        print edge  # DEBUG
            # Get the src node
            src_uri = edge[0]
            attr = g.node[src_uri]
    #        print "Node {0} with attributes:\n{1}".format(src_uri, attr)  # DEBUG
            # Get/Create node in titan
            src = titan_graph.vertices.get_or_create("uri", src_uri, attr) # WARNING: This only works if g was created correctly
            # Update the times
            if "start_time" in attr and attr["start_time"] is not "":
                if "start_time" in src and (src.start_time == "" or
                                            datetime.strptime(src.start_time, "%Y-%m-%dT%H:%M:%SZ") >
                                            datetime.strptime(attr["start_time"], "%Y-%m-%dT%H:%M:%SZ")):
                    src.start_time = attr["start_time"]
            if "finish_time" in attr:
                if "finish_time" in src and (src.finish_time == "" or
                                             datetime.strptime(src.finish_time, "%Y-%m-%dT%H:%M:%SZ") <
                                             datetime.strptime(attr["finish_time"], "%Y-%m-%dT%H:%M:%SZ")):
                    src.finish_time = attr["finish_time"]
            src.save()

            # Get the dst node
            dst_uri = edge[1]
            attr = g.node[dst_uri]
            # Get/Create node in titan
            dst = titan_graph.vertices.get_or_create("uri", dst_uri, attr) # WARNING: This only works if g was created correctly
            # Update the times
            if "start_time" in attr and attr["start_time"] is not "":
                if "start_time" in dst and (dst.start_time == "" or
                                            datetime.strptime(dst.start_time, "%Y-%m-%dT%H:%M:%SZ") >
                                            datetime.strptime(attr["start_time"], "%Y-%m-%dT%H:%M:%SZ")):
                    dst.start_time = attr["start_time"]
            if "finish_time" in attr:
                if "finish_time" in dst and (dst.finish_time == "" or
                                             datetime.strptime(dst.finish_time, "%Y-%m-%dT%H:%M:%SZ") <
                                             datetime.strptime(attr["finish_time"], "%Y-%m-%dT%H:%M:%SZ")):
                    dst.finish_time = attr["finish_time"]
            dst.save()

    #        print "edge 2 before relationship is\n{0}".format(edge[2])  # DEBUG

            # Create the edge if it doesn't exist
            ## This matches on src, dst, the relationship & it's chain (relationship->described_by->___) and origin
            # fixed "described_by" relationship for how it's stored in TitanDB
            try:
                relationship = edge[2].pop('relationship')
            except:
                # default to 'described_by'
                relationship = 'describedBy'
            if relationship == 'described_by':
                relationship = 'describedBy'
            if relationship == 'talks_to':
                relationship = 'talksTo'
            # Match on the relationship chain
            chain = relationship
            edge_attr = ""
    #        print "edge 2 before while is\n{0}".format(edge[2])  # DEBUG
            while chain in edge[2]:
                edge_attr += "it.{0} == '{1}' & ".format(chain, edge[2][chain])
                chain = edge[2][chain]
            # Remove the irrelevant edge properties
    #        print "edge 2 before origin is\n{0}".format(edge[2])  # DEBUG
            if 'origin' in edge[2]:
                edge_attr += "it.origin == '{0}' & ".format(edge[2]['origin'])
            else:
                edge_attr = ""
            if edge_attr:
                edge_attr = ".filter{0}".format("{" + edge_attr.rstrip(" & ") + "}")
            # Execute a gremlin query from src to dst to get the edges between them that match the attributes of the edge
            query = "g.v({0}).outE('{3}'){2}.as('r').inV.retain([g.v({1})]).back('r')".format(
                    src.eid,
                    dst.eid,
                    edge_attr,
                    relationship
                )
    #        print query  # DEBUG
            edges = titan_graph.gremlin.query(query)
            # If an edge exists, update it's times, otherwise create the edge
            if edges:
                e = edges.next()
    #            print "e is\n".format(e)  # DEBUG
    #            print "edge 2 is\n{0}".format(edge[2])
                if "start_time" in e and (e.start_time == "" or
                                          datetime.strptime(e.start_time, "%Y-%m-%dT%H:%M:%SZ") >
                                          datetime.strptime(edge[2]["start_time"], "%Y-%m-%dT%H:%M:%SZ")):
                    e.start_time = edge[2]["start_time"]
                if "finish_time" in e and (e.finish_time == "" or
                                           datetime.strptime(e.finish_time, "%Y-%m-%dT%H:%M:%SZ") <
                                           datetime.strptime(edge[2]["finish_time"], "%Y-%m-%dT%H:%M:%SZ")):
                    e.finish_time = edge[2]["finish_time"]
                e.save()
            else:
                if relationship in edge[2]:
                    edge[2]["rel_{0}".format(relationship)] = edge[2].pop(relationship) # Titan can't handle a property key being the same as the relationship value
                try:
    #                print "src:{0}\ndst:{1}\nAttr:\n{2}\n".format(src, dst, edge[2])
                    if relationship == 'describedBy':
                        titan_graph.described_by.create(src, dst, edge[2])
                    elif relationship == 'talksTo':
                        titan_graph.talks_to.create(src, dst, edge[2])
                    elif relationship == 'influences':
                        titan_graph.influences.create(src, dst, edge[2])
                    else:
                        titan_graph.edges.create(src, ''.join(e for e in str(relationship) if e.isalnum()), dst, edge[2])
                except:
                    print "src:{0}\ndst:{1}\nAttr:\n{2}".format(src, dst, edge[2])
                    raise
    #                raise error, None, sys.exc_info()[2]
    #            print "edge 2 after adding edge is\n{0}".format(edge[2])  # DEBUG

        # Get all nodes with no neighbors
        nodes = [k for k,v in g.degree().iteritems() if v==0]
        # For those nodes, get or create them in the graph and update the times
        for node_uri in nodes:
            attr = g.node[node_uri]
            # Get/Create node in titan
            node = titan_graph.vertices.get_or_create("uri", node_uri, attr) # WARNING: This only works if g was created correctly
            # Update the times
            if node.start_time == "" or datetime.strptime(node.start_time, "%Y-%m-%dT%H:%M:%SZ") > \
               datetime.strptime(attr["start_time"], "%Y-%m-%dT%H:%M:%SZ"):
                node.start_time = attr["start_time"]
            if "finish_time" in node and datetime.strptime(node.finish_time, "%Y-%m-%dT%H:%M:%SZ") < \
               datetime.strptime(attr["finish_time"], "%Y-%m-%dT%H:%M:%SZ"):
                node.finish_time = attr["finish_time"]
            node.save()

        return






