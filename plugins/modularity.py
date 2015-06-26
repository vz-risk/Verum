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
PLUGIN_CONFIG_FILE = "modularity.yapsy-plugin"
NAME = "Modularity"


########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
import networkx as nx
from datetime import datetime # timedelta imported above
import uuid
import ConfigParser
import inspect
try:
    import community
    module_import_success = True
except:
    module_import_success = False
import numpy as np

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
    #  TODO: The init should contain anything to load modules or data files that should be variables of the  plugin object
    def __init__(self):
        pass

    #  TODO: Configuration needs to set the values needed to identify the plugin in the plugin database as well as ensure everyhing loaded correctly
    #  TODO: Current  layout is for an enrichment plugin
    #  TODO: enrichment [type, successful_load, name, description, inputs to enrichment such as 'ip', cost, speed]
    #  TODO: interface [type, successful_load, name]
    #  TODO: query [TBD]
    #  TODO: minion [TBD]
    def configure(self):
        """

        :return: return list of [configure success (bool), name, description, list of acceptable inputs, resource cost (1-10, 1=low), speed (1-10, 1=fast)]
        """
        config_options = config.options("Configuration")

        if 'cost' in config_options:
            cost = config.get('Configuration', 'cost')
        else:
            cost = 9999
        if 'speed' in config_options:
            speed = config.get('Configuration', 'speed')
        else:
            speed = 9999

        if config.has_section('Documentation') and 'description' in config.options('Documentation'):
            description = config.get('Configuration', 'type')
        else:
            logging.error("'Description not in config file.")
            return [None, False, NAME, None, cost, speed]

        if 'type' in config_options:
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, False, NAME, description, cost, speed]

        if not module_import_success:
            logging.error("Module import failure caused configuration failure.")
            return [plugin_type, False, NAME, description, cost, speed]
        else:
            return [plugin_type, True, NAME, description, cost, speed]



    def score(self, sg, *args, **xargs):  # get_modularity_cluster
        """

        :param sg: subgraph
        :return: A dictionary of the modularity scores of the nodes in the subgraph
        """
        # args/xargs collected so that passing a topic doesn't mess things up

        # Convert to diGraph
        if sg.is_multigraph():
            sg = self.multigraph_to_digraph(sg)
        # Convert to undirected
        sg = sg.to_undirected()

        return community.best_partition(sg)


    def multigraph_to_digraph(self, g):
        """

        :param g: takes a networkx mulitgraph
        :return: returns a networkx digraph with edge weights representing the number of edges

        NOTE: This butchers duplicate edge properties.  If converting to score, use original edges in output.
        """
        G = nx.DiGraph()
        edge_attributes = {}

        # if g isn't really a multigraph, just return it
        if not g.is_multigraph():
            return g

        # collapse down to a diagraph
        G.add_nodes_from(g.nodes(data=True))
        G.add_edges_from(g.edges(data=True))

        # for each edge, weight the confidence by the number of edges
        '''
        # captures a multiple of the confidence on the edge in the output graph
        for edge in G.edges():
            count = g.edges().count(edge)
            if "count" > 1:
                if "confidence" in G.edge[edge[0]][edge[1]]:
                    G.edge[edge[0]][edge[1]]['confidence'] *= count
                else:
                    G.edge[edge[0]][edge[1]]["confidence"] = count
        '''
        # Captures every confidence
        for edge in G.edges():
            confidence = 0
            for src_edge in g.edge[edge[0]][edge[1]].values():
                confidence += src_edge.get('confidence', 1)
            G.edge[edge[0]][edge[1]]['confidence'] = confidence
    #    # collapse down to a diagraph
    #    G.add_nodes_from(g.nodes(data=True))
    #    G.add_edges_from(g.edges(data=True))

        return G


    ### DISTANCE WEIGHTS ###
    def linear_weight(self, distance, ddp=.2):
        """

        :param distance: distance from topic
        :param ddp: percentage to degrade
        :return: Linear weighting factor as float
        """
        return 1 - (distance * ddp)


    def log_weight(self, distance, a=1, b=1, n=3, pwr=1):
        """

        :param distance: distance: distance from topic
        :param a: constant to shape graph. Adjusts hight at 0 = a / (1 + b)
        :param b: constant to shape graph.
        :param n: constant to shape graph.
        :param pwr: constant to shape graph.
        :return: log weighting factor as float
        """
        return a / (1 + b*np.exp((distance-n) * pwr))


    def exponential_weight(self, distance, b=2):
        return np.exp(-distance/b)


    def normal_weight(self, distance, pwr=2, a=1.1, b=10, c=1):
        """

        :param distance: distance from topic
        :param pwr: constant to shape graph.  Higher = steeper decline
        :param b: constant to shape graph.  lower = greater spread
        :return: normal weighting factor as float
        pwr = 2.5, a = 1, c = 0, b = 30
        """
        return a * np.exp(-(distance + c)**pwr/b)