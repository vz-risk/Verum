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
PLUGIN_CONFIG_FILE = "bayes_net.yapsy-plugin"
NAME = "BayesNet"


########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
import networkx as nx
from datetime import datetime # timedelta imported above
import uuid
import ConfigParser
import inspect
from collections import defaultdict
import random
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
            description = config.get('Documentation', 'description')
        else:
            logging.error("'Description not in config file.")
            return [None, False, NAME, None, cost, speed]

        if 'type' in config_options:
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, False, NAME, description, cost, speed]

        return [plugin_type, True, NAME, description, cost, speed]


    def score(self, sg, topic):  # get_bayesian_network_probability
        """

        :param sg: egocentric subgraph around topic in networkx format
        :param topic: graph of topics
        :param distance_degradation: A factor for degrading as distance from the topic increases
        :return: Dictionary of probabilities keyed by node

        NOTE: Will error on cycles in graph
        """
        # Calculate the probability of each node given the topic nodes
        # TODO: Capture the context of relationships as well
        # TODO: Handle loops more elegantly than failing
        # TODO: handle the markov blanket

        # setup
        confidences = nx.get_edge_attributes(sg, 'confidence')
        probabilities = defaultdict(lambda: 0)
        queue = list()
        complete_history = random.sample(xrange(10000), 1000)
        complete = set()

        for node in topic.nodes():
            probabilities[node] = 1  # The topic nodes are by definition true
            complete.add(node)  # The topic nodes are by definition complete
        for node in sg.nodes():
            for successor in sg.successors(node):
                queue.append(successor)
        print "Starting probability loop"
        while len(queue) > 0:
            temp = complete_history.pop(0)
            complete_history.append(len(complete))
            if len(set(complete_history)) < 2:
                print "Error, nothing completed in 1000 rounds."
                print "Queue length is {0} with {1} unique values".format(len(queue), len(set(queue)))
                print "Complete is\n{0}".format(len(complete))
                break
            node = queue.pop(0)
            if node not in complete:  # Only
                ready_to_calculate = True
                for predecessor in sg.predecessors(node):
                    if predecessor not in complete:
                        queue.append(predecessor)  # if the node is not complete, enqueue it
                        ready_to_calculate = False  # before we can complete a node, it's predecessors must be complete
                if ready_to_calculate:
                    try:
                        # INSERT WEIGHTING FUNCTION BELOW
                        cpt = np.array(self.normal_weight(sg.node[node]['topic_distance']))
                    except Exception as e:
                        print "Node: {0}, Attributes: {1}".format(node, sg.node[node])
                        raise e
                    for predecessor in sg.predecessors(node):
                        # If an edge has a confidence, we use it.  Otherwise we assume 100%
                        if (predecessor, node) in confidences:

                            confidence = confidences[(predecessor, node)]
                        else:
                            confidence = 1
                    # Calculate the probability based on the bayesian network
                    # Reference: http://cs.nyu.edu/faculty/davise/ai/bayesnet.html
                    # Reference: http://en.wikipedia.org/wiki/Bayes'_theorem
                    # Reference: http://en.wikipedia.org/wiki/Bayesian_network
                    for i in range(2**len(sg.predecessors(node))):
                        # double the rows
                        cpt = np.vstack((cpt, cpt))
                        # create a list that is first half the compliment of the probability and second half the probability
                        new_col = []
                        for j in range(cpt.shape[0]):
                            if j < cpt.shape[0] / float(2):
                                new_col.append(1 - (confidence * probabilities[predecessor]))
                            else:
                                new_col.append(confidence * probabilities[predecessor])
                        # Add that column to the CPT
                        cpt = np.column_stack((cpt, new_col))

                    # Remove first (all false) row as it should not be summed into the probability
                    #  This is in leu of making the prior probability zero for that row
                    cpt = np.delete(cpt, (0), axis=0)

                    # sum the product of each column to get the node probability
                    probabilities[node] = cpt.prod(axis=1).sum()
                    queue = queue + sg.successors(node)  # queue successors to the node
                    complete.add(node)  # add the node as completed

                else:  # It's not ready to be completed
                    queue.append(node)  # requeue the node after it's predecessors

        return probabilities


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