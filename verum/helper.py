#!/usr/bin/env python
"""
 AUTHOR: Gabriel Bassett
 DATE: <01-23-2015>
 DEPENDENCIES: <a list of modules requiring installation>
 Copyright 2015 Gabriel Bassett

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
 <A description of the software>

 NOTES:
 <No Notes>

 ISSUES:
 <No Issues>

 TODO:
 <No TODO>

"""
# PRE-USER SETUP
import logging

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
CONFIG_FILE = ""
LOGLEVEL = logging.DEBUG
LOG = None

########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS

import argparse
import ConfigParser
import networkx as nx
import urlparse
import numpy as np
from scipy import stats  # for percentile

## SETUP
__author__ = "Gabriel Bassett"

if __name__ == "__main__":
    # Parse Arguments (should correspond to user variables)
    parser = argparse.ArgumentParser(description='This script processes a graph.')
    parser.add_argument('-d', '--debug',
                        help='Print lots of debugging statements',
                        action="store_const", dest="loglevel", const=logging.DEBUG,
                        default=LOGLEVEL
                       )
    parser.add_argument('-v', '--verbose',
                        help='Be verbose',
                        action="store_const", dest="loglevel", const=logging.INFO
                       )
    parser.add_argument('--log', help='Location of log file', default=LOG)
    args = parser.parse_args()

# add config arguments
if __name__ == "__main__":
    CONFIG_FILE = args.config
try:
  config = ConfigParser.SafeConfigParser()
  config.readfp(open(CONFIG_FILE))
  config_exists = True
except:
  config_exists = False
if config_exists:
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
    if args.log is not None:
        logging.basicConfig(filename=args.log, level=args.loglevel)
    else:
        logging.basicConfig(level=args.loglevel)
# <add other setup here>


## GLOBAL EXECUTION
pass


## FUNCTION DEFINITION
def create_topic(properties, prefix=""):
    """

    :param properties: A dictionary of properties
    :param prefix: If nodes are stored with a pref
    :return: A topic graph in networkx format with one node per property

    NOTE: If multiple values of a certain type, (e.g. multiple IPs) make the value of the type
           in the dictionary a list.
    """
    g = nx.DiGraph()

    if type(properties) == dict:
        iterator = properties.iteritems()
    else:
        iterator = iter(properties)


    for key, value in iterator:
        if type(value) in (list, set, np.ndarray):
            for v in value:
                node_uri = "{2}class=attribute&key={0}&value={1}".format(key, v, prefix)
                g.add_node(node_uri, {
                    'class': 'attribute',
                    'key': key,
                    'value': v,
                    'uri': node_uri
                })
        else:
            node_uri = "{2}class=attribute&key={0}&value={1}".format(key, value, prefix)
            g.add_node(node_uri, {
                'class': 'attribute',
                'key': key,
                'value': value,
                'uri': node_uri
            })

    return g


def validate_uri(uri):
    """

    :param uri: a URI string to be validated
    :return: bool true if valid, false if not
    """
    # TODO: Validate the order properties are in (important for uri hash lookup)

    try:
        properties = urlparse.parse_qs(urlparse.urlparse(uri).query)
    except:
        return False
    if u'key' not in properties:
        return False
    elif len(properties[u'key']) != 1:
        return False
    if u'value' not in properties:
        return False
    elif len(properties[u'value']) != 1:
        return False
    if u'attribute' not in properties:
        return False
    elif len(properties[u'attribute']) != 1:
        return False
    # Nothing failed, return true
    return True


def get_topic_distance(sg, topic):
    """

    :param sg: an egocentric subgraph in networkx format
    :param topic: a networkx graph of nodes representing the topic
    :return: a dictionary of key node name and value distance as integer
    """
    distances = dict()

    # get all the distances
    for tnode in topic.nodes():
        if tnode in sg.nodes():
            distances[tnode] = nx.shortest_path_length(sg, source=tnode)

    # get the smallest distance per key
    min_dist = dict()
    for key in distances:
        for node in distances[key]:
            if node not in min_dist:
                min_dist[node] = distances[key][node]
            elif distances[key][node] < min_dist[node]:
                min_dist[node] = distances[key][node]


    # Return the dict
    return min_dist


def compare_classifications(scores, node1, node2=None, output="print"):
    """

    :param scores: dictionary keyed by nodes and values of scores
    :param node1: dictionary of {"class":<class>, "key":<key>, "value":<value>}
    :param node2: dictionary of {"class":<class>, "key":<key>, "value":<value>}.  If empty, score will be compared to the median
    :param output: string representing how to output the data.  "print" to print it, dictionary otherwise
    :return: ratio of node 1 to node 2 scores normalized to the lower score as dictionary
    """
    node1_uri = "class={0}&key={1}&value={2}".format(node1['class'], node1['key'], node1['value'])

    node1_score = scores[node1_uri]
    if node2 is None:
        node2_score = np.median(scores.values())
    else:
        node2_uri = "class={0}&key={1}&value={2}".format(node2['class'], node2['key'], node2['value'])
        node2_score = scores[node2_uri]

    if node1_score > node2_score:
        larger = "node1"
    else:
        larger = "node2"

    if output == "print":
        if node2 is None:
            if larger == "node2":
                print "The ratio of node 1 ({0}:{1}) to the median ({2}) is {3}:{4}.".format(node1['key'],
                                                                                             node1['value'],
                                                                                             node2_score,
                                                                                             node1_score/float(node1_score),
                                                                                             node2_score/float(node1_score))
            else:
                print "The ratio of node 1 ({0}:{1}) to the median ({2}) is {3}:{4}.".format(node1['key'],
                                                                                             node1['value'],
                                                                                             node2_score,
                                                                                             node1_score/float(node2_score),
                                                                                             node2_score/float(node2_score))   
        else:
            if larger == "node2":
                print "The ratio of node 1 ({0}:{1}) to node 2 ({2}:{3}) is {4}:{5}.".format(node1['key'],
                                                                                             node1['value'],
                                                                                             node2['key'],
                                                                                             node2['value'],
                                                                                             node1_score/float(node1_score),
                                                                                             node2_score/float(node1_score))
            else:
                print "The ratio of node 1 ({0}:{1}) to node 2 ({2}:{3}) is {4}:{5}.".format(node1['key'],
                                                                                             node1['value'],
                                                                                             node2['key'],
                                                                                             node2['value'],
                                                                                             node1_score/float(node2_score),
                                                                                             node2_score/float(node2_score))        
    else:
        if larger == "node2":
            return {"node1": node1_score/float(node1_score), "node2":node2_score/float(node1_score)}
        else:
            return {"node1": node1_score/float(node2_score), "node2":node2_score/float(node2_score)}


def score_percentile(scores, node1, output="print"):
    """

    :param scores: dictionary keyed by nodes and values of scores
    :param node1: dictionary of {"class":<class>, "key":<key>, "value":<value>}
    :param output: string representing how to output the data.  "print" to print it, dictionary otherwise
    :return: the percentile the node is in.  Higher means more likely.ff
    """
    node1_uri = "class={0}&key={1}&value={2}".format(node1['class'], node1['key'], node1['value'])

    p =stats.percentileofscore(scores.values(), scores[node1_uri])

    if output == "print":
        print "The percentile of the node is {0}.".format(p)
    else:
        return p


def merge_graphs(g1, g2):
    """

    """
    g = g1.copy()
    for node, props in g2.nodes(data=True):
        g.add_node(node, props)
    for edge in g2.edges(data=True):
        g.add_edge(edge[0], edge[1], attr_dict=edge[2])

    return g


def removeNonAscii(s): return u"".join(i for i in s if ord(i)<128).encode('utf8')


def remove_non_ascii_from_graph(g):
    """ networkx graph -> networkx graph

    :param g: A networkx graph
    :return: a networkx graph with nonAscii removed from all node and edge attributes
    """
    # ascii safe node key and value
    for node, data in g.nodes(data=True):
        for attr in data.keys():
            data[attr] = removeNonAscii(data[attr])
        g.node[node] = data

    if type(g) in [nx.classes.multidigraph.MultiDiGraph, nx.classes.multigraph.MultiGraph]:
        for edge in g.edges(data=True, keys=True):
            edge_attr = edge[3]
            for attr in edge_attr:
                if type(edge_attr[attr]) is str:
                    edge_attr[attr] = removeNonAscii(edge_attr[attr])
            g.edge[edge[0]][edge[1]][edge[2]] = edge_attr
    else:
        for edge in g.edges(data=True):
            edge_attr = edge[2]
            for attr in edge_attr:
                if type(edge_attr[attr]) is str:
                    edge_attr[attr] = removeNonAscii(edge_attr[attr])
            g.edge[edge[0]][edge[1]] = edge_attr


    # return the safed node
    return g

## MAIN LOOP EXECUTION
def main():
    logging.info('Beginning main loop.')

    logging.info('Ending main loop.')

if __name__ == "__main__":
    main()