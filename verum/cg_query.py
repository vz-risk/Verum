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
 Functions necessary to query the context graph.

"""
# PRE-USER SETUP
pass

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
pass


########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
import networkx as nx
import argparse
import logging
import cg_query

## SETUP
__author__ = "Gabriel Bassett"
# Parse Arguments (should correspond to user variables)
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
# <add arguments here>
#args = parser.parse_args()
## Set up Logging
#if args.log is not None:
#    logging.basicConfig(filename=args.log, level=args.loglevel)
#else:
#    logging.basicConfig(level=args.loglevel)
# <add other setup here>


## EXECUTION
def write_graph(G, subgraph_file):
    logging.info("Writing graph.")
    # write the graph out
    file_format = subgraph_file.split(".")[-1]
    if file_format == "graphml":
        nx.write_graphml(G, subgraph_file)
    elif file_format == "gml":
        nx.write_gml(G, subgraph_file)
    elif file_format == "gexf":
        nx.write_gexf(G, subgraph_file)
    elif file_format == "net":
        nx.write_pajek(G, subgraph_file)
    elif file_format == "yaml":
        nx.write_yaml(G, subgraph_file)
    elif file_format == "gpickle":
        nx.write_gpickle(G, subgraph_file)
    else:
        print "File format not found, writing graphml."
        nx.write_graphml(G, subgraph_file)


def score_and_store_topic(G, topic, topic_name = None):
    """

    :param G: networkx subgraph
    :param topic: topic(s) as networkx graph.  Name of graph will be used for property names on graphs unless topic_name listed
    :param topic_name: name to use for topic.  Graph name used if not specified.
    :return: subgraph with topic score
    """
    pass
    # TODO


def score_and_cluster(G, exclude = ("enrichment", "classification")):
    """

    :param G: a networkx subgraph
    :param exclude: set or list of node keys to exclude.  Typically, high-degree nodes that cross clusters
    :return: the subgraph with cluster IDs stored as 'subgraph_cluster' property on nodes
    """
    # Calculate Modularity
    sg_copy = G.copy()
    # Remove enrichments and classifications to improve the clusters
    nodes_to_remove = list()
    for node, data in sg_copy.nodes(data=True):
        if "key" in data and data["key"] in exclude:
            sg_copy.remove_node(node)
    partitions = cg_query.get_modularity_cluster(sg_copy)
    for node, cluster in partitions:
        G.node[node]['subgraph_cluster'] = cluster

    return G

def get_node_dataframe(G):
    """ For returning nodes for manual or automated standard data analysis

    :param G: scored networkx subgraph
    :return: tuple of pandas dataframe of nodes and edges
    """
    pass
    # TODO


# TODO - Create GUI output


# TODO - Create M2M output based on need


