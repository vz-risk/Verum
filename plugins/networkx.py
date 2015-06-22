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
NX_CONFIG_FILE = "networkx.yapsy-plugin"
NAME = "Networkx Interface"


########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
import networkx as nx
from datetime import datetime # timedelta imported above
import uuid
import ConfigParser
import inspect
import os.path


## SETUP
loc = inspect.getfile(inspect.currentframe())
ind = loc.rfind("/")
loc = loc[:ind+1]
config = ConfigParser.SafeConfigParser()
config.readfp(open(loc + NX_CONFIG_FILE))

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
    context_graph = nx.MultiDiGraph()
    context_graph_file = None

    def __init__(self):
        if 'context_graph_file' in config.options("Configuration"):
            self.context_graph_file = config.get('Configuration', 'context_graph_file')


    def configure(self):
        """

        :return: return list of [type, successful_load, name]
        """
        config_options = config.options("Configuration")

        if os.path.isfile(self.context_graph_file):
            try:
                self.context_graph = self.read_graph(self.context_graph_file) 
            except:
                pass
        else:
            logging.info("Networkx file not for import.")

        if 'type' in config_options:
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, False, NAME]

        return [plugin_type, True, NAME]


    def enrich(self, g):  # Networkx
        """

        :param g: networkx graph to be merged
        :return: Nonetype

        Note: Neo4j operates differently from the current titan import.  The neo4j import does not aggregate edges which
               means they must be handled at query time.  The current titan algorithm aggregates edges based on time on
               merge.
        """
        for uri, data in g.nodes(data=True):
        # For each node:
            # Get node by URI
            # (should we double check the the class/key/value match?)
            # If it exists in the receiving graph, going to need to merge properties (replacing with newer)
            if uri in self.context_graph.nodes():
                self.context_graph.node[uri].update(data)
            else:
                self.context_graph.add_node(uri, attr_dict=data)
        # For each edge:
        for edge in g.edges(data=True):
            # Add it
            self.context_graph.add_edge(edge[0], edge[1], attr_dict=data)


    def query(self, topic, max_depth=4, config=None, dont_follow=['enrichment', 'classification']):
        """
            :param topic: a  graph to return the context of.  At least one node ID in topic \
             must be in full graph g to return any context.
            :param max_depth: The maximum distance from the topic to search
            :param config: The titanDB configuration to use if not using the one configured with the plugin
            :param dont_follow: A list of attribute types to not follow
            :return: subgraph in networkx format
        """
        distances = dict()

        if config is None:
            config = self.context_graph

        # Conver topic from a graph into a set of nodes
        topic_nodes = set()
        for n, d in topic.nodes(data=True):
            topic_nodes.add("class={0}&key={1}&value={2}".format(d['class'], d['key'], d['value']))

        nodes = topic_nodes.copy()

        for t in topic:
            # get all nodes within max_depth distance from each topic and add them to the set
            new_distances = nx.single_source_shortest_path_length(self.context_graph.to_undirected(), t, cutoff=max_depth)
            nodes = nodes.union(set(new_distances.keys()))

            # Update shortest distances from topic to node
            for n in new_distances.keys():
                if n in distances:
                    if new_distances[n] < distances[n]:
                        distances[n] = new_distances[n]
                else:
                    distances[n] = new_distances[n]

        # remove dont_follow nodes:
        nodes_to_remove = set()
        for n in nodes:
            if self.context_graph.node[n]['key'] in dont_follow:
                nodes_to_remove.add(n)
        nodes = nodes.difference(nodes_to_remove)

        # Get the subgraph represented by the nodes:
        g = nx.MultiDiGraph(self.context_graph.subgraph(nodes))

        # Prune out non-relevant components by removing those that contain no topic nodes.
        #  This gets ride of nodes that were found by following dont_follow nodes
        for component in nx.connected_components(g.to_undirected()):
            if len(topic_nodes.intersection(set(component))) <= 0:  # if there's no overlap betweent the component and topic
                g.remove_nodes_from(component)  # remove the component

        # add the topic distances to the subgraph
        for n in g.nodes():
            g.node[n]['topic_distance'] = distances[n]

        return g


    def get_graph(self):
        return self.context_graph


    def write_graph(self, G=None, subgraph_file=None):
        if G is None:
            G = self.context_graph
        if subgraph_file is None:
            subgraph_file = self.context_graph_file
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

    def read_graph(self, subgraph_file=None):
        if subgraph_file is None:
            subraph_file = self.context_graph_file
        logging.info("Writing graph.")
        # write the graph out
        file_format = subgraph_file.split(".")[-1]
        if file_format == "graphml":
            return nx.read_graphml(subgraph_file)
        elif file_format == "gml":
            return nx.read_gml(subgraph_file)
        elif file_format == "gexf":
            return nx.read_gexf(subgraph_file)
        elif file_format == "net":
            return nx.read_pajek(subgraph_file)
        elif file_format == "yaml":
            return nx.read_yaml(subgraph_file)
        elif file_format == "gpickle":
            return nx.read_gpickle(subgraph_file)
        else:
            logging.warning("File format not found, returning empty graph.")
        return nx.MultiDiGraph()