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
NEO4J_CONFIG_FILE = "neo4j.yapsy-plugin"
# Below values will be overwritten if in the config file or specified at the command line
NEO4J_HOST = 'localhost'
NEO4J_PORT = '7474'
LOGLEVEL = logging.INFO
LOGFILE = None
USERNAME = None
PASSWORD = None
NAME = 'Neo4j'



########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
from datetime import datetime # timedelta imported above
try:
    from py2neo import Graph as py2neoGraph
    from py2neo import Node as py2neoNode
    from py2neo import Relationship as py2neoRelationship
    from py2neo import authenticate as py2neoAuthenticate
    neo_import = True
except:
    logging.error("Neo4j plugin did not load.")
    neo_import = False
try:
    from yapsy.PluginManager import PluginManager
    plugin_import = True
except:
    logging.error("Yapsy plugin manager did not load for neo4j plugin.")
    plugin_import = False
import ConfigParser
import sqlite3
import networkx as nx
import os
import inspect
import uuid

## SETUP
__author__ = "Gabriel Bassett"
# Read Config File - Will overwrite file User Variables Section
loc = inspect.getfile(inspect.currentframe())
i = loc.rfind("/")
loc = loc[:i+1]
config = ConfigParser.SafeConfigParser()
config.readfp(open(loc + NEO4J_CONFIG_FILE))
if config.has_section('neo4j'):
    if 'host' in config.options('neo4j'):
        NEO4J_HOST = config.get('neo4j', 'host')
    if 'port' in config.options('neo4j'):
        NEO4J_PORT = config.get('neo4j', 'port')
    if 'username' in config.options('neo4j'):
        USERNAME = config.get('neo4j', 'username')
    if 'password' in config.options('neo4j'):
        PASSWORD = config.get('neo4j', 'password')
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
class PluginOne(IPlugin):
    neo4j_config = None

    def __init__(self):
        pass


    def configure(self):
        """

        :return: return list of [configure success (bool), name, description, list of acceptable inputs, resource cost (1-10, 1=low), speed (1-10, 1=fast)]
        """
        config_options = config.options("Configuration")

        # Create neo4j config
        # TODO: Import host, port, graph from config file
        try:
            self.set_neo4j_config(NEO4J_HOST, NEO4J_PORT, USERNAME, PASSWORD)
            config_success = True
        except:
            config_success = False

        # Set success of configuration
        if config_success and neo_import and plugin_import:
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


    def set_neo4j_config(self, host, port, username=None, password=None):
        if username and password:
            py2neoAuthenticate("{0}:{1}".format(host, port), username, password)
            self.neo4j_config = "http://{2}:{3}@{0}:{1}/db/data/".format(host, port, username, password)
        else:
            self.neo4j_config = "http://{0}:{1}/db/data/".format(host, port)


    def removeNonAscii(self, s): return "".join(i for i in s if ord(i)<128)


    def enrich(self, g):  # Neo4j
        """

        :param g: networkx graph to be merged
        :param neo4j: bulbs neo4j config
        :return: Nonetype

        Note: Neo4j operates differently from the current titan import.  The neo4j import does not aggregate edges which
               means they must be handled at query time.  The current titan algorithm aggregates edges based on time on
               merge.
        """
        #neo4j_graph = NEO_Graph(neo4j)  # Bulbs
        neo_graph = py2neoGraph(self.neo4j_config)
        nodes = set()
        node_map = dict()
        edges = set()
        settled = set()
        # Merge all nodes first
        tx = neo_graph.cypher.begin()
        cypher = ("MERGE (node: {0} {1}) "
                  "ON CREATE SET node = {2} "
                  "RETURN collect(node) as nodes"
                 )
        # create transaction for all nodes
        for node, data in g.nodes(data=True):
            query = cypher.format(data['class'], "{key:{KEY}, value:{VALUE}}", "{MAP}")
            props = {"KEY": data['key'], "VALUE":data['value'], "MAP": data}
            # TODO: set "start_time" and "finish_time" to dummy variables in attr.
            # TODO:  Add nodes to graph, and cyper/gremlin query to compare to node start_time & end_time to dummy
            # TODO:  variable update if node start > dummy start & node finish < dummy finish, and delete dummy
            # TODO:  variables.
            tx.append(query, props)
        # commit transaction and create mapping of returned nodes to URIs for edge creation
        for record_list in tx.commit():
            for record in record_list:
    #            print record, record.nodes[0]._Node__id, len(record.nodes)
                for n in record.nodes:
    #                print n._Node__id
                    attr = n.properties
                    uri = "class={0}&key={1}&value={2}".format(attr['class'], attr['key'], attr['value'])
                    node_map[uri] = int(n.ref.split("/")[1])
    #                node_map[uri] = n._Node__id
    #    print node_map  # DEBUG

        # Create edges
        cypher = ("MATCH (src: {0}), (dst: {1}) "
                  "WHERE id(src) = {2} AND id(dst) = {3} "
                  "CREATE (src)-[rel: {4} {5}]->(dst) "
                 )
        tx = neo_graph.cypher.begin()
        for edge in g.edges(data=True):
            try:
                if 'relationship' in edge[2]:
                    relationship = edge[2].pop('relationship')
                else:
                    # default to 'described_by'
                    relationship = 'describedBy'

                query = cypher.format(g.node[edge[0]]['class'],
                                      g.node[edge[1]]['class'],
                                     "{SRC_ID}",
                                     "{DST_ID}",
                                      relationship,
                                      "{MAP}"
                                     )
                props = {
                    "SRC_ID": node_map[edge[0]],
                    "DST_ID": node_map[edge[1]],
                    "MAP": edge[2]
                }

                # create the edge
                # NOTE: No attempt is made to deduplicate edges between the graph to be merged and the destination graph.
                #        The query scripts should handle this.
        #        print edge, query, props  # DEBUG
                tx.append(query, props)
        #        rel = py2neoRelationship(node_map[src_uri], relationship, node_map[dst_uri])
        #        rel.properties.update(edge[2])
        #        neo_graph.create(rel)  # Debug
        #        edges.add(rel)
            except:
                print edge
                print node_map
                raise

        # create edges all at once
        #print edges  # Debug
    #    neo_graph.create(*edges)
        tx.commit()


    def query(self, topic, max_depth=4, dont_follow=['enrichment', 'classification'], config=None):
        """

            :param topic: a  graph to return the context of.  At least one node ID in topic \
             must be in full graph g to return any context.
            :param max_depth: The maximum distance from the topic to search
            :param config: The titanDB configuration to use if not using the one configured with the plugin
            :param dont_follow: A list of attribute types to not follow
            :return: subgraph in networkx format
        """
        if config is None:
            config = self.neo4j_config

        neo_graph = py2neoGraph(config)
        sg = nx.MultiDiGraph()

        # Get IDs of topic nodes in graph (if they exist).  Also add topics to subgraph
        topic_ids = set()
        for t, data in topic.nodes(data=True):
            cypher = ("MATCH (topic: {0} {1}) "
                      "RETURN collect(topic) as topics").format(data['class'], "{key:{KEY}, value:{VALUE}}")
            props = {"KEY":data['key'], "VALUE":data['value']}
            records = neo_graph.cypher.execute(cypher, props)
            #print cypher, props  #  DEBUG
            #print type(records)
            for record in records:
                #print record  # DEBUG
                for tnode in record.topics:
                    attr = dict(tnode.properties)
                    uri = u'class={0}&key={1}&value={2}'.format(attr['class'],attr['key'], attr['value'])
                    sg.add_node(uri, attr)
                    topic_ids.add(int(tnode.ref.split("/")[-1]))

        # Add nodes at depth 1  (done separately as it doesn't include the intermediary
        nodes = dict()
        if max_depth > 0:
            if max_depth == 1:
                cypher = ("path=MATCH (topic)-[rel:describedBy|influences]-(node: attribute)"
                          "WHERE id(topic) IN {TOPICS}"
                          "RETURN DISTINCT extract(r IN rels(path) | r) as rels, extract(n IN nodes(path) | n) as nodes ")
                attr = {"TOPICS":list(topic_ids)}
            else:
                cypher = ("MATCH path=(topic: attribute)-[rel:describedBy|influences]-(node: attribute) "
                          "WHERE id(topic) IN {TOPICS} "
                          "RETURN DISTINCT extract(r IN rels(path) | r) as rels, extract(n IN nodes(path) | n) as nodes "
                          "UNION "
                          "MATCH path=(topic: attribute)-[rel1: describedBy|influences]-(intermediate: attribute)-[rel2: describedBy|influences]-(node: attribute) "
                          "WHERE id(topic) IN {TOPICS} AND NOT intermediate.key in {DONT_FOLLOW} and length(path) <= {MAX_DEPTH} "
                          "RETURN DISTINCT extract(r IN rels(path) | r) as rels, extract(n IN nodes(path) | n) as nodes ")
                attr = {"MAX_DEPTH": max_depth,
                        "TOPICS": list(topic_ids),
                        "DONT_FOLLOW": dont_follow}
            #print cypher, attr  # DEBUG
            for record in neo_graph.cypher.stream(cypher, attr):  # Prefer streaming to execute, if it works
#            for record in neo_graph.cypher.execute(cypher, attr):
                #print record  # DEBUG
                for node in record.nodes:
                    attr = dict(node.properties)
                    uri = 'class={0}&key={1}&value={2}'.format(attr['class'],attr['key'], attr['value'])
                    sg.add_node(uri, attr)
                    nodes[node.ref.split("/")[-1]] = uri
                for rel in record.rels:
    #                print type(rel) # DEBUG
                    # add edges SRC node
    #                src_attr = dict(rel.start_node.properties)
    #                src_uri = u"class={0}&key={1}&value={2}".format(src_attr['class'], src_attr['key'], src_attr['value'])
    #                sg.add_node(src_uri, src_attr)
                    src_uri = nodes[rel.start_node.ref.split("/")[-1]]  # src node uri from neo4j ID

                    # Add edge DST node
    #                dst_attr = dict(rel.end_node.properties)
    #                dst_uri = u"class={0}&key={1}&value={2}".format(dst_attr['class'], dst_attr['key'], dst_attr['value'])
    #                sg.add_node(dst_uri, dst_attr)
                    dst_uri = nodes[rel.end_node.ref.split("/")[-1]]  # dst node uri from neo4j ID

                    # add edge
                    edge_attr = dict(rel.properties)
                    edge_attr['relationship'] = rel.type
                    source_hash = uuid.uuid3(uuid.NAMESPACE_URL, src_uri)
                    dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, dst_uri)
                    edge_uri = u"source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
                    rel_chain = u"relationship"
                    while rel_chain in edge_attr:
                        edge_uri = edge_uri + u"&{0}={1}".format(rel_chain,edge_attr[rel_chain])
                        rel_chain = edge_attr[rel_chain]
                    if "origin" in edge_attr:
                        edge_uri += u"&{0}={1}".format(u"origin", edge_attr["origin"])
                    edge_attr["uri"] = edge_uri
                    sg.add_edge(src_uri, dst_uri, edge_uri, edge_attr)

        # Set the topic distances
        distances = self.get_topic_distance(sg.to_undirected(), topic)
        nx.set_node_attributes(sg, u'topic_distance', distances)

        # TODO:  Handle duplicate edges (may dedup but leave in for now)
        #          Take edges into dataframe
        #          group by combine on features to be deduplicated.  Return edge_id's in each group.  Combine those edge_ids using a combine algorithm
        #          Could do the same with dedup algo, but just return the dedup edge_ids and delete them from the graph

        return sg


    def get_topic_distance(self, sg, topic):
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