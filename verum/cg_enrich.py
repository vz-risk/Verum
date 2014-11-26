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

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
CONFIG_FILE = "verum.cfg"
# Below values will be overwritten if in the config file or specified at the command line
TITAN_HOST = "localhost"
TITAN_PORT = "8182"
TITAN_GRAPH = "vzgraph"
PluginFolder = "./plugins"
NEO4J_HOST = 'localhost'
NEO4J_PORT = '7474'



########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
import argparse
import logging
from datetime import datetime # timedelta imported above
# todo: Import with IMP and don't import the titan graph functions if they don't import
from cags_schema import TalksTo, DescribedBy, Influences
from bulbs.titan import Graph as TITAN_Graph
from bulbs.titan import Config as TITAN_Config
from py2neo import Graph as py2neoGraph
from yapsy.PluginManager import PluginManager
import ConfigParser
import sqlite3
import networkx as nx

## SETUP
__author__ = "Gabriel Bassett"
# Read Config File - Will overwrite file User Variables Section
config = ConfigParser.SafeConfigParser()
config.readfp(open(CONFIG_FILE))
if config.has_section('TITANDB'):
    if 'host' in config.options('TITANDB'):
        TITAN_HOST = config.get('TITANDB', 'host')
    if 'port' in config.options('TITANDB'):
        TITAN_PORT = config.get('TITANDB', 'port')
    if 'graph' in config.options('TITANDB'):
        TITAN_GRAPH = config.get('TITANDB', 'graph')
if config.has_section('NEO4j'):
    if 'host' in config.options('NEO4J'):
        NEO4J_HOST = config.get('NEO4J', 'host')
    if 'port' in config.options('NEO4J'):
        NEO4J_PORT = config.get('NEO4J', 'port')
if config.has_section('Core'):
    if 'plugins' in config.options('Core'):
        PluginFolder = config.get('Core', 'plugins')

# Parse Arguments - Will overwrite Config File
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
parser.add_argument('--plugins', help="Location of plugin directory", default=PluginFolder)
parser.add_argument('--titan_host', help="Host for titanDB database.", default=TITAN_HOST)
parser.add_argument('--titan_port', help="Port for titanDB database.", default=TITAN_PORT)
parser.add_argument('--titan_graph', help="Graph for titanDB database.", default=TITAN_GRAPH)
parser.add_argument('--neo4j_host', help="Host for Neo4j database.", default=NEO4J_HOST)
parser.add_argument('--neo4j_port', help="Port for Neo4j database.", default=NEO4J_PORT)

## Set up Logging
args = parser.parse_args()
if args.log is not None:
    logging.basicConfig(filename=args.log, level=args.loglevel)
else:
    logging.basicConfig(level=args.loglevel)
# <add other setup here>


## EXECUTION
class enrich():
    titandb_config = None
    neo4j_config = None
    enrichment_db = None

    def __init__(self):

        # Set up the TitanDB Config
        self.set_titan_config(args.titan_host, args.titan_port, args.titan_graph)

        # Set up the Neo4j Config
        self.set_neo4j_config(args.neo4j_host, args.neo4j_port)

        # Load enrichments database
        self.enrichment_db = self.set_enrichment_db()

        # Load the plugins Directory
        self.load_plugins()
        # TODO: set up the plugin directory

    # Load the plugins from the plugin directory.
    def load_plugins(self):
        print "Configuring Plugin manager."
        manager = PluginManager()
        manager.setPluginPlaces([PluginFolder])
        manager.collectPlugins()
        print "Plugin manager configured."

        # Loop round the plugins and print their names.
        cur = self.enrichment_db.cursor()
        for plugin in manager.getAllPlugins():
            print "Configuring plugin {0}.".format(plugin.name)
            config = plugin.plugin_object.configure()
            if config[0]:
                success = 1
            else:
                success = 0
            # Insert enrichment
            # TODO
            cur.execute("INSERT INTO enrichments VALUES (?, ?, ?, ?, ?)", (config[1],
                                                                           success,
                                                                           config[2],
                                                                           config[4],
                                                                           config[5])

            )
            for input in config[3]:
                # Insert into inputs table
                cur.execute("INSERT INTO inputs VALUES (?,?)", config[1], input)
        self.enrichment_db.commit()


    def set_enrichment_db(self):
        conn = sqlite3.connect(":memory:")
        c = conn.cursor()
        # Create enrichments table
        c.execute('''CREATE TABLE enrichments (name text NOT NULL PRIMARY KEY,
                                               config int,
                                               description text,
                                               cost int,
                                               speed int)''')
        # Create inputs table
        c.execute('''CREATE TABLE inputs (name text NOT NULL,
                                          input text NOT NULL,
                                          PRIMARY KEY (name, input),
                                          FOREIGN KEY (name) REFERENCES enrichments(name))''')
        conn.commit()
        return conn


    def removeNonAscii(self, s): return "".join(i for i in s if ord(i)<128)


    def set_neo4j_config(self, host, port):
        self.neo4j_config = "http://{0}:{1}/db/data/".format(host, port)


    def merge_neo4j(self, g):  # Neo4j
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
                relationship = edge[2].pop('relationship')
            except:
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

        # create edges all at once
        #print edges  # Debug
    #    neo_graph.create(*edges)
        tx.commit()


    def get_enrichments(self, inputs, cost=10, speed=10, enabled=True):
        """

        :param inputs: list of input types.   (e.g. ["ip", "domain"])  All enrichments that match at least 1 input type will be returned.
        :param cost:  integer 1-10 of resource cost of running the enrichment.  (1 = cheapest)
        :param speed: integer 1-10 speed of enrichment. (1 = fastest)
        :param enabled: Plugin is correctly configured.  If false, plugin may not run correctly.
        :return: list of names of enrichments matching the criteria
        """


    def run_enrichments(self, topic, topic_type, cost=10, speed=10):
        """

        :param topic: topic to enrich (e.g. "1.1.1.1", "www.google.com")
        :param topic_type: type of topic (e.g. "ip", "domain")
        :param cost: integer 1-10 of resource cost of running the enrichment.  (1 = cheapest)
        :param speed: integer 1-10 speed of enrichment. (1 = fastest)
        :return: networkx graph representing the enrichment of the topic
        """
        enrichments = self.get_enrichments([topic_type], cost, speed, enabled=True)
        g = nx.MultiDiGraph()

        for enrichment in enrichments:
            # TODO: Run enrichment
            # TODO: Merge enrichment graph with g
            pass

        return g


    def set_titan_config(self, host, port, graph):
        self.titan_config = TITAN_Config('http://{0}:{1}/graphs/{2}'.format(host, port, graph))


    def merge_titandb(self, g):
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