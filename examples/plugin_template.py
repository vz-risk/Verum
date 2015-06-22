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
PLUGIN_CONFIG_FILE = "plugin_template.yapsy-plugin"
NAME = "<NAME FROM CONFIG FILE AS BACKUP IF CONFIG FILE DOESN'T LOAD>"


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
    import tldextract
    module_import_success = True
except:
    module_import_success = False
    logging.error("Module import failed.  Please install the following module: tldextract.")
    raise


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

    #  TODO: The init should contain anything to load modules or data files that should be variables of the  plugin object
    def __init__(self):
        pass

    #  TODO: Configuration needs to set the values needed to identify the plugin in the plugin database as well as ensure everyhing loaded correctly
    #  TODO: Current  layout is for an enrichment plugin
    #  TODO: enrichment [type, successful_load, name, description, inputs to enrichment such as 'ip', cost, speed]
    #  TODO: interface [type, successful_load, name]
    #  TODO: score [TBD]
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
            return [None, False, NAME, description, None, cost, speed]

        if 'inputs' in config_options:
            self.inputs = config.get('Configuration', 'Inputs')
            self.inputs = [l.strip().lower() for l in self.inputs.split(",")]
        else:
            logging.error("No input types specified in config file.")
            return [plugin_type, False, NAME, description, None, cost, speed]

        if not module_import_success:
            logging.error("Module import failure caused configuration failure.")
            return [plugin_type, False, NAME, description, self.inputs, cost, speed]
        else:
            return [plugin_type, True, NAME, description, self.inputs, cost, speed]


    #  TODO: The correct type of execution function must be defined for the type of plugin
    #  TODO: enrichment: "run(<thing to enrich>, inputs, start_time, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  TODO: interface: enrich(graph, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  TODO:            query(topic, max_depth, config, dont_follow, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  TODO: score: score(subgraph, topic, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  TODO: minion [TBD] 
    #  TODO: Enrichment plugin specifics:
    #  -     Created nodes/edges must follow http://blog.infosecanalytics.com/2014/11/cyber-attack-graph-schema-cags-20.html
    #  -     The enrichment should include a node for the <thing to enrich>
    #  -     The enrichment should include a node for the enrichment which is is statically defined & key of "enrichment"
    #  -     An edge should exist from <thing to enrich> to the enrichment node, created at the end after enrichment
    #  -     Each enrichment datum should have a node
    #  -     An edge should exist from <thing to enrich> to each enrichment datum
    #  -     The run function should then return a networkx directed multi-graph including the nodes and edges
    #  TODO: Interface plugin specifics:
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
    #  TODO: Score plugin specifics:
    #  -     Scoring plugins should take a topic and networkx (sub)graph and return a dictionary keyed with the node (name) and with
    #  -     values of the score assigned to the node for the given topic.
    #  TODO: Minion plugin specifics:
    #  -     [TBD]
    def run(self, domain, inputs=None, start_time="", include_subdomain=False):
        """

        :param domain: a string containing a domain to look up
        :param include_subdomain: Boolean value.  Default False.  If true, subdomain will be returned in enrichment graph
        :return: a networkx graph representing the sections of the domain
        """
        if inputs is None:
            inputs = self.inputs

        if type(start_time) is str:
            try:
                time = datetime.strptime("%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H:%M:%SZ")
            except:
                time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        elif type(star_time) is datetime:
            time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        ext = tldextract.extract(domain)
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        g = nx.MultiDiGraph()

        # Get or create Domain node
        domain_uri = "class=attribute&key={0}&value={1}".format("domain", domain)
        g.add_node(domain_uri, {
            'class': 'attribute',
            'key': "domain",
            "value": domain,
            "start_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),  # graphml does not support 'none'
            "uri": domain_uri
        })

        # Get or create Enrichment node
        tld_extract_uri = "class=attribute&key={0}&value={1}".format("enrichment", "tld_extract")
        g.add_node(tld_extract_uri, {
            'class': 'attribute',
            'key': "enrichment",
            "value": "tld_extract",
            "start_time": now,
            "uri": tld_extract_uri
        })

        # Get or create TLD node
        tld_uri = "class=attribute&key={0}&value={1}".format("domain", ext.suffix)
        g.add_node(tld_uri, {
            'class': 'attribute',
            'key': "domain",
            "value": ext.suffix,
            "start_time": now,
            "uri": tld_uri
        })

        # Link domain to tld
        edge_attr = {
            "relationship": "describedBy",
            "start_time": now,
            "origin": "tld_extract",
            "describedBy":"suffix"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, tld_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, tld_uri, edge_uri, edge_attr)


        # Get or create mid domain node
        mid_domain_uri = "class=attribute&key={0}&value={1}".format("domain", ext.domain)
        g.add_node(mid_domain_uri, {
            'class': 'attribute',
            'key': "domain",
            "value": ext.domain,
            "start_time": now,
            "uri": mid_domain_uri
        })

        # Link domain to mid_domain
        edge_attr = {
            "relationship": "describedBy",
            "start_time": now,
            "origin": "tld_extract",
            "describedBy":"domain"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, mid_domain_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, mid_domain_uri, edge_uri, edge_attr)


        # if including subdomains, create subdomain and node
        if include_subdomain:
            # Get or create mid domain node
            subdomain_uri = "class=attribute&key={0}&value={1}".format("domain", ext.subdomain)
            g.add_node(subdomain_uri, {
                'class': 'attribute',
                'key': "domain",
                "value": ext.domain,
                "start_time": now,
                "uri": subdomain_uri
            })

            # Link domain to mid_domain
            edge_attr = {
                "relationship": "describedBy",
                "start_time": now,
                "origin": "tld_extract",
                "describedBy":"subdomain"
            }
            source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
            dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, subdomain_uri)
            edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
            rel_chain = "relationship"
            while rel_chain in edge_attr:
                edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
                rel_chain = edge_attr[rel_chain]
            if "origin" in edge_attr:
                edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
            edge_attr["uri"] = edge_uri
            g.add_edge(domain_uri, subdomain_uri, edge_uri, edge_attr)

        # Link domain to enrichment
        edge_attr = {
            "relationship": "describedBy",
            "start_time": now,
            "origin": "tld_extract"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, tld_extract_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, tld_extract_uri, edge_uri, edge_attr)

        return g


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