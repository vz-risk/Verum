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
MAXMIND_FILE = "./GeoIPASNum.dat"
TITAN_HOST = "localhost"
TITAN_PORT = "8182"
TITAN_GRAPH = "vzgraph"
STATES = {'AA': 'armed forces americas', 'AE': 'armed forces middle east', 'AK': 'alaska', 'AL': 'alabama',
          'AP': 'armed forces pacific', 'AR': 'arkansas', 'AS': 'american samoa', 'AZ': 'arizona', 'CA': 'california',
          'CO': 'colorado', 'CT': 'connecticut', 'DC': 'district of columbia', 'DE': 'delaware', 'FL': 'florida',
          'FM': 'federated states of micronesia', 'GA': 'georgia', 'GU': 'guam', 'HI': 'hawaii', 'IA': 'iowa',
          'ID': 'idaho', 'IL': 'illinois', 'IN': 'indiana', 'KS': 'kansas', 'KY': 'kentucky', 'LA': 'louisiana',
          'MA': 'massachusetts', 'MD': 'maryland', 'ME': 'maine', 'MH': 'marshall islands', 'MI': 'michigan',
          'MN': 'minnesota', 'MO': 'missouri', 'MP': 'northern mariana islands', 'MS': 'mississippi', 'MT': 'montana',
          'NC': 'north carolina', 'ND': 'north dakota', 'NE': 'nebraska', 'NH': 'new hampshire', 'NJ': 'new jersey',
          'NM': 'new mexico', 'NV': 'nevada', 'NY': 'new york', 'OH': 'ohio', 'OK': 'oklahoma', 'OR': 'oregon',
          'PA': 'pennsylvania', 'PR': 'puerto rico', 'PW': 'palau', 'RI': 'rhode island', 'SC': 'south carolina',
          'SD': 'south dakota', 'TN': 'tennessee', 'TX': 'texas', 'UT': 'utah', 'VA': 'virginia',
          'VI': 'virgin islands', 'VT': 'vermont', 'WA': 'washington', 'WI': 'wisconsin', 'WV': 'west virginia',
          'WY': 'wyoming'}



########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
import networkx as nx
import tldextract
import argparse
import logging
import sys
from datetime import datetime # timedelta imported above
import uuid
import copy
from cymru import CymruIPtoASNService
from cags_schema import TalksTo, DescribedBy, Influences
import GeoIP
from bulbs.titan import Graph as TITAN_Graph
from bulbs.titan import Config as TITAN_Config
#import csv
import ipaddress
import pandas as pd
from urlparse import urlparse
from collections import defaultdict
import socket
import tldextract
from bulbs.neo4jserver import NEO4J_URI
from bulbs.neo4jserver import Graph as NEO_Graph
from bulbs.neo4jserver import Config as NEO_Config
from py2neo import Graph as py2neoGraph
from py2neo import Relationship as py2neoRelationship
from py2neo import Node as py2neoNode

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
# create titan config
titan_config = TITAN_Config('http://{0}:{1}/graphs/{2}'.format(TITAN_HOST, TITAN_PORT, TITAN_GRAPH))
neo4j_config = NEO_Config(NEO4J_URI)


## EXECUTION
def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)


#def merge_neo4j(g, neo4j=neo4j_config):  # Bulbs
def merge_neo4j(g, neo4j="http://localhost:7474/db/data/"):  # Neo4j
    """

    :param g: networkx graph to be merged
    :param neo4j: bulbs neo4j config
    :return: Nonetype

    Note: Neo4j operates differently from the current titan import.  The neo4j import does not aggregate edges which
           means they must be handled at query time.  The current titan algorithm aggregates edges based on time on
           merge.
    """
    #neo4j_graph = NEO_Graph(neo4j)  # Bulbs
    neo_graph = py2neoGraph(neo4j)
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
    tx.commit()
    #print edges  # Debug
#    neo_graph.create(*edges)


def merge_titandb(g, titan=titan_config):
    """

    :param g: graph to be merged
    :param titan: reference to titan database
    :return: Nonetype

    NOTE: Merge occurs on node name rather than attributes
    NOTE: Merge iterates through edges, finds the edge's nodes, looks for the edge & creates if it doesn't exist.
           Any nodes without edges are iterated through and created if they do not already exist.
    """
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


def cymru_enrichment(ips, start_time = ""):
    """ From https://gist.github.com/zakird/11196064

    :param ips: list of IP addresses to enrich in the graph
    :param start_time: a default start time to use in %Y-%m-%dT%H:%M:%SZ format
    :return: subgraph
    """
    # Since sometimes I just pass in an IP, we'll fix it here.
    if type(ips) == str:
        ips = [ips]

    # Validate IP
    for ip in ips:
        _ = ipaddress.ip_address(unicode(ip))

    g = nx.MultiDiGraph()

    # Create cymru ASN enrichment node
    cymru_asn_uri = "class=attribute&key={0}&value={1}".format("enrichment", "cymru_asn_enrichment")
    attributes = {
        'class': 'attribute',
        'key': 'enrichment',
        "value": "cymru_asn_enrichment",
        'uri': cymru_asn_uri,
        'start_time': start_time
    }
    g.add_node(cymru_asn_uri, attributes)

#    print ips

    a = CymruIPtoASNService()

    for result in a.query(ips):
        try:
            t = datetime.strptime(result.allocated_at, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            t = ''
        # Create ip's node
        ip_uri = "class=attribute&key={0}&value={1}".format("ip", result.ip_address)
        g.add_node(ip_uri, {
            'class': 'attribute',
            'key': "ip",
            "value": result.ip_address,
            "start_time": start_time,
            "uri": ip_uri
        })

        # link to cymru ASN enrichment
        edge_attr = {
            "relationship": "describedBy",
            "origin": "cymru_asn_enrichment",
            "start_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, ip_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, cymru_asn_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(ip_uri, cymru_asn_uri, edge_uri, edge_attr)


        # Create bgp prefix node
        bgp_uri = "class=attribute&key={0}&value={1}".format("bgp", result.bgp_prefix)
        attributes = {
            'class': 'attribute',
            'key': 'bgp',
            'value': result.bgp_prefix,
            'uri': bgp_uri,
            'start_time': start_time
        }
        g.add_node(bgp_uri, attributes)

        # Link bgp prefix node to ip
        edge_attr = {
            "relationship": "describedBy",
            "origin": "cymru_asn_enrichment",
            "start_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, ip_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, bgp_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(ip_uri, bgp_uri, edge_uri, edge_attr)


        # create asn node
        asn_uri = "class=attribute&key={0}&value={1}".format("asn", result.as_number)
        attributes = {
            'class': 'attribute',
            'key': 'asn',
            'value': result.as_number,
            'uri': asn_uri,
            'start_time': start_time
        }
        try:
            attributes['owner'] = result.as_name
        except:
            pass
        g.add_node(asn_uri, attributes)

        # link bgp prefix to asn node
        edge_attr = {
            "relationship": "describedBy",
            "origin": "cymru_asn_enrichment",
            "start_time": t,
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, ip_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, asn_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(ip_uri, asn_uri, edge_uri, edge_attr)


    # Return the data enriched IP as a graph
    return g


def maxmind_enrichment(ip, start_time = "", dat_file = MAXMIND_FILE):
    """

    :param ip: IP address to enrich in graph
    :param start_time: a default start time to use in %Y-%m-%dT%H:%M:%SZ format
    :return: enrichment graph
    """
    # Validate IP
    _ = ipaddress.ip_address(unicode(ip))

    # open maxmind ASN data
    try:
        gi = GeoIP.open(dat_file, GeoIP.GEOIP_STANDARD)
    except:
        logging.error("Please specify a valid dat file.")
        raise

    g = nx.MultiDiGraph()
    # Create the maxmind ASN node
    maxmind_asn_uri = "class=attribute&key={0}&value={1}".format("enrichment", "maxmind_asn")  # Move prefix assignment to merge_titan
    g.add_node(maxmind_asn_uri, {
        'class': 'attribute',
        'key': "enrichment",
        "value": "maxmind_asn",
        "start_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "uri": maxmind_asn_uri
    })

    # set IP URI
    ip_uri = "class=attribute&key={0}&value={1}".format("ip", ip)
    g.add_node(ip_uri, {
        'class': 'attribute',
        'key': "ip",
        "value": ip,
        "start_time": start_time,
        "uri": ip_uri
    })

    # retrieve maxmind enrichment
    ASN = gi.name_by_addr(ip)
    if ASN:
        ASN = ASN.split(" ", 1)

        # create ASN node
        asn_uri = "class=attribute&key={0}&value={1}".format("asn", ASN[0][2:])
        attributes = {
            'class': 'attribute',
            'key': 'asn',
            'value': ASN[0][2:],
            "uri": asn_uri,
            "start_time": ""
        }
        if len(ASN) > 1:
            attributes['owner'] = ASN[1]
        g.add_node(asn_uri, attributes)

        # link ip to ASN node
        edge_attr = {
            "relationship": "describedBy",
            "origin": "maxmind_enrichment",
            "start_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, ip_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, asn_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(ip_uri, asn_uri, edge_uri, edge_attr)


        # link ip to maxmind enrichment
        edge_attr = {
            "relationship": "describedBy",
            "origin": "maxmind_enrichment",
            "start_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, ip_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, maxmind_asn_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(ip_uri, maxmind_asn_uri, edge_uri, edge_attr)


    else:
        logging.debug("Maxmind miss on {0}".format(ip))

    # Reuturn the data enriched graph
    return g


def whois_record(record, start_time=""):
    """

    :param record: Takes a domain name as a list: [row,Date,Domain,Reg_name,Reg_org,Reg_addr,Reg_city,Reg_state,Reg_country,Reg_email]
    :param start_time: A default start time
    :return: a networkx graph representing the response.  (All fields captured.)
    """
    # Create the graph
    g = nx.MultiDiGraph()

    # try and validate the record
    if type(record) == list and \
       len(record) == 10:
        pass
    else:
        raise ValueError("Record not in correct format.")
    try:
        _ = datetime.strptime(record[1], "%Y-%m-%d")
    except:
        raise ValueError("Record date in wrong format.")
    try:
        _ = tldextract.extract(record[2])
    except:
        raise ValueError("Record domain is not valid.")
    if type(record[3]) in (int, str, None) and \
        type(record[4]) in (int, str, None) and \
        type(record[5]) in (int, str, None) and \
        type(record[6]) in (int, str, None) and \
        type(record[7]) in (int, str, None) and \
        type(record[8]) in (int, str, None) and \
        type(record[9]) in (int, str, None):
        pass
    else:
        raise ValueError("Record contains incompatible types.")



    # Get or create Domain node
    domain_uri = "class=attribute&key={0}&value={1}".format("domain", record[2])
    g.add_node(domain_uri, {
        'class': 'attribute',
        'key': "domain",
        "value": record[2],
        "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "uri": domain_uri
    })

    # If 'no parser', there's no data, just return just the domain node
    if 'No Parser' in record:
        return g

    # Get or create Enrichment node
    whois_record_uri = "class=attribute&key={0}&value={1}".format("enrichment", "whois_record")
    g.add_node(whois_record_uri, {
        'class': 'attribute',
        'key': "enrichment",
        "value": "whois_record",
        "start_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "uri": whois_record_uri
    })

    if record[3] and record[3].lower() != 'none':
        # Registrant Name node
        name_uri = "class=attribute&key={0}&value={1}".format("name", record[3].encode("ascii", "ignore"))
        g.add_node(name_uri, {
            'class': 'attribute',
            'key': "name",
            "value": record[3],
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "uri": name_uri
        })

        # Registrant Name Edge
        edge_attr = {
            "relationship": "describedBy",
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "describeBy": "registrant_name",
            "origin": "whois_record_enrichment"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, name_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, name_uri, edge_uri, edge_attr)

    if record[4] and record[4].lower() != 'none':
        # Registrant Organization Node
        reg_org_uri = "class=attribute&key={0}&value={1}".format("organization", record[4].encode("ascii", "ignore"))
        g.add_node(reg_org_uri, {
            'class': 'attribute',
            'key': "organization",
            "value": record[4],
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "uri": reg_org_uri
        })

        # Registrant Organization Edge
        edge_attr = {
            "relationship": "describedBy",
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "describeBy": "registrant_organization",
            "origin": "whois_record_enrichment"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, reg_org_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, reg_org_uri, edge_uri, edge_attr)

    if record[5] and record[5].lower() != 'none':
        # Registrant Organization Address Node
        reg_org_addr_uri = "class=attribute&key={0}&value={1}".format("address", record[5].encode("ascii", "ignore"))
        g.add_node(reg_org_addr_uri, {
            'class': 'attribute',
            'key': "address",
            "value": record[5],
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "uri": reg_org_addr_uri
        })

        # Registrant Organization Address Edge
        edge_attr = {
            "relationship": "describedBy",
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "describeBy": "registrant_organization_address",
            "origin": "whois_record_enrichment"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, reg_org_addr_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, reg_org_addr_uri, edge_uri, edge_attr)

    if record[6] and record[6].lower() != 'none':
        # Registrant Organization City Node
        reg_org_city_uri = "class=attribute&key={0}&value={1}".format("city", record[6].encode("ascii", "ignore").lower())
        g.add_node(reg_org_city_uri, {
            'class': 'attribute',
            'key': "city",
            "value": record[6].lower(),
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "uri": reg_org_city_uri
        })

        # Registrant Organization City Edge
        edge_attr = {
            "relationship": "describedBy",
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "describeBy": "registrant_organization_city",
            "origin": "whois_record_enrichment"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, reg_org_city_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, reg_org_city_uri, edge_uri, edge_attr)

    if record[7] and record[7].lower() != 'none':
        # Check for state abbreviation
        if len(record[7]) == 2 and record[7] in STATES:
            state = STATES[record[7]]
        else:
            state = record[7]
        # Registrant Organization State Node
        reg_org_state_uri = "class=attribute&key={0}&value={1}".format("state", state.encode("ascii", "ignore").lower())
        g.add_node(reg_org_state_uri, {
            'class': 'attribute',
            'key': "state",
            "value": state.lower(),
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "uri": reg_org_state_uri
        })

        # Registrant Organization State Edge
        edge_attr = {
            "relationship": "describedBy",
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "describeBy": "registrant_organization_state",
            "origin": "whois_record_enrichment"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, reg_org_state_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, reg_org_state_uri, edge_uri, edge_attr)

    if record[8] and record[8].lower() != 'none':
    # Registrant Organization Country Node
        reg_org_country_uri = "class=attribute&key={0}&value={1}".format("country", record[8].encode("ascii", "ignore").lower())
        g.add_node(reg_org_country_uri, {
            'class': 'attribute',
            'key': "country",
            "value": record[8].lower(),
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "uri": reg_org_country_uri
        })

        # Registrant Organization Country Edge
        edge_attr = {
            "relationship": "describedBy",
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "describeBy": "registrant_organization_country",
            "origin": "whois_record_enrichment"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, reg_org_country_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, reg_org_country_uri, edge_uri, edge_attr)

    if record[9] and record[9].lower() != 'none':
        # Registrant Organization email Node
        reg_org_email_uri = "class=attribute&key={0}&value={1}".format("email_address", record[9].encode("ascii", "ignore"))
        g.add_node(reg_org_email_uri, {
            'class': 'attribute',
            'key': "email_address",
            "value": record[9],
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "uri": reg_org_email_uri
        })

        # Registrant Organization email Edge
        edge_attr = {
            "relationship": "describedBy",
            "start_time": datetime.strptime(record[1], "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "describeBy": "registrant_email",
            "origin": "whois_record_enrichment"
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, reg_org_email_uri)
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(domain_uri, reg_org_email_uri, edge_uri, edge_attr)

    # Enrichment Edge
    edge_attr = {
        "relationship": "describedBy",
        "start_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "origin": "whois_record_enrichment"
    }
    source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
    dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, whois_record_uri)
    edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
    rel_chain = "relationship"
    while rel_chain in edge_attr:
        edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
        rel_chain = edge_attr[rel_chain]
    if "origin" in edge_attr:
        edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
    edge_attr["uri"] = edge_uri
    g.add_edge(domain_uri, whois_record_uri, edge_uri, edge_attr)

    return g


def dns_enrichment(domain):
    """

    :param domain: a string containing a domain to lookup up
    :return: a networkx graph representing the response.
    """
    ip = socket.gethostbyname(domain)
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
    dns_uri = "class=attribute&key={0}&value={1}".format("enrichment", "dns")
    g.add_node(dns_uri, {
        'class': 'attribute',
        'key': "enrichment",
        "value": "dns",
        "start_time": now,
        "uri": dns_uri
    })

    ip_uri = "class=attribute&key={0}&value={1}".format("ip", ip)
    g.add_node(ip_uri, {
        'class': 'attribute',
        'key': "ip",
        "value": ip,
        "start_time": now,
        "uri": ip_uri
    })

    # Create edge from domain to ip node
    edge_attr = {
        "relationship": "describedBy",
        "start_time": now,
        "origin": "dns"
    }
    source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
    dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, ip_uri)
    edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
    rel_chain = "relationship"
    while rel_chain in edge_attr:
        edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
        rel_chain = edge_attr[rel_chain]
    if "origin" in edge_attr:
        edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
    edge_attr["uri"] = edge_uri
    g.add_edge(domain_uri, ip_uri, edge_uri, {"start_time": now})

    # Link domain to enrichment
    edge_attr = {
        "relationship": "describedBy",
        "start_time": now,
        "origin": "dns"
    }
    source_hash = uuid.uuid3(uuid.NAMESPACE_URL, domain_uri)
    dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, dns_uri)
    edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
    rel_chain = "relationship"
    while rel_chain in edge_attr:
        edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
        rel_chain = edge_attr[rel_chain]
    if "origin" in edge_attr:
        edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
    edge_attr["uri"] = edge_uri
    g.add_edge(domain_uri, dns_uri, edge_uri, edge_attr)

    return g


def tld_enrichment(domain, include_subdomain=False):
    """

    :param domain: a string containing a domain to look up
    :param include_subdomain: Boolean value.  Default False.  If true, subdomain will be returned in enrichment graph
    :return: a networkx graph representing the sections of the domain
    """

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
        "value": ext.suffice,
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