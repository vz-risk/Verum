# TODO: Refactor as plugin
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
# TODO: use 'imp' to import this
from cymru_api import CymruIPtoASNService
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
