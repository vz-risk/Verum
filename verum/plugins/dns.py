#!/usr/bin/env python
"""
 AUTHOR: Gabriel Bassett
 DATE: 11-22-2014
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
pass

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
DNS_CONFIG_FILE = "./dns.yapsy-plugin"


########### NOT USER EDITABLE BELOW THIS POINT #################

## IMPORTS
from yapsy.IPlugin import IPlugin
import networkx as nx
from datetime import datetime
import socket
import uuid
import ConfigParser
import logging

## SETUP
__author__ = "Gabriel Bassett"
config = ConfigParser.SafeConfigParser()
config.readfp(open(DNS_CONFIG_FILE))

## EXECUTION
class PluginOne(IPlugin):
    def __init__(self):
        pass

    def configure(self):
        """

        :return: return list of [configure success (bool), name, description, list of acceptable inputs, resource cost (1-10, 1=low), speed (1-10, 1=fast)]
        """
        config_options = config.options("Configuration")

        if 'Cost' in config_options:
            cost = config.get('Configuration', 'cost')
        else:
            cost = 9999
        if 'Speed' in config_options:
            speed = config.get('Configuration', 'speed')
        else:
            speed = 9999

        if 'Type' in config_options:
            type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [False, 'whois', "Takes a whois record as a list of strings in a specific format and returns a networkx graph of the information.", None, cost, speed, None]

        if 'Inputs' in config_options:
            inputs = config.get('Configuration', 'Inputs')
            inputs = inputs.split(",").strip().lower()
        else:
            logging.error("No input types specified in config file.")
            return [False, 'dns', "Takes an IP string and returns the DNS resolved IP address as networkx graph.", None, cost, speed, type]

        return [True, "dns", "Takes an IP string and returns the DNS resolved IP address as networkx graph.", inputs, cost, speed, type]


    def run(self, domain):
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
