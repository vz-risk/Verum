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
CYMRU_CONFIG_FILE = "./cymru.yapsy-plugin"



########### NOT USER EDITABLE BELOW THIS POINT #################



## IMPORTS
import networkx as nx
from yapsy.IPlugin import IPlugin
import logging
import ConfigParser
from datetime import datetime # timedelta imported above
import uuid
import imp
import ipaddress

## SETUP
__author__ = "Gabriel Bassett"
config = ConfigParser.SafeConfigParser()
config.readfp(open(CYMRU_CONFIG_FILE))

if config.has_section('Configuration') and 'Cymru_Module' in config.options('Configuration'):
    cymru_file = config.get('Configuration', 'Cymru_Module')
    cymru_dir = "/".join(cymru_file.split("/")[:-1]) + "/"
    cymru_module = cymru_file.split("/")[-1].strip(".py")
    try:
        fp, pathname, description = imp.find_module(cymru_module, [cymru_dir])
        cymru_api = imp.load_module(cymru_module, fp, pathname, description)
        module_import_success = True
    except:
        module_import_success = False

else:
    module_import_success = False

## EXECUTION

if module_import_success:
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
                return [False, 'cymru', "Takes a list of IPs and returns ASN and BGP information as networkx graph of the information.", None, cost, speed, type]

            if not module_import_success:
                logging.error("Module import failure caused configuration failure.")
                return [False, "cymru", "Takes a list of IPs and returns ASN and BGP information as networkx graph of the information.", inputs, cost, speed, type]
            else:
                return [True, "cymru", "Takes a list of IPs and returns ASN and BGP information as networkx graph of the information.", inputs, cost, speed, type]


        def run(self, ips, start_time = ""):
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

            a = cymru_api.CymruIPtoASNService()

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
