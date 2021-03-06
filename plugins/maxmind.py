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
pass

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
MAXMIND_FILE = "./GeoIPASNum.dat"
MAXMIND_CONFIG_FILE = "maxmind.yapsy-plugin"
NAME = "Maxmind ASN Enrichment"

########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
from datetime import datetime # timedelta imported above
import dateutil  # to parse variable time strings
import uuid
import ConfigParser
import os
import inspect
try:
    import networkx as nx
    import GeoIP
    import ipaddress
    module_import_success = True
except:
    module_import_success = False
    logging.error("Module import failed.  Please install the following modules: networkx, GeoIP, ipaddress.")
    raise

## SETUP
__author__ = "Gabriel Bassett"
loc = inspect.getfile(inspect.currentframe())
ind = loc.rfind("/")
loc = loc[:ind+1]
config = ConfigParser.SafeConfigParser()
config.readfp(open(loc + MAXMIND_CONFIG_FILE))

if config.has_section('Core'):
    if 'name' in config.options('Core'):
        NAME = config.get('Core', 'name')

## EXECUTION
class PluginOne(IPlugin):
    gi = None
    dat_file_success = False

    def __init__(self, conf=config, dat_file=MAXMIND_FILE):
        try:
            maxmind_file = config.get('Configuration', 'dat_file')
            if maxmind_file[0] != "/":
                maxmind_file = loc + maxmind_file
            #print maxmind_file  # DEBUG
            self.gi = GeoIP.open(maxmind_file, GeoIP.GEOIP_STANDARD)
            self.dat_file_success = True
        except:
            pass
        if not self.dat_file_success:
            try:
                if dat_file[0] != "/":
                    dat_file = loc + dat_file
                #print dat_file  # DEBUG
                self.gi = GeoIP.open(dat_file, GeoIP.GEOIP_STANDARD)
                self.dat_file_success = True
            except:
                pass

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

        if 'type' in config_options:
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, False, NAME, "Takes an IP and returns the ASN of the IP.", None, cost, speed]

        if 'inputs' in config_options:
            inputs = config.get('Configuration', 'Inputs')
            inputs = [l.strip().lower() for l in inputs.split(",")]
        else:
            logging.error("No input types specified in config file.")
            return [plugin_type, False, NAME, "Takes an IP and returns the ASN of the IP.", None, cost, speed]

        if not self.dat_file_success:
            return [plugin_type, False, NAME, "Takes an IP and returns the ASN of the IP.", inputs, cost, speed]
        elif not module_import_success:
            logging.error("Module import failure caused configuration failure.")
            return [plugin_type, False, NAME, "Takes an IP and returns the ASN of the IP.", inputs, cost, speed]
        else:
            return [plugin_type, True, NAME, "Takes an IP and returns the ASN of the IP.", inputs, cost, speed]


    def run(self, ip, start_time=""):
        """ str, str -> networkx multiDiGraph

        :param ip: IP address to enrich in graph
        :param start_time: string in ISO 8601 combined date and time format (e.g. 2014-11-01T10:34Z) or datetime object.
        :return: enrichment graph
        """

        # Parse the start_time
        if type(start_time) is str:
            try:
                time = dateutil.parser.parse(start_time).strftime("%Y-%m-%dT%H:%M:%SZ")
            except:
                time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        elif type(start_time) is datetime:
            time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Validate IP
        _ = ipaddress.ip_address(unicode(ip))

        # open maxmind ASN data
        gi = self.gi

        g = nx.MultiDiGraph()
        # Create the maxmind ASN node
        maxmind_asn_uri = "class=attribute&key={0}&value={1}".format("enrichment", "maxmind_asn")  # Move prefix assignment to merge_titan
        g.add_node(maxmind_asn_uri, {
            'class': 'attribute',
            'key': "enrichment",
            "value": "maxmind_asn",
            "start_time": time,
            "uri": maxmind_asn_uri
        })

        # set IP URI
        ip_uri = "class=attribute&key={0}&value={1}".format("ip", ip)
        g.add_node(ip_uri, {
            'class': 'attribute',
            'key': "ip",
            "value": ip,
            "start_time": time,
            "uri": ip_uri
        })

        # retrieve maxmind enrichment
        ASN = gi.name_by_addr(ip)

        #print ASN  # DEBUG
        #print type(gi)  # DEBUG
        #print ip  # DEBUG

        if ASN:
            ASN = ASN.split(" ", 1)

            # create ASN node
            asn_uri = "class=attribute&key={0}&value={1}".format("asn", ASN[0][2:])
            attributes = {
                'class': 'attribute',
                'key': 'asn',
                'value': ASN[0][2:],
                "uri": asn_uri,
                "start_time": time
            }
            if len(ASN) > 1:
                attributes['owner'] = ASN[1]
            g.add_node(asn_uri, attributes)

            # link ip to ASN node
            edge_attr = {
                "relationship": "describedBy",
                "origin": "maxmind_enrichment",
                "start_time": time,
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
                "start_time": time,
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
