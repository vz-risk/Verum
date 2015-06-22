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
TLD_CONFIG_FILE = "tld.yapsy-plugin"
NAME = "TLD Enrichment"


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
__author__ = "Gabriel Bassett"
loc = inspect.getfile(inspect.currentframe())
ind = loc.rfind("/")
loc = loc[:ind+1]
config = ConfigParser.SafeConfigParser()
config.readfp(open(loc + TLD_CONFIG_FILE))

if config.has_section('Core'):
    if 'name' in config.options('Core'):
        NAME = config.get('Core', 'name')

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

            if 'cost' in config_options:
                cost = config.get('Configuration', 'cost')
            else:
                cost = 9999
            if 'speed' in config_options:
                speed = config.get('Configuration', 'speed')
            else:
                speed = 9999

            if 'type' in config_options:
                plugin_type = config.get('Configuration', 'Type')
            else:
                logging.error("'Type' not specified in config file.")
                return [None, False, NAME, "Takes a domain name and returns the top level domain, mid-domain, and sub-domain as networkx graph.", None, cost, speed]

            if 'inputs' in config_options:
                inputs = config.get('Configuration', 'Inputs')
                inputs = [l.strip().lower() for l in inputs.split(",")]
            else:
                logging.error("No input types specified in config file.")
                return [plugin_type, False, NAME, "Takes a domain name and returns the top level domain, mid-domain, and sub-domain as networkx graph.", None, cost, speed]

            if not module_import_success:
                logging.error("Module import failure caused configuration failure.")
                return [plugin_type, False, NAME, "Takes a domain name and returns the top level domain, mid-domain, and sub-domain as networkx graph.", inputs, cost, speed]
            else:
                return [plugin_type, True, NAME, "Takes a domain name and returns the top level domain, mid-domain, and sub-domain as networkx graph.", inputs, cost, speed]


        def run(self, domain, inputs=None, start_time="", include_subdomain=False):
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