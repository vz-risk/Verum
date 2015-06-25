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
PLUGIN_CONFIG_FILE = "generic.yapsy-plugin"
NAME = "generic"


########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
import networkx as nx
from datetime import datetime # timedelta imported above
import dateutil  # to parse variable time strings
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

        return [plugin_type, True, NAME, description, self.inputs, cost, speed]


    def run(self, enrichment_dict, start_time="", confidence=1):
        """ dict, str -> networkx multiDiGraph

        :param enrichment_dict: a dictionary of the form {'key': <key of atomic to describe>, 'value':<value of atomic to describe>, 'describing_key':<key of describing atomic>, 'describing_value':<value of describing atomic>}
        :param start_time: string in ISO 8601 combined date and time format (e.g. 2014-11-01T10:34Z) or datetime object.
        :param include_subdomain: Boolean value.  Default False.  If true, subdomain will be returned in enrichment graph
        :return: a networkx graph representing the sections of the domain
        """
        described_key = enrichment_dict['key']
        described_value = enrichment_dict['value']
        describing_key = enrichment_dict['describing_key']
        describing_value = enrichment_dict['describing_value']

        g = nx.MultiDiGraph()

        if type(start_time) is str:
            try:
                time = dateutil.parser.parse(start_time).strftime("%Y-%m-%dT%H:%M:%SZ")
            except:
                time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        elif type(start_time) is datetime:
            time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Get or create target node
        described_uri = "class=attribute&key={0}&value={1}".format(described_key, described_value)
        g.add_node(described_uri, {
            'class': 'attribute',
            'key': described_key,
            "value": described_value,
            "start_time": time,
            "uri": described_uri
        })

        # Get or create classification node
        describing_uri = "class=attribute&key={0}&value={1}".format(describing_key, describing_value)
        g.add_node(describing_uri , {
            'class': 'attribute',
            'key': describing_key,
            "value": describing_value,
            "start_time": time,
            "uri": describing_uri 
        })


        # Link target to classification
        edge_attr = {
            "relationship": "describedBy",
            "start_time": time,
            "origin": "generic",
            "confidence": confidence
        }
        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, described_uri)
        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, describing_uri )
        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
        rel_chain = "relationship"
        while rel_chain in edge_attr:
            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
            rel_chain = edge_attr[rel_chain]
        if "origin" in edge_attr:
            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
        edge_attr["uri"] = edge_uri
        g.add_edge(described_uri, describing_uri , edge_uri, edge_attr)

        return g