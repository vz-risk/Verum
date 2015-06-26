#!/usr/bin/env python
"""
 AUTHOR: Gabriel Bassett
 DATE: 12-17-2013
 DEPENDENCIES: ipwhois
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
WHOIS_CONFIG_FILE = "ipwhois.yapsy-plugin"
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
NAME = "IP Whois Enrichment"

########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
import ConfigParser
import networkx as nx
from datetime import datetime # timedelta imported above
import dateutil  # to parse variable time strings
import uuid
import inspect
import socket
try:
    from ipwhois import IPWhois
    module_import_success = True
except:
    module_import_success = False
    logging.error("Module import failed.  Please install the following module: ipwhois.")
    raise

## SETUP
__author__ = "Gabriel Bassett"
loc = inspect.getfile(inspect.currentframe())
ind = loc.rfind("/")
loc = loc[:ind+1]
config = ConfigParser.SafeConfigParser()
config.readfp(open(loc + WHOIS_CONFIG_FILE))

if config.has_section('Core'):
    if 'name' in config.options('Core'):
        NAME = config.get('Core', 'name')

## EXECUTION
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
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, False, NAME, "Takes a whois record as a list of strings in a specific format and returns a networkx graph of the information.", None, cost, speed]

        if 'inputs' in config_options:
            inputs = config.get('Configuration', 'Inputs')
            inputs = [l.strip().lower() for l in inputs.split(",")]
        else:
            logging.error("No input types specified in config file.")
            return [plugin_type, False, NAME, "Takes a whois record as a list of strings in a specific format and returns a networkx graph of the information.", None, cost, speed]

        if not module_import_success:
            logging.error("Module import failure caused configuration failure.")
            return [plugin_type, False, NAME, "Takes a whois record as a list of strings in a specific format and returns a networkx graph of the information.", inputs, cost, speed]
        else:
            return [plugin_type, True, NAME, "Takes a whois record as a list of strings in a specific format and returns a networkx graph of the information.", inputs, cost, speed]


    def run(self, domain, start_time=""):
        """ str, str -> networkx multiDiGraph

        :param domain: a string containing a domain to look up
        :param start_time: string in ISO 8601 combined date and time format (e.g. 2014-11-01T10:34Z) or datetime object.
        :return: a networkx graph representing the whois information about the domain
        """
        ip = socket.gethostbyname(domain)  # This has a habit of failing
        record = [None] * 10
        obj = IPWhois(ip)
        results = obj.lookup()

        nets = results.pop("nets")

        for i in range(len(nets)):
            net = nets[i]
            record[0] = i
            if "updated" in net:
                record[1] = net['updated'][:10]
            elif "created" in net:
                record[1] = net['created'][:10]
            record[2] = domain
            if "name" in net:
                record[3] = net['name']
            if "organization" in net:
                record[4] = net['organization']
            if 'address' in net:
                record[5] = net['address']
            if 'city' in net:
                record[6] = net['city']
            if 'state' in net:
                record[7] = net['state']
            if 'country' in net:
                record[8] = net['country']
            if 'misc_emails' in net and net['misc_emails'] is not None:
                emails = net['misc_emails'].split("\n")
                record[9] = emails[0]

            self.enrich_record(record, start_time)


    def enrich_record(self, record, start_time=""):
        """

        :param record: Takes a domain name as a list: [row,Date,Domain,Reg_name,Reg_org,Reg_addr,Reg_city,Reg_state,Reg_country,Reg_email]
        :param start_time: A default start time
        :return: a networkx graph representing the response.  (All fields captured.)
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

        # Create the graph
        g = nx.MultiDiGraph()

        # try and validate the record
        if type(record) == list and \
           len(record) == 10:
            pass
        else:
            raise ValueError("Record not in correct format.")
        try:
            record_time = dateutil.parser.parse(record[1]).strftime("%Y-%m-%dT%H:%M:%SZ")
        except:
            raise ValueError("Record date in wrong format.")
        try:
            _ = tldextract.extract(record[2])
        except:
            raise ValueError("Record domain is not valid.")
        if type(record[3]) in (int, str, type(None)) and \
            type(record[4]) in (int, str, type(None)) and \
            type(record[5]) in (int, str, type(None)) and \
            type(record[6]) in (int, str, type(None)) and \
            type(record[7]) in (int, str, type(None)) and \
            type(record[8]) in (int, str, type(None)) and \
            type(record[9]) in (int, str, type(None)):
            pass
        else:
            raise ValueError("Record contains incompatible types.")



        # Get or create Domain node
        domain_uri = "class=attribute&key={0}&value={1}".format("domain", record[2])
        g.add_node(domain_uri, {
            'class': 'attribute',
            'key': "domain",
            "value": record[2],
            "start_time": record_time,
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
            "start_time": time,
            "uri": whois_record_uri
        })

        if record[3] and record[3].lower() != 'none':
            # Registrant Name node
            name_uri = "class=attribute&key={0}&value={1}".format("name", record[3].encode("ascii", "ignore"))
            g.add_node(name_uri, {
                'class': 'attribute',
                'key': "name",
                "value": record[3],
                "start_time": record_time,
                "uri": name_uri
            })

            # Registrant Name Edge
            edge_attr = {
                "relationship": "describedBy",
                "start_time": record_time,
                "describeBy": "registrant_name",
                "origin": "ipwhois_record_enrichment"
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
                "start_time": record_time,
                "uri": reg_org_uri
            })

            # Registrant Organization Edge
            edge_attr = {
                "relationship": "describedBy",
                "start_time": record_time,
                "describeBy": "registrant_organization",
                "origin": "ipwhois_record_enrichment"
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
                "start_time": record_time,
                "uri": reg_org_addr_uri
            })

            # Registrant Organization Address Edge
            edge_attr = {
                "relationship": "describedBy",
                "start_time": record_time,
                "describeBy": "registrant_organization_address",
                "origin": "ipwhois_record_enrichment"
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
                "start_time": record_time,
                "uri": reg_org_city_uri
            })

            # Registrant Organization City Edge
            edge_attr = {
                "relationship": "describedBy",
                "start_time": record_time,
                "describeBy": "registrant_organization_city",
                "origin": "ipwhois_record_enrichment"
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
                "start_time": record_time,
                "uri": reg_org_state_uri
            })

            # Registrant Organization State Edge
            edge_attr = {
                "relationship": "describedBy",
                "start_time": record_time,
                "describeBy": "registrant_organization_state",
                "origin": "ipwhois_record_enrichment"
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
                "start_time": record_time,
                "uri": reg_org_country_uri
            })

            # Registrant Organization Country Edge
            edge_attr = {
                "relationship": "describedBy",
                "start_time": record_time,
                "describeBy": "registrant_organization_country",
                "origin": "ipwhois_record_enrichment"
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
                "start_time": record_time,
                "uri": reg_org_email_uri
            })

            # Registrant Organization email Edge
            edge_attr = {
                "relationship": "describedBy",
                "start_time": record_time,
                "describeBy": "registrant_email",
                "origin": "ipwhois_record_enrichment"
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
            "start_time": time,
            "origin": "ipwhois_record_enrichment"
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
