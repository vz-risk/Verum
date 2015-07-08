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
PLUGIN_CONFIG_FILE = "osint_bambenekconsulting_com_v2.yapsy-plugin"
NAME = "OSINT Bambenek Consulting V2"
keys = {u'IP': "ip", u'Domain': "domain", u'Nameserver IP': "ip", u'Nameserver': "domain"}
nameserver = {u'IP': False, u'Domain': False, u'Nameserver IP': True, u'Nameserver': True}
FEED = "http://osint.bambenekconsulting.com/feeds/c2-masterlist.txt"
SLEEP_TIME = 14400  # 4 hours in seconds

########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
from yapsy.IPlugin import IPlugin
import logging
import networkx as nx
from datetime import datetime # timedelta imported above
import dateutil
import uuid
import ConfigParser
import inspect
import pandas as pd  # for organizing the intel list data
import requests  # for downloading the intel list
import ipaddress  # for validating ip addresses
import time  # for sleep
import threading  # import threading so minion doesn't block the app
import imp  # Importing imp to import verum

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
    thread = None
    app = None  # The object instance
    Verum = None  # the module
    today = datetime.strptime("1970", "%Y")  # Today's date
    shutdown = False  # Used to trigger shutdown of hte minion
    parent = None  # The parent instance of the verum app object

    #  CHANGEME: The init should contain anything to load modules or data files that should be variables of the  plugin object
    def __init__(self):
        """

        """
        pass

    #  CHANGEME: Configuration needs to set the values needed to identify the plugin in the plugin database as well as ensure everyhing loaded correctly
    #  CHANGEME: Current  layout is for an enrichment plugin
    #  CHANGEME: enrichment [type, successful_load, name, description, inputs to enrichment such as 'ip', cost, speed]
    #  CHANGEME: interface [type, successful_load, name]
    #  CHANGEME: score [type, successful_load, name, description, cost, speed]
    #  CHANGEME: minion [TBD]
    def configure(self, parent=None):
        """

        :param verum: The directory of the verum module 
        :return: return list of [configure success (bool), name, description, list of acceptable inputs, resource cost (1-10, 1=low), speed (1-10, 1=fast)]
        """
        global FEED

        config_options = config.options("Configuration")

        if 'cost' in config_options:
            cost = config.get('Configuration', 'cost')
        else:
            cost = 9999

        if config.has_section('Documentation') and 'description' in config.options('Documentation'):
            description = config.get('Documentation', 'description')
        else:
            logging.error("'Description not in config file.")
            return [None, False, NAME, None, cost]

        if 'type' in config_options:
            plugin_type = config.get('Configuration', 'type')
        else:
            logging.error("'Type' not specified in config file.")
            return [None, False, NAME, description, cost]

        #  Module import success
        if parent is not None:
            self.parent = parent
        else:
            logging.info("Parent verum app instance not passed to minion.  Please rerun, passing the parent object instance to successfully configure.")
            return [plugin_type, False, NAME, description, cost]

        if self.parent.loc is not None:
            # Import the app object so that acces app features (such as the storage backend) can be used.
            fp, pathname, mod_description = imp.find_module("verum", [self.parent.loc])
            self.Verum = imp.load_module("verum", fp, pathname, mod_description)
        else:
            logging.error("'verum' location not supplied to minion configuration function.  Rerun with the location of the verum module specified.")
            return [plugin_type, False, NAME, description, cost]

        if 'feed' in config_options:
            FEED = config.get('Configuration', 'feed')
        else:
            logging.error("'Feed' not specified in config file.")
            return [plugin_type, False, NAME, description, cost]

        # Return success
        return [plugin_type, True, NAME, description, cost]


    def minion(self,  storage=None, *args, **xargs):
        self.app = self.Verum.app(self.parent.PluginFolder, None)
        # set storage
        if storage is None:
            storage = self.parent.storage
        self.app.set_interface(storage)

        # Check until stopped
        while not self.shutdown:
            # Check to see if it's the same day, if it is, sleep for a while, otherwise run the import
            delta = datetime.utcnow() - self.today
            if delta.days <= 0:
                time.sleep(SLEEP_TIME)
            else:
                # Get the file
                r = requests.get(FEED)

                # split it out
                feed = r.text.split("\n")

                # Create list of IPs for cymru enrichment
                ips = set()

                for row in feed:
                    # Parse date
                    l = row.find("Feed generated at:")
                    if l > -1:
                        dt = row[l+18:].strip()
                        dt = dateutil.parser.parse(dt).strftime("%Y-%m-%dT%H:%M:%SZ")
                        next
                    row = row.split(",")

                    # if it's a record, parse the record
                    if len(row) == 6:
                        try:
                            # split out sub values
                            # row[0] -> domain
                            row[1] = row[1].split("|")  # ip
                            row[2] = row[2].split("|")  # nameserver domain
                            row[3] = row[3].split("|")  # nameserver ip
                            row[4] = row[4][26:-22]  # malware
                            # row[5] -> source

                            # add the ips to the set of ips
                            ips = ips.union(set(row[1])).union(set(row[3]))

                            g = nx.MultiDiGraph()

                            # Add indicator to graph
                            ## (Must account for the different types of indicators)
                            target_uri = "class=attribute&key={0}&value={1}".format('domain', row[0]) 
                            g.add_node(target_uri, {
                                'class': 'attribute',
                                'key': 'domain',
                                "value": row[0],
                                "start_time": dt,
                                "uri": target_uri
                            })


                            # Threat node
                            threat_uri = "class=attribute&key={0}&value={1}".format("malware", row[4]) 
                            g.add_node(threat_uri, {
                                'class': 'attribute',
                                'key': "malware",
                                "value": row[4],
                                "start_time": dt,
                                "uri": threat_uri
                            })

                            # Threat Edge
                            edge_attr = {
                                "relationship": "describedBy",
                                "origin": row[5],
                                "start_time": dt
                            }
                            source_hash = uuid.uuid3(uuid.NAMESPACE_URL, target_uri)
                            dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, threat_uri)
                            edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
                            rel_chain = "relationship"
                            while rel_chain in edge_attr:
                                edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
                                rel_chain = edge_attr[rel_chain]
                            if "origin" in edge_attr:
                                edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
                            edge_attr["uri"] = edge_uri
                            g.add_edge(target_uri, threat_uri, edge_uri, edge_attr)                        

                            # for each IP associated with the domain, connect it to the target
                            for ip in row[1]:
                                # Create IP node
                                target_ip_uri = "class=attribute&key={0}&value={1}".format("ip", ip) 
                                g.add_node(target_ip_uri, {
                                    'class': 'attribute',
                                    'key': "ip",
                                    "value": ip,
                                    "start_time": dt,
                                    "uri": target_ip_uri
                                })

                                # ip Edge
                                edge_attr = {
                                    "relationship": "describedBy",
                                    "origin": row[5],
                                    "start_time": dt,
                                }
                                source_hash = uuid.uuid3(uuid.NAMESPACE_URL, target_uri)
                                dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, target_ip_uri)
                                edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
                                rel_chain = "relationship"
                                while rel_chain in edge_attr:
                                    edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
                                    rel_chain = edge_attr[rel_chain]
                                if "origin" in edge_attr:
                                    edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
                                edge_attr["uri"] = edge_uri
                                g.add_edge(target_uri, target_ip_uri, edge_uri, edge_attr)


                            for nameserver in row[2]:
                                # Create nameserver node
                                ns_uri = "class=attribute&key={0}&value={1}".format("domain", nameserver) 
                                g.add_node(ns_uri, {
                                    'class': 'attribute',
                                    'key': "domain",
                                    "value": nameserver,
                                    "start_time": dt,
                                    "uri": ns_uri
                                })

                                # nameserver Edge
                                edge_attr = {
                                    "relationship": "describedBy",
                                    "origin": row[5],
                                    "start_time": dt,
                                    'describedBy': 'nameserver'
                                }
                                source_hash = uuid.uuid3(uuid.NAMESPACE_URL, target_uri)
                                dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, target_ip_uri)
                                edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
                                rel_chain = "relationship"
                                while rel_chain in edge_attr:
                                    edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
                                    rel_chain = edge_attr[rel_chain]
                                if "origin" in edge_attr:
                                    edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
                                edge_attr["uri"] = edge_uri
                                g.add_edge(target_uri, ns_uri, edge_uri, edge_attr)

                            # if the number of NS IPs is a multiple of the # of NS's, we'll aassume each NS gets some of the ips
                            if len(row[3]) % len(row[2]) == 0:
                                for i in range(len(row[2])):
                                    for j in range(len(row[3])/len(row[2])):
                                        # Create NS IP node
                                        ns_ip_uri = "class=attribute&key={0}&value={1}".format("ip", row[3][i*len(row[3])/len(row[2]) + j]) 
                                        g.add_node(ns_ip_uri, {
                                            'class': 'attribute',
                                            'key': "ip",
                                            "value": ip,
                                            "start_time": dt,
                                            "uri": ns_ip_uri
                                        })

                                        # create NS uri
                                        ns_uri = "class=attribute&key={0}&value={1}".format("domain", row[2][i]) 


                                        # link NS to IP
                                        edge_attr = {
                                            "relationship": "describedBy",
                                            "origin": row[5],
                                            "start_time": dt
                                        }
                                        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, ns_uri)
                                        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, ns_ip_uri)
                                        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
                                        rel_chain = "relationship"
                                        while rel_chain in edge_attr:
                                            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
                                            rel_chain = edge_attr[rel_chain]
                                        if "origin" in edge_attr:
                                            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
                                        edge_attr["uri"] = edge_uri
                                        g.add_edge(ns_uri, ns_ip_uri, edge_uri, edge_attr)


                            # otherwise we'll attach each IP to each NS
                            else:
                                for ip in row[3]:
                                    # Create NS IP node
                                    ns_ip_uri = "class=attribute&key={0}&value={1}".format("ip", row[3][i*len(row[3])/len(row[2]) + j]) 
                                    g.add_node(ns_ip_uri, {
                                        'class': 'attribute',
                                        'key': "ip",
                                        "value": ip,
                                        "start_time": dt,
                                        "uri": ns_ip_uri
                                    })
                                    
                                    for ns in row[2]:
                                        # create NS uri
                                        ns_uri = "class=attribute&key={0}&value={1}".format("domain", ns)

                                         # link NS to IP
                                        edge_attr = {
                                            "relationship": "describedBy",
                                            "origin": row[5],
                                            "start_time": dt
                                        }
                                        source_hash = uuid.uuid3(uuid.NAMESPACE_URL, ns_uri)
                                        dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, ns_ip_uri)
                                        edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
                                        rel_chain = "relationship"
                                        while rel_chain in edge_attr:
                                            edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
                                            rel_chain = edge_attr[rel_chain]
                                        if "origin" in edge_attr:
                                            edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
                                        edge_attr["uri"] = edge_uri
                                        g.add_edge(ns_uri, ns_ip_uri, edge_uri, edge_attr)

                            # classify malicious and merge with current graph
                            g = self.Verum.merge_graphs(g, self.app.classify.run({'key': 'domain', 'value': row[0], 'classification': 'malice'}))

                            # enrich depending on type
                            for domain in [row[0]] + row[2]:
                                try:
                                    g = self.Verum.merge_graphs(g, self.app.run_enrichments(domain, "domain", names=['TLD Enrichment']))
                                    g = self.Verum.merge_graphs(g, self.app.run_enrichments(domain, "domain", names=['IP Whois Enrichment']))
                                except Exception as e:
                                    #print "Enrichment of {0} failed due to {1}.".format(row[1]['indicator'], e)  # DEBUG
                                    logging.info("Enrichment of {0} failed due to {1}.".format(domain, e))
                                    pass
                            for ip in row[1] + row[3]:
                                try:
                                    g = self.Verum.merge_graphs(g, self.app.run_enrichments(ip, "ip", names=[u'Maxmind ASN Enrichment']))
                                except Exception as e:
                                    #print "Enrichment of {0} failed due to {1}.".format(row[1]['indicator'], e)  # DEBUG
                                    logging.info("Enrichment of {0} failed due to {1}.".format(ip, e))
                                    pass

                            try:
                                self.app.store_graph(self.Verum.remove_non_ascii_from_graph(g))
                            except:
                                print g.nodes(data=True)  # DEBUG
                                print g.edges(data=True)  # DEBUG
                                raise


                            if len(ips) >= 50:
                                # Do cymru enrichment
                                try:
                                    self.app.store_graph(self.app.run_enrichments(ips, 'ip', names=[u'Cymru Enrichment']))
                                except:
                                    logging.info("Cymru enrichment of {0} IPs failed.".format(len(ips)))
                                    pass
                            ips = set()
                        except Exception as e:
                            print row
                            print e

                # Copy today's date to today
                self.today = datetime.utcnow()


    def start(self, *args, **xargs):
        self.thread = threading.Thread(target=self.minion, *args, **xargs)
        self.thread.start()

    def isAlive(self):
        if self.thread is None:
            return False
        else:
            return self.thread.isAlive()


    def stop(self, force=True):
        if force:
            self.thread = None  # zero out thread
        else:
            self.shutdown = False  # just dont' iterate.  May take up to (SLEEP_TIME) hours
