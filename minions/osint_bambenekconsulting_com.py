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
PLUGIN_CONFIG_FILE = "osint_bambenekconsulting_com.yapsy-plugin"
NAME = "OSINT Bambenek Consulting"
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
    yesterday = pd.DataFrame(columns=("indicator", "context", "date", "source", "key", "threat"))  # Yesterday's data
    today = None  # Today's date
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


    #  CHANGEME: The correct type of execution function must be defined for the type of plugin
    #  CHANGEME: enrichment: "run(<thing to enrich>, inputs, start_time, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  CHANGEME: interface: enrich(graph, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  CHANGEME:            query(topic, max_depth, config, dont_follow, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  CHANGEME: score: score(subgraph, topic, any other plugin-specific attributes-MUST HAVE DEFAULTS)
    #  CHANGEME: minion [TBD] 
    #  CHANGEME: Enrichment plugin specifics:
    #  -     Created nodes/edges must follow http://blog.infosecanalytics.com/2014/11/cyber-attack-graph-schema-cags-20.html
    #  -     The enrichment should include a node for the <thing to enrich>
    #  -     The enrichment should include a node for the enrichment which is is statically defined & key of "enrichment"
    #  -     An edge should exist from <thing to enrich> to the enrichment node, created at the end after enrichment
    #  -     Each enrichment datum should have a node
    #  -     An edge should exist from <thing to enrich> to each enrichment datum
    #  -     The run function should then return a networkx directed multi-graph including the nodes and edges
    #  CHANGEME: Interface plugin specifics:
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
    #  CHANGEME: Score plugin specifics:
    #  -     Scoring plugins should take a topic and networkx (sub)graph and return a dictionary keyed with the node (name) and with
    #  -     values of the score assigned to the node for the given topic.
    #  CHANGEME: Minion plugin specifics:
    #  -     [TBD]
    def minion(self,  storage=None, *args, **xargs):
        self.app = self.Verum.app(self.parent.PluginFolder, None)
        # set storage
        if storage is None:
            storage = self.parent.storage
        self.app.set_interface(storage)

        # Check until stopped
        while not self.shutdown:
            # Check to see if it's the same day, if it is, sleep for a while, otherwise run the import
            if datetime.utcnow() == self.today:
                time.sleep(SLEEP_TIME)
            else:
                # Get the file
                r = requests.get(FEED)

                # split it out
                feed = r.text.split("\n")

                df = pd.DataFrame(columns=("indicator", "context", "date", "source"))
                # load the feed into a dataframe line, by, line.  I know it's slow.  
                for line in feed:
                    if line and line[0] != "#":
                        df.loc[df.shape[0]] = line.split(",")

                # Index([u'indicator', u'context', u'date', u'source', u'key', u'threat'], dtype='object')
                df = pd.concat([df, pd.DataFrame(df.context.str.split(' used by ',1).tolist(), columns = ['key','threat'])], axis=1)

                # Create list of IPs for cymru enrichment
                ips = set()

                for row in df.iterrows():
                    # Don't add it if it was added yesterday w/ same origination date
                    if not ((self.yesterday['indicator'] == row[1]['indicator']) & 
                            (self.yesterday['date'] == row[1]['date']) & 
                            (self.yesterday['key'] == row[1]['key']) & 
                            (self.yesterday['threat'] == row[1]['threat'])).any():

                        g = nx.MultiDiGraph()
                        
                        # convert date to correct format
                        dt = dateutil.parser.parse(row[1]['date']).strftime("%Y-%m-%dT%H:%M:%SZ")

                        # Add indicator to graph
                        ## (Must account for the different types of indicators)
                        key = keys[row[1]['key']]
                        target_uri = "class=attribute&key={0}&value={1}".format(key, row[1]['indicator']) 
                        g.add_node(target_uri, {
                            'class': 'attribute',
                            'key': key,
                            "value": row[1]['indicator'],
                            "start_time": dt,
                            "uri": target_uri
                        })

                        # Add threat to list
                        if row[1]['threat'][-4:] == u' C&C':
                            CandC = True
                            threat = row[1]['threat'][:-4]
                        else:
                            CandC = False
                            threat = row[1]['threat']

                        # Threat node
                        threat_uri = "class=attribute&key={0}&value={1}".format("malware", threat) 
                        g.add_node(threat_uri, {
                            'class': 'attribute',
                            'key': "malware",
                            "value": threat,
                            "start_time": dt,
                            "uri": threat_uri
                        })

                        # Threat Edge
                        edge_attr = {
                            "relationship": "describedBy",
                            "origin": row[1]['source'],
                            "start_time": dt,
                        }
                        # test for nameserver and update edge_attr
                        if nameserver[row[1]['key']] == True:
                            edge_attr['describedBy'] = 'nameserver'
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

                        # Add C&C to list if applicable
                        if CandC:
                            # C2 node
                            c2_uri = "class=attribute&key={0}&value={1}".format("classification", "c2") 
                            g.add_node(c2_uri, {
                                'class': 'attribute',
                                'key': "classification",
                                "value": "c2",
                                "start_time": dt,
                                "uri": c2_uri
                            })

                            # C2 Edge
                            edge_attr = {
                                "relationship": "describedBy",
                                "origin": row[1]['source'],
                                "start_time": dt,
                            }
                            # test for nameserver and update edge_attr
                            if nameserver[row[1]['key']] == True:
                                edge_attr['describedBy'] = 'nameserver'
                            source_hash = uuid.uuid3(uuid.NAMESPACE_URL, target_uri)
                            dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, c2_uri)
                            edge_uri = "source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
                            rel_chain = "relationship"
                            while rel_chain in edge_attr:
                                edge_uri = edge_uri + "&{0}={1}".format(rel_chain,edge_attr[rel_chain])
                                rel_chain = edge_attr[rel_chain]
                            if "origin" in edge_attr:
                                edge_uri += "&{0}={1}".format("origin", edge_attr["origin"])
                            edge_attr["uri"] = edge_uri
                            g.add_edge(target_uri, c2_uri, edge_uri, edge_attr)


                        # classify malicious and merge with current graph
                        g = self.Verum.merge_graphs(g, self.app.classify.run({'key': key, 'value': row[1]['indicator'], 'classification': 'malice'}))

                        # enrich depending on type
                        try:
                            g = self.Verum.merge_graphs(g, self.app.run_enrichments(row[1]['indicator'], key, names=[u'DNS Enrichment', u'TLD Enrichment', u'Maxmind ASN Enrichment', 'IP Whois Enrichment']))
                        except:
                            logging.info("Enrichment of {0} failed.".format(row[1]['indicator']))
                            pass
                            
                        # add to ip list if appropriate
                        if key == "ip":
                            try:
                                _ = ipaddress.ip_address(unicode(row[1]['indicator']))
                                ips.add(self.app.run_enrichments(row[1]['indicator']))
                            except:
                                pass

                        #print g.nodes(data=True)  # DEBUG
                        #print g.edges(data=True)  # DEBUG

                        self.app.store_graph(g)

                # Do cymru enrichment
                self.app.store_graph(self.app.run_enrichments(ips, 'ip', names=[u'Cymru Enrichment']))
                
                # Copy today's data to yesterday
                self.yesterday = df

                # Copy today's date to today
                self.today = datetime.utcnow()


    def start(self, *args, **xargs):
        self.thread = threading.Thread(target=self.minion, *args, **xargs)
        self.thread.start()

    def isAlive(self):
        return self.thread.isAlive()


    def stop(self, force=True):
        if force:
            self.thread = None  # zero out thread
        else:
            self.shutdown = False  # just dont' iterate.  May take up to (SLEEP_TIME) hours


"""
        #Some Threading stuff
        # http://stackoverflow.com/questions/7168508/background-function-in-python
        
        Do something like this:

        def function_that_downloads(my_args):
            # do some long download here
        then inline, do something like this:

        import threading
        def my_inline_function(some_args):
            #do some stuff
            download_thread = threading.Thread(target=function_that_downloads, args=my_args)
            download_thread.start()
            #continue doing stuff
        You may want to check if the thread has finished before going on to other things by calling download_thread.isAlive()

"""