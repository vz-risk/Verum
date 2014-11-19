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
 Functions necessary to query the context graph.

"""
# PRE-USER SETUP
pass

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
pass


########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS
import networkx as nx
import argparse
import logging
from datetime import datetime
from bulbs.titan import Graph as TITAN_Graph
from bulbs.titan import Config as TITAN_Config
import copy
import numpy as np
import uuid
import random
from collections import defaultdict
import urlparse
import urllib # It appears urllib2 or urllib3 and requests don't have urlencode
import community
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


## EXECUTION
def validate_uri(uri):
    """

    :param uri: a URI string to be validated
    :return: bool true if valid, false if not
    """
    # TODO: Validate the order properties are in (important for uri hash lookup)

    try:
        properties = urlparse.parse_qs(urlparse.urlparse(uri).query)
    except:
        return False
    if u'key' not in properties:
        return False
    elif len(properties[u'key']) != 1:
        return False
    if u'value' not in properties:
        return False
    elif len(properties[u'value']) != 1:
        return False
    if u'attribute' not in properties:
        return False
    elif len(properties[u'attribute']) != 1:
        return False
    # Nothing failed, return true
    return True


def multigraph_to_digraph(g):
    """

    :param g: takes a networkx mulitgraph
    :return: returns a networkx digraph with edge weights representing the number of edges

    NOTE: This butchers duplicate edge properties.  If converting to score, use original edges in output.
    """
    G = nx.DiGraph()
    edge_attributes = {}

    # if g isn't really a multigraph, just return it
    if not g.is_multigraph():
        return g

    # collapse down to a diagraph
    G.add_nodes_from(g.nodes(data=True))
    G.add_edges_from(g.edges(data=True))

    # for each edge, weight the confidence by the number of edges
    '''
    # captures a multiple of the confidence on the edge in the output graph
    for edge in G.edges():
        count = g.edges().count(edge)
        if "count" > 1:
            if "confidence" in G.edge[edge[0]][edge[1]]:
                G.edge[edge[0]][edge[1]]['confidence'] *= count
            else:
                G.edge[edge[0]][edge[1]]["confidence"] = count
    '''
    # Captures every confidence
    for edge in G.edges():
        confidence = 0
        for src_edge in g.edge[edge[0]][edge[1]].values():
            confidence += src_edge.get('confidence', 1)
        G.edge[edge[0]][edge[1]]['confidence'] = confidence
#    # collapse down to a diagraph
#    G.add_nodes_from(g.nodes(data=True))
#    G.add_edges_from(g.edges(data=True))

    return G


### DISTANCE WEIGHTS ###
def linear_weight(distance, ddp=.2):
    """

    :param distance: distance from topic
    :param ddp: percentage to degrade
    :return: Linear weighting factor as float
    """
    return 1 - (distance * ddp)


def log_weight(distance, a=1, b=1, n=3, pwr=1):
    """

    :param distance: distance: distance from topic
    :param a: constant to shape graph. Adjusts hight at 0 = a / (1 + b)
    :param b: constant to shape graph.
    :param n: constant to shape graph.
    :param pwr: constant to shape graph.
    :return: log weighting factor as float
    """
    return a / (1 + b*np.exp((distance-n) * pwr))


def exponential_weight(distance, b=2):
    return np.exp(-distance/b)


def normal_weight(distance, pwr=2, a=1.1, b=10, c=1):
    """

    :param distance: distance from topic
    :param pwr: constant to shape graph.  Higher = steeper decline
    :param b: constant to shape graph.  lower = greater spread
    :return: normal weighting factor as float
    pwr = 2.5, a = 1, c = 0, b = 30
    """
    return a * np.exp(-(distance + c)**pwr/b)


### QUERY FULL GRAPH ###


def create_topic(properties, prefix=""):
    """

    :param properties: A dictionary of properties
    :param prefix: If nodes are stored with a pref
    :return: A topic graph in networkx format with one node per property

    NOTE: If multiple values of a certain type, (e.g. multiple IPs) make the value of the type
           in the dictionary a list.
    """
    g = nx.DiGraph()

    if type(properties) == dict:
        iterator = properties.iteritems()
    else:
        iterator = iter(properties)


    for key, value in iterator:
        if type(value) in (list, set, np.ndarray):
            for v in value:
                node_uri = "{2}class=attribute&key={0}&value={1}".format(key, v, prefix)
                g.add_node(node_uri, {
                    'class': 'attribute',
                    'key': key,
                    'value': v,
                    'uri': node_uri
                })
        else:
            node_uri = "{2}class=attribute&key={0}&value={1}".format(key, value, prefix)
            g.add_node(node_uri, {
                'class': 'attribute',
                'key': key,
                'value': value,
                'uri': node_uri
            })

    return g


def get_neo_subgraph(topic, max_depth=4, neo_conf="http://localhost:7474/db/data/"):
    """

    :param neo_conf:
    :param topic:
    :param max_depth:
    :return:
    """
    neo_graph = py2neoGraph(neo_conf)
    sg = nx.MultiDiGraph()

    # Get IDs of topic nodes in graph (if they exist).  Also add topics to subgraph
    topic_ids = set()
    for t, data in topic.nodes(data=True):
        cypher = ("MATCH (topic: {0}) "
                  "WHERE topic.key = {1} AND topic.value = {2} "
                  "RETURN topic as topics").format(data['class'], "{KEY}", "{VALUE}")
        props = {"KEY":data['key'], "VALUE":data['value']}
        for record in neo_graph.cypher.execute(cypher, props):
            print record  # DEBUG
            for topic in record.topics:
                attr = dict(topic.properties)
                uri = u'class={0}&key={1}&value={2}'.format(attr['class'],attr['key'], attr['value'])
                sg.add_node(uri, attr)
                topic_ids.add(topic._Node__id)

    # Add nodes at depth 1  (done separately as it doesn't include the intermediary
    if max_depth > 0:
        if max_depth == 1:
            cypher = ("path=MATCH (topic)-[rel:describedBy|influences]-(node: attribute)"
                      "WHERE id(topic) IN {TOPICS}"
                      "RETURN DISTINCT(r IN rels(path) | r) as rels")
            attr = {"TOPICS":list(topic_ids)}
        else:
            cypher = ("MATCH path=(topic: attribute)-[rel:describedBy|influences]-(node: attribute) "
                      "WHERE id(topic) IN {TOPICS} "
                      "RETURN DISTINCT extract(r IN rels(path) | r) as rels "
                      "UNION "
                      "MATCH path=(topic: attribute)-[rel1: describedBy|influences]-(intermediate: attribute)-[rel2: describedBy|influences]-(node: attribute) "
                      "WHERE id(topic) IN {TOPIC} AND NOT intermediate.key in {DONT_FOLLOW} and length(path) <= {MAX_DEPTH} "
                      "RETURN DISTINCT extract(r IN rels(path) | r) as rels")
            attr = {"MAX_DEPTH": max_depth,
                    "TOPIC": list(topic_ids),
                    "DONT_FOLLOW": ['enrichment', 'classification']}

        for record in neo_graph.cypher.stream(cypher, attr):
            for rel in record.rels:
                # add edges SRC node
                src_attr = dict(rel.start_node.properties)
                src_uri = u"class={0}&key={1}&value={2}".format(src_attr['class'], src_attr['key'], src_attr['value'])
                sg.add_node(src_uri, src_attr)

                # Add edge DST node
                dst_attr = dict(rel.end_node.properties)
                dst_uri = u"class={0}&key={1}&value={2}".format(dst_attr['class'], dst_attr['key'], dst_attr['value'])
                sg.add_node(dst_uri, dst_attr)

                # add edge
                edge_attr = dict(record.rels.properties)
                edge_attr['relationship'] = record.rels.type
                source_hash = uuid.uuid3(uuid.NAMESPACE_URL, src_uri)
                dest_hash = uuid.uuid3(uuid.NAMESPACE_URL, dst_uri)
                edge_uri = u"source={0}&destionation={1}".format(str(source_hash), str(dest_hash))
                rel_chain = u"relationship"
                while rel_chain in edge_attr:
                    edge_uri = edge_uri + u"&{0}={1}".format(rel_chain,edge_attr[rel_chain])
                    rel_chain = edge_attr[rel_chain]
                if "origin" in edge_attr:
                    edge_uri += u"&{0}={1}".format(u"origin", edge_attr["origin"])
                edge_attr["uri"] = edge_uri
                sg.add_edge(src_uri, dst_uri, edge_uri, edge_attr)

    # TODO:  Handle duplicate edges (may dedup but leave in for now)
    #          Take edges into dataframe
    #          group by combine on features to be deduplicated.  Return edge_id's in each group.  Combine those edge_ids using a combine algorithm
    #          Could do the same with dedup algo, but just return the dedup edge_ids and delete them from the graph

    return sg


def get_titan_subgraph(titan_conf, topic, max_depth, pivot_on=list(), dont_pivot_on=list(), direction='successors'):
    """

        :param og: The full graph to be operated on (nx).
        :param topic: a  graph to return the context of.  At least one node ID in topic \
         must be in full graph g to return any context.
        :param max_depth: The maximum distance from the topic to search
        :param pivot_on: A list of attribute types to pivot on.
        :param dont_pivot_on: A list of attribute types to not pivot on.
        :param direction: The direction to transverse the graph
        :return: subgraph in networkx format

        NOTE: If an attribute is in both pivot_on and dont_pivot_on it will not be pivoted on
    """
    # Connect to TitanDB Database
    titan_graph = TITAN_Graph(titan_conf)

    # Convert the topic nodes into titanDB eids
    current_nodes = set()
    eid_uri_map = {}
    # Validate the node URI
    for node in topic.nodes():
        titan_node = titan_graph.vertices.index.get_unique("uri", topic.node[node]["uri"])
        if titan_node:
            current_nodes.add(titan_node.eid)
            eid_uri_map[titan_node.eid] = node
    topic_nodes = frozenset(current_nodes)
    subgraph_nodes = current_nodes
    #sg = copy.deepcopy(topic)
    sg = nx.MultiDiGraph()
    sg.add_nodes_from(topic.nodes(data=True))
    sg.add_edges_from(topic.edges(data=True))
    distances = {node: 0 for node in topic.nodes()}
#    Below 1 line is probably not necessary
#    pivot_edges = list()
#    print "Initial current Nodes: {0}".format(current_nodes)  # DEBUG
    for i in range(1, max_depth + 1):
        new_nodes = set()
        new_out_edges = set()
        new_in_edges = set()
        for eid in current_nodes:
#            properties = og.node[node]
            node = titan_graph.vertices.get(eid)
            # If all directions, get all neighbors
            if direction == 'all' or eid in topic_nodes:
                try:
                    new_nodes = new_nodes.union({n.eid for n in titan_graph.gremlin.query("g.v({0}).both".format(eid))})
                except:
                    pass
                try:
                    new_out_edges = new_out_edges.union({n.eid for n in titan_graph.gremlin.query(
                                                    "g.v({0}).outE".format(eid))})
                except:
                    pass
                try:
                    new_in_edges = new_in_edges.union({n.eid for n in titan_graph.gremlin.query(
                                                    "g.v({0}).inE".format(eid))})
                except:
                    pass
            # If there is a list of things to NOT pivot on, pivot on everything else
            elif dont_pivot_on and 'attribute' in node and node.map()['attribute'] not in dont_pivot_on:
                try:
                    new_nodes = new_nodes.union({n.eid for n in titan_graph.gremlin.query("g.v({0}).both".format(eid))})
                except:
                    pass
                try:
                    new_out_edges = new_out_edges.union({n.eid for n in titan_graph.gremlin.query(
                                                    "g.v({0}).outE".format(eid))})
                except:
                    pass
                try:
                    new_in_edges = new_in_edges.union({n.eid for n in titan_graph.gremlin.query(
                                                    "g.v({0}).inE".format(eid))})
                except:
                    pass
            # Otherwise, only get all neighbors if the node is to be pivoted on.
            elif 'attribute' in node and \
                  node['attribute'] in pivot_on and \
                  node['attribute'] not in dont_pivot_on:
                try:
                    new_nodes = new_nodes.union({n.eid for n in titan_graph.gremlin.query("g.v({0}).both".format(eid))})
                except:
                    pass
                try:
                    new_out_edges = new_out_edges.union({n.eid for n in titan_graph.gremlin.query(
                                                    "g.v({0}).outE".format(eid))})
                except:
                    pass
                try:
                    new_in_edges = new_in_edges.union({n.eid for n in titan_graph.gremlin.query(
                                                    "g.v({0}).inE".format(eid))})
                except:
                    pass
            # If not all neighbors and not in pivot, if we are transversing up, get predecessors
            elif direction == 'predecessors':
                # add edges to make predecessors successors for later probability calculation
                try:
                    new_nodes = new_nodes.union({n.eid for n in titan_graph.gremlin.query("g.v({0}).out".format(eid))})
                except:
                    pass
                # add the reverse edges. These opposite of these edges will get placed in the subgraph
                try:
                    new_in_edges = new_in_edges.union({n.eid for n in titan_graph.gremlin.query(
                                                    "g.v({0}).inE".format(eid))})
                except:
                    pass
            # Otherwise assume we are transversing down and get all successors
            else:  # default to successors
                try:
                    new_nodes = new_nodes.union({n.eid for n in titan_graph.gremlin.query("g.v({0}).both".format(eid))})
                except:
                    pass
                try:
                    new_out_edges = new_out_edges.union({n.eid for n in titan_graph.gremlin.query(
                                                    "g.v({0}).outE".format(eid))})
                except:
                    pass

        # Remove nodes from new_nodes that are already in the subgraph so we don't overwrite their topic distance
        current_nodes = new_nodes - subgraph_nodes
        # combine the new nodes into the subgraph nodes set
        subgraph_nodes = subgraph_nodes.union(current_nodes)

        # Copy nodes, out-edges, in-edges, and reverse in-edges into subgraph
        # Add nodes
        for neighbor_eid in new_nodes:
            attr = titan_graph.vertices.get(neighbor_eid).map()
            sg.add_node(attr['uri'], attr)
            eid_uri_map[neighbor_eid] = attr['uri']
        # Add predecessor edges
        for out_eid in new_out_edges:
            out_edge = titan_graph.edges.get(out_eid)
            attr = out_edge.map()
            sg.add_edge(eid_uri_map[out_edge._outV], eid_uri_map[out_edge._inV], out_eid, attr)
        # Add successor edges & reverse pivot edges
        for in_eid in new_in_edges:
            in_edge = titan_graph.edges.get(in_eid)
            attr = in_edge.map()
            attr['origin'] = "subgraph_creation_pivot"
            sg.add_edge(eid_uri_map[in_edge._inV], eid_uri_map[in_edge._outV], in_eid, attr)

        # Set the distance from the topic on the nodes in the graph
        for eid in current_nodes:
            if eid_uri_map[eid] not in distances:
                distances[eid_uri_map[eid]] = i
#        logging.debug("Current nodes: {0}".format(current_nodes))  # DEBUG

    # add the distances to the subgraph
    nx.set_node_attributes(sg, "topic_distance", distances)

    logging.debug(nx.info(sg))  # DEBUG
    # Return the subgraph
    return sg


def time_filter(sg, start, finish, graph_filter="both", time_filter="both"): #TODO
    """

    :param sg: networkx subgraph to be filtered
    :param start: start time in XXX format
    :param finish: finish time in XXX format
    :param graph_filter: What to test times of in graph.  "nodes", "edges", or "both".  (nodes or edges only will still effect the other)
    :param time_filter: What to test times of graph object in. "start", "finish", "both".
    :return: Networkx subgraph with nodes/edges not in the time frame removed..
    """
    pass
    # TODO: Build a script that returns only edges within a specific time.
    #        option:  filter edges, nodes, or both  (with obvious impact on nodes if edges chosen and visa versa)
    #        option:  Sort edges/nodes by start/end time to make searching more efficient.
    #        option:  filter on start, end, or both times.
    # TODO: (write in such a way that a graph can be sliced into time slices to build a graph over time.)


### SUBGRAPH WEIGHTING FUNCTIONS ###
def get_topic_distance(sg, topic):
    """

    :param sg: an egocentric subgraph in networkx format
    :param topic: a networkx graph of nodes representing the topic
    :return: a dictionary of key node name and value distance as integer
    """
    distances = dict()

    # get all the distances
    for tnode in topic.nodes():
        if tnode in sg.nodes():
            distances[tnode] = nx.shortest_path_length(sg, source=tnode)

    # get the smallest distance per key
    min_dist = dict()
    for key in distances:
        for node in distances[key]:
            if node not in min_dist:
                min_dist[node] = distances[key][node]
            elif distances[key][node] < min_dist[node]:
                min_dist[node] = distances[key][node]


    # Return the dict
    return min_dist


def get_modularity_cluster(sg):
    """

    :param sg: subgraph
    :return: A dictionary of the modularity scores of the nodes in the subgraph
    """
    # Convert to diGraph
    if sg.is_multigraph():
        sg = multigraph_to_digraph(sg)
    # Convert to undirected
    sg = sg.to_undirected()

    return community.best_partition(sg)


def get_pagerank_probability_2(sg, topic, personalization=None):
    """

    :param sg: egocentric subgraph around topic in networkx format
    :param topic: A factor for degrading as distance from the topic increases
    :param personalization: Dictionary with key of a node and value of a node weight.  If none specified, defaults to the linear weight of the 'topic_distance' feature of the nodes.  The topic_distance is the topic for which the subgraph was generated.
    :return: Dictionary of probabilities keyed by node
    """
    if sg.is_multigraph():
        sg = multigraph_to_digraph(sg)

    if personalization == None:
        personalization = {}
        for node in sg.nodes():
    #        personalized[node] = linear_weight(sg.node[node]['topic_distance'], distance_degradation)
            # INSERT WEIGHTING FUNCTION BELOW
            personalization[node] = linear_weight(sg.node[node]['topic_distance'])

    # Build topic weights to start topic with all weight and always jump to topic

    topic_weight = 1/float(len(topic.nodes()))
    topic_weighted = {k if 1 else k: topic_weight if k in topic.nodes() else 0 for k in sg.nodes()}

    # return the pagerank scores
    return nx.pagerank(sg,
                       personalization=personalization,
                       weight='confidence',
                       nstart=topic_weighted,
                       dangling=topic_weighted)


def get_path_count_probability(sg, topic, max_depth):
    """

    :param sg: egocentric subgraph around topic in networkx format
    :param topic: graph of topics
    :param max_depth: maximum length of paths
    :return: Dictionary of probabilities keyed by node
    """
    #  THIS IS I CRITICAL PER the 1-1-1-1-t-3-9-1 graph
    #  THIS WILL NOT TOLERATE LOOPS WITHOUT ADDITIONAL EFFORT
    targets = set(sg.nodes()).difference(set(topic.nodes()))
    paths = {}
    probabilities = {}

    # Create a meta node to represent the topic nodes
    # Based on https://gist.github.com/Zulko/7629206
    meta_node_uuid = str(uuid.uuid4())

    sg.add_node(meta_node_uuid)  # Add the 'merged' node

    for n1, n2, data in sg.edges(data=True):
        # For all edges related to one of the nodes to merge,
        # make an edge going to or coming from the `new gene`.
        if n1 in topic.nodes():
            sg.add_edge(meta_node_uuid, n2, data=data)
        elif n2 in topic.nodes():
            sg.add_edge(n1, meta_node_uuid, data=data)

    # retrieve all paths to all nodes
    for target in targets:
        paths[target] = nx.all_simple_paths(sg, meta_node_uuid, target, cutoff=max_depth)

    # Combine the multiple paths from multiple topics to a single score per node
    for target in targets:
        probabilities[target] = 0
        for path in paths[target]:
            # develop a weight based on the length of the path
            # INSERT WEIGHTING FUNCTION BELOW
            path_weight = normal_weight(len(path))
            # Calculate the confidence in the path
            confidence = 1
            for node in path:
                if 'confidence' in sg.node[node]:
                    confidence *= sg.node[node]['confidence']
            # Sum the path score.  The path's score is it's confidence multiplied by it's weight
            probabilities[target] += confidence * path_weight

    # Make the topic nodes the highest probabilities just to put them on top
    max_p = max(probabilities.values())
    for node in topic.nodes():
        probabilities[node] = max_p

    # TODO: Could normalize values to 1....

    # remove the meta node
    sg.remove_node(meta_node_uuid)

    # return probabilities
    return probabilities


def get_bayesian_network_probability(sg, topic):
    """

    :param sg: egocentric subgraph around topic in networkx format
    :param topic: graph of topics
    :param distance_degradation: A factor for degrading as distance from the topic increases
    :return: Dictionary of probabilities keyed by node

    NOTE: Will error on cycles in graph
    """
    # Calculate the probability of each node given the topic nodes
    # TODO: Capture the context of relationships as well
    # TODO: Handle loops more elegantly than failing
    # TODO: handle the markov blanket

    # setup
    confidences = nx.get_edge_attributes(sg, 'confidence')
    probabilities = defaultdict(lambda: 0)
    queue = list()
    complete_history = random.sample(xrange(10000), 1000)
    complete = set()

    for node in topic.nodes():
        probabilities[node] = 1  # The topic nodes are by definition true
        complete.add(node)  # The topic nodes are by definition complete
    for node in sg.nodes():
        for successor in sg.successors(node):
            queue.append(successor)
    print "Starting probability loop"
    while len(queue) > 0:
        temp = complete_history.pop(0)
        complete_history.append(len(complete))
        if len(set(complete_history)) < 2:
            print "Error, nothing completed in 1000 rounds."
            print "Queue length is {0} with {1} unique values".format(len(queue), len(set(queue)))
            print "Complete is\n{0}".format(len(complete))
            break
        node = queue.pop(0)
        if node not in complete:  # Only
            ready_to_calculate = True
            for predecessor in sg.predecessors(node):
                if predecessor not in complete:
                    queue.append(predecessor)  # if the node is not complete, enqueue it
                    ready_to_calculate = False  # before we can complete a node, it's predecessors must be complete
            if ready_to_calculate:
                try:
                    # INSERT WEIGHTING FUNCTION BELOW
                    cpt = np.array(normal_weight(sg.node[node]['topic_distance']))
                except Exception as e:
                    print "Node: {0}, Attributes: {1}".format(node, sg.node[node])
                    raise e
                for predecessor in sg.predecessors(node):
                    # If an edge has a confidence, we use it.  Otherwise we assume 100%
                    if (predecessor, node) in confidences:

                        confidence = confidences[(predecessor, node)]
                    else:
                        confidence = 1
                # Calculate the probability based on the bayesian network
                # Reference: http://cs.nyu.edu/faculty/davise/ai/bayesnet.html
                # Reference: http://en.wikipedia.org/wiki/Bayes'_theorem
                # Reference: http://en.wikipedia.org/wiki/Bayesian_network
                for i in range(2**len(sg.predecessors(node))):
                    # double the rows
                    cpt = np.vstack((cpt, cpt))
                    # create a list that is first half the compliment of the probability and second half the probability
                    new_col = []
                    for j in range(cpt.shape[0]):
                        if j < cpt.shape[0] / float(2):
                            new_col.append(1 - (confidence * probabilities[predecessor]))
                        else:
                            new_col.append(confidence * probabilities[predecessor])
                    # Add that column to the CPT
                    cpt = np.column_stack((cpt, new_col))

                # Remove first (all false) row as it should not be summed into the probability
                #  This is in leu of making the prior probability zero for that row
                cpt = np.delete(cpt, (0), axis=0)

                # sum the product of each column to get the node probability
                probabilities[node] = cpt.prod(axis=1).sum()
                queue = queue + sg.successors(node)  # queue successors to the node
                complete.add(node)  # add the node as completed

            else:  # It's not ready to be completed
                queue.append(node)  # requeue the node after it's predecessors

    return probabilities


def get_pagerank_probability(sg):
    """

    :param sg: egocentric subgraph around topic in networkx format
    :param distance_degradation: A factor for degrading as distance from the topic increases
    :return: Dictionary of probabilities keyed by node
    """
    # convert to digraph if needed
    if sg.is_multigraph():
        sg = multigraph_to_digraph(sg)

    personalized = {}
    for node in sg.nodes():
#        personalized[node] = linear_weight(sg.node[node]['topic_distance'], distance_degradation)
        # INSERT WEIGHTING FUNCTION BELOW
        personalized[node] = exponential_weight(sg.node[node]['topic_distance'])

    # return the pagerank scores
    return nx.pagerank(sg, personalization=personalized, weight='confidence')