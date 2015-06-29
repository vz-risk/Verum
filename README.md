Verum
=====

Implementation of Context-Graph algorithms for graph enrichment and querying. 

Context Graph Enrichment:
cg_enrich.py provides functions to enrich the context graph.

Context Graph Query:
cg_query.py provides functions necessary to query the context graph for a specific topic.

Context Graph Presentation:
cg_present.py provides functions necessary to present the data to various clients.


## Installation

Clone the Repository
```
git clone https://github.com/vz-risk/Verum.git
```


## Usage
Initialize storage.  In this case, [neo4j] (http://neo4j.com/).
1.  [Download neo4j] (http://neo4j.com/download/).
2.  Unzip it, (if *nix or Mac OS X).
3.  Run it, ('./bin/neo4j start' on *nix or Mac OS X).

(If using [TitanDB] (http://thinkaurelius.github.io/titan/), follow the [installation documentation] (http://s3.thinkaurelius.com/docs/titan/0.9.0-M2/getting-started.html#_downloading_titan_and_running_the_gremlin_shell) provided for Titan.)

### Enrichment

Run the following within your python code or at a python console to initialize the package.
```
# import imp to load verum
import imp
# set verum location
LOCATION = "/Users/v685573/Documents/Development/verum/"
# import verum
fp, pathname, description = imp.find_module("verum", [LOCATION])
Verum = imp.load_module("verum", fp, pathname, description)
# Load plugins. NOTE: if your directory is wrong, you won't receive an error but will see no individual plugins listed as successfully configured.
verum = Verum.app("~/Documents/Development/verum/plugins")
# display loaded plugins directly using yapsy
for plugin in verum.plugins.getAllPlugins():
    print plugin.name
```

Define some data to enrich:
```
ips = ['98.124.199.1',
'178.62.219.229',
 '98.124.198.1',
 '209.216.10.148',
 '124.248.237.26',
 '134.170.185.211',
 '223.29.248.252',
 '117.18.73.98']
domains = ['81.java-se.com',
 'stifie.com',
 'microsoftor.com',
 'pop1.java-sec.com',
 '*.mynethood.com',
 'www.btipnow.com',
 '*.searchenginewatch.us.com',
 'google3853ed273b89687a.mynethood.com',
 'pop.java-sec.com',
 'm-stone.co.jp',
 'www.mynethood.com',
 'jre76.java-sec.com',
 'cdn.foxitsoftwares.com',
 'u.java-se.com',
 'bloger2.microsoftor.com',
 'kai.jztok.com',
 'ns1.searchenginewatch.us.com',
 '*.microsoftor.com',
 's3m7ke.microsoftor.com',
 'mynethood.com',
 's3m7ker.microsoftor.com',
 'officesoft.microsoftor.com',
 'foxitsoftwares.com']
ips2 = ['107.160.143.10',
 '107.167.73.219',
 '148.163.104.35',
 '148.163.104.35',
 '184.164.70.204',
 '184.164.81.11',
 '216.244.93.247',
 '50.117.38.170',
 '50.117.38.170']
domains2 = ['4uexs.rxlijd.bbs.mythem.es',
 'abdebassetbenhassen.org',
 'acid.borec.cz',
 'blogs.burlingtonfreepress.com',
 'buysacramentoproperties.com',
 'cancunluxurystyle.com',
 'cate-rina.net',
 'cdn.servehttp.com',
 'chuamun.com',
 'dayapramana.com',
 'dirtychook.com',
 'f1wot.bbs.mythem.es',
 'fybic.com',
 'gotoe3.tw',
 'haft-honar.com',
 'ichener-duwackstumbe.de',
'iotqduzgha.vtre.qvofj.qypvthu.loqu.forum.mythem.es',
 'jigsore.nasky.net',
 'kitsoft.ru',
 'lytovp.istmein.de',
 'meeting-rsvp.com',
 'mignonfilet.com',
 'myinfo.any-request-allowed.com',
 'oceanspirit.com',
 'opm-learning.org',
 'opmsecurity.org',
 'pejoratively.bloq.ro',
 'subhashmadhu.com',
 'tlvegan.com',
 'tommyhumphreys.com',
 'transcandence.com',
 'travelingmu.com',
 'tsv-albertshofen.net',
 'universofoot.com.br',
 'WDC-News-post.com',
 'wdc-news-post.com',
 'woodcreations.com.pk',
 'xn--80aa4agmizb8a.xn--p1ai',
'yodotink.rjtp.nxrlecd.tcsq.qypvthu.loqu.forum.mythem.es']
 ```

Run the following to test enrichment.
```
# Query IP & domain plugins
print verum.get_enrichments(['ip'])
print verum.get_enrichments(['domain'])
# Query cheap IP plugins
print verum.get_enrichments(['ip'], cost=3)
# Query fast domain plugins
print verum.get_enrichments(['domain'], speed=2)
# Run maxmind enrichments of an IP
import networkx as nx
g = verum.run_enrichments(ips[0], 'ip', names=[u'Maxmind ASN Enrichment'])
print nx.info(g)
```

Run the following to test querying.  (Note: the storage interface modules expect graphs to be in a specific schema.  If they are not, the interface module will error trying to store them.)
```
# (If you didn't create a graph above through an enrichment)
g = Verum.create_topic({'ip': ['184.164.70.204', '184.164.81.11'], 'domain': ['WDC-News-post.com', 'wdc-news-post.com']})
```

```
# See what storage interfaces are configured
print verum.get_interfaces(configured=True)
# Set the storage interface
verum.set_interface('Neo4j')
# Store the graph in the storage interface
verum.store_graph(g)
```

Finally, Attempt to enrich multiple pieces of data to form a robust context graph:
```
# Enrich IPs
for ip in ips + ips2:
    verum.store_graph(verum.run_enrichments(ip, 'ip', names=[u'Maxmind ASN Enrichment']))
# Enrich Domains (passing exceptions so if a plugin fails it doesn't stop the loop)
for domain in domains + domains2:
    try:
        verum.store_graph(verum.run_enrichments(domain, 'domain', names=[u'DNS Enrichment', u'TLD Enrichment']))
    except:
        pass
# Bulk enrich IPs with Cymru
verum.store_graph(verum.run_enrichments(ips + ips2, 'ip', names=[u'Cymru Enrichment']))
```

Now open `http://locahost:7474/` in a browser and enter the Cypher Query:
```
MATCH (n:attribute) 
WHERE n.key = 'ip' and n.value = "98.124.198.1" 
RETURN n;
```
You can then visually explore the graph associated with that IP.

We want to classify all these domains and IPs as malicious:
```
# Classify all IPs and Domains as Malicious
for ip in ips + ips2:
    verum.store_graph(verum.classify.run({'key': 'ip', 'value': ip, 'classification': 'malice'))
for domain in domains + domains2:
    verum.store_graph(verum.classify.run({'key': 'domain', 'value': domain, 'classification': 'malice'}))
```

### Querying

Now that we have built an enriched context graph, we can query it.

```
#Find out if < '117.18.73.98',> is malicious
# Create a topic to score
topic = Verum.create_topic({"ip": '117.18.73.98'})
# Retrieve the subgraph associated with it
sg = verum.run_query(topic)
# List out configured scoring plugins available.
verum.get_scoring_plugins()
# Set the default scoring plugin
verum.set_scoring_plugin('PageRank2')
# Check to ensure it was set
verum.get_default_scoring_plugin()
scores = verum.score_subgraph(topic, sg)
print scores
```

To understand the scores, we can do some relative comparisons.  We compare the malice score both to the topic as well as to other nodes and see that the malice node is stronger than average but not overly strong.
```
# Compare the malice node to the average score
Verum.compare_classifications(scores, {"class":"attribute", "key":"classification", "value":"malice"}, output="print")
# Compare the malice node to the topic node
Verum.compare_classifications(scores, {"class":"attribute", "key":"classification", "value":"malice"}, {"class":"attribute", "key":"ip", "value":"117.18.73.98"}, output="print")
# Score the percentile of the malice score
Verum.score_percentile(scores, {"class":"attribute", "key":"classification", "value":"malice"}, output="print")
```

Note, if you wanted to know about malice, you could rescore the subgraph with the malice node as the topic and compare the node you are interested in, (117.18.73.98 in our example), and compare it to the other nodes as above.


## Contributing
1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D


## License

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