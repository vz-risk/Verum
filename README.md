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

Run the following within your python code or at a python console to initialize the package.
```
import imp
LOCATION = "/Users/v685573/Documents/Development/verum/"
fp, pathname, description = imp.find_module("verum", [LOCATION])
VERUM = imp.load_module("verum", fp, pathname, description)
ENRICH = VERUM.enrich("~/Documents/Development/verum/plugins")
#ENRICH.set_plugin_folder("~/Documents/Development/verum/plugins")
#ENRICH.load_plugins()
# TODO - Complete
```

Run the following to test enrichment.
```
# TODO: connect to backend storage and enrich something
```

Run the following to test querying.
```
# TODO: query the graph for something
```


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