#!/usr/bin/env python
"""
 AUTHOR: Gabriel Bassett
 DATE: 11-21-2014
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
 Loads enrichment plugins

 NOTES:
 Based on http://lkubuntu.wordpress.com/2012/10/02/writing-a-python-plugin-api/

"""
# PRE-USER SETUP
pass

########### NOT USER EDITABLE ABOVE THIS POINT #################


# USER VARIABLES
PluginFolder = "./plugins"
MainModule = "__init__"


########### NOT USER EDITABLE BELOW THIS POINT #################


## IMPORTS

import imp
import os


## SETUP
pass


## EXECUTION

def getPlugins():
    plugins = []
    possibleplugins = os.listdir(PluginFolder)
    for i in possibleplugins:
        location = os.path.join(PluginFolder, i)
        if not os.path.isdir(location) or not MainModule + ".py" in os.listdir(location):
            continue
        info = imp.find_module(MainModule, [location])
        plugins.append({"name": i, "info": info})
    return plugins

def loadPlugin(plugin):
    return imp.load_module(MainModule, *plugin["info"])

def main():
    for i in getPlugins():
        print("Loading plugin " + i["name"])
        plugin = loadPlugin(i)
        plugin.run()
        # TODO: Need to align each enrichment to ensure they get loaded here

if __name__ == "__main__":
    main()