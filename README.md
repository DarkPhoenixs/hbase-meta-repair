<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# hbase-meta-repair

[![Organization](https://img.shields.io/badge/org-%20DarkPhoenixs-yellow.svg)](http://www.darkphoenixs.org)
[![GitHub release](https://img.shields.io/github/release/DarkPhoenixs/hbase-meta-repair.svg)](https://github.com/DarkPhoenixs/hbase-meta-repair/releases)
[![License](https://img.shields.io/badge/license-%20Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

Repair hbase metadata for [Apache HBase&trade;](https://hbase.apache.org) 
versions before 2.0.3 and 2.1.1 (hbase versions without HBCK2).

## Configuration

* [application.properties](src/main/resources/application.properties)
    ```properties
    # hbase zk host:port
    zookeeper.address=host:port,host:port,host:port
    # hbase zk root
    zookeeper.nodeParent=/hbase
    # hbase hdfs root
    hdfs.root.dir=hdfs://nameservice/hbase
    ```
* [core-site.xml](src/main/resources/core-site.xml) Using profiles on Hadoop clusters(copy from your cluster).
                             
* [hdfs-site.xml](src/main/resources/hdfs-site.xml) Using profiles on Hadoop clusters(copy from your cluster).

* [pom.xml](pom.xml) Change `hbase.version` to your version.

## Building repair

```bash
$ mvn install
```

## Running repair

```bash
$ java -jar -Drepair.tableName={tableName} hbase-repair-{version}.jar
```
