---
title: "Partition Statistics Spec"
url: partition-statistics-spec
toc: true
disableSidebar: true
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Partition Statistics file format

This is a specification for partition statistics files. It is designed to store statistics information 
for every partition value as a row in the **table default format** sorted based on the `partition`.

The schema of the file is as follows:

| Field Id | Field Name                   | Field Type   | isOptional | Doc                                                                                                                         |
|----------|------------------------------|--------------|------------|-----------------------------------------------------------------------------------------------------------------------------|
| 1        | partition                    | StructType   | false      | See [PartitionData](https://github.com/apache/iceberg/blob/master/core/src/main/java/org/apache/iceberg/PartitionData.java) |                                                                                  
| 2        | spec_id                      | IntegerType  | false      | Partition spec id                                                                                                           |
| 3        | data_record_count            | LongType     | false      | Count of records in data files                                                                                              |                                               
| 4        | data_file_count              | IntegerType  | false      | Count of data files                                                                                                         |
| 5        | position_delete_record_count | LongType     | true       | Count of records in position delete files                                                                                   |                                                          
| 6        | position_delete_file_count   | IntegerType  | true       | Count of position delete files                                                                                              |                                   
| 7        | equality_delete_record_count | LongType     | true       | Count of records in equality delete files                                                                                   |                                  
| 8        | equality_delete_file_count   | IntegerType  | true       | Count of equality delete files                                                                                              |                                

Each snapshot may create a new partition statistics file. The name of the partition statistics file is as follows:       
`partition-stats-${snapshotId}.${tableDefaultFormat}` 
