/*
 * Copyright (c) 2009.  The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.contrib.hbql;

import org.apache.hadoop.hbase.contrib.hbql.client.Column;
import org.apache.hadoop.hbase.contrib.hbql.client.Family;
import org.apache.hadoop.hbase.contrib.hbql.client.Table;

import java.io.Serializable;

// START SNIPPET: annotatedExample1

@Table(name = "example2",
       families = {
               @Family(name = "f1", maxVersions = 10)
       })
public class AnnotatedExample implements Serializable {

    @Column(key = true)
    public String keyval = null;

    @Column(family = "f1")
    public String val1;

    @Column(family = "f1")
    public int val2;

    @Column(family = "f1")
    public String val3 = "This is a default value";
}

// START SNIPPET: annotatedExample1
