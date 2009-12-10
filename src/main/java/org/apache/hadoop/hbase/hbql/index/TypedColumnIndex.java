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

package org.apache.hadoop.hbase.hbql.index;

import org.apache.hadoop.hbase.client.tableindexed.IndexKeyGenerator;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;

import java.util.List;

public class TypedColumnIndex extends IndexSpecification {


    private TypedColumnIndex(final String indexName,
                             final byte[][] indexedColumns,
                             final byte[][] additionalColumns,
                             final IndexKeyGenerator keyGenerator) {
        super(indexName, indexedColumns, additionalColumns, new TypedKeyGenerator());
    }

    public static TypedColumnIndex newTypedColumnIndex(final String indexName,
                                                       final List<String> attribList,
                                                       final List<String> additionalList) {

        return null;
    }
}
