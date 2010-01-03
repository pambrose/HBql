/*
 * Copyright (c) 2010.  The Apache Software Foundation
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

import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.client.tableindexed.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Util;

import java.util.List;

public class SingleColumnIndex extends IndexSpecification {


    private SingleColumnIndex(final String indexName,
                              final byte[][] indexedColumns,
                              final byte[][] additionalColumns) {
        super(indexName, indexedColumns, additionalColumns, new SimpleIndexKeyGenerator(indexedColumns[0]));
    }

    public static SingleColumnIndex newIndex(final String indexName,
                                             final List<String> indexList,
                                             final List<String> includeList) throws HBqlException {

        if (indexList.size() != 1)
            throw new HBqlException("Invalid index " + indexName + ". Only single column indexes allowed.");

        return new SingleColumnIndex(indexName,
                                     Util.getStringsAsBytes(indexList),
                                     Util.getStringsAsBytes(includeList));
    }
}
