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

package org.apache.yaoql.util;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.yaoql.client.ObjectQuery;
import org.apache.yaoql.client.ObjectQueryManager;
import org.apache.yaoql.client.ObjectResults;

import java.util.Collection;

public class ObjectTests<T> {

    protected void assertResultCount(final Collection<T> objList, final String expr,
                                     final int expected_cnt) throws HBqlException {

        final ObjectQuery<T> query = ObjectQueryManager.newObjectQuery(expr);

        int cnt = 0;
        ObjectResults<T> results = query.getResults(objList);
        for (final T val : results)
            cnt++;

        System.out.println("Count = " + cnt);

        org.junit.Assert.assertTrue(expected_cnt == cnt);
    }

    protected void assertTrue(final boolean cond) {
        org.junit.Assert.assertTrue(cond);
    }
}