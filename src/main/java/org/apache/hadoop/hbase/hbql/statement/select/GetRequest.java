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

package org.apache.hadoop.hbase.hbql.statement.select;

import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class GetRequest implements RowRequest {

    final Get getValue;

    public GetRequest(final Get getValue) {
        this.getValue = getValue;
    }

    private Get getGetValue() {
        return this.getValue;
    }

    public int getMaxVersions() {
        return this.getGetValue().getMaxVersions();
    }

    public ResultScanner getResultScanner(final HTable table) throws IOException {

        // We need to fake a ResultScanner with the Get result
        final Result result = table.get(this.getGetValue());
        final List<Result> resultList = Lists.newArrayList();
        if (result != null && !result.isEmpty())
            resultList.add(result);

        return new ResultScanner() {

            public Result next() throws IOException {
                return null;
            }

            public Result[] next(final int nbRows) throws IOException {
                return null;
            }

            public Iterator<Result> iterator() {
                return resultList.iterator();
            }

            public void close() {

            }
        };
    }
}