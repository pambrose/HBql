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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTable;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;

import java.io.IOException;

public class ScanRequest implements RowRequest {

    final Scan scanValue;

    public ScanRequest(final Scan scanValue) {
        this.scanValue = scanValue;
    }

    private Scan getScanValue() {
        return this.scanValue;
    }

    public int getMaxVersions() {
        return this.getScanValue().getMaxVersions();
    }

    public ResultScanner getResultScanner(final WithArgs withArgs, final HTable table) throws HBqlException {
        try {

            if (withArgs.hasAnIndex()) {
                final IndexedTable index = (IndexedTable)table;
                return index.getIndexedScanner(withArgs.getIndexName(), null, null, null, null, null);
            }
            else {
                return table.getScanner(this.getScanValue());
            }
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }
}