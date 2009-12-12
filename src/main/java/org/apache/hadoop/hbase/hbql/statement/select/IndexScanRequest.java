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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTable;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;

public class IndexScanRequest implements RowRequest {

    final Scan scanValue;
    final Collection<ColumnAttrib> columnAttribs;

    public IndexScanRequest(final Scan scanValue, final Collection<ColumnAttrib> columnAttribs) {
        this.scanValue = scanValue;
        this.columnAttribs = columnAttribs;
    }

    private Scan getScanValue() {
        return this.scanValue;
    }

    private Collection<ColumnAttrib> getColumnAttribs() {
        return this.columnAttribs;
    }

    public int getMaxVersions() {
        return this.getScanValue().getMaxVersions();
    }

    private byte[][] getColumns() throws HBqlException {

        final byte[][] attribs;

        if (this.getColumnAttribs() == null) {
            attribs = null;
        }
        else {
            attribs = new byte[this.getColumnAttribs().size()][];
            int cnt = 0;
            for (final ColumnAttrib columnAttrib : this.getColumnAttribs())
                attribs[cnt++] = columnAttrib.getFamilyQualifiedNameAsBytes();
        }
        return attribs;
    }

    public ResultScanner getResultScanner(final Mapping mapping,
                                          final WithArgs withArgs,
                                          final HTable table) throws HBqlException {
        try {

            final byte[] startRow = this.getScanValue().getStartRow();
            final byte[] stopRow = this.getScanValue().getStopRow();

            byte[] startKey = null;
            byte[] stopKey = null;

            if (startRow != HConstants.EMPTY_START_ROW) {
                final TableMapping tableMapping = (TableMapping)mapping;
                tableMapping.validateKeyInfo(withArgs.getIndexName());
                final int width = tableMapping.getKeyInfo().getKeyWidth();
                startKey = Bytes.add(startRow, Util.getFixedWidthString(Character.MIN_VALUE, width));
            }

            if (stopRow != HConstants.EMPTY_END_ROW) {
                final TableMapping tableMapping = (TableMapping)mapping;
                tableMapping.validateKeyInfo(withArgs.getIndexName());
                final int width = tableMapping.getKeyInfo().getKeyWidth();
                stopKey = Bytes.add(stopRow, Util.getFixedWidthString(Character.MAX_VALUE, width));
            }

            final IndexedTable index = (IndexedTable)table;

            return index.getIndexedScanner(withArgs.getIndexName(),
                                           startKey,
                                           stopKey,
                                           null,
                                           null,
                                           getColumns());
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }
}