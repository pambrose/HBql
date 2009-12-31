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
import org.apache.hadoop.hbase.client.tableindexed.IndexedTable;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class IndexRequest implements RowRequest {

    private final byte[] startRow;
    private final byte[] stopRow;
    private final Set<ColumnAttrib> columnAttribs;

    public IndexRequest(final byte[] startRow, final byte[] stopRow, final Set<ColumnAttrib> columnAttribs) {
        this.startRow = startRow;
        this.stopRow = stopRow;
        this.columnAttribs = columnAttribs;
    }

    private Set<ColumnAttrib> getColumnAttribs() {
        return this.columnAttribs;
    }

    private byte[] getStartRow() {
        return this.startRow;
    }

    private byte[] getStopRow() {
        return this.stopRow;
    }

    private byte[][] getColumns() throws HBqlException {

        final byte[][] attribs;

        if (this.getColumnAttribs() == null) {
            attribs = null;
        }
        else {
            final List<String> columnList = Lists.newArrayList();
            for (final ColumnAttrib columnAttrib : this.getColumnAttribs())
                columnList.add(columnAttrib.isASelectFamilyAttrib() ? columnAttrib.getFamilyName()
                                                                    : columnAttrib.getFamilyQualifiedName());
            attribs = Util.getStringsAsBytes(columnList);
        }
        return attribs;
    }

    public ResultScanner getResultScanner(final Mapping mapping,
                                          final WithArgs withArgs,
                                          final HTable table) throws HBqlException {

        final IndexedTable index = (IndexedTable)table;

        byte[] startKey = null;
        byte[] stopKey = null;

        if (this.getStartRow() != HConstants.EMPTY_START_ROW) {
            final TableMapping tableMapping = (TableMapping)mapping;
            tableMapping.validateKeyInfo(withArgs.getIndexName());
            final int width = tableMapping.getKeyInfo().getWidth();
            startKey = Bytes.add(this.getStartRow(), Util.getFixedWidthString(Character.MIN_VALUE, width));
        }

        if (this.getStopRow() != HConstants.EMPTY_END_ROW) {
            final TableMapping tableMapping = (TableMapping)mapping;
            tableMapping.validateKeyInfo(withArgs.getIndexName());
            final int width = tableMapping.getKeyInfo().getWidth();
            stopKey = Bytes.add(this.getStopRow(), Util.getFixedWidthString(Character.MAX_VALUE, width));
        }

        try {
            return index.getIndexedScanner(withArgs.getIndexName(),
                                           startKey,
                                           stopKey,
                                           withArgs.getColumnsUsedInIndexWhereExpr(),
                                           withArgs.getFilterForIndex(),
                                           getColumns());
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }
}