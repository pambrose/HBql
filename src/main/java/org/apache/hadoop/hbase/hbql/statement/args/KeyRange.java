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

package org.apache.hadoop.hbase.hbql.statement.args;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.statement.select.GetRequest;
import org.apache.hadoop.hbase.hbql.statement.select.IndexRequest;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.statement.select.ScanRequest;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class KeyRange extends SelectStatementArgs {
    private final KeyRangeArgs.Type type;

    private KeyRange() {
        super(Type.NOARGSKEY);
        this.type = KeyRangeArgs.Type.ALL;
    }

    private KeyRange(final KeyRangeArgs.Type type, final GenericValue arg0) {
        super(Type.SINGLEKEY, arg0);
        this.type = type;
    }

    private KeyRange(final GenericValue arg0, final GenericValue arg1) {
        super(Type.KEYRANGE, arg0, arg1);
        this.type = KeyRangeArgs.Type.RANGE;
    }

    public static KeyRange newRange(final GenericValue arg0, final GenericValue arg1) {
        return new KeyRange(arg0, arg1);
    }

    public static KeyRange newSingleKey(final GenericValue arg0) {
        return new KeyRange(KeyRangeArgs.Type.SINGLE, arg0);
    }

    public static KeyRange newFirstRange(final GenericValue arg0) {
        return new KeyRange(KeyRangeArgs.Type.FIRST, arg0);
    }

    public static KeyRange newLastRange(final GenericValue arg0) {
        return new KeyRange(KeyRangeArgs.Type.LAST, arg0);
    }

    public static KeyRange newAllRange() {
        return new KeyRange();
    }

    private KeyRangeArgs.Type getType() {
        return this.type;
    }

    private boolean isFirstRange() {
        return this.getType() == KeyRangeArgs.Type.FIRST;
    }

    private boolean isLastRange() {
        return this.getType() == KeyRangeArgs.Type.LAST;
    }

    public boolean isSingleKey() {
        return this.getType() == KeyRangeArgs.Type.SINGLE;
    }

    private boolean isRowRange() {
        return this.getType() == KeyRangeArgs.Type.RANGE;
    }

    public boolean isAllRows() {
        return this.getType() == KeyRangeArgs.Type.ALL;
    }

    public void validateArgTypes() throws HBqlException {
        this.validateTypes(this.allowColumns(), this.isSingleKey());
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        if (this.isAllRows()) {
            sbuf.append("KEYS ALL");
        }
        else if (this.isSingleKey()) {
            sbuf.append("KEY " + this.getGenericValue(0).asString());
        }
        else if (this.isFirstRange()) {
            sbuf.append("KEYS FIRST TO " + this.getGenericValue(0).asString());
        }
        else if (this.isLastRange()) {
            sbuf.append("KEYS " + this.getGenericValue(0).asString() + "' TO LAST");
        }
        else {
            sbuf.append("KEYS " + this.getGenericValue(0).asString());
            sbuf.append(" TO ");
            sbuf.append(this.getGenericValue(1).asString());
        }
        return sbuf.toString();
    }

    // This returns an object because it might be a collection in the case of a param
    private Object getFirstArg(final boolean allowCollections) throws HBqlException {
        return this.evaluateConstant(null, 0, allowCollections, null);
    }

    private String getSecondArg() throws HBqlException {
        return (String)this.evaluateConstant(null, 1, false, null);
    }

    public List<RowRequest> getRowRequestList(final WithArgs withArgs,
                                              final ColumnAttrib keyAttrib,
                                              final Set<ColumnAttrib> columnAttribs) throws HBqlException {

        return this.isSingleKey() ? this.getGetRequest(withArgs, keyAttrib, columnAttribs)
                                  : this.getScanRequest(withArgs, keyAttrib, columnAttribs);
    }

    private List<RowRequest> getGetRequest(final WithArgs withArgs,
                                           final ColumnAttrib keyAttrib,
                                           final Set<ColumnAttrib> columnAttribs) throws HBqlException {

        final List<RowRequest> rowRequestList = Lists.newArrayList();

        // Check if the value returned is a collection
        final Object objval = this.getFirstArg(true);
        if (TypeSupport.isACollection(objval)) {
            for (final GenericValue val : (Collection<GenericValue>)objval) {
                try {
                    final String rangeValue = (String)val.getValue(null, null);
                    final RowRequest rowRequest = this.newGet(withArgs, columnAttribs, keyAttrib, rangeValue);
                    rowRequestList.add(rowRequest);
                }
                catch (ResultMissingColumnException e) {
                    throw new InternalErrorException(val.asString());
                }
            }
        }
        else {
            final String rangeValue = (String)objval;
            final RowRequest rowRequest = this.newGet(withArgs, columnAttribs, keyAttrib, rangeValue);
            rowRequestList.add(rowRequest);
        }

        return rowRequestList;
    }

    private RowRequest newGet(final WithArgs withArgs,
                              final Set<ColumnAttrib> columnAttribs,
                              final ColumnAttrib keyAttrib,
                              final String rangeValue) throws HBqlException {

        this.verifyRangeValueWidth(keyAttrib, rangeValue);

        final byte[] lowerBytes = IO.getSerialization().getStringAsBytes(rangeValue);

        if (withArgs.hasAnIndex()) {
            return new IndexRequest(lowerBytes, lowerBytes, columnAttribs);
        }
        else {
            final Get get = new Get(lowerBytes);
            withArgs.setGetArgs(get, columnAttribs);
            return new GetRequest(get);
        }
    }

    private List<RowRequest> getScanRequest(final WithArgs withArgs,
                                            final ColumnAttrib keyAttrib,
                                            final Set<ColumnAttrib> columnAttribs) throws HBqlException {

        final Scan scan = new Scan();

        if (this.isAllRows()) {
            // Scan will default to all rows
        }
        else if (this.isFirstRange()) {
            final String rangeValue = (String)this.getFirstArg(false);
            this.verifyRangeValueWidth(keyAttrib, rangeValue);
            final byte[] upperBytes = IO.getSerialization().getStringAsBytes(rangeValue);
            scan.setStopRow(upperBytes);
        }
        else if (this.isLastRange()) {
            final String rangeValue = (String)this.getFirstArg(false);
            this.verifyRangeValueWidth(keyAttrib, rangeValue);
            final byte[] lowerBytes = IO.getSerialization().getStringAsBytes(rangeValue);
            scan.setStartRow(lowerBytes);
        }
        else {
            final String firstRangeValue = (String)this.getFirstArg(false);
            final String secondRangeValue = this.getSecondArg();
            this.verifyRangeValueWidth(keyAttrib, firstRangeValue, secondRangeValue);
            final byte[] lowerBytes = IO.getSerialization().getStringAsBytes(firstRangeValue);
            final byte[] upperBytes = IO.getSerialization().getStringAsBytes(secondRangeValue);
            scan.setStartRow(lowerBytes);
            scan.setStopRow(upperBytes);
        }

        withArgs.setScanArgs(scan, columnAttribs);

        final RowRequest rowRequest = withArgs.hasAnIndex()
                                      ? new IndexRequest(scan.getStartRow(), scan.getStopRow(), columnAttribs)
                                      : new ScanRequest(scan);

        return Lists.newArrayList(rowRequest);
    }

    private void verifyRangeValueWidth(final ColumnAttrib keyAttrib, final String... rangeValues) throws HBqlException {

        final KeyInfo keyInfo = keyAttrib.getColumnDefinition().getKeyInfo();
        if (keyInfo != null) {
            for (final String rangeValue : rangeValues) {
                final int width = keyInfo.getKeyWidth();
                if (width > 0 && rangeValue.length() != width)
                    throw new HBqlException("Invalid key range length in " + this.asString()
                                            + " expecting width " + width + " but found " + rangeValue.length()
                                            + " with key \"" + rangeValue + "\"");
            }
        }
    }
}
