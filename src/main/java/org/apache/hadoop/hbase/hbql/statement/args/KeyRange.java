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

package org.apache.hadoop.hbase.hbql.statement.args;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.idx.IdxScan;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.statement.select.GetRequest;
import org.apache.hadoop.hbase.hbql.statement.select.IndexRequest;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.statement.select.ScanRequest;
import org.apache.hadoop.hbase.hbql.util.Lists;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class KeyRange extends SelectStatementArgs {

    private enum RangeType {
        SINGLE, RANGE, FIRST, LAST, ALL
    }

    private final RangeType rangeType;

    private KeyRange() {
        super(ArgType.NOARGSKEY);
        this.rangeType = RangeType.ALL;
    }

    private KeyRange(final RangeType rangeType, final GenericValue arg0) {
        super(ArgType.SINGLEKEY, arg0);
        this.rangeType = rangeType;
    }

    private KeyRange(final GenericValue arg0, final GenericValue arg1) {
        super(ArgType.KEYRANGE, arg0, arg1);
        this.rangeType = RangeType.RANGE;
    }

    public static KeyRange newRange(final GenericValue arg0, final GenericValue arg1) {
        return new KeyRange(arg0, arg1);
    }

    public static KeyRange newSingleKey(final GenericValue arg0) {
        return new KeyRange(RangeType.SINGLE, arg0);
    }

    public static KeyRange newFirstRange(final GenericValue arg0) {
        return new KeyRange(RangeType.FIRST, arg0);
    }

    public static KeyRange newLastRange(final GenericValue arg0) {
        return new KeyRange(RangeType.LAST, arg0);
    }

    public static KeyRange newAllRange() {
        return new KeyRange();
    }

    private RangeType getKeyRangeType() {
        return this.rangeType;
    }

    public boolean isSingleKey() {
        return this.getKeyRangeType() == RangeType.SINGLE;
    }

    public void validate() throws HBqlException {
        this.validateTypes(this.allowColumns(), this.isSingleKey());
    }

    // This returns an object because it might be a collection in the case of a param
    private Object getFirstArg(final boolean allowCollections) throws HBqlException {
        return this.evaluateConstant(0, allowCollections);
    }

    private String getSecondArg() throws HBqlException {
        return (String)this.evaluateConstant(1, false);
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
                    throw new InternalErrorException("Missing column: " + val.asString());
                }
                catch (NullColumnValueException e) {
                    throw new InternalErrorException("Null value: " + e.getMessage());
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
            final byte[] upperBytes = Arrays.copyOf(lowerBytes, lowerBytes.length);
            // Increment final byte because the range in index is inclusive/exclusive
            upperBytes[lowerBytes.length - 1]++;
            return new IndexRequest(lowerBytes, upperBytes, columnAttribs);
        }
        else {
            // Use Get if no server filter is used
            if (withArgs.getServerExpressionTree() == null) {
                final Get get = new Get(lowerBytes);
                withArgs.setGetArgs(get, columnAttribs);
                return new GetRequest(get);
            }
            else {
                // TODO This is temporay until Get is fixed for Filters
                final Scan scan = new Scan();

                scan.setStartRow(lowerBytes);

                final byte[] upperBytes = Arrays.copyOf(lowerBytes, lowerBytes.length);
                // Increment final byte because the range in index is inclusive/exclusive
                upperBytes[lowerBytes.length - 1]++;
                scan.setStopRow(upperBytes);

                withArgs.setScanArgs(scan, columnAttribs);
                return new ScanRequest(scan);
            }
        }
    }

    private List<RowRequest> getScanRequest(final WithArgs withArgs,
                                            final ColumnAttrib keyAttrib,
                                            final Set<ColumnAttrib> columnAttribs) throws HBqlException {

        final Scan scan;

        if (withArgs.getIndexExpressionTree() == null) {
            scan = new Scan();
        }
        else {
            final IdxScan idxScan = new IdxScan();
            final Expression indexExpr = withArgs.getIndexExpressionTree().getIndexExpression();
            idxScan.setExpression(indexExpr);
            scan = idxScan;
        }

        this.setStartStopRows(scan, keyAttrib);
        withArgs.setScanArgs(scan, columnAttribs);

        final RowRequest rowRequest = withArgs.hasAnIndex()
                                      ? new IndexRequest(scan.getStartRow(), scan.getStopRow(), columnAttribs)
                                      : new ScanRequest(scan);

        return Lists.newArrayList(rowRequest);
    }

    private void setStartStopRows(final Scan scan, final ColumnAttrib keyAttrib) throws HBqlException {

        switch (this.getKeyRangeType()) {
            case ALL: {
                // Scan will default to all rows
                break;
            }
            case FIRST: {
                final String rangeValue = (String)this.getFirstArg(false);
                this.verifyRangeValueWidth(keyAttrib, rangeValue);
                final byte[] upperBytes = IO.getSerialization().getStringAsBytes(rangeValue);
                scan.setStopRow(upperBytes);
                break;
            }
            case LAST: {
                final String rangeValue = (String)this.getFirstArg(false);
                this.verifyRangeValueWidth(keyAttrib, rangeValue);
                final byte[] lowerBytes = IO.getSerialization().getStringAsBytes(rangeValue);
                scan.setStartRow(lowerBytes);
                break;
            }
            case RANGE: {
                final String firstRangeValue = (String)this.getFirstArg(false);
                final String secondRangeValue = this.getSecondArg();
                this.verifyRangeValueWidth(keyAttrib, firstRangeValue, secondRangeValue);
                final byte[] lowerBytes = IO.getSerialization().getStringAsBytes(firstRangeValue);
                final byte[] upperBytes = IO.getSerialization().getStringAsBytes(secondRangeValue);
                scan.setStartRow(lowerBytes);
                scan.setStopRow(upperBytes);
                break;
            }
            default:
                throw new InternalErrorException("Invalid range type: " + this.getKeyRangeType().name());
        }
    }

    private void verifyRangeValueWidth(final ColumnAttrib keyAttrib, final String... rangeValues) throws HBqlException {

        final ColumnWidth columnWidth = keyAttrib.getColumnDefinition().getColumnWidth();
        if (columnWidth.isWidthSpecified()) {
            for (final String rangeValue : rangeValues) {
                final int width = columnWidth.getWidth();
                if (width > 0 && rangeValue.length() != width)
                    throw new HBqlException("Invalid key range length in " + this.asString()
                                            + " expecting width " + width + " but found " + rangeValue.length()
                                            + " with key \"" + rangeValue + "\"");
            }
        }
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        switch (this.getKeyRangeType()) {
            case ALL: {
                sbuf.append("KEYS ALL");
                break;
            }
            case SINGLE: {
                sbuf.append("KEY " + this.getGenericValue(0).asString());
                break;
            }
            case FIRST: {
                sbuf.append("KEYS FIRST TO " + this.getGenericValue(0).asString());
                break;
            }
            case LAST: {
                sbuf.append("KEYS " + this.getGenericValue(0).asString() + "' TO LAST");
                break;
            }
            case RANGE: {
                sbuf.append("KEYS " + this.getGenericValue(0).asString());
                sbuf.append(" TO ");
                sbuf.append(this.getGenericValue(1).asString());
                break;
            }
            default:
                throw new RuntimeException("Invalid range type: " + this.getKeyRangeType().name());
        }

        return sbuf.toString();
    }
}
