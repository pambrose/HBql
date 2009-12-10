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
import org.apache.hadoop.hbase.hbql.statement.select.IndexScanRequest;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.statement.select.ScanRequest;

import java.util.Collection;
import java.util.List;

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

    // This is an object because it might be a collection in the case of a param
    private Object getFirstArg(final boolean allowCollections) throws HBqlException {
        return this.evaluateConstant(null, 0, allowCollections, null);
    }

    private String getSecondArg() throws HBqlException {
        return (String)this.evaluateConstant(null, 1, false, null);
    }

    private KeyRangeArgs.Type getType() {
        return this.type;
    }

    private byte[] getUpperAsBytes() throws HBqlException {
        final String upper = this.getSecondArg();
        return IO.getSerialization().getStringAsBytes(upper);
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

    private RowRequest newGet(final WithArgs withArgs,
                              final Collection<ColumnAttrib> columnAttribSet,
                              final String lower) throws HBqlException {
        final byte[] lowerBytes = IO.getSerialization().getStringAsBytes(lower);
        final Get get = new Get(lowerBytes);
        withArgs.setGetArgs(get, columnAttribSet);
        return new GetRequest(get);
    }

    private List<RowRequest> getRowRequestForGet(final WithArgs withArgs,
                                                 final Collection<ColumnAttrib> columnAttribs) throws HBqlException {

        final List<RowRequest> retval = Lists.newArrayList();

        // Check if the value returned is a collection
        final Object objval = this.getFirstArg(true);
        if (TypeSupport.isACollection(objval)) {
            for (final GenericValue val : (Collection<GenericValue>)objval) {
                try {
                    final String lower = (String)val.getValue(null, null);
                    retval.add(this.newGet(withArgs, columnAttribs, lower));
                }
                catch (ResultMissingColumnException e) {
                    throw new InternalErrorException(val.asString());
                }
            }
        }
        else {
            final String lower = (String)objval;
            retval.add(this.newGet(withArgs, columnAttribs, lower));
        }

        return retval;
    }

    private RowRequest getRowRequestForScan(final WithArgs withArgs,
                                            final Collection<ColumnAttrib> columnAttribs) throws HBqlException {

        final Scan scan = new Scan();

        if (this.isAllRows()) {
            // Let scan default to all rows
        }
        else if (this.isFirstRange()) {
            final byte[] upperBytes = IO.getSerialization().getStringAsBytes((String)this.getFirstArg(false));
            scan.setStopRow(upperBytes);
        }
        else if (this.isLastRange()) {
            final byte[] lowerBytes = IO.getSerialization().getStringAsBytes((String)this.getFirstArg(false));
            scan.setStartRow(lowerBytes);
        }
        else {
            final byte[] lowerBytes = IO.getSerialization().getStringAsBytes((String)this.getFirstArg(false));
            final byte[] upperBytes = IO.getSerialization().getStringAsBytes(this.getSecondArg());
            scan.setStartRow(lowerBytes);
            scan.setStopRow(upperBytes);
        }

        withArgs.setScanArgs(scan, columnAttribs);

        if (withArgs.hasAnIndex())
            return new IndexScanRequest(scan, this, columnAttribs);
        else
            return new ScanRequest(scan);
    }

    public void process(final WithArgs withArgs,
                        final List<RowRequest> rowRequestList,
                        final Collection<ColumnAttrib> columnAttribSet) throws HBqlException {

        if (this.isSingleKey())
            rowRequestList.addAll(this.getRowRequestForGet(withArgs, columnAttribSet));
        else
            rowRequestList.add(this.getRowRequestForScan(withArgs, columnAttribSet));
    }
}
