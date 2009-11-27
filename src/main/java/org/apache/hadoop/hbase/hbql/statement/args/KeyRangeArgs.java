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
import org.apache.expreval.expr.var.NamedParameter;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;
import org.apache.hadoop.hbase.hbql.statement.select.GetRequest;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.statement.select.ScanRequest;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public class KeyRangeArgs implements Serializable {

    private final List<Range> rangeList;
    private final List<NamedParameter> namedParamList = Lists.newArrayList();

    private enum Type {
        SINGLE, RANGE, FIRST, LAST, ALL
    }

    public static class Range extends SelectArgs {
        private final KeyRangeArgs.Type type;

        private Range() {
            super(SelectArgs.Type.NOARGSKEY);
            this.type = KeyRangeArgs.Type.ALL;
        }

        private Range(final KeyRangeArgs.Type type, final GenericValue arg0) {
            super(SelectArgs.Type.SINGLEKEY, arg0);
            this.type = type;
        }

        private Range(final GenericValue arg0, final GenericValue arg1) {
            super(SelectArgs.Type.KEYRANGE, arg0, arg1);
            this.type = KeyRangeArgs.Type.RANGE;
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
                sbuf.append("ALL");
            }
            else if (this.isSingleKey()) {
                sbuf.append("'" + this.getGenericValue(0).asString() + "'");
            }
            else if (this.isFirstRange()) {
                sbuf.append("FIRST TO '" + this.getGenericValue(0).asString());
            }
            else if (this.isLastRange()) {
                sbuf.append("'" + this.getGenericValue(0).asString() + "' TO LAST");
            }
            else {
                sbuf.append("'" + this.getGenericValue(0).asString() + "'");
                sbuf.append(" TO ");
                sbuf.append("'" + this.getGenericValue(1).asString() + "'");
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

        private List<RowRequest> getGet(final WithArgs withArgs,
                                        final Collection<ColumnAttrib> columnAttribSet) throws HBqlException {

            final List<RowRequest> retval = Lists.newArrayList();

            // Check if the value returned is a collection
            final Object objval = this.getFirstArg(true);
            if (TypeSupport.isACollection(objval)) {
                for (final GenericValue val : (Collection<GenericValue>)objval) {
                    try {
                        final String lower = (String)val.getValue(null, null);
                        retval.add(this.newGet(withArgs, columnAttribSet, lower));
                    }
                    catch (ResultMissingColumnException e) {
                        throw new InternalErrorException(val.asString());
                    }
                }
            }
            else {
                final String lower = (String)objval;
                retval.add(this.newGet(withArgs, columnAttribSet, lower));
            }

            return retval;
        }

        private RowRequest getScan(final WithArgs withArgs,
                                   final Collection<ColumnAttrib> columnAttribSet) throws HBqlException {

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

            withArgs.setScanArgs(scan, columnAttribSet);
            return new ScanRequest(scan);
        }

        public void process(final WithArgs withArgs,
                            final List<RowRequest> rowRequestList,
                            final Collection<ColumnAttrib> columnAttribSet) throws HBqlException {

            if (this.isSingleKey())
                rowRequestList.addAll(this.getGet(withArgs, columnAttribSet));
            else
                rowRequestList.add(this.getScan(withArgs, columnAttribSet));
        }
    }

    public KeyRangeArgs() {
        this(null);
    }

    public KeyRangeArgs(final List<Range> rangeList) {
        if (rangeList == null) {
            this.rangeList = Lists.newArrayList();
            this.getRangeList().add(newAllRange());
        }
        else {
            this.rangeList = rangeList;
        }
    }

    public static Range newRange(final GenericValue arg0, final GenericValue arg1) {
        return new Range(arg0, arg1);
    }

    public static Range newSingleKey(final GenericValue arg0) {
        return new Range(Type.SINGLE, arg0);
    }

    public static Range newFirstRange(final GenericValue arg0) {
        return new Range(Type.FIRST, arg0);
    }

    public static Range newLastRange(final GenericValue arg0) {
        return new Range(Type.LAST, arg0);
    }

    public static Range newAllRange() {
        return new Range();
    }

    public List<Range> getRangeList() {
        return this.rangeList;
    }

    public void setStatementContext(final StatementContext statementContext) {
        for (final Range range : this.getRangeList())
            range.setStatementContext(statementContext);

        for (final Range range : this.getRangeList())
            this.getParameterList().addAll(range.getParameterList());
    }

    public String asString() {
        final StringBuilder sbuf = new StringBuilder("KEYS ");
        boolean first = true;
        for (final Range range : this.getRangeList()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(range.asString());
            first = false;
        }
        return sbuf.toString();
    }

    public List<NamedParameter> getParameterList() {
        return this.namedParamList;
    }

    public void reset() {
        for (final Range range : this.getRangeList())
            range.reset();
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        int cnt = 0;
        for (final Range range : this.getRangeList())
            cnt += range.setParameter(name, val);
        return cnt;
    }
}
