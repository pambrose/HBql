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

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.var.NamedParameter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.filter.HBqlFilter;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.hadoop.hbase.hbql.util.Sets;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class WithArgs implements Serializable {

    private static final long serialVersionUID = 1L;

    private String indexName = null;
    private KeyRangeArgs keyRangeArgs = null;
    private TimestampArgs timestampArgs = null;
    private VersionArgs versionArgs = null;
    private ScannerCacheArgs scannerCacheArgs = null;
    private LimitArgs limitArgs = null;
    private ExpressionTree clientExpressionTree = null;
    private ExpressionTree serverExpressionTree = null;

    private StatementContext statementContext = null;

    // Keep track of args set multiple times
    private final Set<String> multipleSetValues = Sets.newHashSet();

    public void setStatementContext(final StatementContext statementContext) throws HBqlException {

        this.statementContext = statementContext;

        this.validateNoDuplicateWithArgs();

        if (this.getKeyRangeArgs() == null)
            this.setKeyRangeArgs(new KeyRangeArgs());    // Default to ALL records

        this.getKeyRangeArgs().setStatementContext(this.getStatementContext());

        if (this.getTimestampArgs() != null)
            this.getTimestampArgs().setStatementContext(this.getStatementContext());

        if (this.getVersionArgs() != null)
            this.getVersionArgs().setStatementContext(this.getStatementContext());

        if (this.getLimitArgs() != null)
            this.getLimitArgs().setStatementContext(this.getStatementContext());

        if (this.getServerExpressionTree() != null) {
            this.getServerExpressionTree().setStatementContext(this.getStatementContext());
            this.getServerExpressionTree().setUseResultData(false);
        }

        if (this.getClientExpressionTree() != null) {
            this.getClientExpressionTree().setStatementContext(this.getStatementContext());
            this.getClientExpressionTree().setUseResultData(true);
        }
    }

    public void validate(final HConnectionImpl conn, final TableMapping mapping) throws HBqlException {
        if (conn != null) {
            if (this.hasAnIndex())
                conn.validateIndexExistsForTable(this.getIndexName(), mapping.getTableName());
        }
    }

    public void validateArgTypes() throws HBqlException {

        if (this.getKeyRangeArgs() != null)
            this.getKeyRangeArgs().validate();

        if (this.getTimestampArgs() != null)
            this.getTimestampArgs().validate();

        if (this.getVersionArgs() != null)
            this.getVersionArgs().validate();

        if (this.getLimitArgs() != null)
            this.getLimitArgs().validate();
    }


    private void validateNoDuplicateWithArgs() throws HBqlException {
        if (this.multipleSetValues.size() > 0) {
            final StringBuilder sbuf = new StringBuilder();
            boolean firstTime = true;
            for (final String str : this.multipleSetValues) {
                if (!firstTime)
                    sbuf.append(", ");
                sbuf.append(str);
                firstTime = false;
            }
            throw new HBqlException("Select args specificed multiple times: " + sbuf);
        }
    }

    private StatementContext getStatementContext() {
        return this.statementContext;
    }

    private void addError(final String str) {
        this.multipleSetValues.add(str);
    }

    public KeyRangeArgs getKeyRangeArgs() {
        return this.keyRangeArgs;
    }

    public void setIndexName(final String indexName) {
        this.indexName = indexName;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public boolean hasAnIndex() {
        return Utils.isValidString(this.getIndexName());
    }

    public void setKeyRangeArgs(final KeyRangeArgs keyRangeArgs) {
        if (this.getKeyRangeArgs() != null)
            this.addError("Keys");
        this.keyRangeArgs = keyRangeArgs;
    }

    private TimestampArgs getTimestampArgs() {
        return this.timestampArgs;
    }

    public void setTimestampArgs(final TimestampArgs timestampArgs) {
        if (this.getTimestampArgs() != null)
            this.addError("Time Range");
        this.timestampArgs = timestampArgs;
    }

    private VersionArgs getVersionArgs() {
        return this.versionArgs;
    }

    public void setVersionArgs(final VersionArgs versionArgs) {
        if (this.getVersionArgs() != null)
            this.addError("Version");
        this.versionArgs = versionArgs;
    }

    private ScannerCacheArgs getScannerCacheArgs() {
        return this.scannerCacheArgs;
    }

    public void setScannerCacheArgs(final ScannerCacheArgs scannerCacheArgs) {
        if (this.getVersionArgs() != null)
            this.addError("Scanner_Cache_Size");
        this.scannerCacheArgs = scannerCacheArgs;
    }

    public LimitArgs getLimitArgs() {
        return this.limitArgs;
    }

    public void setLimitArgs(final LimitArgs limitArgs) {
        if (this.getLimitArgs() != null)
            this.addError("Limit");
        this.limitArgs = limitArgs;
    }

    public ExpressionTree getClientExpressionTree() {
        return this.clientExpressionTree;
    }

    public void setClientExpressionTree(final ExpressionTree clientExpressionTree) {
        if (this.getClientExpressionTree() != null)
            this.addError("Client Where");
        this.clientExpressionTree = clientExpressionTree;
    }

    public ExpressionTree getServerExpressionTree() {
        return this.serverExpressionTree;
    }

    public void setServerExpressionTree(final ExpressionTree serverExpressionTree) {
        if (this.getServerExpressionTree() != null)
            this.addError("Server Where");
        this.serverExpressionTree = serverExpressionTree;
    }

    public long getLimit() throws HBqlException {
        return (this.getLimitArgs() != null) ? this.getLimitArgs().getValue() : 0;
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder("WITH ");

        if (this.getKeyRangeArgs() != null)
            sbuf.append(this.getKeyRangeArgs().asString() + "\n");

        if (this.getTimestampArgs() != null)
            sbuf.append(this.getTimestampArgs().asString() + "\n");

        if (this.getVersionArgs() != null)
            sbuf.append(this.getVersionArgs().asString() + "\n");

        if (this.getScannerCacheArgs() != null)
            sbuf.append(this.getScannerCacheArgs().asString() + "\n");

        if (this.getLimitArgs() != null)
            sbuf.append(this.getLimitArgs().asString() + "\n");

        if (this.getServerExpressionTree() != null)
            sbuf.append("SERVER FILTER " + this.getServerExpressionTree().asString() + "\n");

        if (this.getClientExpressionTree() != null)
            sbuf.append("CLIENT FILTER " + this.getClientExpressionTree().asString() + "\n");

        return sbuf.toString();
    }

    public List<NamedParameter> getParameterList() {

        final List<NamedParameter> parameterList = Lists.newArrayList();

        if (this.getKeyRangeArgs() != null)
            parameterList.addAll(this.getKeyRangeArgs().getParameterList());

        if (this.getTimestampArgs() != null)
            parameterList.addAll(this.getTimestampArgs().getParameterList());

        if (this.getVersionArgs() != null)
            parameterList.addAll(this.getVersionArgs().getParameterList());

        if (this.getScannerCacheArgs() != null)
            parameterList.addAll(this.getScannerCacheArgs().getParameterList());

        if (this.getLimitArgs() != null)
            parameterList.addAll(this.getLimitArgs().getParameterList());

        if (this.getServerExpressionTree() != null)
            parameterList.addAll(this.getServerExpressionTree().getParameterList());

        if (this.getClientExpressionTree() != null)
            parameterList.addAll(this.getClientExpressionTree().getParameterList());

        return parameterList;
    }

    public void reset() {

        if (this.getKeyRangeArgs() != null)
            this.getKeyRangeArgs().reset();

        if (this.getTimestampArgs() != null)
            this.getTimestampArgs().reset();

        if (this.getVersionArgs() != null)
            this.getVersionArgs().reset();

        if (this.getScannerCacheArgs() != null)
            this.getScannerCacheArgs().reset();

        if (this.getLimitArgs() != null)
            this.getLimitArgs().reset();

        if (this.getServerExpressionTree() != null)
            this.getServerExpressionTree().reset();

        if (this.getClientExpressionTree() != null)
            this.getClientExpressionTree().reset();
    }

    public int setParameter(final String name, final Object val) throws HBqlException {

        int cnt = 0;

        if (this.getKeyRangeArgs() != null)
            cnt += this.getKeyRangeArgs().setParameter(name, val);

        if (this.getTimestampArgs() != null)
            cnt += this.getTimestampArgs().setParameter(name, val);

        if (this.getVersionArgs() != null)
            cnt += this.getVersionArgs().setParameter(name, val);

        if (this.getScannerCacheArgs() != null)
            cnt += this.getScannerCacheArgs().setParameter(name, val);

        if (this.getLimitArgs() != null)
            cnt += this.getLimitArgs().setParameter(name, val);

        if (this.getServerExpressionTree() != null)
            cnt += this.getServerExpressionTree().setParameter(name, val);

        if (this.getClientExpressionTree() != null)
            cnt += this.getClientExpressionTree().setParameter(name, val);

        return cnt;
    }

    public Filter getFilterForIndex() throws HBqlException {
        if (this.getServerExpressionTree() != null)
            return this.getServerExpressionTree().getFilter();
        else
            return null;
    }

    public byte[][] getColumnsUsedInIndexWhereExpr() {
        final byte[][] indexColumns;
        final Set<ColumnAttrib> columnAttribs = this.getColumnsUsedInServerWhereExpr();
        if (columnAttribs.size() == 0) {
            indexColumns = null;
        }
        else {
            final List<String> columnList = Lists.newArrayList();
            for (final ColumnAttrib columnAttrib : columnAttribs) {
                // Ignore keys
                if (!columnAttrib.isAKeyAttrib())
                    columnList.add(columnAttrib.isASelectFamilyAttrib() ? columnAttrib.getFamilyName()
                                                                        : columnAttrib.getFamilyQualifiedName());
            }

            indexColumns = Util.getStringsAsBytes(columnList);
        }
        return indexColumns;
    }

    private Set<ColumnAttrib> getColumnsUsedInServerWhereExpr() {
        final Set<ColumnAttrib> serverAttribs = Sets.newHashSet();
        if (this.getServerExpressionTree() != null)
            serverAttribs.addAll(this.getServerExpressionTree().getAttribsUsedInExpr());
        return serverAttribs;
    }

    private Set<ColumnAttrib> getColumnsUsedInClientWhereExpr() {
        final Set<ColumnAttrib> clientAttribs = Sets.newHashSet();
        if (this.getClientExpressionTree() != null)
            clientAttribs.addAll(this.getClientExpressionTree().getAttribsUsedInExpr());
        return clientAttribs;
    }

    public Set<ColumnAttrib> getColumnsUsedInAllWhereExprs() {
        final Set<ColumnAttrib> allAttribs = Sets.newHashSet();
        allAttribs.addAll(this.getColumnsUsedInServerWhereExpr());
        allAttribs.addAll(this.getColumnsUsedInClientWhereExpr());
        return allAttribs;
    }


    public List<RowRequest> getRowRequestList(final HConnectionImpl conn,
                                              final Mapping mapping,
                                              final Set<ColumnAttrib> columnAttribs) throws HBqlException {

        final ColumnAttrib keyAttrib;

        if (this.hasAnIndex()) {
            // Need to look up the index
            final IndexSpecification index = conn.getIndexForTable(this.getIndexName(), mapping.getTableName());
            final byte[][] cols = index.getIndexedColumns();
            final String indexedColumName = new String(cols[0]);
            keyAttrib = mapping.getAttribByVariableName(indexedColumName);
        }
        else {
            keyAttrib = mapping.getKeyAttrib();
        }

        final List<RowRequest> retval = Lists.newArrayList();

        for (final KeyRange keyRange : this.getKeyRangeArgs().getKeyRangeList()) {
            final List<RowRequest> rowRequestList = keyRange.getRowRequestList(this, keyAttrib, columnAttribs);
            retval.addAll(rowRequestList);
        }

        return retval;
    }

    public void setGetArgs(final Get get, final Set<ColumnAttrib> columnAttribSet) throws HBqlException {

        // Set column names
        for (final ColumnAttrib attrib : columnAttribSet) {

            // Do not bother to request because it will always be returned
            if (attrib.isAKeyAttrib())
                continue;

            // If it is a map, then request all columns for family
            if (attrib.isASelectFamilyAttrib())
                get.addFamily(attrib.getFamilyNameAsBytes());
            else
                get.addColumn(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes());
        }

        if (this.getTimestampArgs() != null)
            this.getTimestampArgs().setTimeStamp(get);

        if (this.getVersionArgs() != null)
            this.getVersionArgs().setMaxVersions(get);

        // Do not call scanner cache args call for get

        if (this.getServerExpressionTree() != null)
            get.setFilter(this.getServerFilter());
    }

    public void setScanArgs(final Scan scan, final Set<ColumnAttrib> columnAttribs) throws HBqlException {

        // Set column names
        for (final ColumnAttrib attrib : columnAttribs) {

            // Do not bother to request because it will always be returned
            if (attrib.isAKeyAttrib())
                continue;

            if (attrib.isASelectFamilyAttrib())
                scan.addFamily(attrib.getFamilyNameAsBytes());
            else
                scan.addColumn(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes());
        }

        if (this.getTimestampArgs() != null)
            this.getTimestampArgs().setTimeStamp(scan);

        if (this.getVersionArgs() != null)
            this.getVersionArgs().setMaxVersions(scan);

        if (this.getScannerCacheArgs() != null)
            this.getScannerCacheArgs().setScannerCacheSize(scan);

        if (this.getServerExpressionTree() != null)
            scan.setFilter(this.getServerFilter());
    }

    private Filter getServerFilter() throws HBqlException {

        try {
            return this.getServerExpressionTree().getFilter();
        }
        catch (HBqlException e) {
            // Try HBqlFilter instead
            if (this.getServerExpressionTree() != null)
                this.getServerExpressionTree().setStatementContext(this.getStatementContext());
            return HBqlFilter.newHBqlFilter(this.getServerExpressionTree());
        }
    }

    public int getMaxVersions() throws HBqlException {
        return this.getVersionArgs() != null ? this.getVersionArgs().getMaxVersions() : Integer.MAX_VALUE;
    }
}
