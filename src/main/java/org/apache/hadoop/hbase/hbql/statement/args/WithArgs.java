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

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.var.NamedParameter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.filter.RecordFilter;
import org.apache.hadoop.hbase.hbql.filter.RecordFilterList;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.hbql.mapping.MappingContext;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.hadoop.hbase.hbql.util.Sets;

import java.util.List;
import java.util.Set;

public class WithArgs {

    private String indexName = null;
    private KeyRangeArgs keyRangeArgs = null;
    private TimestampArgs timestampArgs = null;
    private VersionArgs versionArgs = null;
    private ScannerCacheArgs scannerCacheArgs = null;
    private LimitArgs limitArgs = null;
    private ExpressionTree clientExpressionTree = null;
    private ExpressionTree serverExpressionTree = null;

    private MappingContext mappingContext = null;

    // Keep track of args set multiple times
    private final Set<String> multipleSetValues = Sets.newHashSet();

    public void setMappingContext(final MappingContext mappingContext) throws HBqlException {

        this.mappingContext = mappingContext;

        this.validateNoDuplicateWithArgs();

        if (this.getKeyRangeArgs() == null)
            this.setKeyRangeArgs(new KeyRangeArgs());    // Default to ALL records

        this.getKeyRangeArgs().setMappingContext(this.getMappingContext());

        if (this.getTimestampArgs() != null)
            this.getTimestampArgs().setMappingContext(this.getMappingContext());

        if (this.getVersionArgs() != null)
            this.getVersionArgs().setMappingContext(this.getMappingContext());

        if (this.getLimitArgs() != null)
            this.getLimitArgs().setMappingContext(this.getMappingContext());

        if (this.getServerExpressionTree() != null) {
            this.getServerExpressionTree().setMappingContext(this.getMappingContext());
            this.getServerExpressionTree().setUseResultData(false);
        }

        if (this.getClientExpressionTree() != null) {
            this.getClientExpressionTree().setMappingContext(this.getMappingContext());
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

    private MappingContext getMappingContext() {
        return this.mappingContext;
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

    public void setGetArgs(final Get get, final Set<ColumnAttrib> columnAttribs) throws HBqlException {

        // Set column names
        // First add the columns and then add the families -- the order matters!
        // Do not bother to request key because it will always be returned
        for (final ColumnAttrib attrib : columnAttribs) {
            if (!attrib.isAKeyAttrib() && !attrib.isASelectFamilyAttrib())
                get.addColumn(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes());
        }

        for (final ColumnAttrib attrib : columnAttribs) {
            if (!attrib.isAKeyAttrib() && attrib.isASelectFamilyAttrib())
                get.addFamily(attrib.getFamilyNameAsBytes());
        }

        if (this.getTimestampArgs() != null)
            this.getTimestampArgs().setTimeStamp(get);

        if (this.getVersionArgs() != null)
            this.getVersionArgs().setMaxVersions(get);

        // Do not call scanner cache args call for get

        final Filter filter = this.getServerFilter(false);
        if (filter != null)
            get.setFilter(filter);
    }

    public void setScanArgs(final Scan scan, final Set<ColumnAttrib> columnAttribs) throws HBqlException {

        // Set column names
        // First add the columns and then add the families -- the order matters!
        // Do not bother to request key because it will always be returned
        for (final ColumnAttrib attrib : columnAttribs) {
            if (!attrib.isAKeyAttrib() && !attrib.isASelectFamilyAttrib())
                scan.addColumn(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes());
        }

        for (final ColumnAttrib attrib : columnAttribs) {
            if (!attrib.isAKeyAttrib() && attrib.isASelectFamilyAttrib())
                scan.addFamily(attrib.getFamilyNameAsBytes());
        }

        if (this.getTimestampArgs() != null)
            this.getTimestampArgs().setTimeStamp(scan);

        if (this.getVersionArgs() != null)
            this.getVersionArgs().setMaxVersions(scan);

        if (this.getScannerCacheArgs() != null)
            this.getScannerCacheArgs().setScannerCacheSize(scan);

        final Filter filter = this.getServerFilter(true);
        if (filter != null)
            scan.setFilter(filter);
    }

    private Filter getServerFilter(final boolean applyLimit) throws HBqlException {

        if (this.getServerExpressionTree() == null) {
            if (applyLimit && this.getLimit() > 0)
                return new PageFilter(this.getLimit());
            else
                return null;
        }
        else {
            Filter exprFilter;
            try {
                exprFilter = this.getServerExpressionTree().getFilter();
            }
            catch (HBqlException e) {
                // Use RecordFilter instead
                if (this.getServerExpressionTree() != null)
                    this.getServerExpressionTree().setMappingContext(this.getMappingContext());

                exprFilter = RecordFilter.newRecordFilter(this.getServerExpressionTree());
            }

            // Now apply PageFilter if limit was specified
            if (applyLimit && this.getLimit() > 0) {
                final Filter limitFilter = new PageFilter(this.getLimit());
                final List<Filter> filterList = Lists.newArrayList(limitFilter, exprFilter);
                return new RecordFilterList(RecordFilterList.Operator.MUST_PASS_ALL, filterList);
            }
            else {
                return exprFilter;
            }
        }
    }

    public int getMaxVersions() throws HBqlException {
        return this.getVersionArgs() != null ? this.getVersionArgs().getMaxVersions() : Integer.MAX_VALUE;
    }
}
