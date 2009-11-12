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

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Sets;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.Query;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.client.ResultSet;
import org.apache.hadoop.hbase.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.schema.HRecordMapping;
import org.apache.hadoop.hbase.hbql.schema.Mapping;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class QueryImpl<T> implements Query<T> {

    private final HConnection connection;
    private final SelectStatement selectStatement;
    private final Mapping mapping;

    private List<QueryListener<T>> listeners = null;

    public QueryImpl(final HConnection connection,
                     final SelectStatement selectStatement,
                     final Mapping mapping) throws HBqlException {
        this.connection = connection;
        this.selectStatement = selectStatement;
        this.mapping = mapping != null ? mapping : new HRecordMapping(selectStatement.getSchema());
    }

    public QueryImpl(final HConnection connection,
                     final String query,
                     final Mapping mapping) throws HBqlException {
        this(connection, HBqlShell.parseSelectStatement(connection, query), mapping);
    }

    public synchronized void addListener(final QueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    public HConnection getConnection() {
        return this.connection;
    }

    public Mapping getMapping() {
        return this.mapping;
    }

    public SelectStatement getSelectStatement() {
        return this.selectStatement;
    }

    public List<RowRequest> getRowRequestList() throws HBqlException, IOException {

        final WithArgs withArgs = this.getSelectStatement().getWithArgs();

        // Get list of all columns that are used in select list and expr tree
        final Set<ColumnAttrib> allAttribs = Sets.newHashSet();
        allAttribs.addAll(this.getSelectStatement().getSelectAttribList());
        allAttribs.addAll(withArgs.getAllColumnsUsedInExprs());

        return withArgs.getRowRequestList(allAttribs);
    }

    public List<QueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void setParameter(final String name, final Object val) throws HBqlException {
        int cnt = this.getSelectStatement().setParameter(name, val);
        if (cnt == 0)
            throw new HBqlException("Parameter name " + name + " does not exist in "
                                    + this.getSelectStatement().asString());
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }

    public ResultSet<T> getResults() throws HBqlException {

        // Set it once per evaluation
        DateLiteral.resetNow();

        if (this.getListeners() != null) {
            for (final QueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }

        this.getSelectStatement().determineIfAggregateQuery();

        return new ResultsImpl<T>(this);
    }

    public List<T> getResultList() throws HBqlException {

        final List<T> retval = Lists.newArrayList();

        ResultSet<T> results = null;

        try {
            results = this.getResults();

            for (T val : results)
                retval.add(val);
        }
        finally {
            if (results != null)
                results.close();
        }

        return retval;
    }
}
