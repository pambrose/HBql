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

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.QueryListener;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;

import java.util.List;

public abstract class HResultSetImpl<T> implements HResultSet<T> {

    protected int maxVersions = 0;

    private final List<ResultScanner> resultScannerList = Lists.newArrayList();
    private ResultScanner currentResultScanner = null;

    private final Query<T> query;
    private final ExpressionTree clientExpressionTree;
    private HTableWrapper tableWrapper;
    private volatile boolean closed = false;

    protected HResultSetImpl(final Query<T> query) throws HBqlException {
        this.query = query;

        this.clientExpressionTree = getWithArgs().getClientExpressionTree();
        this.tableWrapper = getHConnectionImpl().newHTableWrapper(getWithArgs(), getTableName());

        this.getQuery().getSelectStmt().determineIfAggregateQuery();

        // Set it once per evaluation
        DateLiteral.resetNow();

        if (this.getListeners() != null) {
            for (final QueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }
    }

    protected List<ResultScanner> getResultScannerList() {
        return this.resultScannerList;
    }

    protected ResultScanner getCurrentResultScanner() {
        return this.currentResultScanner;
    }

    protected void setCurrentResultScanner(final ResultScanner currentResultScanner) {
        // First close previous ResultScanner before reassigning
        closeResultScanner(getCurrentResultScanner(), true);
        this.currentResultScanner = currentResultScanner;
        getResultScannerList().add(getCurrentResultScanner());
    }

    protected void closeResultScanner(final ResultScanner scanner, final boolean removeFromList) {
        if (scanner != null) {
            try {
                scanner.close();
            }
            catch (Exception e) {
                // Do nothing
            }

            if (removeFromList)
                getResultScannerList().remove(scanner);
        }
    }

    public void close() {
        if (!this.closed) {
            synchronized (this) {
                if (!this.closed) {
                    for (final ResultScanner scanner : this.getResultScannerList())
                        closeResultScanner(scanner, false);
                    this.getResultScannerList().clear();
                    this.closed = true;
                }
            }
        }
    }

    protected int getMaxVersions() {
        return this.maxVersions;
    }

    protected void setMaxVersions(final int maxVersions) {
        this.maxVersions = maxVersions;
    }

    protected Query<T> getQuery() {
        return this.query;
    }

    protected ExpressionTree getClientExpressionTree() {
        return this.clientExpressionTree;
    }

    protected HTableWrapper getHTableWrapper() {
        return this.tableWrapper;
    }

    protected void setTableWrapper(final HTableWrapper tableWrapper) {
        this.tableWrapper = tableWrapper;
    }

    public void addQueryListener(QueryListener<T> listener) {
        this.getQuery().addListener(listener);
    }

    public void clearQueryListeners() {
        this.getQuery().clearListeners();
    }

    protected HConnectionImpl getHConnectionImpl() {
        return this.getQuery().getHConnectionImpl();
    }

    protected SelectStatement getSelectStmt() {
        return this.getQuery().getSelectStmt();
    }

    protected String getTableName() {
        return this.getSelectStmt().getMapping().getTableName();
    }

    protected WithArgs getWithArgs() {
        return this.getSelectStmt().getWithArgs();
    }

    protected List<QueryListener<T>> getListeners() {
        return this.getQuery().getListeners();
    }
}
