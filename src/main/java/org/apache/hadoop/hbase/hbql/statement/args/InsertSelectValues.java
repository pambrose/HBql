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

import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.NamedParameter;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.impl.Query;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.select.SelectElement;
import org.apache.hadoop.hbase.hbql.statement.select.SingleExpressionContext;

import java.util.Iterator;
import java.util.List;

public class InsertSelectValues extends InsertValueSource {

    private final SelectStatement selectStatement;
    private Iterator<HRecord> resultsIterator = null;
    private HRecord currentRecord = null;


    public InsertSelectValues(final SelectStatement selectStatement) {
        this.selectStatement = selectStatement;
    }

    public SelectStatement getSelectStatement() {
        return this.selectStatement;
    }

    public List<NamedParameter> getParameterList() {
        return this.getSelectStatement().getNamedParameters().getParameterList();
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        return this.getSelectStatement().setParameter(name, val);
    }

    public void validate() throws HBqlException {

        for (final SelectElement element : this.getSelectStatement().getSelectElementList()) {
            if (element.isAFamilySelect())
                throw new TypeException("Family select items are not valid in INSERT statement");
        }

        this.getSelectStatement().validate(this.getInsertStatement().getConnection());
    }

    private Iterator<HRecord> getResultsIterator() {
        return this.resultsIterator;
    }

    private void setResultsIterator(final Iterator<HRecord> resultsIterator) {
        this.resultsIterator = resultsIterator;
    }

    private HRecord getCurrentRecord() {
        return this.currentRecord;
    }

    private void setCurrentRecord(final HRecord currentRecord) {
        this.currentRecord = currentRecord;
    }

    public void execute() throws HBqlException {
        final Query<HRecord> query = Query.newQuery(this.getInsertStatement().getConnection(),
                                                    this.getSelectStatement(),
                                                    HRecord.class);
        final HResultSet<HRecord> results = query.getResults();
        this.setResultsIterator(results.iterator());
    }

    public List<Class<? extends GenericValue>> getValuesTypeList() throws HBqlException {
        final List<Class<? extends GenericValue>> typeList = Lists.newArrayList();
        for (final SelectElement element : this.getSelectStatement().getSelectElementList()) {
            if (element instanceof SingleExpressionContext) {
                final SingleExpressionContext expr = (SingleExpressionContext)element;
                final Class<? extends GenericValue> type = expr.getExpressionType();
                typeList.add(type);
            }
        }
        return typeList;
    }

    public void reset() {
        this.getSelectStatement().reset();
    }

    public String asString() {
        return this.getSelectStatement().asString();
    }

    public Object getValue(final int i) throws HBqlException {
        final SelectElement element = this.getSelectStatement().getSelectElementList().get(i);
        final String name = element.getElementName();
        return this.getCurrentRecord().getCurrentValue(name);
    }

    public boolean isDefaultValue(final int i) throws HBqlException {
        return false;
    }

    public boolean hasValues() {
        if (this.getResultsIterator().hasNext()) {
            this.setCurrentRecord(this.getResultsIterator().next());
            return true;
        }
        else {
            return false;
        }
    }
}