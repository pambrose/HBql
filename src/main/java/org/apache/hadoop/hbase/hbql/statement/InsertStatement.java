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

package org.apache.hadoop.hbase.hbql.statement;

import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.literal.DefaultKeyword;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.statement.args.InsertValueSource;
import org.apache.hadoop.hbase.hbql.statement.select.SingleExpressionContext;

import java.util.List;

public class InsertStatement extends MappingContext implements ParameterStatement, ConnectionStatement {

    private final List<SingleExpressionContext> columnList = Lists.newArrayList();
    private final InsertValueSource insertValuesSource;
    private final NamedParameters namedParameters = new NamedParameters();

    private transient HConnectionImpl connection = null;
    private HRecord record = null;
    private boolean validated = false;

    public InsertStatement(final String schemaName,
                           final List<GenericValue> columnList,
                           final InsertValueSource insertValuesSource) {
        super(schemaName);

        for (final GenericValue val : columnList)
            this.getInsertColumnList().add(SingleExpressionContext.newSingleExpression(val, null));

        this.insertValuesSource = insertValuesSource;
        this.getInsertValuesSource().setInsertStatement(this);
    }

    public NamedParameters getNamedParameters() {
        return this.namedParameters;
    }

    private boolean isValidated() {
        return this.validated;
    }

    public void validate(final HConnectionImpl connection) throws HBqlException {

        if (this.isValidated())
            return;
        else
            this.validated = true;

        this.connection = connection;
        this.validateMappingName(this.getConnection());
        this.record = this.getConnection().getSchema(this.getMappingName()).newHRecord();

        for (final SingleExpressionContext element : this.getInsertColumnList()) {

            element.validate(this, this.getConnection());

            if (!element.isASimpleColumnReference())
                throw new TypeException(element.asString() + " is not a column reference in " + this.asString());
        }

        if (!this.hasAKeyValue())
            throw new TypeException("Missing a key value in attribute list in " + this.asString());

        this.getInsertValuesSource().validate();

        this.collectParameters();
    }

    public void validateTypes() throws HBqlException {

        final List<Class<? extends GenericValue>> columnsTypeList = this.getColumnsTypeList();
        final List<Class<? extends GenericValue>> valuesTypeList = this.getInsertValuesSource().getValuesTypeList();

        if (columnsTypeList.size() != valuesTypeList.size())
            throw new HBqlException("Number of columns not equal to number of values in " + this.asString());

        for (int i = 0; i < columnsTypeList.size(); i++) {

            final Class<? extends GenericValue> type1 = columnsTypeList.get(i);
            final Class<? extends GenericValue> type2 = valuesTypeList.get(i);

            // Skip Default values
            if (type2 == DefaultKeyword.class) {
                final String name = this.getInsertColumnList().get(i).asString();
                final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);
                if (!attrib.hasDefaultArg())
                    throw new HBqlException("No DEFAULT value specified for " + attrib.getNameToUseInExceptions()
                                            + " in " + this.asString());
                continue;
            }

            if (!TypeSupport.isParentClass(type1, type2))
                throw new TypeException("Type mismatch in argument " + i
                                        + " expecting " + type1.getSimpleName()
                                        + " but found " + type2.getSimpleName()
                                        + " in " + this.asString());
        }
    }

    private List<Class<? extends GenericValue>> getColumnsTypeList() throws HBqlException {
        final List<Class<? extends GenericValue>> typeList = Lists.newArrayList();
        for (final SingleExpressionContext element : this.getInsertColumnList()) {
            final Class<? extends GenericValue> type = element.getExpressionType();
            typeList.add(type);
        }
        return typeList;
    }

    private boolean hasAKeyValue() {
        for (final SingleExpressionContext element : this.getInsertColumnList()) {
            if (element.isAKeyValue())
                return true;
        }
        return false;
    }

    private void collectParameters() {
        this.getNamedParameters().addParameters(this.getInsertValuesSource().getParameterList());
    }

    public void reset() {
        this.getInsertValuesSource().reset();
        this.getHRecord().reset();
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        final int cnt = this.getInsertValuesSource().setParameter(name, val);
        if (cnt == 0)
            throw new HBqlException("Parameter name " + name + " does not exist in " + this.asString());
        return cnt;
    }

    private HRecord getHRecord() {
        return this.record;
    }

    public HConnectionImpl getConnection() {
        return this.connection;
    }

    private List<SingleExpressionContext> getInsertColumnList() {
        return this.columnList;
    }

    private InsertValueSource getInsertValuesSource() {
        return this.insertValuesSource;
    }

    public ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        this.validate(connection);

        this.validateTypes();

        int cnt = 0;

        this.getInsertValuesSource().execute();

        while (this.getInsertValuesSource().hasValues()) {

            final HBatch batch = new HBatch(connection);

            for (int i = 0; i < this.getInsertColumnList().size(); i++) {
                final String name = this.getInsertColumnList().get(i).asString();
                final Object val;
                if (this.getInsertValuesSource().isDefaultValue(i)) {
                    final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);
                    val = attrib.getDefaultValue();
                }
                else {
                    val = this.getInsertValuesSource().getValue(i);
                }
                this.getHRecord().setCurrentValue(name, val);
            }

            batch.insert(this.getHRecord());

            batch.apply();
            cnt++;
        }

        final ExecutionResults results = new ExecutionResults(cnt + " record" + ((cnt > 1) ? "s" : "") + " inserted");
        results.setCount(cnt);
        return results;
    }

    public ExecutionResults execute() throws HBqlException {
        return this.execute(this.getConnection());
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("INSERT INTO ");
        sbuf.append(this.getMappingName());
        sbuf.append(" (");

        boolean firstTime = true;
        for (final SingleExpressionContext val : this.getInsertColumnList()) {
            if (!firstTime)
                sbuf.append(", ");
            firstTime = false;

            sbuf.append(val.asString());
        }

        sbuf.append(") ");
        sbuf.append(this.getInsertValuesSource().asString());
        return sbuf.toString();
    }
}