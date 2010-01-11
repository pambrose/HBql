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

package org.apache.hadoop.hbase.hbql.statement;

import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.function.DelegateFunction;
import org.apache.expreval.expr.literal.DefaultKeyword;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.InvalidTypeException;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.statement.args.InsertValueSource;
import org.apache.hadoop.hbase.hbql.statement.select.SelectExpressionContext;
import org.apache.hadoop.hbase.hbql.util.Lists;

import java.util.List;

public class InsertStatement extends StatementWithMapping implements ParameterStatement, ConnectionStatement {

    private final NamedParameters namedParameters = new NamedParameters();
    private final List<SelectExpressionContext> columnList = Lists.newArrayList();
    private final InsertValueSource insertValuesSource;

    private HConnectionImpl connection = null;
    private boolean validated = false;
    private HRecord record = null;
    private String invalidInsertColumn = null;

    public InsertStatement(final StatementPredicate predicate,
                           final String mappingName,
                           final List<GenericValue> columnList,
                           final InsertValueSource insertValuesSource) {
        super(predicate, mappingName);

        for (final GenericValue val : columnList) {
            // See if a group of columns are indicated with family(col1, col2), which looks like a function call
            if (val instanceof DelegateFunction) {
                final DelegateFunction function = (DelegateFunction)val;
                final String familyName = function.getFunctionName();
                for (final GenericValue columnarg : function.getGenericValueList()) {
                    if (columnarg instanceof DelegateColumn) {
                        final String columnName = ((DelegateColumn)columnarg).getVariableName();
                        final DelegateColumn col = new DelegateColumn(familyName + ":" + columnName);
                        this.getInsertColumnList().add(SelectExpressionContext.newExpression(col, null));
                    }
                    else {
                        // Throw exception in validate()
                        if (invalidInsertColumn == null)
                            invalidInsertColumn = columnarg.asString();
                    }
                }
            }
            else {
                this.getInsertColumnList().add(SelectExpressionContext.newExpression(val, null));
            }
        }

        this.insertValuesSource = insertValuesSource;
        this.getInsertValuesSource().setInsertStatement(this);
    }

    public NamedParameters getNamedParameters() {
        return this.namedParameters;
    }

    private boolean isValidated() {
        return this.validated;
    }

    public void validate(final HConnectionImpl conn) throws HBqlException {

        if (this.isValidated())
            return;
        else
            this.validated = true;

        if (this.invalidInsertColumn != null)
            throw new InvalidTypeException(this.invalidInsertColumn + " is not a column reference in " + this.asString());

        this.connection = conn;
        this.getMappingContext().validateMappingName(this.getConnection());
        this.record = this.getConnection().getMapping(this.getMappingContext().getMappingName()).newHRecord();

        for (final SelectExpressionContext element : this.getInsertColumnList()) {

            element.validate(this.getMappingContext(), this.getConnection());

            if (!element.isADelegateColumnReference())
                throw new InvalidTypeException(element.asString() + " is not a column reference in " + this.asString());
        }

        if (!this.hasAKeyValue())
            throw new InvalidTypeException("Missing a key value in attribute list in " + this.asString());

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
                final ColumnAttrib attrib = this.getMappingContext().getMapping().getAttribByVariableName(name);
                if (!attrib.hasDefaultArg())
                    throw new HBqlException("No DEFAULT value specified for " + attrib.getNameToUseInExceptions()
                                            + " in " + this.asString());
                continue;
            }

            if (!TypeSupport.isParentClass(type1, type2))
                throw new InvalidTypeException("Type mismatch in argument " + i
                                               + " expecting " + type1.getSimpleName()
                                               + " but found " + type2.getSimpleName()
                                               + " in " + this.asString());
        }
    }

    private List<Class<? extends GenericValue>> getColumnsTypeList() throws HBqlException {
        final List<Class<? extends GenericValue>> typeList = Lists.newArrayList();
        for (final SelectExpressionContext element : this.getInsertColumnList()) {
            final Class<? extends GenericValue> type = element.getExpressionType();
            typeList.add(type);
        }
        return typeList;
    }

    private boolean hasAKeyValue() {
        for (final SelectExpressionContext element : this.getInsertColumnList()) {
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

    private List<SelectExpressionContext> getInsertColumnList() {
        return this.columnList;
    }

    private InsertValueSource getInsertValuesSource() {
        return this.insertValuesSource;
    }

    protected ExecutionResults execute(final HConnectionImpl conn) throws HBqlException {

        this.validate(conn);

        this.validateTypes();

        int cnt = 0;

        this.getInsertValuesSource().execute();

        while (this.getInsertValuesSource().hasValues()) {

            final HBatch<HRecord> batch = HBatch.newHBatch(conn);

            for (int i = 0; i < this.getInsertColumnList().size(); i++) {
                final String name = this.getInsertColumnList().get(i).asString();
                final Object val;
                if (this.getInsertValuesSource().isDefaultValue(i)) {
                    final ColumnAttrib attrib = this.getMappingContext().getMapping().getAttribByVariableName(name);
                    val = attrib.getDefaultValue();
                }
                else {
                    val = this.getInsertValuesSource().getValue(conn, i);
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

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("INSERT INTO ");
        sbuf.append(this.getMappingContext().getMappingName());
        sbuf.append(" (");

        boolean firstTime = true;
        for (final SelectExpressionContext val : this.getInsertColumnList()) {
            if (!firstTime)
                sbuf.append(", ");
            firstTime = false;

            sbuf.append(val.asString());
        }

        sbuf.append(") ");
        sbuf.append(this.getInsertValuesSource().asString());
        return sbuf.toString();
    }

    public static String usage() {
        return "INSERT INTO [MAPPING] mapping_name (column_name_list) insert_values [IF bool_expr]";
    }
}