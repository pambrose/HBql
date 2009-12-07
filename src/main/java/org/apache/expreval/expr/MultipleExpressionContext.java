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

package org.apache.expreval.expr;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.ObjectValue;
import org.apache.expreval.expr.var.GenericColumn;
import org.apache.expreval.expr.var.NamedParameter;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InvalidTypeException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.HBaseTableMapping;
import org.apache.hadoop.hbase.hbql.mapping.HRecordResultAccessor;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.hbql.mapping.ResultAccessor;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class MultipleExpressionContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private boolean needsTypeValidation = true;
    private boolean needsOptimization = true;
    private boolean needsContextSetting = true;

    private final List<GenericColumn> columnsUsedInExpr = Lists.newArrayList();
    private final List<ColumnAttrib> attribsUsedInExpr = Lists.newArrayList();
    private final List<NamedParameter> namedParamList = Lists.newArrayList();
    private final Map<String, List<NamedParameter>> namedParamMap = Maps.newHashMap();

    private StatementContext statementContext = null;
    private final TypeSignature typeSignature;
    private final List<GenericValue> expressions = Lists.newArrayList();

    protected MultipleExpressionContext(final TypeSignature typeSignature, final GenericValue... vals) {
        this.typeSignature = typeSignature;
        if (vals != null) {
            for (final GenericValue val : vals)
                this.addExpression(val);
        }
    }

    public abstract String asString();

    public abstract boolean useResultData();

    public abstract boolean allowColumns();

    public List<GenericColumn> getColumnsUsedInExpression() {
        return this.columnsUsedInExpr;
    }

    public List<ColumnAttrib> getAttribsUsedInExpr() {
        return this.attribsUsedInExpr;
    }

    public void addExpression(final GenericValue genericValue) {
        this.getExpressionList().add(genericValue);
    }

    public Map<String, List<NamedParameter>> getNamedParamMap() {
        return this.namedParamMap;
    }

    protected List<GenericValue> getExpressionList() {
        return this.expressions;
    }

    private TypeSignature getTypeSignature() {
        return this.typeSignature;
    }

    public StatementContext getStatementContext() {
        return this.statementContext;
    }

    public Mapping getMapping() throws HBqlException {
        return this.getStatementContext().getMapping();
    }

    public HBaseTableMapping getHBaseTableMapping() {
        return this.getStatementContext().getHBaseTableMapping();
    }

    public ResultAccessor getResultAccessor() throws HBqlException {
        return this.getStatementContext().getResultAccessor();
    }

    public void setStatementContext(final StatementContext statementContext) throws HBqlException {
        this.statementContext = statementContext;

        if (this.getStatementContext() != null && this.getStatementContext().getResultAccessor() == null)
            this.getStatementContext().setResultAccessor(new HRecordResultAccessor(statementContext));

        this.setExpressionListContext();
    }

    private synchronized void setExpressionListContext() throws HBqlException {
        if (this.needsContextSetting()) {
            for (final GenericValue val : this.getExpressionList())
                val.setExpressionContext(this);
            this.setNeedsContextSetting(false);
        }
    }

    protected GenericValue getGenericValue(final int i) {
        return this.getExpressionList().get(i);
    }

    public Object evaluate(final HConnectionImpl connection,
                           final int i,
                           final boolean allowColumns,
                           final boolean allowCollections,
                           final Object object) throws HBqlException, ResultMissingColumnException {
        this.validateTypes(allowColumns, allowCollections);
        this.optimize();
        return this.getGenericValue(i).getValue(connection, object);
    }

    public Object evaluateConstant(final HConnectionImpl connection,
                                   final int i,
                                   final boolean allowCollections,
                                   final Object object) throws HBqlException {
        try {
            return this.evaluate(connection, i, false, allowCollections, object);
        }
        catch (ResultMissingColumnException e) {
            throw new InternalErrorException();
        }
    }

    public void reset() {

        this.setNeedsTypeValidation(true);
        this.setNeedsOptimization(true);

        for (final GenericValue val : this.getExpressionList())
            val.reset();
    }

    protected void setGenericValue(final int i, final GenericValue treeRoot) {
        this.getExpressionList().set(i, treeRoot);
    }

    public void optimize() throws HBqlException {
        if (this.needsOptimization()) {
            for (int i = 0; i < this.getExpressionList().size(); i++)
                this.setGenericValue(i, this.getGenericValue(i).getOptimizedValue());
            this.setNeedsOptimization(false);
        }
    }

    public void validateTypes(final boolean allowColumns,
                              final boolean allowCollections) throws HBqlException {

        if (this.needsTypeValidation()) {

            if (!allowColumns && this.getColumnsUsedInExpression().size() > 0)
                throw new InvalidTypeException("Invalid column reference"
                                               + (this.getColumnsUsedInExpression().size() > 1 ? "s" : "")
                                               + " in " + this.asString());

            // Collect return types of all args
            // This is run even if TypeSignature is null because it calls validateTypes()
            final List<Class<? extends GenericValue>> clazzList = Lists.newArrayList();
            for (final GenericValue val : this.getExpressionList()) {
                final Class<? extends GenericValue> returnType = val.validateTypes(null, allowCollections);
                clazzList.add(returnType);
            }

            // Check against signature if there is one
            if (this.getTypeSignature() != null) {

                if (this.getExpressionList().size() != this.getTypeSignature().getArgCount())
                    throw new InvalidTypeException("Incorrect number of variables in " + this.asString());

                for (int i = 0; i < this.getTypeSignature().getArgCount(); i++) {

                    final Class<? extends GenericValue> parentClazz = this.getTypeSignature().getArg(i);
                    final Class<? extends GenericValue> clazz = clazzList.get(i);

                    // See if they are both NumberValues.  If they are, then check ranks
                    if (TypeSupport.isParentClass(NumberValue.class, parentClazz, clazz)) {
                        final int parentRank = NumericType.getTypeRanking(parentClazz);
                        final int clazzRank = NumericType.getTypeRanking(clazz);
                        if (clazzRank > parentRank)
                            throw new InvalidTypeException("Cannot assign a " + clazz.getSimpleName()
                                                           + " value to a " + parentClazz.getSimpleName()
                                                           + " value in " + this.asString());
                    }
                    else if (parentClazz == ObjectValue.class) {
                        // Do nothing
                    }
                    else {
                        if (!parentClazz.isAssignableFrom(clazz))
                            throw new InvalidTypeException("Expecting type " + parentClazz.getSimpleName()
                                                           + " but found type " + clazz.getSimpleName()
                                                           + " in " + this.asString());
                    }
                }
            }

            this.setNeedsTypeValidation(false);
        }
    }

    public List<NamedParameter> getParameterList() {
        return this.namedParamList;
    }

    public void addNamedParameter(final NamedParameter param) {

        this.getParameterList().add(param);

        final String name = param.getParamName();
        final List<NamedParameter> paramList;

        if (!this.getNamedParamMap().containsKey(name)) {
            paramList = Lists.newArrayList();
            this.getNamedParamMap().put(name, paramList);
        }
        else {
            paramList = this.getNamedParamMap().get(name);
        }

        paramList.add(param);
    }

    public int setParameter(final String name, final Object val) throws HBqlException {

        final String fullname = name.startsWith(":") ? name : (":" + name);

        if (!this.getNamedParamMap().containsKey(fullname))
            return 0;

        // Set all occurences to param value
        final List<NamedParameter> paramList = this.getNamedParamMap().get(fullname);
        for (final NamedParameter param : paramList)
            param.setParameter(val);

        this.setNeedsTypeValidation(true);

        return paramList.size();
    }

    public void addColumnToUsedList(final GenericColumn column) {
        this.getColumnsUsedInExpression().add(column);
        this.getAttribsUsedInExpr().add(column.getColumnAttrib());
    }

    private boolean needsTypeValidation() {
        return needsTypeValidation;
    }

    private void setNeedsTypeValidation(final boolean inNeedOfTypeValidation) {
        this.needsTypeValidation = inNeedOfTypeValidation;
    }

    private boolean needsOptimization() {
        return needsOptimization;
    }

    private void setNeedsOptimization(final boolean inNeedOfOptimization) {
        this.needsOptimization = inNeedOfOptimization;
    }

    private boolean needsContextSetting() {
        return needsContextSetting;
    }

    private void setNeedsContextSetting(final boolean needsContextSetting) {
        this.needsContextSetting = needsContextSetting;
    }
}
