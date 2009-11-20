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
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.schema.HRecordMapping;
import org.apache.hadoop.hbase.hbql.schema.Mapping;
import org.apache.hadoop.hbase.hbql.schema.Schema;
import org.apache.hadoop.hbase.hbql.statement.SchemaContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class MultipleExpressionContext implements Serializable {

    private boolean inNeedOfTypeValidation = true;
    private boolean inNeedOfOptimization = true;
    private boolean inNeedOfSettingContext = true;

    private final List<GenericColumn> columnsUsedInExpr = Lists.newArrayList();
    private final List<ColumnAttrib> attribsUsedInExpr = Lists.newArrayList();
    private final List<NamedParameter> namedParamList = Lists.newArrayList();
    private final Map<String, List<NamedParameter>> namedParamMap = Maps.newHashMap();

    private SchemaContext schemaContext = null;
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

    public SchemaContext getSchemaContext() {
        return this.schemaContext;
    }

    public Schema getSchema() throws HBqlException {
        return this.getSchemaContext().getSchema();
    }

    public HBaseSchema getHBaseSchema() throws HBqlException {
        return this.getSchemaContext().getHBaseSchema();
    }

    public Mapping getMapping() throws HBqlException {
        return this.getSchemaContext().getMapping();
    }

    public void setSchemaContext(final SchemaContext schemaContext) {
        this.schemaContext = schemaContext;

        if (this.getSchemaContext() != null && this.getSchemaContext().getMapping() == null)
            this.getSchemaContext().setMapping(new HRecordMapping(schemaContext));

        this.setContext();
    }

    protected GenericValue getGenericValue(final int i) {
        return this.getExpressionList().get(i);
    }

    public Object evaluate(final int i,
                           final boolean allowColumns,
                           final boolean allowCollections,
                           final Object object) throws HBqlException, ResultMissingColumnException {
        this.validateTypes(allowColumns, allowCollections);
        this.optimize();
        return this.getGenericValue(i).getValue(object);
    }

    public Object evaluateConstant(final int i,
                                   final boolean allowCollections,
                                   final Object object) throws HBqlException {
        try {
            return this.evaluate(i, false, allowCollections, object);
        }
        catch (ResultMissingColumnException e) {
            throw new InternalErrorException();
        }
    }

    protected void setContext() {
        if (this.isInNeedOfSettingContext()) {
            try {
                for (final GenericValue val : this.getExpressionList())
                    val.setExpressionContext(this);
            }
            catch (HBqlException e) {
                //  TODO This needs addressing
                e.printStackTrace();
            }
            this.setInNeedOfSettingContext(false);
        }
    }

    public void reset() {

        this.setInNeedOfTypeValidation(true);
        this.setInNeedOfOptimization(true);

        for (final GenericValue val : this.getExpressionList())
            val.reset();
    }

    protected void setGenericValue(final int i, final GenericValue treeRoot) {
        this.getExpressionList().set(i, treeRoot);
    }

    private void optimize() throws HBqlException {
        if (this.isInNeedOfOptimization()) {
            for (int i = 0; i < this.getExpressionList().size(); i++)
                this.setGenericValue(i, this.getGenericValue(i).getOptimizedValue());
            this.setInNeedOfOptimization(false);
        }
    }

    public void validateTypes(final boolean allowColumns, final boolean allowCollections) throws HBqlException {

        if (this.isInNeedOfTypeValidation()) {

            if (!allowColumns && this.getColumnsUsedInExpression().size() > 0)
                throw new TypeException("Invalid column reference"
                                        + (this.getColumnsUsedInExpression().size() > 1 ? "s" : "")
                                        + " in " + this.asString());

            // Collect return types of all args
            final List<Class<? extends GenericValue>> clazzList = Lists.newArrayList();
            for (final GenericValue val : this.getExpressionList())
                clazzList.add(val.validateTypes(null, allowCollections));

            // Check against signature if there is one
            if (this.getTypeSignature() != null) {

                if (this.getExpressionList().size() != this.getTypeSignature().getArgCount())
                    throw new TypeException("Incorrect number of variables in " + this.asString());

                for (int i = 0; i < this.getTypeSignature().getArgCount(); i++) {

                    final Class<? extends GenericValue> parentClazz = this.getTypeSignature().getArg(i);
                    final Class<? extends GenericValue> clazz = clazzList.get(i);

                    // See if they are both NumberValues.  If they are, then check ranks
                    if (TypeSupport.isParentClass(NumberValue.class, parentClazz, clazz)) {
                        final int parentRank = NumericType.getTypeRanking(parentClazz);
                        final int clazzRank = NumericType.getTypeRanking(clazz);
                        if (clazzRank > parentRank)
                            throw new TypeException("Cannot assign a " + clazz.getSimpleName()
                                                    + " value to a " + parentClazz.getSimpleName()
                                                    + " value in " + this.asString());
                    }
                    else if (parentClazz == ObjectValue.class) {
                        // Do nothing
                    }
                    else {
                        if (!parentClazz.isAssignableFrom(clazz))
                            throw new TypeException("Expecting type " + parentClazz.getSimpleName()
                                                    + " but found type " + clazz.getSimpleName()
                                                    + " in " + this.asString());
                    }
                }
            }

            this.setInNeedOfTypeValidation(false);
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

        this.setInNeedOfTypeValidation(true);

        return paramList.size();
    }

    public void addColumnToUsedList(final GenericColumn column) {
        this.getColumnsUsedInExpression().add(column);
        this.getAttribsUsedInExpr().add(column.getColumnAttrib());
    }

    private boolean isInNeedOfTypeValidation() {
        return inNeedOfTypeValidation;
    }

    private void setInNeedOfTypeValidation(final boolean inNeedOfTypeValidation) {
        this.inNeedOfTypeValidation = inNeedOfTypeValidation;
    }

    private boolean isInNeedOfOptimization() {
        return inNeedOfOptimization;
    }

    private void setInNeedOfOptimization(final boolean inNeedOfOptimization) {
        this.inNeedOfOptimization = inNeedOfOptimization;
    }

    private boolean isInNeedOfSettingContext() {
        return inNeedOfSettingContext;
    }

    private void setInNeedOfSettingContext(final boolean inNeedOfSettingContext) {
        this.inNeedOfSettingContext = inNeedOfSettingContext;
    }
}
