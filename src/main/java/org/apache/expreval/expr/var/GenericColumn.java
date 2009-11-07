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

package org.apache.expreval.expr.var;

import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.ObjectValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.schema.FieldType;

public abstract class GenericColumn<T extends GenericValue> implements GenericValue {

    private final ColumnAttrib columnAttrib;
    private MultipleExpressionContext expressionContext = null;

    protected GenericColumn(final ColumnAttrib attrib) {
        this.columnAttrib = attrib;
    }

    protected FieldType getFieldType() {
        return this.getColumnAttrib().getFieldType();
    }

    public ColumnAttrib getColumnAttrib() {
        return this.columnAttrib;
    }

    public String getVariableName() {
        return this.getColumnAttrib().getFamilyQualifiedName();
    }

    public T getOptimizedValue() throws HBqlException {
        return (T)this;
    }

    public boolean isAConstant() {
        return false;
    }

    public boolean isDefaultKeyword() {
        return false;
    }

    public boolean isAggregateValue() {
        return false;
    }

    public boolean hasAColumnReference() {
        return true;
    }

    public void reset() {
        if (this.getExprContext() != null)
            this.getExprContext().reset();
    }

    public void setExpressionContext(final MultipleExpressionContext context) throws HBqlException {
        this.expressionContext = context;
        this.getExprContext().addColumnToUsedList(this);
    }

    protected MultipleExpressionContext getExprContext() {
        return this.expressionContext;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        if (this.getColumnAttrib().isAnArray())
            return ObjectValue.class;
        else
            return this.getFieldType().getExprType();
    }

    public String asString() {
        return this.getVariableName();
    }
}
