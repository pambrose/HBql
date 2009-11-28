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

package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.util.ArrayList;

public class DelegateCase extends GenericCase {

    public DelegateCase() {
        super(null, new ArrayList<GenericCaseWhen>(), null);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {

        final Class<? extends GenericValue> type = this.getWhenExprList().get(0).validateTypes(this, false);
        final Class<? extends GenericValue> argType = this.determineGenericValueClass(type);

        for (final GenericCaseWhen val : this.getWhenExprList())
            this.validateParentClass(argType, val.validateTypes(this, false));

        if (this.getElseExpr() != null)
            this.validateParentClass(argType, this.getElseExpr().validateTypes(parentExpr, false));

        if (TypeSupport.isParentClass(StringValue.class, argType))
            this.setTypedExpr(new StringCase(this.getWhenExprList(), this.getElseExpr()));
        else if (TypeSupport.isParentClass(NumberValue.class, argType))
            this.setTypedExpr(new NumberCase(this.getWhenExprList(), this.getElseExpr()));
        else if (TypeSupport.isParentClass(DateValue.class, argType))
            this.setTypedExpr(new DateCase(this.getWhenExprList(), this.getElseExpr()));
        else if (TypeSupport.isParentClass(BooleanValue.class, argType))
            this.setTypedExpr(new BooleanCase(this.getWhenExprList(), this.getElseExpr()));
        else
            this.throwInvalidTypeException(argType);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeAllArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Object getValue(final HConnectionImpl connection,
                           final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(connection, object);
    }

    public void addWhen(final GenericValue pred, final GenericValue value) {
        this.getWhenExprList().add(new DelegateCaseWhen(pred, value));
    }

    public void addElse(final GenericValue value) {
        if (value != null)
            this.setElseExpr(new DelegateCaseElse(value));
    }
}