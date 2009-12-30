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

package org.apache.expreval.expr.calculation;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public class DelegateCalculation extends GenericCalculation {

    public DelegateCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(null, arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {

        final Class<? extends GenericValue> type1 = this.getExprArg(0).validateTypes(this, false);
        final Class<? extends GenericValue> type2 = this.getExprArg(1).validateTypes(this, false);

        if (TypeSupport.isParentClass(StringValue.class, type1, type2))
            this.setTypedExpr(new StringCalculation(this.getExprArg(0), this.getOperator(), this.getExprArg(1)));
        else if (TypeSupport.isParentClass(NumberValue.class, type1, type2))
            this.setTypedExpr(new NumberCalculation(this.getExprArg(0), this.getOperator(), this.getExprArg(1)));
        else if (TypeSupport.isParentClass(DateValue.class, type1, type2))
            this.setTypedExpr(new DateCalculation(this.getExprArg(0), this.getOperator(), this.getExprArg(1)));
        else
            this.throwInvalidTypeException(type1, type2);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeAllArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Object getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                   ResultMissingColumnException,
                                                                                   NullColumnValueException {
        return this.getTypedExpr().getValue(conn, object);
    }
}