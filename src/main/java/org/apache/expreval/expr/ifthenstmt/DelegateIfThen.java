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

package org.apache.expreval.expr.ifthenstmt;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public class DelegateIfThen extends GenericIfThen {

    public DelegateIfThen(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(null, arg0, arg1, arg2);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {

        this.validateParentClass(BooleanValue.class, this.getExprArg(0).validateTypes(this, false));

        final Class<? extends GenericValue> type1 = this.getExprArg(1).validateTypes(this, false);
        final Class<? extends GenericValue> type2 = this.getExprArg(2).validateTypes(this, false);

        if (TypeSupport.isParentClass(StringValue.class, type1, type2))
            this.setTypedExpr(new StringIfThen(this.getExprArg(0), this.getExprArg(1), this.getExprArg(2)));
        else if (TypeSupport.isParentClass(NumberValue.class, type1, type2))
            this.setTypedExpr(new NumberIfThen(this.getExprArg(0), this.getExprArg(1), this.getExprArg(2)));
        else if (TypeSupport.isParentClass(DateValue.class, type1, type2))
            this.setTypedExpr(new DateIfThen(this.getExprArg(0), this.getExprArg(1), this.getExprArg(2)));
        else if (TypeSupport.isParentClass(BooleanValue.class, type1, type2))
            this.setTypedExpr(new BooleanIfThen(this.getExprArg(0), this.getExprArg(1), this.getExprArg(2)));
        else
            this.throwInvalidTypeException(type1, type2);

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
}