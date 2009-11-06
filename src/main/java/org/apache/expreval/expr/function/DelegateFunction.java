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

package org.apache.expreval.expr.function;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.DelegateStmt;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InvalidFunctionException;

import java.util.List;

public class DelegateFunction extends DelegateStmt<Function> {

    private final String functionName;

    public DelegateFunction(final String functionName, final List<GenericValue> exprList) {
        super(null, exprList);
        this.functionName = functionName;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        Function function = Function.FunctionType.getFunction(this.getFunctionName(), this.getArgList());

        if (function == null)
            function = DateFunction.IntervalType.getFunction(this.getFunctionName(), this.getArgList());

        if (function == null)
            function = DateFunction.ConstantType.getFunction(this.getFunctionName());

        if (function == null)
            throw new InvalidFunctionException(this.getFunctionName() + " in " + parentExpr.asString());

        this.setTypedExpr(function);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }

    public String getFunctionName() {
        return this.functionName;
    }

    public String asString() {
        return this.getFunctionName() + super.asString();
    }
}
