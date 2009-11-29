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

import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.List;

public abstract class DelegateStmt<T extends GenericExpression> extends GenericExpression {

    private T typedExpr = null;

    protected DelegateStmt(final ExpressionType type, GenericValue... args) {
        super(type, args);
    }

    protected DelegateStmt(final ExpressionType type, final List<GenericValue> args) {
        super(type, args);
    }

    protected DelegateStmt(final ExpressionType type, final GenericValue arg, final List<GenericValue> argList) {
        super(type, arg, argList);
    }

    protected T getTypedExpr() {
        return this.typedExpr;
    }

    protected void setTypedExpr(final T typedExpr) throws HBqlException {
        this.typedExpr = typedExpr;

        this.getTypedExpr().setExpressionContext(this.getExpressionContext());
    }
}
