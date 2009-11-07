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

package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

public abstract class GenericLiteral<T> implements GenericValue {

    private final T value;

    public GenericLiteral(final T value) {
        this.value = value;
    }

    public T getValue(final Object object) {
        return this.value;
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        return this;
    }

    public boolean isAConstant() {
        return true;
    }

    public boolean isDefaultKeyword() {
        return false;
    }

    public boolean isAggregateValue() {
        return false;
    }

    public boolean hasAColumnReference() {
        return false;
    }

    public void reset() {

    }

    public void setExpressionContext(final MultipleExpressionContext context) {
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {
        return this.getReturnType();
    }

    protected abstract Class<? extends GenericValue> getReturnType();

    public String asString() {
        return "" + this.getValue(null);
    }
}
