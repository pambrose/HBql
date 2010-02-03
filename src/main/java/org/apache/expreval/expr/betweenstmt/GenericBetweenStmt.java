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

package org.apache.expreval.expr.betweenstmt;

import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.NotValue;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.hbql.impl.InvalidServerFilterException;

public abstract class GenericBetweenStmt extends NotValue<GenericBetweenStmt> implements BooleanValue {

    protected GenericBetweenStmt(final ExpressionType type,
                                 final boolean not,
                                 final GenericValue arg0,
                                 final GenericValue arg1,
                                 final GenericValue arg2) {
        super(type, not, arg0, arg1, arg2);
    }

    public String asString() {
        return this.getExprArg(0).asString() + notAsString() + " BETWEEN "
               + this.getExprArg(1).asString() + " AND " + this.getExprArg(2).asString();
    }

    protected abstract static class GenericBetweenComparable<T> implements WritableByteArrayComparable {

        private T lowerValue;
        private T upperValue;

        protected T getLowerValue() {
            return this.lowerValue;
        }

        protected T getUpperValue() {
            return this.upperValue;
        }

        protected void setLowerValue(final T lowerValue) {
            this.lowerValue = lowerValue;
        }

        protected void setUpperValue(final T upperValue) {
            this.upperValue = upperValue;
        }
    }

    protected void validateArgsForBetweenFilter() throws InvalidServerFilterException {

        if (this.getExprArg(0).isAColumnReference()
            && this.getExprArg(1).isAConstant()
            && this.getExprArg(2).isAConstant())
            return;

        throw new InvalidServerFilterException("Filter requires a column reference and two constants");
    }
}
