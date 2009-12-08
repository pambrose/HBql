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

package org.apache.expreval.expr.compare;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.GenericExpression;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.literal.BooleanLiteral;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.GenericColumn;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InvalidServerFilterExpressionException;
import org.apache.hadoop.hbase.hbql.client.InvalidTypeException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public abstract class GenericCompare extends GenericExpression implements BooleanValue {

    private final Operator operator;

    protected GenericCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(null, arg0, arg1);
        this.operator = operator;
    }

    protected Operator getOperator() {
        return this.operator;
    }

    protected Object getValue(final int pos,
                              final HConnectionImpl connection,
                              final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getExprArg(pos).getValue(connection, object);
    }

    protected void validateArgsForFilter() throws InvalidServerFilterExpressionException {
        // One of the values must be a single column reference and the other a constant
        if ((this.getExprArg(0).isAColumnReference() && this.getExprArg(1).isAConstant())
            || (this.getExprArg(0).isAConstant() && this.getExprArg(1).isAColumnReference()))
            return;

        throw new InvalidServerFilterExpressionException("Filter comparison requires a column reference and a constant expression");
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeAllArgs();
        if (!this.isAConstant())
            return this;
        else
            try {
                return new BooleanLiteral(this.getValue(null, null));
            }
            catch (ResultMissingColumnException e) {
                throw new InternalErrorException();
            }
    }

    protected Class<? extends GenericValue> validateType(final Class<? extends GenericValue> clazz) throws InvalidTypeException {
        try {
            this.validateParentClass(clazz,
                                     this.getExprArg(0).validateTypes(this, false),
                                     this.getExprArg(1).validateTypes(this, false));
        }
        catch (HBqlException e) {
            e.printStackTrace();
        }

        return BooleanValue.class;
    }

    protected Filter newSingleColumnValueFilter(final GenericColumn column,
                                                final CompareFilter.CompareOp compareOp,
                                                final WritableByteArrayComparable comparator) throws HBqlException {

        return new SingleColumnValueFilter(column.getColumnAttrib().getFamilyNameAsBytes(),
                                           column.getColumnAttrib().getColumnNameAsBytes(),
                                           compareOp,
                                           comparator);
    }

    public String asString() {
        final StringBuilder sbuf = new StringBuilder();
        sbuf.append(this.getExprArg(0).asString());
        sbuf.append(" " + this.getOperator() + " ");
        sbuf.append(this.getExprArg(1).asString());
        return sbuf.toString();
    }
}