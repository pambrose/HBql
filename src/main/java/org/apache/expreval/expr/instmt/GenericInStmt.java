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

package org.apache.expreval.expr.instmt;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.NotValue;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.InvalidServerFilterException;

import java.util.List;

public abstract class GenericInStmt extends NotValue<GenericInStmt> implements BooleanValue {

    protected GenericInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(ExpressionType.INSTMT, not, arg0, inList);
    }

    protected abstract boolean evaluateInList(final Object object) throws HBqlException,
                                                                          ResultMissingColumnException,
                                                                          NullColumnValueException;

    protected List<GenericValue> getInList() {
        return this.getSubArgs(1);
    }

    public Boolean getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException,
                                                                                    NullColumnValueException {
        final boolean retval = this.evaluateInList(object);
        return (this.isNot()) ? !retval : retval;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {
        return BooleanValue.class;
    }

    public String asString() {
        final StringBuilder sbuf = new StringBuilder(this.getExprArg(0).asString() + notAsString() + " IN (");

        boolean first = true;
        for (final GenericValue valueExpr : this.getInList()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(valueExpr.asString());
            first = false;
        }
        sbuf.append(")");
        return sbuf.toString();
    }

    private boolean inListIsConstant() {
        for (final GenericValue valueExpr : this.getInList()) {
            if (!valueExpr.isAConstant())
                return false;
        }
        return true;
    }

    protected abstract static class GenericInComparable<T> extends WritableByteArrayComparable {

        private List<T> inValues;

        protected List<T> getInValues() {
            return this.inValues;
        }

        protected void setInValue(final List<T> inValues) {
            this.inValues = inValues;
        }
    }

    protected void validateArgsForInFilter() throws InvalidServerFilterException {
        if (this.getExprArg(0).isAColumnReference() && this.inListIsConstant())
            return;

        throw new InvalidServerFilterException("Filter requires a column reference and list of constants");
    }
}