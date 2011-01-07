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

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.InvalidTypeException;

import java.util.List;

public class DelegateInStmt extends GenericInStmt {

    public DelegateInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(arg0, not, inList);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {

        final Class<? extends GenericValue> type = this.getExprArg(0).validateTypes(this, false);

        final Class<? extends GenericValue> inType = this.getGenericValueClass(type);

        // Make sure all the types are matched
        for (final GenericValue val : this.getInList())
            this.validateParentClass(inType, val.validateTypes(this, true));

        if (TypeSupport.isParentClass(StringValue.class, type))
            this.setTypedExpr(new StringInStmt(this.getExprArg(0), this.isNot(), this.getInList()));
        else if (TypeSupport.isParentClass(NumberValue.class, type))
            this.setTypedExpr(new NumberInStmt(this.getExprArg(0), this.isNot(), this.getInList()));
        else if (TypeSupport.isParentClass(DateValue.class, type))
            this.setTypedExpr(new DateInStmt(this.getExprArg(0), this.isNot(), this.getInList()));
        else if (TypeSupport.isParentClass(BooleanValue.class, type))
            this.setTypedExpr(new BooleanInStmt(this.getExprArg(0), this.isNot(), this.getInList()));
        else
            throw new InvalidTypeException(this.getInvalidTypeMsg(type));

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    protected boolean evaluateInList(final Object object) throws HBqlException {
        throw new InternalErrorException();
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeAllArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Boolean getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException,
                                                                                    NullColumnValueException {
        return this.getTypedExpr().getValue(conn, object);
    }

    public Filter getFilter() throws HBqlException {
        return this.getTypedExpr().getFilter();
    }
}