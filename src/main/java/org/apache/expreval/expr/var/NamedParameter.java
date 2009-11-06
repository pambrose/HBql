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

package org.apache.expreval.expr.var;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.literal.BooleanLiteral;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.expr.literal.DoubleLiteral;
import org.apache.expreval.expr.literal.FloatLiteral;
import org.apache.expreval.expr.literal.IntegerLiteral;
import org.apache.expreval.expr.literal.LongLiteral;
import org.apache.expreval.expr.literal.ObjectLiteral;
import org.apache.expreval.expr.literal.ShortLiteral;
import org.apache.expreval.expr.literal.StringLiteral;
import org.apache.expreval.expr.literal.StringNullLiteral;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;

import java.util.Collection;
import java.util.Date;
import java.util.List;

public class NamedParameter implements GenericValue {

    private MultipleExpressionContext context = null;
    private GenericValue typedExpr = null;
    private List<GenericValue> typedExprList = null;

    private final String paramName;

    public NamedParameter(final String paramName) {
        this.paramName = paramName;
    }

    public String getParamName() {
        return this.paramName;
    }

    private boolean isScalarValueSet() {
        return this.getTypedExpr() != null;
    }

    private GenericValue getTypedExpr() {
        return this.typedExpr;
    }

    private List<GenericValue> getTypedExprList() {
        return this.typedExprList;
    }

    public void reset() {
        this.typedExpr = null;
        this.typedExprList = null;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        if (this.getTypedExpr() == null && this.getTypedExprList() == null)
            throw new TypeException("Parameter " + this.getParamName() + " not assigned a value");

        if (this.isScalarValueSet()) {
            return this.getTypedExpr().getClass();
        }
        else {
            // Make sure a list is legal in this expr
            if (!allowsCollections)
                throw new TypeException("Parameter " + this.getParamName()
                                        + " is assigned a collection which is not allowed in the context "
                                        + parentExpr.asString());

            // if it is a list, then ensure that all the types in list are valid and consistent
            if (this.getTypedExprList().size() == 0)
                throw new TypeException("Parameter " + this.getParamName() + " is assigned a collection with no values");

            // Look at the type of the first item and then make sure the rest match that one
            final GenericValue firstval = this.getTypedExprList().get(0);
            final Class<? extends GenericValue> clazzToMatch = TypeSupport.getGenericExprType(firstval);

            for (final GenericValue val : this.getTypedExprList()) {

                final Class<? extends GenericValue> clazz = TypeSupport.getGenericExprType(val);

                if (clazz == null)
                    throw new TypeException("Parameter " + this.getParamName()
                                            + " assigned a collection value with invalid type "
                                            + firstval.getClass().getSimpleName());

                if (!clazz.equals(clazzToMatch))
                    throw new TypeException("Parameter " + this.getParamName()
                                            + " assigned a collection value with type "
                                            + firstval.getClass().getSimpleName()
                                            + " which is inconsistent with the type of the first element");
            }

            return clazzToMatch;
        }
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.isScalarValueSet())
            return this.getTypedExpr().getValue(object);
        else
            return this.getTypedExprList();
    }

    public void setExpressionContext(final MultipleExpressionContext context) {
        this.context = context;
        this.getContext().addNamedParameter(this);
    }

    private MultipleExpressionContext getContext() {
        return this.context;
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        return this;
    }

    public boolean isAConstant() {
        return false;
    }

    public boolean hasAColumnReference() {
        return false;
    }

    public boolean isDefaultKeyword() {
        return false;
    }

    public void setParameter(final Object val) throws HBqlException {

        // Reset both values
        this.reset();

        if (val != null && TypeSupport.isACollection(val)) {
            this.typedExprList = Lists.newArrayList();
            for (final Object elem : (Collection)val)
                this.getTypedExprList().add(this.getValueExpr(elem));
        }
        else {
            this.typedExpr = this.getValueExpr(val);
        }
    }

    private GenericValue getValueExpr(final Object val) throws TypeException {

        if (val == null)
            return new StringNullLiteral();

        if (val instanceof Boolean)
            return new BooleanLiteral((Boolean)val);

        if (val instanceof Character)
            return new ShortLiteral((short)((Character)val).charValue());

        if (val instanceof Short)
            return new ShortLiteral((Short)val);

        if (val instanceof Integer)
            return new IntegerLiteral((Integer)val);

        if (val instanceof Long)
            return new LongLiteral((Long)val);

        if (val instanceof Float)
            return new FloatLiteral((Float)val);

        if (val instanceof Double)
            return new DoubleLiteral((Double)val);

        if (val instanceof String)
            return new StringLiteral((String)val);

        if (val instanceof Date)
            return new DateLiteral((Date)val);

        if (val instanceof Object)
            return new ObjectLiteral(val);

        throw new TypeException("Parameter " + this.getParamName()
                                + " assigned an unsupported type " + val.getClass().getSimpleName());
    }

    public String asString() {
        return this.getParamName();
    }
}