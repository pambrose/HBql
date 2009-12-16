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

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.FunctionTypeSignature;
import org.apache.expreval.expr.GenericExpression;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.DoubleValue;
import org.apache.expreval.expr.node.FloatValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.IntegerValue;
import org.apache.expreval.expr.node.LongValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.ShortValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InvalidServerFilterExpressionException;
import org.apache.hadoop.hbase.hbql.client.InvalidTypeException;

import java.util.List;
import java.util.Random;

public abstract class GenericFunction extends GenericExpression {

    static Random randomVal = new Random();

    public static enum FunctionType {

        // Dealt with in DateFunction
        DATEINTERVAL(new FunctionTypeSignature(DateValue.class, LongValue.class), false, true),
        DATECONSTANT(new FunctionTypeSignature(DateValue.class), false, true),

        // Date functions
        DATE(new FunctionTypeSignature(DateValue.class, StringValue.class, StringValue.class), false, true),
        LONGTODATE(new FunctionTypeSignature(DateValue.class, LongValue.class), false, true),
        RANDOMDATE(new FunctionTypeSignature(DateValue.class), false, true),

        // String functions
        TRIM(new FunctionTypeSignature(StringValue.class, StringValue.class), false, true),
        LOWER(new FunctionTypeSignature(StringValue.class, StringValue.class), false, true),
        UPPER(new FunctionTypeSignature(StringValue.class, StringValue.class), false, true),
        CONCAT(new FunctionTypeSignature(StringValue.class, StringValue.class, StringValue.class), false, true),
        REPLACE(new FunctionTypeSignature(StringValue.class, StringValue.class, StringValue.class, StringValue.class), false, true),
        SUBSTRING(new FunctionTypeSignature(StringValue.class, StringValue.class, IntegerValue.class, IntegerValue.class), false, true),
        ZEROPAD(new FunctionTypeSignature(StringValue.class, LongValue.class, IntegerValue.class), false, true),
        REPEAT(new FunctionTypeSignature(StringValue.class, StringValue.class, IntegerValue.class), false, true),

        // Number functions
        LENGTH(new FunctionTypeSignature(IntegerValue.class, StringValue.class), false, true),
        INDEXOF(new FunctionTypeSignature(IntegerValue.class, StringValue.class, StringValue.class), false, true),

        DATETOLONG(new FunctionTypeSignature(LongValue.class, DateValue.class), false, true),

        SHORT(new FunctionTypeSignature(ShortValue.class, StringValue.class), false, true),
        INTEGER(new FunctionTypeSignature(IntegerValue.class, StringValue.class), false, true),
        LONG(new FunctionTypeSignature(LongValue.class, StringValue.class), false, true),
        FLOAT(new FunctionTypeSignature(FloatValue.class, StringValue.class), false, true),
        DOUBLE(new FunctionTypeSignature(DoubleValue.class, StringValue.class), false, true),

        COUNT(new FunctionTypeSignature(LongValue.class), true, false),
        MIN(new FunctionTypeSignature(NumberValue.class, NumberValue.class), true, false),
        MAX(new FunctionTypeSignature(NumberValue.class, NumberValue.class), true, false),

        ABS(new FunctionTypeSignature(NumberValue.class, NumberValue.class), false, true),
        LESSER(new FunctionTypeSignature(NumberValue.class, NumberValue.class, NumberValue.class), false, true),
        GREATER(new FunctionTypeSignature(NumberValue.class, NumberValue.class, NumberValue.class), false, true),

        RANDOMINTEGER(new FunctionTypeSignature(IntegerValue.class), false, false),
        RANDOMLONG(new FunctionTypeSignature(LongValue.class), false, false),
        RANDOMFLOAT(new FunctionTypeSignature(FloatValue.class), false, false),
        RANDOMDOUBLE(new FunctionTypeSignature(DoubleValue.class), false, false),

        // Boolean functions
        RANDOMBOOLEAN(new FunctionTypeSignature(BooleanValue.class), false, false),
        DEFINEDINROW(new FunctionTypeSignature(BooleanValue.class, GenericValue.class), false, false),
        EVAL(new FunctionTypeSignature(BooleanValue.class, StringValue.class), false, false),

        MAPPINGEXISTS(new FunctionTypeSignature(BooleanValue.class, StringValue.class), false, false),
        FAMILYEXISTS(new FunctionTypeSignature(BooleanValue.class, StringValue.class, StringValue.class), false, false),
        TABLEEXISTS(new FunctionTypeSignature(BooleanValue.class, StringValue.class), false, false),
        TABLEENABLED(new FunctionTypeSignature(BooleanValue.class, StringValue.class), false, false),
        INDEXEXISTSONTABLE(new FunctionTypeSignature(BooleanValue.class, StringValue.class, StringValue.class), false, false),
        INDEXEXISTSONMAPPING(new FunctionTypeSignature(BooleanValue.class, StringValue.class, StringValue.class), false, false);


        private final FunctionTypeSignature typeSignature;
        private final boolean anAggregateValue;
        private final boolean optimiziable;

        private FunctionType(final FunctionTypeSignature typeSignature, final boolean anAggregateValue, final boolean optimiziable) {
            this.typeSignature = typeSignature;
            this.anAggregateValue = anAggregateValue;
            this.optimiziable = optimiziable;
        }

        private FunctionTypeSignature getTypeSignature() {
            return this.typeSignature;
        }

        public boolean isAnAggregateValue() {
            return this.anAggregateValue;
        }

        public boolean isOptimiziable() {
            return this.optimiziable;
        }

        public static GenericFunction getFunction(final String functionName, final List<GenericValue> exprList) {

            final FunctionType type;

            try {
                type = FunctionType.valueOf(functionName.toUpperCase());
            }
            catch (IllegalArgumentException e) {
                return null;
            }

            final Class<? extends GenericValue> returnType = type.getTypeSignature().getReturnType();

            if (TypeSupport.isParentClass(BooleanValue.class, returnType))
                return new BooleanFunction(type, exprList);
            else if (TypeSupport.isParentClass(StringValue.class, returnType))
                return new StringFunction(type, exprList);
            else if (TypeSupport.isParentClass(NumberValue.class, returnType))
                return new NumberFunction(type, exprList);
            else if (TypeSupport.isParentClass(DateValue.class, returnType))
                return new DateFunction(type, exprList);

            return null;
        }
    }

    private final FunctionType functionType;

    public GenericFunction(final FunctionType functionType, final List<GenericValue> exprs) {
        super(null, exprs);
        this.functionType = functionType;
    }

    protected FunctionType getFunctionType() {
        return this.functionType;
    }

    protected FunctionTypeSignature getTypeSignature() {
        return this.getFunctionType().getTypeSignature();
    }

    protected boolean isIntervalDate() {
        return this.getFunctionType() == FunctionType.DATEINTERVAL;
    }

    public boolean isAnAggregateValue() {
        return this.getFunctionType().isAnAggregateValue();
    }

    protected boolean isConstantDate() {
        return this.getFunctionType() == FunctionType.DATECONSTANT;
    }

    public boolean isAConstant() {
        if (!this.getFunctionType().isOptimiziable())
            return false;

        return super.isAConstant();
    }


    protected void checkForNull(final String... vals) throws HBqlException {
        for (final Object val : vals) {
            if (val == null)
                throw new HBqlException("Null value in " + this.asString());
        }
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {

        int i = 0;
        if (this.getGenericValueList().size() != this.getTypeSignature().getArgCount())
            throw new InvalidTypeException("Incorrect number of arguments in function " + this.getFunctionType().name()
                                           + " in " + this.asString());

        for (final Class<? extends GenericValue> clazz : this.getTypeSignature().getArgs()) {
            final Class<? extends GenericValue> type = this.getExprArg(i).validateTypes(this, false);
            try {
                this.validateParentClass(clazz, type);
            }
            catch (InvalidTypeException e) {
                // Catch the exception and improve message
                throw new InvalidTypeException("Invalid type " + type.getSimpleName() + " for arg " + i + " in function "
                                               + this.getFunctionName() + " in "
                                               + this.asString() + ".  Expecting type " + clazz.getSimpleName() + ".");
            }
            i++;
        }

        return this.getTypeSignature().getReturnType();
    }

    protected String getFunctionName() {
        return this.getFunctionType().name();
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeAllArgs();
        if (!this.isAConstant())
            return this;
        else
            try {
                return this.getFunctionType().getTypeSignature().newLiteral(this.getValue(null, null));
            }
            catch (ResultMissingColumnException e) {
                throw new InternalErrorException();
            }
    }

    public String asString() {
        return this.getFunctionName() + super.asString();
    }

    public Filter getFilter() throws HBqlException {
        throw new InvalidServerFilterExpressionException();
    }
}