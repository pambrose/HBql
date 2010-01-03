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

package org.apache.expreval.expr;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.literal.DoubleLiteral;
import org.apache.expreval.expr.literal.FloatLiteral;
import org.apache.expreval.expr.literal.IntegerLiteral;
import org.apache.expreval.expr.literal.LongLiteral;
import org.apache.expreval.expr.literal.ShortLiteral;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InvalidTypeException;
import org.apache.hadoop.hbase.hbql.impl.AggregateValue;
import org.apache.hadoop.hbase.hbql.impl.InvalidServerFilterExpressionException;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.util.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class GenericExpression implements GenericValue {

    // These are used to cache type of the args for exprs with numberic args
    private Class<? extends GenericValue> highestRankingNumericArgFoundInValidate = NumberValue.class;
    private Class rankingClass = null;
    private boolean useDecimal = false;
    private boolean useShort = false;
    private boolean useInteger = false;
    private boolean useLong = false;
    private boolean useFloat = false;
    private boolean useDouble = false;

    private final ExpressionType type;
    private final List<GenericValue> genericValueList = Lists.newArrayList();
    private final AtomicBoolean allArgsOptimized = new AtomicBoolean(false);

    private MultipleExpressionContext expressionContext = null;

    protected GenericExpression(final ExpressionType type, final GenericValue... exprs) {
        this(type, Arrays.asList(exprs));
    }

    protected GenericExpression(final ExpressionType type, final List<GenericValue> genericValueList) {
        this.type = type;
        if (genericValueList != null)
            this.getGenericValueList().addAll(genericValueList);
    }

    protected GenericExpression(final ExpressionType type,
                                final GenericValue expr,
                                final List<GenericValue> genericValueList) {
        this.type = type;
        this.getGenericValueList().add(expr);
        if (genericValueList != null)
            this.getGenericValueList().addAll(genericValueList);
    }

    protected FunctionTypeSignature getTypeSignature() {
        return this.type.getTypeSignature();
    }

    public List<GenericValue> getGenericValueList() {
        return this.genericValueList;
    }

    protected List<GenericValue> getSubArgs(final int i) {
        return this.getGenericValueList().subList(i, this.getGenericValueList().size());
    }

    private Class<? extends GenericValue> getHighestRankingNumericArgFoundInValidate() {
        return this.highestRankingNumericArgFoundInValidate;
    }

    // These require getHighestRankingNumericArg() be called first to set value
    protected boolean useDecimal() {
        return this.useDecimal;
    }

    protected Number getValueWithCast(final long result) throws HBqlException {
        if (this.useShort)
            return (short)result;
        else if (this.useInteger)
            return (int)result;
        else if (this.useLong)
            return result;
        else
            throw new HBqlException("Invalid class: " + rankingClass.getName());
    }

    protected Number getValueWithCast(final double result) throws HBqlException {
        if (this.useFloat)
            return (float)result;
        else if (this.useDouble)
            return result;
        else
            throw new HBqlException("Invalid class: " + rankingClass.getName());
    }

    protected Class validateNumericArgTypes(final Object... objs) {

        if (rankingClass == null) {

            // If we do not already know the specific types, then look at the class of both args
            if (this.getHighestRankingNumericArgFoundInValidate() == NumberValue.class)
                this.rankingClass = NumericType.getHighestRankingNumericArg(objs);
            else
                this.rankingClass = this.getHighestRankingNumericArgFoundInValidate();

            this.useDecimal = NumericType.useDecimalNumericArgs(rankingClass);

            this.useShort = NumericType.isAShort(rankingClass);
            this.useInteger = NumericType.isAnInteger(rankingClass);
            this.useLong = NumericType.isALong(rankingClass);
            this.useFloat = NumericType.isAFloat(rankingClass);
            this.useDouble = NumericType.isADouble(rankingClass);
        }

        return this.rankingClass;
    }

    public boolean isAConstant() {

        if (this.getGenericValueList().size() == 0)
            return false;

        for (final GenericValue val : this.getGenericValueList())
            if (!val.isAConstant())
                return false;

        return true;
    }

    public boolean isDefaultKeyword() {
        return false;
    }

    public boolean isAnAggregateValue() {
        return false;
    }

    public void initAggregateValue(final AggregateValue aggregateValue) throws HBqlException {
        throw new InternalErrorException("Not applicable");
    }

    public void applyResultToAggregateValue(final AggregateValue aggregateValue, final Result result) throws HBqlException, ResultMissingColumnException, NullColumnValueException {
        throw new InternalErrorException("Not applicable");
    }

    public boolean hasAColumnReference() {
        for (final GenericValue val : this.getGenericValueList())
            if (val.hasAColumnReference())
                return true;
        return false;
    }

    public boolean isAColumnReference() {
        return false;
    }

    public void reset() {
        for (final GenericValue val : this.getGenericValueList())
            val.reset();
    }

    public void setExpressionContext(final MultipleExpressionContext expressionContext) throws HBqlException {

        this.expressionContext = expressionContext;

        for (final GenericValue val : this.getGenericValueList())
            val.setExpressionContext(expressionContext);
    }

    protected MultipleExpressionContext getExpressionContext() {
        return this.expressionContext;
    }

    private AtomicBoolean getAllArgsOptimized() {
        return this.allArgsOptimized;
    }

    protected void optimizeAllArgs() throws HBqlException {
        if (!this.getAllArgsOptimized().get())
            synchronized (this) {
                if (!this.getAllArgsOptimized().get()) {
                    for (int i = 0; i < this.getGenericValueList().size(); i++)
                        this.setArg(i, this.getExprArg(i).getOptimizedValue());
                    this.getAllArgsOptimized().set(true);
                }
            }
    }

    protected Filter newSingleColumnValueFilter(final ColumnAttrib attrib,
                                                final CompareFilter.CompareOp compareOp,
                                                final WritableByteArrayComparable comparator) throws HBqlException {

        // Bail if expression uses a row key.
        if (attrib.isAKeyAttrib())
            throw new InvalidServerFilterExpressionException("Cannot use a key attribute");

        final SingleColumnValueFilter filter = new SingleColumnValueFilter(attrib.getFamilyNameAsBytes(),
                                                                           attrib.getColumnNameAsBytes(),
                                                                           compareOp,
                                                                           comparator);
        filter.setFilterIfMissing(true);
        return filter;
    }

    protected Object getConstantValue(final int pos) throws HBqlException {
        try {
            return this.getExprArg(pos).getValue(null, null);
        }
        catch (ResultMissingColumnException e) {
            throw new InternalErrorException("Missing column: " + e.getMessage());
        }
        catch (NullColumnValueException e) {
            throw new InternalErrorException("Null value: " + e.getMessage());
        }
    }

    public GenericValue getExprArg(final int i) {
        return this.getGenericValueList().get(i);
    }

    public void setArg(final int i, final GenericValue val) {
        this.getGenericValueList().set(i, val);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {

        if (this.getGenericValueList().size() != this.getTypeSignature().getArgCount())
            throw new InvalidTypeException("Incorrect number of arguments in " + this.asString());

        for (int i = 0; i < this.getTypeSignature().getArgCount(); i++)
            this.validateParentClass(this.getTypeSignature().getArg(i), this.getExprArg(i).validateTypes(this, false));

        return this.getTypeSignature().getReturnType();
    }

    protected Class<? extends GenericValue> validateNumericTypes() throws HBqlException {

        if (this.getGenericValueList().size() != this.getTypeSignature().getArgCount())
            throw new InvalidTypeException("Incorrect number of arguments in " + this.asString());

        // Return the type of the highest ranking numeric arg
        int highestRank = -1;
        for (int i = 0; i < this.getTypeSignature().getArgCount(); i++) {

            final Class<? extends GenericValue> clazz = this.getExprArg(i).validateTypes(this, false);
            this.validateParentClass(this.getTypeSignature().getArg(i), clazz);

            final int rank = NumericType.getTypeRanking(clazz);
            if (rank > highestRank) {
                highestRank = rank;
                this.highestRankingNumericArgFoundInValidate = clazz;
            }
        }

        return this.getHighestRankingNumericArgFoundInValidate();
    }

    public GenericValue getOptimizedValue() throws HBqlException {

        this.optimizeAllArgs();

        if (!this.isAConstant())
            return this;

        try {
            final Object obj = this.getValue(null, null);

            if (this.getTypeSignature().getReturnType() == BooleanValue.class
                || this.getTypeSignature().getReturnType() == StringValue.class
                || this.getTypeSignature().getReturnType() == DateValue.class)
                return this.getTypeSignature().newLiteral(obj);

            if (TypeSupport.isParentClass(NumberValue.class, this.getTypeSignature().getReturnType())) {

                if (obj instanceof Short)
                    return new ShortLiteral((Short)obj);

                if (obj instanceof Integer)
                    return new IntegerLiteral((Integer)obj);

                if (obj instanceof Long)
                    return new LongLiteral((Long)obj);

                if (obj instanceof Float)
                    return new FloatLiteral((Float)obj);

                if (obj instanceof Double)
                    return new DoubleLiteral((Double)obj);
            }
            throw new InternalErrorException(this.getTypeSignature().getReturnType().getSimpleName());
        }
        catch (ResultMissingColumnException e) {
            throw new InternalErrorException("Missing column: " + e.getMessage());
        }
        catch (NullColumnValueException e) {
            throw new InternalErrorException("Null value: " + e.getMessage());
        }
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder("(");

        boolean first = true;
        for (final GenericValue val : this.getGenericValueList()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(val.asString());
            first = false;
        }

        sbuf.append(")");

        return sbuf.toString();
    }

    public void validateParentClass(final Class<? extends GenericValue> parentClazz,
                                    final Class<? extends GenericValue>... clazzes) throws InvalidTypeException {

        List<Class<? extends GenericValue>> classList = Lists.newArrayList();

        for (final Class<? extends GenericValue> clazz : clazzes) {

            if (clazz != null) {
                if (TypeSupport.isParentClass(NumberValue.class, parentClazz)) {
                    if (!TypeSupport.isParentClass(NumberValue.class, clazz)) {
                        classList.add(clazz);
                    }
                    else {
                        if (!NumericType.isAssignable(parentClazz, clazz))
                            classList.add(clazz);
                    }
                }
                else {
                    if (!parentClazz.isAssignableFrom(clazz))
                        classList.add(clazz);
                }
            }
        }

        if (classList.size() > 0) {
            final StringBuilder sbuf = new StringBuilder("Expecting type " + parentClazz.getSimpleName()
                                                         + " but encountered type"
                                                         + ((classList.size() > 1) ? "s" : "") + " ");
            boolean first = true;
            for (final Class clazz : classList) {
                if (!first)
                    sbuf.append(", ");
                sbuf.append(clazz.getSimpleName());
                first = false;
            }

            sbuf.append(" in expression: " + this.asString());

            throw new InvalidTypeException(sbuf.toString());
        }
    }

    public void throwInvalidTypeException(final Class<? extends GenericValue>... clazzes) throws InvalidTypeException {

        final List<Class> classList = Lists.newArrayList();

        for (final Class clazz : clazzes)
            if (clazz != null)
                classList.add(clazz);

        final StringBuilder sbuf = new StringBuilder("Invalid type");
        sbuf.append(((classList.size() > 1) ? "s " : " "));

        boolean first = true;
        for (final Class<? extends GenericValue> clazz : clazzes) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(clazz.getSimpleName());
            first = false;
        }
        sbuf.append(" in expression " + this.asString());

        throw new InvalidTypeException(sbuf.toString());
    }

    protected Class<? extends GenericValue> determineGenericValueClass(final Class<? extends GenericValue> clazz) throws InvalidTypeException {

        final List<Class<? extends GenericValue>> types = Arrays.asList(StringValue.class,
                                                                        NumberValue.class,
                                                                        DateValue.class,
                                                                        BooleanValue.class);

        for (final Class<? extends GenericValue> type : types)
            if (TypeSupport.isParentClass(type, clazz))
                return type;

        this.throwInvalidTypeException(clazz);
        return null;
    }

    public Filter getFilter() throws HBqlException {
        throw new InvalidServerFilterExpressionException();
    }
}
