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

import org.apache.expreval.expr.node.ByteValue;
import org.apache.expreval.expr.node.DoubleValue;
import org.apache.expreval.expr.node.FloatValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.IntegerValue;
import org.apache.expreval.expr.node.LongValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.ShortValue;

public enum NumericType {

    ByteType(ByteValue.class, Byte.class),
    ShortType(ShortValue.class, Short.class),
    IntegerType(IntegerValue.class, Integer.class),
    LongType(LongValue.class, Long.class),
    FloatType(FloatValue.class, Float.class),
    DoubleType(DoubleValue.class, Double.class),
    NumberType(NumberValue.class, Number.class);  // NumberType is not explicitly referenced, but it is iterated on

    final Class<? extends GenericValue> exprType;
    final Class<? extends Number> primaryType;

    private NumericType(final Class<? extends GenericValue> exprType, final Class<? extends Number> primaryType) {
        this.exprType = exprType;
        this.primaryType = primaryType;
    }

    private Class<? extends GenericValue> getExprType() {
        return this.exprType;
    }

    private Class<? extends Number> getPrimaryType() {
        return this.primaryType;
    }

    public static int getTypeRanking(final Class clazz) {
        for (final NumericType type : values())
            if (clazz.equals(type.getExprType()) || clazz.equals(type.getPrimaryType()))
                return type.ordinal();
        return -1;
    }

    public static boolean isAssignable(final Class parentClass, final Class childClass) {
        final int parentRanking = getTypeRanking(parentClass);
        final int childRanking = getTypeRanking(childClass);

        return childRanking <= parentRanking;
    }

    public static Class getHighestRankingNumericArg(final Object... vals) {

        Class highestRankingNumericArg = NumberValue.class;
        int highestRank = -1;
        for (final Object obj : vals) {

            final Class clazz = obj.getClass();
            final int rank = NumericType.getTypeRanking(clazz);
            if (rank > highestRank) {
                highestRank = rank;
                highestRankingNumericArg = clazz;
            }
        }
        return highestRankingNumericArg;
    }

    public static boolean useDecimalNumericArgs(final Class clazz) {
        return isAFloat(clazz) || isADouble(clazz);
    }

    public static boolean isANumber(final Class clazz) {
        return getTypeRanking(clazz) != -1;
    }

    public static boolean isAByte(final Class clazz) {
        return clazz == ByteType.getExprType() || clazz == (ByteType.getPrimaryType());
    }

    public static boolean isAShort(final Class clazz) {
        return clazz == ShortType.getExprType() || clazz == ShortType.getPrimaryType();
    }

    public static boolean isAnInteger(final Class clazz) {
        return clazz == IntegerType.getExprType() || clazz == IntegerType.getPrimaryType();
    }

    public static boolean isALong(final Class clazz) {
        return clazz == LongType.getExprType() || clazz == LongType.getPrimaryType();
    }

    public static boolean isAFloat(final Class clazz) {
        return clazz == FloatType.getExprType() || clazz == FloatType.getPrimaryType();
    }

    public static boolean isADouble(final Class clazz) {
        return clazz == DoubleType.getExprType() || clazz == DoubleType.getPrimaryType();
    }
}
