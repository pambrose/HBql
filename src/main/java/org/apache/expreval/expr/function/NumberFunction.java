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
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.AggregateValue;

import java.util.List;

public class NumberFunction extends Function implements NumberValue {

    public NumberFunction(final FunctionType functionType, final List<GenericValue> exprs) {
        super(functionType, exprs);
    }

    public void initAggregateValue(final AggregateValue aggregateValue) throws HBqlException {

        switch (this.getFunctionType()) {

            case COUNT: {
                aggregateValue.setCurrentValue(0, 0L);
                break;
            }

            default:
                throw new InternalErrorException("Invalid aggregate function: " + this.getFunctionType());
        }
    }

    public void applyResultToAggregateValue(final AggregateValue aggregateValue,
                                            final Result result) throws HBqlException {

        switch (this.getFunctionType()) {

            case COUNT: {
                long val = (Long)aggregateValue.getCurrentValue();
                aggregateValue.setCurrentValue(0, val + 1);
                break;
            }

            default:
                throw new InternalErrorException("Invalid aggregate function: " + this.getFunctionType());
        }
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        switch (this.getFunctionType()) {

            case LENGTH: {
                final String val = (String)this.getArg(0).getValue(object);
                this.checkForNull(val);
                return val.length();
            }

            case INDEXOF: {
                final String v1 = (String)this.getArg(0).getValue(object);
                final String v2 = (String)this.getArg(1).getValue(object);
                this.checkForNull(v1, v2);
                return v1.indexOf(v2);
            }

            case DATETOLONG: {
                return (Long)this.getArg(0).getValue(object);
            }

            case SHORT: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Short.valueOf(v1);
            }

            case INTEGER: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Integer.valueOf(v1);
            }

            case LONG: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Long.valueOf(v1);
            }

            case FLOAT: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Float.valueOf(v1);
            }

            case DOUBLE: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Double.valueOf(v1);
            }

            case ABS: {
                final Number v1 = (Number)this.getArg(0).getValue(object);

                if (v1 instanceof Short)
                    return Math.abs(v1.shortValue());
                else if (v1 instanceof Integer)
                    return Math.abs(v1.intValue());
                else if (v1 instanceof Long)
                    return Math.abs(v1.longValue());
                else if (v1 instanceof Float)
                    return Math.abs(v1.floatValue());
                else if (v1 instanceof Double)
                    return Math.abs(v1.doubleValue());
            }

            case LESSER: {
                final Number v1 = (Number)this.getArg(0).getValue(object);
                final Number v2 = (Number)this.getArg(1).getValue(object);

                if (v1 instanceof Short)
                    return Math.min(v1.shortValue(), v2.shortValue());
                else if (v1 instanceof Integer)
                    return Math.min(v1.intValue(), v2.intValue());
                else if (v1 instanceof Long)
                    return Math.min(v1.longValue(), v2.longValue());
                else if (v1 instanceof Float)
                    return Math.min(v1.floatValue(), v2.floatValue());
                else if (v1 instanceof Double)
                    return Math.min(v1.doubleValue(), v2.doubleValue());
            }

            case GREATER: {
                final Number v1 = (Number)this.getArg(0).getValue(object);
                final Number v2 = (Number)this.getArg(1).getValue(object);

                if (v1 instanceof Short)
                    return Math.max(v1.shortValue(), v2.shortValue());
                else if (v1 instanceof Integer)
                    return Math.max(v1.intValue(), v2.intValue());
                else if (v1 instanceof Long)
                    return Math.max(v1.longValue(), v2.longValue());
                else if (v1 instanceof Float)
                    return Math.max(v1.floatValue(), v2.floatValue());
                else if (v1 instanceof Double)
                    return Math.max(v1.doubleValue(), v2.doubleValue());
            }

            case RANDOMINTEGER: {
                return Function.randomVal.nextInt();
            }

            case RANDOMLONG: {
                return Function.randomVal.nextLong();
            }

            case RANDOMFLOAT: {
                return Function.randomVal.nextFloat();
            }

            case RANDOMDOUBLE: {
                return Function.randomVal.nextDouble();
            }

            default:
                throw new InternalErrorException("Invalid function: " + this.getFunctionType());
        }
    }
}