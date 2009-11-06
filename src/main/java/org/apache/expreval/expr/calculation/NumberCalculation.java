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

package org.apache.expreval.expr.calculation;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public class NumberCalculation extends GenericCalculation implements NumberValue {

    public NumberCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(ExpressionType.NUMBERCALCULATION, arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {
        return this.validateNumericTypes();
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        final Object obj0 = this.getArg(0).getValue(object);
        final Object obj1 = this.getArg(1).getValue(object);

        this.validateNumericArgTypes(obj0, obj1);

        if (!this.useDecimal()) {

            final long val0 = ((Number)obj0).longValue();
            final long val1 = ((Number)obj1).longValue();

            final long result;

            switch (this.getOperator()) {
                case PLUS:
                    result = val0 + val1;
                    break;
                case MINUS:
                    result = val0 - val1;
                    break;
                case MULT:
                    result = val0 * val1;
                    break;
                case DIV:
                    result = val0 / val1;
                    break;
                case MOD:
                    result = val0 % val1;
                    break;
                case NEGATIVE:
                    result = val0 * -1;
                    break;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }

            return this.getValueWithCast(result);
        }
        else {

            final double val0 = ((Number)obj0).doubleValue();
            final double val1 = ((Number)obj1).doubleValue();

            final double result;

            switch (this.getOperator()) {
                case PLUS:
                    result = val0 + val1;
                    break;
                case MINUS:
                    result = val0 - val1;
                    break;
                case MULT:
                    result = val0 * val1;
                    break;
                case DIV:
                    result = val0 / val1;
                    break;
                case MOD:
                    result = val0 % val1;
                    break;
                case NEGATIVE:
                    result = val0 * -1;
                    break;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }

            return this.getValueWithCast(result);
        }
    }
}