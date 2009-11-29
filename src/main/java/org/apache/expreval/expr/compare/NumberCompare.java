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

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public class NumberCompare extends GenericCompare {

    public NumberCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {
        return this.validateType(NumberValue.class);
    }

    public Boolean getValue(final HConnectionImpl connection,
                            final Object object) throws HBqlException, ResultMissingColumnException {

        final Object obj0 = this.getValue(0, connection, object);
        final Object obj1 = this.getValue(1, connection, object);

        this.validateNumericArgTypes(obj0, obj1);

        if (!this.useDecimal()) {

            final long val0 = ((Number)obj0).longValue();
            final long val1 = ((Number)obj1).longValue();

            switch (this.getOperator()) {
                case EQ:
                    return val0 == val1;
                case GT:
                    return val0 > val1;
                case GTEQ:
                    return val0 >= val1;
                case LT:
                    return val0 < val1;
                case LTEQ:
                    return val0 <= val1;
                case NOTEQ:
                    return val0 != val1;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }
        }
        else {

            final double val0 = ((Number)obj0).doubleValue();
            final double val1 = ((Number)obj1).doubleValue();

            switch (this.getOperator()) {
                case EQ:
                    return val0 == val1;
                case GT:
                    return val0 > val1;
                case GTEQ:
                    return val0 >= val1;
                case LT:
                    return val0 < val1;
                case LTEQ:
                    return val0 <= val1;
                case NOTEQ:
                    return val0 != val1;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }
        }
    }
}