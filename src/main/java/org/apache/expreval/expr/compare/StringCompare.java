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
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public class StringCompare extends GenericCompare {

    public StringCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {
        return this.validateType(StringValue.class);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        final String val1 = (String)this.getArg(0).getValue(object);
        final String val2 = (String)this.getArg(1).getValue(object);

        switch (this.getOperator()) {
            case EQ:
                return val1.equals(val2);
            case NOTEQ:
                return !val1.equals(val2);
            case GT:
                return val1.compareTo(val2) > 0;
            case GTEQ:
                return val1.compareTo(val2) >= 0;
            case LT:
                return val1.compareTo(val2) < 0;
            case LTEQ:
                return val1.compareTo(val2) <= 0;
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }
}
