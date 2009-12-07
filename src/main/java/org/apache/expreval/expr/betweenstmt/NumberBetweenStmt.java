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

package org.apache.expreval.expr.betweenstmt;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public class NumberBetweenStmt extends GenericBetweenStmt {

    public NumberBetweenStmt(final GenericValue arg0,
                             final boolean not,
                             final GenericValue arg1,
                             final GenericValue arg2) {
        super(ExpressionType.NUMBERBETWEEN, not, arg0, arg1, arg2);
    }

    public Boolean getValue(final HConnectionImpl connection,
                            final Object object) throws HBqlException, ResultMissingColumnException {

        final Object obj0 = this.getExprArg(0).getValue(connection, object);
        final Object obj1 = this.getExprArg(1).getValue(connection, object);
        final Object obj2 = this.getExprArg(2).getValue(connection, object);

        this.validateNumericArgTypes(obj0, obj1, obj2);

        final boolean retval;

        if (!this.useDecimal()) {

            final long val0 = ((Number)obj0).longValue();
            final long val1 = ((Number)obj1).longValue();
            final long val2 = ((Number)obj2).longValue();

            retval = val0 >= val1 && val0 <= val2;
        }
        else {

            final double val0 = ((Number)obj0).doubleValue();
            final double val1 = ((Number)obj1).doubleValue();
            final double val2 = ((Number)obj2).doubleValue();

            retval = val0 >= val1 && val0 <= val2;
        }

        return (this.isNot()) ? !retval : retval;
    }
}