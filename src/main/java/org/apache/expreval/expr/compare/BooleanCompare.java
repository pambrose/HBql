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
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public class BooleanCompare extends GenericCompare implements BooleanValue {

    public BooleanCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {
        return this.validateType(BooleanValue.class);
    }

    public Boolean getValue(final HConnectionImpl connection,
                            final Object object) throws HBqlException, ResultMissingColumnException {

        switch (this.getOperator()) {
            case OR:
                return (Boolean)this.getValue(0, connection, object) || (Boolean)this.getValue(1, connection, object);
            case AND:
                return (Boolean)this.getValue(0, connection, object) && (Boolean)this.getValue(1, connection, object);
            case EQ: {
                boolean val0 = (Boolean)this.getValue(0, connection, object);
                boolean val1 = (Boolean)this.getValue(1, connection, object);
                return val0 == val1;
            }
            case NOTEQ: {
                boolean val0 = (Boolean)this.getValue(0, connection, object);
                boolean val1 = (Boolean)this.getValue(1, connection, object);
                return val0 != val1;
            }
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }
}
