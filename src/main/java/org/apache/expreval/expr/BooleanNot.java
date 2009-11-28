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

package org.apache.expreval.expr;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.literal.BooleanLiteral;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public class BooleanNot extends GenericExpression implements BooleanValue {

    private final boolean not;

    public BooleanNot(final boolean not, final GenericValue arg0) {
        super(null, arg0);
        this.not = not;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {
        this.validateParentClass(BooleanValue.class, this.getArg(0).validateTypes(this, false));
        return BooleanValue.class;
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeAllArgs();
        if (!this.isAConstant())
            return this;
        else
            try {
                return new BooleanLiteral(this.getValue(null, null));
            }
            catch (ResultMissingColumnException e) {
                throw new InternalErrorException();
            }
    }

    public Boolean getValue(final HConnectionImpl connection,
                            final Object object) throws HBqlException, ResultMissingColumnException {
        final boolean retval = (Boolean)this.getArg(0).getValue(connection, object);
        return (this.not) ? !retval : retval;
    }

    public String asString() {
        return (this.not ? "NOT " : "") + this.getArg(0).asString();
    }
}
