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

package org.apache.expreval.expr.nullcomp;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.NotValue;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public abstract class GenericNullCompare extends NotValue<GenericNullCompare> implements BooleanValue {

    protected GenericNullCompare(final ExpressionType type, final boolean not, final GenericValue arg0) {
        super(type, not, arg0);
    }

    public String asString() {
        return this.getExprArg(0).asString() + " IS" + notAsString() + " NULL";
    }

    public Boolean getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException {
        boolean retval;
        try {
            final Object val = this.getExprArg(0).getValue(conn, object);
            retval = (val == null);
        }
        catch (NullColumnValueException e) {
            retval = true;
        }
        return (this.isNot()) ? !retval : retval;
    }
}