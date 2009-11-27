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

package org.apache.hadoop.hbase.hbql.statement.select;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.statement.NonStatement;

public class SimpleExpressionContext extends MultipleExpressionContext {

    public SimpleExpressionContext(final GenericValue genericValue) {
        super(null, genericValue);
    }

    private GenericValue getGenericValue() {
        return this.getGenericValue(0);
    }

    public boolean isAConstant() {
        return this.getGenericValue().isAConstant();
    }

    public Object getValue(final HConnectionImpl connection) throws HBqlException {
        try {
            this.setStatementContext(new NonStatement(null, null));
            return this.evaluate(connection, 0, true, false, connection);
        }
        catch (ResultMissingColumnException e) {
            throw new InternalErrorException();
        }
    }

    public String asString() {
        return this.getGenericValue().asString();
    }

    public boolean useResultData() {
        return true;
    }
}