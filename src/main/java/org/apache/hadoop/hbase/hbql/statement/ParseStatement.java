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

package org.apache.hadoop.hbase.hbql.statement;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.statement.select.SimpleExpressionContext;

public class ParseStatement extends BasicStatement implements ConnectionStatement {

    private final HBqlStatement stmt;
    private final GenericValue genericValue;

    public ParseStatement(final HBqlStatement stmt) {
        super(null);
        this.stmt = stmt;
        this.genericValue = null;
    }

    public ParseStatement(final GenericValue genericValue) {
        super(null);
        this.stmt = null;
        this.genericValue = genericValue;
    }

    private HBqlStatement getStmt() {
        return this.stmt;
    }

    private GenericValue getGenericValue() {
        return this.genericValue;
    }

    public ExecutionResults execute(HConnectionImpl connection) throws HBqlException {

        final ExecutionResults retval = new ExecutionResults("Parsed successfully");

        if (this.getStmt() != null)
            retval.out.println(this.getStmt().getClass().getSimpleName());

        if (this.getGenericValue() != null) {
            final SimpleExpressionContext expr = new SimpleExpressionContext(this.getGenericValue());
            expr.validate();
            final Object val = expr.getValue(connection);
            retval.out.println(this.getGenericValue().asString() + " = " + val);
        }

        return retval;
    }
}