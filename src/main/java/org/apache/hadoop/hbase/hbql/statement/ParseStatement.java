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

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.ExecutionOutput;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

public class ParseStatement implements SchemaManagerStatement {

    private final ShellStatement stmt;
    private final GenericValue value;

    public ParseStatement(final ShellStatement stmt) {
        this.stmt = stmt;
        this.value = null;
    }

    public ParseStatement(final GenericValue value) {
        this.stmt = null;
        this.value = value;
    }

    private ShellStatement getStmt() {
        return this.stmt;
    }

    private GenericValue getValue() {
        return this.value;
    }

    public ExecutionOutput execute() throws HBqlException {
        final ExecutionOutput retval = new ExecutionOutput("Parsed successfully");
        if (this.getStmt() != null)
            retval.out.println(this.getStmt().getClass().getSimpleName());

        if (this.getValue() != null) {
            Object val = null;
            try {
                this.getValue().validateTypes(null, false);
                val = this.getValue().getValue(null);
            }
            catch (ResultMissingColumnException e) {
                val = "ResultMissingColumnException()";
            }
            retval.out.println(this.getValue().asString() + " = " + val);
        }

        return retval;
    }
}