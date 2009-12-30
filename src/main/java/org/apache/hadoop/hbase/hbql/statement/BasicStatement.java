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

import org.apache.expreval.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.io.Serializable;

public abstract class BasicStatement implements Serializable {

    private static final long serialVersionUID = 1L;

    private StatementPredicate predicate;

    public BasicStatement() {
    }

    protected BasicStatement(final StatementPredicate predicate) {
        this.predicate = predicate;
    }

    private StatementPredicate getPredicate() {
        return this.predicate;
    }

    public ExecutionResults evaluatePredicateAndExecute(final HConnectionImpl conn) throws HBqlException {
        if (this.getPredicate() == null || this.getPredicate().evaluate(conn)) {
            return this.execute(conn);
        }
        else {
            final ExecutionResults results = new ExecutionResults("False predicate");
            results.setPredicate(false);
            return results;
        }
    }

    protected ExecutionResults execute(HConnectionImpl connection) throws HBqlException {
        throw new InternalErrorException();
    }

    public void validate() throws HBqlException {
        // Default
    }
}
