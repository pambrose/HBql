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

package org.apache.hadoop.hbase.hbql.statement;

import org.apache.hadoop.hbase.hbql.client.AsyncExecutorManager;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.executor.AsyncExecutorDefinition;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public class CreateAsyncExecutorStatement extends GenericStatement implements ConnectionStatement {

    private final AsyncExecutorDefinition args;

    public CreateAsyncExecutorStatement(final StatementPredicate predicate,
                                        final AsyncExecutorDefinition args) {
        super(predicate);
        this.args = args;
    }

    private AsyncExecutorDefinition getArgs() {
        return this.args;
    }

    protected ExecutionResults execute(final HConnectionImpl conn) throws HBqlException {

        this.getArgs().validatePropertyList();

        AsyncExecutorManager.newAsyncExecutor(this.getArgs().getName(),
                                              this.getArgs().getMinThreadCount(),
                                              this.getArgs().getMaxThreadCount(),
                                              this.getArgs().getKeepAliveSecs());

        return new ExecutionResults("Async executor " + this.getArgs().getName() + " created.");
    }


    public static String usage() {
        return "CREATE ASYNC EXECUTOR name (MIN_THREAD_COUNT: int_expr, MAX_THREAD_COUNT: int_expr, THREADS_READ_RESULTS: bool_expr, COMPLETION_QUEUE_SIZE: int_expr) [IF bool_expr]";
    }
}