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

import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.QueryExecutorPoolManager;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.statement.args.QueryExecutorPoolArgs;

public class CreateQueryExecutorPoolStatement extends BasicStatement implements ConnectionStatement {

    private final String poolName;
    private final QueryExecutorPoolArgs args;

    public CreateQueryExecutorPoolStatement(final StatementPredicate predicate,
                                            final String poolName,
                                            final QueryExecutorPoolArgs args) {
        super(predicate);
        this.poolName = poolName;
        this.args = args;
    }

    private String getPoolName() {
        return this.poolName;
    }

    private QueryExecutorPoolArgs getArgs() {
        return this.args;
    }

    protected ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        QueryExecutorPoolManager.newQueryExecutorPool(this.getPoolName(),
                                                      this.getArgs().getMaxPoolSize(),
                                                      this.getArgs().getThreadCount(),
                                                      this.getArgs().getThreadsReadResults(),
                                                      this.getArgs().getQueueSize());

        return new ExecutionResults("Executor pool " + this.getPoolName() + " created.");
    }


    public static String usage() {
        return "CREATE [QUERY] EXECUTOR POOL pool_name (MAX_POOL_SIZE: integer_expression, THREAD_COUNT: integer_expression, THREADS_READ_RESULTS: boolean_expression, QUEUE_SIZE: integer_expression) [IF boolean_expression]";
    }
}