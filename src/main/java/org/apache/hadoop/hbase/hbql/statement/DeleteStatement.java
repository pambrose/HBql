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
import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.ExecutionOutput;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DeleteStatement extends SchemaContext implements ConnectionStatement {

    private final WithArgs withArgs;

    public DeleteStatement(final String schemaName, final WithArgs withArgs) {
        super(schemaName);
        if (withArgs == null)
            this.withArgs = new WithArgs();
        else
            this.withArgs = withArgs;
    }

    public WithArgs getWithArgs() {
        return this.withArgs;
    }

    public ExecutionOutput execute(final HConnectionImpl conn) throws HBqlException {

        this.checkIfValidSchemaName();

        this.getWithArgs().setSchemaContext(this);

        final Set<ColumnAttrib> allWhereAttribs = this.getWithArgs().getAllColumnsUsedInExprs();
        final HTable table = conn.getHTable(this.getSchema().getTableName());

        final List<RowRequest> rowRequestList = this.getWithArgs().getRowRequestList(allWhereAttribs);

        int cnt = 0;

        for (final RowRequest rowRequest : rowRequestList)
            cnt += this.delete(table, this.getWithArgs(), rowRequest);

        return new ExecutionOutput("Delete count: " + cnt);
    }

    private int delete(final HTable table,
                       final WithArgs with,
                       final RowRequest rowRequest) throws HBqlException {

        try {
            final ExpressionTree clientExpressionTree = with.getClientExpressionTree();

            int cnt = 0;
            for (final Result result : rowRequest.getResultScanner(table)) {
                try {
                    if (clientExpressionTree == null || clientExpressionTree.evaluate(result)) {
                        table.delete(new Delete(result.getRow()));
                        cnt++;
                    }
                }
                catch (ResultMissingColumnException e) {
                    // Just skip and do nothing
                }
            }
            if (cnt > 0)
                table.flushCommits();
            return cnt;
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }
}
