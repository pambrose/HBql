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
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.RowRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DeleteStatement extends StatementContext implements ParameterStatement, ConnectionStatement {

    private transient HConnectionImpl connection = null;
    private boolean validated = false;
    private final NamedParameters namedParameters = new NamedParameters();
    private final WithArgs withArgs;
    private final List<String> deleteItemList;

    public DeleteStatement(final List<String> deleteItemList, final String mappingName, final WithArgs withArgs) {
        super(mappingName);
        if (withArgs == null)
            this.withArgs = new WithArgs();
        else
            this.withArgs = withArgs;

        if (deleteItemList == null)
            this.deleteItemList = Lists.newArrayList();
        else
            this.deleteItemList = deleteItemList;
    }

    private WithArgs getWithArgs() {
        return this.withArgs;
    }

    private List<String> getDeleteItemList() {
        return this.deleteItemList;
    }

    private HConnectionImpl getConnection() {
        return this.connection;
    }

    private boolean isValidated() {
        return this.validated;
    }

    public NamedParameters getNamedParameters() {
        return this.namedParameters;
    }

    public void validate(final HConnectionImpl connection) throws HBqlException {

        if (this.isValidated())
            return;
        else
            this.validated = true;

        this.connection = connection;

        this.validateMappingName(connection);

        this.getWithArgs().setStatementContext(this);

        this.collectParameters();
    }

    public ExecutionResults execute() throws HBqlException {
        return this.execute(this.getConnection());
    }

    public ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        this.validate(connection);

        final Set<ColumnAttrib> allWhereAttribs = this.getWithArgs().getAllColumnsUsedInExprs();
        final HTable table = connection.newHTable(this.getMapping().getTableName());

        final List<RowRequest> rowRequestList = this.getWithArgs().getRowRequestList(allWhereAttribs);

        int cnt = 0;

        for (final RowRequest rowRequest : rowRequestList)
            cnt += this.delete(table, this.getWithArgs(), rowRequest);

        try {
            table.close();
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }

        final ExecutionResults results = new ExecutionResults("Delete count: " + cnt);
        results.setCount(cnt);
        return results;
    }

    private int delete(final HTable table, final WithArgs with, final RowRequest rowRequest) throws HBqlException {

        try {
            final ExpressionTree clientExpressionTree = with.getClientExpressionTree();

            int cnt = 0;
            final ResultScanner resultScaner = rowRequest.getResultScanner(table);
            for (final Result result : resultScaner) {
                try {
                    if (clientExpressionTree == null || clientExpressionTree.evaluate(result)) {
                        final Delete rowDelete = new Delete(result.getRow());

                        for (final String deleteItem : this.getDeleteItemList()) {
                            if (deleteItem.endsWith(":*")) {
                                final String familyName = deleteItem.substring(0, deleteItem.length() - 2);
                                rowDelete.deleteFamily(familyName.getBytes());
                            }
                            else {
                                rowDelete.deleteColumns(deleteItem.getBytes());
                            }
                        }

                        table.delete(rowDelete);
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

    public void reset() {
        this.getWithArgs().reset();
    }

    private void collectParameters() {
        this.getNamedParameters().addParameters(this.getWithArgs().getParameterList());
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        final int cnt = this.getWithArgs().setParameter(name, val);
        if (cnt == 0)
            throw new HBqlException("Parameter name " + name + " does not exist in " + this.asString());
        return cnt;
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("DELETE FROM ");
        sbuf.append(this.getMappingName());
        sbuf.append(" ");
        sbuf.append(this.getWithArgs().asString());
        return sbuf.toString();
    }
}
