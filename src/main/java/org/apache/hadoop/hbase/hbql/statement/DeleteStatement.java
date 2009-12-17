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
import org.apache.hadoop.hbase.hbql.impl.HTableWrapper;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
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
    private final List<String> deleteItemList = Lists.newArrayList();
    private final List<String> originaltemList;

    public DeleteStatement(final StatementPredicate predicate,
                           final List<String> originaltemList,
                           final String mappingName,
                           final WithArgs withArgs) {
        super(predicate, mappingName);
        if (withArgs == null)
            this.withArgs = new WithArgs();
        else
            this.withArgs = withArgs;

        if (originaltemList == null)
            this.originaltemList = Lists.newArrayList();
        else
            this.originaltemList = originaltemList;
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

        this.validated = true;
        this.connection = connection;

        this.validateMappingName(connection);

        this.getWithArgs().setStatementContext(this);
        this.getWithArgs().validate(connection, this.getTableMapping());

        this.collectParameters();

        // Verify and lookup alias references
        // Build list with family wildcar refs and family qualified column names
        for (final String deleteItem : this.originaltemList) {
            if (deleteItem.contains(":")) {
                this.getDeleteItemList().add(deleteItem);
            }
            else {
                final TableMapping mapping = this.getTableMapping();
                final ColumnAttrib attrib = mapping.getAttribByVariableName(deleteItem);
                if (attrib == null)
                    throw new HBqlException("Invalid variable: " + deleteItem);

                this.getDeleteItemList().add(attrib.getFamilyQualifiedName());
            }
        }
    }

    public void validateTypes() throws HBqlException {
        this.getWithArgs().validateArgTypes();
    }

    protected ExecutionResults execute(final HConnectionImpl hconnectionImpl) throws HBqlException {

        this.validate(hconnectionImpl);

        this.validateTypes();

        final WithArgs withArgs = this.getWithArgs();
        final Set<ColumnAttrib> allWhereAttribs = withArgs.getColumnsUsedInAllWhereExprs();

        HTableWrapper tableWrapper = null;

        try {
            tableWrapper = hconnectionImpl.newHTableWrapper(withArgs, this.getMapping().getTableName());

            final List<RowRequest> rowRequests = withArgs.getRowRequestList(hconnectionImpl,
                                                                            this.getMapping(),
                                                                            allWhereAttribs);

            int cnt = 0;

            for (final RowRequest rowRequest : rowRequests)
                cnt += this.delete(tableWrapper, withArgs, rowRequest);

            try {
                tableWrapper.getHTable().flushCommits();
                tableWrapper.getHTable().close();
            }
            catch (IOException e) {
                throw new HBqlException(e);
            }

            final ExecutionResults results = new ExecutionResults("Delete count: " + cnt);
            results.setCount(cnt);
            return results;
        }
        finally {
            // release to table pool
            if (tableWrapper != null)
                tableWrapper.releaseHTable();
        }
    }

    private int delete(final HTableWrapper tableWrapper,
                       final WithArgs withArgs,
                       final RowRequest rowRequest) throws HBqlException {

        final HTable table = tableWrapper.getHTable();
        final ExpressionTree clientExpressionTree = withArgs.getClientExpressionTree();
        final ResultScanner resultScanner = rowRequest.getResultScanner(this.getMapping(), withArgs, table);

        int cnt = 0;

        try {
            for (final Result result : resultScanner) {
                try {
                    if (clientExpressionTree == null || clientExpressionTree.evaluate(connection, result)) {

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
                    // Just skip and go to next record
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

        sbuf.append("DELETE ");

        boolean firsttime = true;
        for (final String familyName : this.originaltemList) {
            if (!firsttime)
                sbuf.append(", ");
            sbuf.append(familyName);
            firsttime = false;
        }

        sbuf.append(" FROM ");
        sbuf.append(this.getMappingName());
        sbuf.append(" ");
        sbuf.append(this.getWithArgs().asString());
        return sbuf.toString();
    }

    public static String usage() {
        return "DELETE delete_item_list FROM [MAPPING] mapping_name with_clause [IF boolean_expression]";
    }
}
