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

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.expreval.expr.var.NamedParameter;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.statement.HBqlStatement;
import org.apache.hadoop.hbase.hbql.statement.StatementWithParameters;

import java.util.List;

public class HPreparedStatementImpl extends HStatementImpl implements HPreparedStatement {

    private final String sql;
    private final HBqlStatement statement;

    public HPreparedStatementImpl(final HConnectionImpl hbqlConnection, final String sql) throws HBqlException {
        super(hbqlConnection);
        this.sql = sql;

        this.statement = Utils.parseHBqlStatement(sql);

        if ((this.getStatement() instanceof StatementWithParameters)) {
            final StatementWithParameters paramStmt = (StatementWithParameters)this.getStatement();

            // Need to call this here to enable setParameters
            paramStmt.validate(this.getHConnectionImpl());
        }
    }

    public String getSql() {
        return this.sql;
    }

    private HBqlStatement getStatement() {
        return this.statement;
    }

    public HResultSet<HRecord> executeQuery() throws HBqlException {
        return this.executeQuery(this.getStatement(), HRecord.class);
    }

    public <T> HResultSet<T> executeQuery(final Class clazz) throws HBqlException {
        return this.executeQuery(this.getStatement(), clazz);
    }

    public List<HRecord> executeQueryAndFetch() throws HBqlException {
        return this.executeQueryAndFetch(this.getStatement(), HRecord.class);
    }

    public <T> List<T> executeQueryAndFetch(final Class clazz) throws HBqlException {
        return this.executeQueryAndFetch(this.getStatement(), clazz);
    }

    public ExecutionResults executeUpdate() throws HBqlException {
        return this.executeUpdate(this.getStatement());
    }

    public ExecutionResults execute() throws HBqlException {
        return this.execute(this.getStatement());
    }

    public int setParameter(final String name, final Object val) throws HBqlException {
        checkForNullParameterValue(val);
        final StatementWithParameters paramStmt = getParameterStatement(this.getStatement());
        return paramStmt.setStatementParameter(name, val);
    }

    public void setParameter(final int i, final Object val) throws HBqlException {
        checkForNullParameterValue(val);
        final StatementWithParameters paramStmt = getParameterStatement(this.getStatement());
        final NamedParameter param = paramStmt.getNamedParameters().getParameter(i);
        param.setParameter(val);
    }

    public void resetParameters() throws HBqlException {
        final StatementWithParameters paramStmt = getParameterStatement(this.getStatement());
        paramStmt.resetParameters();
    }

    public static void checkForNullParameterValue(final Object val) throws HBqlException {
        //if (val == null)
        //    throw new HBqlException("Parameter value cannot be NULL");
    }

    public static StatementWithParameters getParameterStatement(final HBqlStatement statement) throws HBqlException {

        if (!(statement instanceof StatementWithParameters))
            throw new HBqlException(statement.getClass().getSimpleName() + " statements do not support parameters");

        return (StatementWithParameters)statement;
    }
}
