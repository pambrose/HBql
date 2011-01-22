/*
 * Copyright (c) 2011.  The Apache Software Foundation
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

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.util.List;

public class AlterTableStatement extends GenericStatement implements ConnectionStatement {

    private final String                 tableName;
    private final List<AlterTableAction> alterTableActionList;

    public AlterTableStatement(final StatementPredicate predicate,
                               final String tableName,
                               final List<AlterTableAction> alterTableActionList) {
        super(predicate);
        this.tableName = tableName;
        this.alterTableActionList = alterTableActionList;
    }

    private String getTableName() {
        return this.tableName;
    }

    protected ExecutionResults execute(final HConnectionImpl conn) throws HBqlException {

        conn.validateTableDisabled(this.getTableName(), "alter");

        final HBaseAdmin admin = conn.getHBaseAdmin();

        for (final AlterTableAction alterTableAction : this.alterTableActionList)
            alterTableAction.execute(conn, admin, this.getTableName());

        return new ExecutionResults("Table " + this.getTableName() + " altered.");
    }

    public static String usage() {
        return "ALTER TABLE table_name alter_action_list [IF bool_expr]";
    }
}