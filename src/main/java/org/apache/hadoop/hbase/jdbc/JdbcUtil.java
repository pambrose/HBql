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

package org.apache.hadoop.hbase.jdbc;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ParseException;
import org.apache.hadoop.hbase.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.DeleteStatement;
import org.apache.hadoop.hbase.hbql.statement.HBqlStatement;
import org.apache.hadoop.hbase.hbql.statement.InsertStatement;
import org.apache.hadoop.hbase.hbql.statement.NonConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;


public class JdbcUtil {

    public static HBqlStatement parseJdbcStatement(final String sql) throws HBqlException {

        try {
            final HBqlStatement stmt = HBqlShell.parseJdbcStatement(sql);

            if (stmt instanceof SelectStatement
                || stmt instanceof ConnectionStatement
                || stmt instanceof NonConnectionStatement)
                return stmt;
            else
                throw new HBqlException("Unsupported statement type: " + stmt.getClass().getSimpleName() + " - " + sql);
        }
        catch (ParseException e) {
            throw new HBqlException(e.getErrorMessage());
        }
    }

    public static boolean isSelectStatemet(final HBqlStatement stmt) {
        return stmt instanceof SelectStatement;
    }

    public static boolean isDMLStatement(final HBqlStatement stmt) {
        return stmt instanceof InsertStatement
               || stmt instanceof DeleteStatement;
    }

    public static boolean isConnectionStatemet(final HBqlStatement stmt) {
        return stmt instanceof ConnectionStatement;
    }

    public static boolean isNonConectionStatemet(final HBqlStatement stmt) {
        return stmt instanceof NonConnectionStatement;
    }
}
