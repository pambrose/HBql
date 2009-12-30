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
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.ParseException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.Query;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class ImportStatement extends BasicStatement implements ConnectionStatement {

    private final String filename;

    public ImportStatement(final String filename) {
        super(null);
        this.filename = filename;
    }

    private String getFilename() {
        return filename;
    }

    public ExecutionResults execute(final HConnectionImpl conn) {

        final ExecutionResults results = new ExecutionResults();

        boolean success;
        try {
            success = processInput(new PrintWriter(results.out), conn, readFile(this.getFilename()));
        }
        catch (IOException e) {
            success = false;
            results.out.println(e.getMessage());
        }
        results.setSuccess(success);

        return results;
    }

    public static String readFile(final String filename) throws IOException {

        try {
            final StringBuilder stmtBuffer = new StringBuilder();
            final BufferedReader in = new BufferedReader(new FileReader(filename));
            String str;
            while ((str = in.readLine()) != null)
                stmtBuffer.append(str);
            in.close();
            return stmtBuffer.toString();
        }
        catch (FileNotFoundException e) {
            throw new IOException("Cannot find file: " + filename);
        }
        catch (IOException e) {
            throw new IOException("Unable to read file: " + filename + " - " + e.getMessage());
        }
    }

    public static boolean processInput(final PrintWriter out,
                                       final HConnectionImpl conn,
                                       final String str) {

        try {
            final List<HBqlStatement> stmtList = ParserUtil.parseConsoleStatements(str);

            for (final HBqlStatement stmt : stmtList) {
                if (stmt instanceof SelectStatement)
                    processSelect(out, conn, (SelectStatement)stmt);
                else if (stmt instanceof ConnectionStatement)
                    out.println(((ConnectionStatement)stmt).evaluatePredicateAndExecute(conn));
                else if (stmt instanceof NonConnectionStatement)
                    out.println(((NonConnectionStatement)stmt).execute());
                else
                    out.println("Unsupported statement type: " + stmt.getClass().getSimpleName() + " - " + str);
            }
        }
        catch (ParseException e) {
            out.println(e.getErrorMessage());
            return false;
        }
        catch (HBqlException e) {
            out.println("Error in statement: " + str);
            out.println(e.getMessage());
            return false;
        }
        finally {
            out.flush();
        }

        return true;
    }

    private static void processSelect(final PrintWriter out,
                                      final HConnectionImpl conn,
                                      final SelectStatement selectStatement) throws HBqlException {

        // selectStatement.validate(connection);

        final Query<HRecord> query = Query.newQuery(conn, selectStatement, HRecord.class);
        final HResultSet<HRecord> results = query.newResultSet();
        for (final HRecord rec : results) {
            for (final String columnName : rec.getColumnNameList()) {
                out.println(columnName + ": " + rec.getCurrentValue(columnName));
            }
        }
    }

    public static String usage() {
        return "IMPORT file_name";
    }
}