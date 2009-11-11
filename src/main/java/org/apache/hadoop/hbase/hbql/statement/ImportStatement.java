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

import org.apache.hadoop.hbase.hbql.client.ExecutionOutput;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.ParseException;
import org.apache.hadoop.hbase.hbql.client.Query;
import org.apache.hadoop.hbase.hbql.client.ResultSet;
import org.apache.hadoop.hbase.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.parser.HBqlShell;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class ImportStatement implements ConnectionStatement {

    private final String filename;

    public ImportStatement(final String filename) {
        this.filename = filename;
    }

    private String getFilename() {
        return filename;
    }

    public ExecutionOutput execute(final ConnectionImpl conn) {

        final ExecutionOutput output = new ExecutionOutput();

        try {
            processInput(new PrintWriter(output.out), conn, readFile(this.getFilename()));
        }
        catch (IOException e) {
            output.setSuccess(false);
        }

        return output;
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
            System.out.println("Cannot find file: " + filename);
            throw e;
        }
        catch (IOException e) {
            System.out.println("Unable to read file: " + filename + " - " + e.getMessage());
            throw e;
        }
    }

    public static void processInput(final PrintWriter out,
                                    final ConnectionImpl conn,
                                    final String input) throws IOException {

        try {
            final List<ShellStatement> stmtList = HBqlShell.parseCommands(input);

            for (final ShellStatement stmt : stmtList) {
                if (stmt instanceof SelectStatement)
                    processSelect(out, conn, (SelectStatement)stmt);
                else if (stmt instanceof ConnectionStatement)
                    out.println(((ConnectionStatement)stmt).execute(conn));
                else if (stmt instanceof NonConnectionStatement)
                    out.println(((NonConnectionStatement)stmt).execute());
                else
                    out.println("Unsupported statement type: " + stmt.getClass().getSimpleName() + " - " + input);
            }
        }
        catch (ParseException e) {
            out.println("Error parsing: ");
            out.println(e.getMessage());
            if (e.getRecognitionException() != null) {
                final StringBuilder sbuf = new StringBuilder();
                for (int i = 0; i < e.getRecognitionException().charPositionInLine; i++)
                    sbuf.append("-");
                sbuf.append("^");
                out.println(sbuf.toString());
            }
        }
        catch (HBqlException e) {
            out.println("Error in statement: " + input);
            out.println(e.getMessage());
        }

        out.flush();
    }

    private static void processSelect(final PrintWriter out,
                                      final ConnectionImpl conn,
                                      final SelectStatement selectStatement) throws HBqlException, IOException {

        selectStatement.validate(conn);

        final Query<HRecord> query = conn.newQuery(selectStatement);
        final ResultSet<HRecord> results = query.getResults();

        for (final HRecord rec : results) {
            for (final String columnName : rec.getColumnNameList()) {
                out.println(columnName + ": " + rec.getCurrentValue(columnName));
            }
        }
    }
}