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

package org.apache.hadoop.hbase.hbql;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.ConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Output;
import org.apache.hadoop.hbase.hbql.client.ParseException;
import org.apache.hadoop.hbase.hbql.client.Query;
import org.apache.hadoop.hbase.hbql.client.Record;
import org.apache.hadoop.hbase.hbql.client.Results;
import org.apache.hadoop.hbase.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.SchemaManagerStatement;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.ShellStatement;
import org.apache.hadoop.hbase.hbql.statement.VersionStatement;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Console {

    public static void main(String[] args) throws IOException, HBqlException {

        if (args != null && args.length > 0) {
            final List<String> argList = new LinkedList<String>();
            argList.addAll(Arrays.asList(args));

            while (argList.size() > 0) {
                if (!processArg(argList)) {
                    usage();
                    break;
                }
            }
        }
        else {
            processCommandLineInput();
        }
    }

    private static void usage() {

        System.out.println("Usage: java " + Console.class.getName() + " [-options]");
        System.out.println("\t\t(comand line usage");
        System.out.println("   or  java " + Console.class.getName() + " [-options] [file_names]");
        System.out.println("\t\t(executes the statements in the space-separated file names)");
        System.out.println("\nwhere options include:");
        System.out.println("\t-usage        print this message");
        System.out.println("\t-version      print version info and exit");
    }

    private static boolean processArg(final List<String> argList) throws HBqlException, IOException {

        final String option = argList.remove(0);

        if (option.equals("-usage")) {
            usage();
            return true;
        }

        if (option.equals("-version")) {
            final VersionStatement version = new VersionStatement();
            final Output out = version.execute(null);
            System.out.print(out);
            return true;
        }

        if (!option.startsWith("-")) {

            final StringBuilder stmtBuffer = new StringBuilder();
            try {
                final BufferedReader in = new BufferedReader(new FileReader(option));
                String str;
                while ((str = in.readLine()) != null)
                    stmtBuffer.append(str);
                in.close();
            }
            catch (FileNotFoundException e) {
                System.out.println("Cannot find file: " + option);
                return false;
            }

            final ConnectionImpl conn = (ConnectionImpl)ConnectionManager.newConnection();
            processInput(new PrintWriter(System.out), conn, stmtBuffer.toString());

            return true;
        }

        return false;
    }


    private static void processCommandLineInput() throws IOException {

        final List<SimpleCompletor> completors = Lists.newArrayList();
        completors.add(new SimpleCompletor(new String[]{"select", "insert", "create", "table", "schema",
                                                        "describe", "drop", "enable", "disable", "list"}));

        final ConsoleReader reader = new ConsoleReader();
        reader.setBellEnabled(false);
        reader.setUseHistory(true);
        //reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
        reader.addCompletor(new ArgumentCompletor(completors));

        final PrintWriter out = new PrintWriter(System.out);

        final ConnectionImpl conn = (ConnectionImpl)ConnectionManager.newConnection();

        StringBuilder stmtBuffer = new StringBuilder();
        boolean continuation = false;

        final Map<Boolean, String> prompts = Maps.newHashMap();
        prompts.put(Boolean.FALSE, "HBql> ");
        prompts.put(Boolean.TRUE, "> ");

        while (true) {

            final String line = reader.readLine(prompts.get(continuation));

            if (line == null || line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit"))
                break;

            if (line.trim().length() > 0) {
                stmtBuffer.append(line);

                continuation = !line.trim().endsWith(";");
                if (!continuation) {
                    final String sql = stmtBuffer.toString();
                    stmtBuffer = new StringBuilder();
                    processInput(out, conn, sql);
                }
            }
        }
    }

    private static void processInput(final PrintWriter out,
                                     final ConnectionImpl conn,
                                     final String input) throws IOException {

        try {
            final List<ShellStatement> stmtList = HBqlShell.parseCommands(input);

            for (final ShellStatement stmt : stmtList) {
                if (stmt instanceof SelectStatement)
                    processSelect(out, conn, (SelectStatement)stmt);
                else if (stmt instanceof ConnectionStatement)
                    out.println(((ConnectionStatement)stmt).execute(conn));
                else if (stmt instanceof SchemaManagerStatement)
                    out.println(((SchemaManagerStatement)stmt).execute());
                else
                    out.println("Unsupported statement type");
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

        final Query<Record> query = conn.newQuery(selectStatement);
        final Results<Record> results = query.getResults();

        for (final Record rec : results) {
            for (final String columnName : rec.getColumnNameList()) {
                out.println(columnName + ": " + rec.getCurrentValue(columnName));
            }
        }
    }
}