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
import org.apache.hadoop.hbase.hbql.client.ExecutionOutput;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.ConnectionImpl;
import org.apache.hadoop.hbase.hbql.statement.ImportStatement;
import org.apache.hadoop.hbase.hbql.statement.VersionStatement;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Console {

    private static ConnectionImpl conn = null;

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
            final ExecutionOutput output = version.execute(null);
            System.out.print(output);
            return true;
        }

        if (!option.startsWith("-")) {
            final ImportStatement importStmt = new ImportStatement(option);
            final ExecutionOutput output = importStmt.execute(getConnection());
            System.out.print(output);
            return output.hadSuccess();
        }

        return false;
    }

    private static ConnectionImpl getConnection() {

        if (conn == null)
            conn = (ConnectionImpl)ConnectionManager.newConnection();

        return conn;
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

            if (line == null || line.toLowerCase().startsWith("quit") || line.toLowerCase().startsWith("exit"))
                break;

            if (line.trim().length() > 0) {
                stmtBuffer.append(line);

                continuation = !line.trim().endsWith(";");
                if (!continuation) {
                    final String sql = stmtBuffer.toString();
                    stmtBuffer = new StringBuilder();
                    ImportStatement.processInput(out, conn, sql);
                }
            }
        }
    }
}