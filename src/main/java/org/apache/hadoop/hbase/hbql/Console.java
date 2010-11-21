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

package org.apache.hadoop.hbase.hbql;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.statement.ImportStatement;
import org.apache.hadoop.hbase.hbql.statement.VersionStatement;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.hadoop.hbase.hbql.util.Maps;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Console {

    private static HConnectionImpl conn               = null;
    private static boolean         processCommandLine = true;
    private static Configuration   config             = null;

    public static void main(String[] args) throws HBqlException, IOException {

        if (args != null && args.length > 0) {

            final List<String> argList = new LinkedList<String>();
            argList.addAll(Arrays.asList(args));

            while (argList.size() > 0) {
                if (!processArg(argList))
                    break;
            }
        }

        if (processCommandLine)
            processCommandLineInput();
    }

    private static void usage() {

        System.out.println("Usage: java " + Console.class.getName() + " [-options]");
        System.out.println("\t\t(comand line usage");
        System.out.println("   or  java " + Console.class.getName() + " [-options] [file_names]");
        System.out.println("\t\t(executes the statements in the space-separated file names)");
        System.out.println("\nwhere options include:");
        System.out.println("\t-usage                print this message");
        System.out.println("\t-version              print version info and exit");
        System.out.println("\t-" + HConnectionImpl.MASTER + "=value   set " + HConnectionImpl.MASTER + " value");
    }

    private static boolean processArg(final List<String> argList) throws HBqlException {

        final String option = argList.remove(0);

        if (option.equals("-usage")) {
            processCommandLine = false;
            usage();
            return true;
        }

        if (option.equals("-version")) {
            processCommandLine = false;
            final VersionStatement version = new VersionStatement();
            final ExecutionResults results = version.execute();
            System.out.print(results);
            return true;
        }

        if (option.startsWith("-" + HConnectionImpl.MASTER)) {
            final String[] vals = option.split("=");
            if (vals.length != 2) {
                processCommandLine = false;
                System.out.println("Incorrect syntax: " + option);
                usage();
                return false;
            } else {
                config = HConnectionImpl.getHBaseConfiguration(vals[1]);
                return true;
            }
        }

        if (option.startsWith("-")) {
            processCommandLine = false;
            System.out.println("Unknown option: " + option);
            usage();
            return false;
        } else {
            // Assume that an arg without "-" prefix is a filename
            processCommandLine = false;
            final ImportStatement importStmt = new ImportStatement(option);
            final ExecutionResults results = importStmt.evaluatePredicateAndExecute(getConnection());
            System.out.print(results);
            return results.hadSuccess();
        }
    }

    private synchronized static HConnectionImpl getConnection() throws HBqlException {

        if (conn == null)
            conn = (HConnectionImpl) HConnectionManager.newConnection(config);

        return conn;
    }

    private static void processCommandLineInput() throws IOException, HBqlException {

        final List<SimpleCompletor> completors = Lists.newArrayList();
        completors.add(new SimpleCompletor(new String[]{"select", "insert", "create", "table", "mapping",
                "describe", "drop", "enable", "disable", "show",
                "executor", "pool"}));

        final ConsoleReader reader = new ConsoleReader();
        reader.setBellEnabled(false);
        reader.setUseHistory(true);
        //reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
        reader.addCompletor(new ArgumentCompletor(completors));

        final PrintWriter out = new PrintWriter(System.out);

        StringBuilder stmtBuffer = new StringBuilder();
        boolean continuation = false;

        final Map<Boolean, String> prompts = Maps.newHashMap();
        prompts.put(Boolean.FALSE, "HBql> ");
        prompts.put(Boolean.TRUE, "> ");

        while (true) {

            final String line = reader.readLine(prompts.get(continuation));

            if (line == null || line.toLowerCase().startsWith("quit") || line.toLowerCase().startsWith("exit"))
                break;

            if (Utils.isValidString(line)) {
                stmtBuffer.append(line);

                continuation = !line.trim().endsWith(";");
                if (!continuation) {
                    final String sql = stmtBuffer.toString();
                    stmtBuffer = new StringBuilder();
                    ImportStatement.processInput(out, getConnection(), sql);
                }
            }
        }
    }
}