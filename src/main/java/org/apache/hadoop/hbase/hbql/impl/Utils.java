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

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ParseException;
import org.apache.hadoop.hbase.hbql.mapping.HRecordResultAccessor;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;
import org.apache.hadoop.hbase.hbql.statement.ConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.DeleteStatement;
import org.apache.hadoop.hbase.hbql.statement.HBqlStatement;
import org.apache.hadoop.hbase.hbql.statement.InsertStatement;
import org.apache.hadoop.hbase.hbql.statement.NonConnectionStatement;
import org.apache.hadoop.hbase.hbql.statement.ParameterStatement;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;
import org.apache.hadoop.hbase.hbql.util.Lists;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Random;


public class Utils {

    public static HBqlStatement parseHBqlStatement(final String sql) throws HBqlException {

        try {
            final HBqlStatement stmt = ParserUtil.parseHBqlStatement(sql);

            if (!Utils.isSelectStatement(stmt)
                && !(stmt instanceof ConnectionStatement)
                && !(stmt instanceof NonConnectionStatement))
                throw new HBqlException("Unsupported statement type: " + stmt.getClass().getSimpleName() + " - " + sql);

            if (stmt instanceof StatementContext) {
                final StatementContext statementContext = (StatementContext)stmt;
                statementContext.setResultAccessor(new HRecordResultAccessor(statementContext));
            }

            return stmt;
        }
        catch (ParseException e) {
            throw new HBqlException(e.getErrorMessage());
        }
    }

    public static boolean isSelectStatement(final HBqlStatement stmt) {
        return stmt instanceof SelectStatement;
    }

    public static boolean isDMLStatement(final HBqlStatement stmt) {
        return stmt instanceof InsertStatement || stmt instanceof DeleteStatement;
    }

    public static boolean isConnectionStatemet(final HBqlStatement stmt) {
        return stmt instanceof ConnectionStatement;
    }

    public static boolean isNonConectionStatemet(final HBqlStatement stmt) {
        return stmt instanceof NonConnectionStatement;
    }

    public static ParameterStatement getParameterStatement(final HBqlStatement statement) throws HBqlException {
        if (!(statement instanceof ParameterStatement))
            throw new HBqlException(statement.getClass().getSimpleName() + " statements do not support parameters");

        return (ParameterStatement)statement;
    }

    private static List<Class> classList = Lists.newArrayList();

    public static void checkForDefaultConstructors(final Class clazz) {

        if (!clazz.getName().contains("hadoop"))
            return;

        if (Modifier.isStatic(clazz.getModifiers()))
            return;

        if (classList.contains(clazz))
            return;
        else
            classList.add(clazz);

        if (!hasDefaultConstructor(clazz))
            System.out.println(clazz.getName() + " is missing null constructor");

        Field[] fields = clazz.getDeclaredFields();

        for (final Field field : fields) {
            Class dclazz = field.getType();
            checkForDefaultConstructors(dclazz);
        }

        fields = clazz.getFields();

        for (final Field field : fields) {
            Class dclazz = field.getType();
            checkForDefaultConstructors(dclazz);
        }
    }

    public static boolean hasDefaultConstructor(final Class clazz) {
        try {
            clazz.getConstructor();
        }
        catch (NoSuchMethodException e) {
            return false;
        }
        return true;
    }

    public static void checkForNullParameterValue(final Object val) throws HBqlException {
        if (val == null)
            throw new HBqlException("Parameter value cannot be NULL");
    }

    public static boolean isValidString(final String val) {
        return val != null && val.trim().length() > 0;
    }

    private static Random randomVal = new Random();


    public static boolean getRandomBoolean() {
        return randomVal.nextBoolean();
    }

    public static int getRandomPositiveInt(final int upper) {
        while (true) {
            final int val = randomVal.nextInt();
            // Math.abs(Integer.MIN_VALUE = Integer.MIN_VALUE, which is still negative
            // So try again if it comes up
            if (val != Integer.MIN_VALUE)
                return (Math.abs(val) % upper) + 1;
        }
    }
}
