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

package org.apache.hadoop.hbase.hbql.util;

import org.antlr.runtime.RecognitionException;
import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.var.GenericColumn;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.hbql.antlr.HBqlParser;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.mapping.ResultMapping;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;
import org.apache.hadoop.hbase.hbql.statement.MappingContext;
import org.apache.hadoop.hbase.hbql.statement.NoStatementMappingContext;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.SingleExpressionContext;
import org.apache.yaoql.impl.ReflectionResultMapping;

import java.util.Date;
import java.util.List;

public class TestSupport {

    public final static int keywidth = 10;

    public static void assertTrue(final boolean val) throws HBqlException {
        org.junit.Assert.assertTrue(val);
    }

    public static void assertFalse(final boolean val) throws HBqlException {
        org.junit.Assert.assertFalse(val);
    }

    public Number parseNumberExpr(final String str) throws HBqlException {
        return (Number)ParserUtil.parseExpression(str);
    }

    public String parseStringExpr(final String str) throws HBqlException {
        return (String)ParserUtil.parseExpression(str);
    }

    public Date parseDateExpr(final String str) throws HBqlException {
        return (Date)ParserUtil.parseExpression(str);
    }

    public void assertValidInput(final String expr, String... vals) throws HBqlException {
        assertTrue(evaluateExprColumnNames(expr, vals));
    }

    public void assertInvalidInput(final String expr, String... vals) throws HBqlException {
        assertFalse(evaluateExprColumnNames(expr, vals));
    }

    public static void assertEvalTrue(final String expr) throws HBqlException {
        assertReflectionEvalTrue(null, expr);
    }

    public static void assertAnnotationEvalTrue(final HConnection connection,
                                                final Object recordObj,
                                                final String expr) throws HBqlException {
        assertTrue(evaluateAnnotationExpression(connection, recordObj, expr));
    }

    public static void assertReflectionEvalTrue(final Object recordObj, final String expr) throws HBqlException {
        assertTrue(evaluateReflectionExpression(recordObj, expr));
    }

    public static SingleExpressionContext parseSelectElement(final String str) throws HBqlException {
        return ParserUtil.parseSelectElement(str);
    }

    public static void assertTypeAndValue(final SingleExpressionContext expr,
                                          final Class clazz,
                                          final Object val) throws HBqlException {
        final Object obj = ParserUtil.evaluateSelectElement(expr);
        System.out.println(expr.asString() + " returned value " + obj
                           + " expecting value " + val
                           + " returned type " + obj.getClass().getSimpleName()
                           + " expecting type " + clazz.getSimpleName());
        assertTrue(obj.getClass().equals(clazz) && obj.equals(val));
    }

    public static void assertTypeAndValue(final String str, final Class clazz, final Object val) throws HBqlException {
        SingleExpressionContext expr = parseSelectElement(str);
        assertTypeAndValue(expr, clazz, val);
    }

    public static void assertEvalFalse(final String expr) throws HBqlException {
        assertReflectionEvalFalse(null, expr);
    }

    public static void assertAnnotationEvalFalse(final HConnection connection,
                                                 final Object recordObj,
                                                 final String expr) throws HBqlException {
        assertFalse(evaluateAnnotationExpression(connection, recordObj, expr));
    }

    public static void assertReflectionEvalFalse(final Object recordObj,
                                                 final String expr) throws HBqlException {
        assertFalse(evaluateReflectionExpression(recordObj, expr));
    }

    public static void assertExpressionEvalTrue(final ExpressionTree tree) throws HBqlException {
        assertExpressionEvalTrue(null, tree);
    }

    public static void assertExpressionEvalTrue(final Object recordObj,
                                                final ExpressionTree tree) throws HBqlException {
        assertTrue(evaluateExprression(recordObj, tree));
    }

    public static void assertExpressionEvalFalse(final ExpressionTree tree) throws HBqlException {
        assertEvalFalse(null, tree);
    }

    public static void assertEvalFalse(final Object recordObj, final ExpressionTree tree) throws HBqlException {
        assertFalse(evaluateExprression(recordObj, tree));
    }

    public void assertHasException(final ExpressionTree tree, final Class clazz) {
        this.assertExpressionHasException(null, tree, clazz);
    }

    public void assertExpressionHasException(final Object recordObj, final ExpressionTree tree, final Class clazz) {
        Class eclazz = null;
        try {
            evaluateExprression(recordObj, tree);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            eclazz = e.getClass();
        }
        org.junit.Assert.assertTrue(eclazz != null && eclazz.equals(clazz));
    }

    public void assertHasException(final String str, final Class<? extends Exception> clazz) throws HBqlException {
        final ExpressionTree tree = parseDescWhereExpr(str, null);
        assertExpressionHasException(null, tree, clazz);
    }

    public static void assertExprColumnsMatchTrue(final String expr, String... vals) throws HBqlException {
        assertTrue(evaluateExprColumnNames(expr, vals));
    }

    public static void assertExprColumnsMatchFalse(final String expr, String... vals) throws HBqlException {
        assertFalse(evaluateExprColumnNames(expr, vals));
    }

    public Object evaluateExpr(final String expr) throws HBqlException {
        return ParserUtil.parseExpression(expr);
    }

    public ExpressionTree parseExpr(final String expr) throws HBqlException {
        return this.parseReflectionExpr(null, expr);
    }

    public ExpressionTree parseAnnotationExpr(final HConnection connection,
                                              final Object recordObj,
                                              final String expr) throws HBqlException {
        final MappingContext mappingContext = getAnnotationMappingContext(connection, recordObj);
        return parseDescWhereExpr(expr, mappingContext);
    }

    public ExpressionTree parseReflectionExpr(final Object recordObj, final String expr) throws HBqlException {
        final MappingContext mappingContext = getReflectionMappingContext(recordObj);
        return parseDescWhereExpr(expr, mappingContext);
    }

    private static boolean evaluateAnnotationExpression(final HConnection connection,
                                                        final Object recordObj,
                                                        final String expr) throws HBqlException {
        final MappingContext mappingContext = getAnnotationMappingContext(connection, recordObj);
        final ExpressionTree tree = parseDescWhereExpr(expr, mappingContext);
        return evaluateExprression(recordObj, tree);
    }

    private static boolean evaluateReflectionExpression(final Object recordObj, final String expr) throws HBqlException {
        final MappingContext mappingContext = getReflectionMappingContext(recordObj);
        final ExpressionTree tree = parseDescWhereExpr(expr, mappingContext);
        return evaluateExprression(recordObj, tree);
    }

    private static MappingContext getAnnotationMappingContext(final HConnection connection,
                                                              final Object obj) throws HBqlException {
        if (obj == null)
            return new NoStatementMappingContext(null, null);
        else
            return getAnnotatedMapping(connection, obj).getMappingContext();
    }

    private static MappingContext getReflectionMappingContext(final Object obj) throws HBqlException {
        if (obj == null)
            return new NoStatementMappingContext(null, null);
        else
            return getReflectionMapping(obj).getMappingContext();
    }


    private static boolean evaluateExprression(final Object recordObj, final ExpressionTree tree) throws HBqlException {
        System.out.println("Evaluating: " + tree.asString());
        try {
            return tree.evaluate(recordObj);
        }
        catch (ResultMissingColumnException e) {
            throw new InternalErrorException();
        }
    }

    private static boolean evaluateExprColumnNames(final String expr, String... vals) {

        try {
            final ExpressionTree expressionTree = parseDescWhereExpr(expr, null);

            final List<String> valList = Lists.newArrayList(vals);

            final List<String> attribList = Lists.newArrayList();
            for (final GenericColumn column : expressionTree.getColumnsUsedInExpression())
                attribList.add(column.getVariableName());

            boolean retval = true;

            for (final String val : valList) {
                if (!attribList.contains(val)) {
                    System.out.println("Missing column name in attrib list : " + val);
                    retval = false;
                }
            }

            for (final String var : attribList) {
                if (!valList.contains(var)) {
                    System.out.println("Missing column name in specified list : " + var);
                    retval = false;
                }
            }
            return retval;
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static ExpressionTree parseDescWhereExpr(final String str, final MappingContext sc) throws HBqlException {
        try {
            final HBqlParser parser = ParserUtil.newHBqlParser(str);
            final ExpressionTree expressionTree = parser.descWhereExpr();

            if (expressionTree.getMappingContext() == null) {
                final MappingContext mappingContext = (sc == null) ? new NoStatementMappingContext(null, null) : sc;
                expressionTree.setMappingContext(mappingContext);
            }

            return expressionTree;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing: " + str);
        }
    }

    public void assertValidInput(final String expr) throws HBqlException {
        assertTrue(evaluateWhereValue(expr));
    }

    public void assertInvalidInput(final String expr) throws HBqlException {
        assertFalse(evaluateWhereValue(expr));
    }

    private static boolean evaluateWhereValue(final String expr) {
        try {
            final WithArgs args = ParserUtil.parseWithClause(expr);
            System.out.println("Evaluating: " + args.asString());
            return true;
        }
        catch (HBqlException e) {
            return false;
        }
    }

    public static ResultMapping getAnnotatedMapping(final HConnection connection,
                                                    final Object recordObj) throws HBqlException {

        if (recordObj == null)
            return null;

        return ((HConnectionImpl)connection).getAnnotationMapping(recordObj);
    }

    public static ResultMapping getReflectionMapping(final Object recordObj) throws HBqlException {

        if (recordObj == null)
            return null;

        return new ReflectionResultMapping(recordObj);
    }
}