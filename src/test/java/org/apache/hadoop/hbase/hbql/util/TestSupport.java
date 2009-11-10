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
import org.apache.hadoop.hbase.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.hbql.schema.AnnotationSchema;
import org.apache.hadoop.hbase.hbql.schema.ReflectionSchema;
import org.apache.hadoop.hbase.hbql.schema.Schema;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.SingleExpressionContext;

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
        return (Number)HBqlShell.parseExpression(str);
    }

    public String parseStringExpr(final String str) throws HBqlException {
        return (String)HBqlShell.parseExpression(str);
    }

    public Date parseDateExpr(final String str) throws HBqlException {
        return (Date)HBqlShell.parseExpression(str);
    }

    public void assertValidInput(final String expr, String... vals) throws HBqlException {
        assertTrue(evaluateExprColumnNames(expr, vals));
    }

    public void assertInvalidInput(final String expr, String... vals) throws HBqlException {
        assertFalse(evaluateExprColumnNames(expr, vals));
    }

    public static void assertEvalTrue(final String expr) throws HBqlException {
        assertEvalTrue(null, expr);
    }

    public static void assertEvalTrue(final Object recordObj, final String expr) throws HBqlException {
        assertTrue(evaluateExpression(recordObj, expr));
    }

    public static SingleExpressionContext parseSelectElement(final String str) throws HBqlException {
        return HBqlShell.parseSelectElement(str);
    }

    public static void assertTypeAndValue(final SingleExpressionContext expr, final Class clazz, final Object val) throws HBqlException {
        final Object obj = HBqlShell.evaluateSelectElement(expr);
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
        assertExprEvalFalse(null, expr);
    }

    public static void assertExprEvalFalse(final Object recordObj, final String expr) throws HBqlException {
        assertFalse(evaluateExpression(recordObj, expr));
    }

    public static void assertExpressionEvalTrue(final ExpressionTree tree) throws HBqlException {
        assertExpressionEvalTrue(null, tree);
    }

    public static void assertExpressionEvalTrue(final Object recordObj, final ExpressionTree tree) throws HBqlException {
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
        return HBqlShell.parseExpression(expr);
    }

    public ExpressionTree parseExpr(final String expr) throws HBqlException {
        return this.parseExpr(null, expr);
    }

    public ExpressionTree parseExpr(final Object recordObj, final String expr) throws HBqlException {
        final Schema schema = getObjectSchema(recordObj);
        return parseDescWhereExpr(expr, schema);
    }

    private static boolean evaluateExpression(final Object recordObj, final String expr) throws HBqlException {
        final Schema schema = getObjectSchema(recordObj);
        final ExpressionTree tree = parseDescWhereExpr(expr, schema);
        return evaluateExprression(recordObj, tree);
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
            final ExpressionTree tree = parseDescWhereExpr(expr, null);
            final List<String> valList = Lists.newArrayList(vals);

            final List<String> attribList = Lists.newArrayList();
            for (final GenericColumn column : tree.getColumnsUsedInExpression())
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

    public static ExpressionTree parseDescWhereExpr(final String str, final Schema schema) throws HBqlException {
        try {
            final HBqlParser parser = HBqlShell.newHBqlParser(str);
            final ExpressionTree expressionTree = parser.descWhereExpr();
            expressionTree.setSchemaAndContext(schema);
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
            final WithArgs args = HBqlShell.parseWithClause(expr);
            System.out.println("Evaluating: " + args.asString());
            return true;
        }
        catch (HBqlException e) {
            return false;
        }
    }

    public static Schema getObjectSchema(final Object recordObj) throws HBqlException {

        if (recordObj == null)
            return null;

        try {
            return AnnotationSchema.getAnnotationSchema(recordObj);
        }
        catch (HBqlException e) {
            // Not annotated properly
        }

        return ReflectionSchema.getReflectionSchema(recordObj);
    }
}