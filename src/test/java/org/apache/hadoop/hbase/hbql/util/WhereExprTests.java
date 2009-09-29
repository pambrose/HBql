package org.apache.hadoop.hbase.hbql.util;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.antlr.HBqlParser;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.GenericColumn;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class WhereExprTests {

    public void assertValidInput(final String expr, String... vals) throws HBqlException {
        org.junit.Assert.assertTrue(evalColumnNames(expr, vals));
    }

    public void assertInvalidInput(final String expr, String... vals) throws HBqlException {
        org.junit.Assert.assertFalse(evalColumnNames(expr, vals));
    }

    public static void assertTrue(final boolean val) throws HBqlException {
        org.junit.Assert.assertTrue(val);
    }

    public static void assertFalse(final boolean val) throws HBqlException {
        org.junit.Assert.assertFalse(val);
    }

    public static void assertEvalTrue(final String expr) throws HBqlException {
        assertEvalTrue(null, expr);
    }

    public static void assertEvalTrue(final Object recordObj, final String expr) throws HBqlException {
        org.junit.Assert.assertTrue(evaluateExpression(recordObj, expr));
    }

    public static void assertEvalFalse(final String expr) throws HBqlException {
        assertEvalFalse(null, expr);
    }

    public static void assertEvalFalse(final Object recordObj, final String expr) throws HBqlException {
        org.junit.Assert.assertFalse(evaluateExpression(recordObj, expr));
    }

    public static void assertEvalTrue(final ExprTree tree) throws HBqlException {
        assertEvalTrue(null, tree);
    }

    public static void assertEvalTrue(final Object recordObj, final ExprTree tree) throws HBqlException {
        org.junit.Assert.assertTrue(evaluateTree(recordObj, tree));
    }

    public static void assertEvalFalse(final ExprTree tree) throws HBqlException {
        assertEvalFalse(null, tree);
    }

    public static void assertEvalFalse(final Object recordObj, final ExprTree tree) throws HBqlException {
        org.junit.Assert.assertFalse(evaluateTree(recordObj, tree));
    }

    public void assertHasException(final ExprTree tree, final Class clazz) {
        this.assertHasException(null, tree, clazz);
    }

    public void assertHasException(final Object recordObj, final ExprTree tree, final Class clazz) {
        Class eclazz = null;
        try {
            evaluateTree(recordObj, tree);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            eclazz = e.getClass();
        }
        org.junit.Assert.assertTrue(eclazz != null && eclazz.equals(clazz));
    }

    public static void assertColumnsMatchTrue(final String expr, String... vals) throws HBqlException {
        org.junit.Assert.assertTrue(evalColumnNames(expr, vals));
    }

    public static void assertColumnsMatchFalse(final String expr, String... vals) throws HBqlException {
        org.junit.Assert.assertFalse(evalColumnNames(expr, vals));
    }

    public ExprTree parseExpr(final String expr) throws HBqlException {
        return this.parseExpr(null, expr);
    }

    public ExprTree parseExpr(final Object recordObj, final String expr) throws HBqlException {
        final Schema schema = SchemaManager.getObjectSchema(recordObj);
        return parseDescWhereExpr(expr, schema);
    }

    private static boolean evaluateExpression(final Object recordObj, final String expr) throws HBqlException {
        final Schema schema = SchemaManager.getObjectSchema(recordObj);
        final ExprTree tree = parseDescWhereExpr(expr, schema);
        return evaluateTree(recordObj, tree);
    }

    private static boolean evaluateTree(final Object recordObj, final ExprTree tree) throws HBqlException {
        System.out.println("Evaluating: " + tree);
        return tree.evaluate(recordObj);
    }

    private static boolean evalColumnNames(final String expr, String... vals) {

        try {
            final ExprTree tree = parseDescWhereExpr(expr, null);
            final List<String> valList = Lists.newArrayList(vals);

            final List<String> attribList = Lists.newArrayList();
            for (final GenericColumn column : tree.getColumnList())
                attribList.add(column.getName());

            boolean retval = true;

            for (final String val : valList) {
                if (!attribList.contains(val)) {
                    System.out.println("Missing column name: " + val);
                    retval = false;
                }
            }

            for (final String var : attribList) {
                if (!valList.contains(var)) {
                    System.out.println("Missing column name: " + var);
                    retval = false;
                }
            }

            return retval;
        }
        catch (HBqlException e) {
            return false;
        }
    }

    public static ExprTree parseDescWhereExpr(final String input,
                                              final Schema schema) throws HBqlException {
        try {
            final HBqlParser parser = HBql.newParser(input);
            final ExprTree exprTree = parser.descWhereExpr(schema);
            exprTree.setSchema(schema);
            return exprTree;
        }
        catch (RecognitionException e) {
            e.printStackTrace();
            throw new HBqlException("Error parsing");
        }
    }

}