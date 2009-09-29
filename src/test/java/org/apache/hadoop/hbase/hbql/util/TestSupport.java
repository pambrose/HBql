package org.apache.hadoop.hbase.hbql.util;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.antlr.HBqlParser;
import org.apache.hadoop.hbase.hbql.query.antlr.args.QueryArgs;
import org.apache.hadoop.hbase.hbql.query.antlr.args.WhereArgs;
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
public class TestSupport {

    public void assertValidInput(final String expr, String... vals) throws HBqlException {
        assertTrue(evalExprColumnNames(expr, vals));
    }

    public void assertInvalidInput(final String expr, String... vals) throws HBqlException {
        assertFalse(evalExprColumnNames(expr, vals));
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
        assertTrue(evaluateExpr(recordObj, expr));
    }

    public static void assertEvalFalse(final String expr) throws HBqlException {
        assertExprEvalFalse(null, expr);
    }

    public static void assertExprEvalFalse(final Object recordObj, final String expr) throws HBqlException {
        assertFalse(evaluateExpr(recordObj, expr));
    }

    public static void assertExprTreeEvalTrue(final ExprTree tree) throws HBqlException {
        assertExprTreeEvalTrue(null, tree);
    }

    public static void assertExprTreeEvalTrue(final Object recordObj, final ExprTree tree) throws HBqlException {
        assertTrue(evaluateExprTree(recordObj, tree));
    }

    public static void assertExprTreeEvalFalse(final ExprTree tree) throws HBqlException {
        assertEvalFalse(null, tree);
    }

    public static void assertEvalFalse(final Object recordObj, final ExprTree tree) throws HBqlException {
        assertFalse(evaluateExprTree(recordObj, tree));
    }

    public void assertHasException(final ExprTree tree, final Class clazz) {
        this.assertExprTreeHasException(null, tree, clazz);
    }

    public void assertExprTreeHasException(final Object recordObj, final ExprTree tree, final Class clazz) {
        Class eclazz = null;
        try {
            evaluateExprTree(recordObj, tree);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            eclazz = e.getClass();
        }
        org.junit.Assert.assertTrue(eclazz != null && eclazz.equals(clazz));
    }

    public static void assertExprColumnsMatchTrue(final String expr, String... vals) throws HBqlException {
        assertTrue(evalExprColumnNames(expr, vals));
    }

    public static void assertExprColumnsMatchFalse(final String expr, String... vals) throws HBqlException {
        assertFalse(evalExprColumnNames(expr, vals));
    }

    public ExprTree parseExpr(final String expr) throws HBqlException {
        return this.parseExpr(null, expr);
    }

    public ExprTree parseExpr(final Object recordObj, final String expr) throws HBqlException {
        final Schema schema = SchemaManager.getObjectSchema(recordObj);
        return parseDescWhereExpr(expr, schema);
    }

    private static boolean evaluateExpr(final Object recordObj, final String expr) throws HBqlException {
        final Schema schema = SchemaManager.getObjectSchema(recordObj);
        final ExprTree tree = parseDescWhereExpr(expr, schema);
        return evaluateExprTree(recordObj, tree);
    }

    private static boolean evaluateExprTree(final Object recordObj, final ExprTree tree) throws HBqlException {
        System.out.println("Evaluating: " + tree);
        return tree.evaluate(recordObj);
    }

    public static void assertSelectColumnsMatchTrue(final String expr, String... vals) throws HBqlException {
        assertTrue(evalSelectNames(expr, vals));
    }

    private static boolean evalSelectNames(final String expr, String... vals) {

        try {
            final QueryArgs args = HBql.parseQuery(expr);
            final List<String> valList = Lists.newArrayList(vals);

            final List<String> attribList = args.getColumnNameList();

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
            e.printStackTrace();
            return false;
        }
    }

    private static boolean evalExprColumnNames(final String expr, String... vals) {

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
            e.printStackTrace();
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

    public void assertValidInput(final String expr) throws HBqlException {
        assertTrue(evalWhereValue(expr));
    }

    public void assertInvalidInput(final String expr) throws HBqlException {
        assertFalse(evalWhereValue(expr));
    }

    private static boolean evalWhereValue(final String expr) {
        try {
            final WhereArgs args = HBql.parseWithClause(expr, (Schema)null);
            System.out.println("Evaluating: " + args.asString());
            return true;
        }
        catch (HBqlException e) {
            return false;
        }
    }
}