package com.imap4j.hbase.hbql.test;

import com.google.common.collect.Lists;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;
import com.imap4j.hbase.hbql.schema.ClassSchema;
import com.imap4j.hbase.hbql.schema.FieldType;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HTest {

    private static boolean evalExpr(final HPersistable recordObj, final String expr) throws HPersistException {

        final ClassSchema classSchema = (recordObj != null) ? ClassSchema.getClassSchema(recordObj) : null;
        final ExprEvalTree tree = (ExprEvalTree)HBqlRule.WHERE.parse(expr);
        final EvalContext context = new EvalContext(classSchema, recordObj);

        final boolean no_opt_run = tree.evaluate(context);
        final long no_opt_time = tree.getElapsedNanos();

        tree.optimizeForConstants(context);

        final boolean opt_run = tree.evaluate(context);
        final long opt_time = tree.getElapsedNanos();

        if (no_opt_run != opt_run)
            throw new HPersistException("Different outcome with optimization");

        // System.out.println("Time savings: " + (no_opt_time - opt_time));
        return no_opt_run;

    }

    public static void assertTrue(final boolean val) throws HPersistException {
        org.junit.Assert.assertTrue(val);
    }

    public static void assertFalse(final boolean val) throws HPersistException {
        org.junit.Assert.assertFalse(val);
    }

    public static void assertEvalTrue(final String expr) throws HPersistException {
        assertEvalTrue(null, expr);
    }

    public static void assertEvalTrue(final HPersistable recordObj, final String expr) throws HPersistException {
        org.junit.Assert.assertTrue(evalExpr(recordObj, expr));
    }

    public static void assertEvalFalse(final String expr) throws HPersistException {
        assertEvalFalse(null, expr);
    }

    public static void assertEvalFalse(final HPersistable recordObj, final String expr) throws HPersistException {
        org.junit.Assert.assertFalse(evalExpr(recordObj, expr));
    }

    public static void assertColumnsMatchTrue(final String expr, String... vals) throws HPersistException {
        org.junit.Assert.assertTrue(evalColumnNames(expr, vals));
    }

    public static void assertColumnsMatchFalse(final String expr, String... vals) throws HPersistException {
        org.junit.Assert.assertFalse(evalColumnNames(expr, vals));
    }

    private static boolean evalColumnNames(final String expr, String... vals) {

        final List<String> valList = Lists.newArrayList(vals);
        final ExprEvalTree tree = (ExprEvalTree)HBqlRule.WHERE.parse(expr);
        final List<ExprVariable> attribs = tree.getExprVariables();

        boolean retval = true;

        for (final String val : valList) {
            if (!attribs.contains(new ExprVariable(FieldType.StringType, val))) {
                System.out.println("Missing column name: " + val);
                retval = false;
            }
        }

        for (final ExprVariable var : attribs) {
            if (!valList.contains(var.getName())) {
                System.out.println("Missing column name: " + var);
                retval = false;
            }
        }

        return retval;
    }

}