package com.imap4j.hbase.hbql.test;

import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.predicate.WhereExpr;
import com.imap4j.hbase.hbql.schema.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HTest {

    private static boolean test(final HPersistable recordObj, final String str) throws HPersistException {

        final ClassSchema classSchema = (recordObj != null) ? ClassSchema.getClassSchema(recordObj) : null;
        final WhereExpr expr = (WhereExpr)HBqlRule.WHERE.parse(str);
        final EvalContext context = new EvalContext(classSchema, recordObj);

        final boolean no_opt_run = expr.evaluate(context);
        final long no_opt_time = expr.getElapsedNanos();

        expr.optimizeForConstants(context);

        final boolean opt_run = expr.evaluate(context);
        final long opt_time = expr.getElapsedNanos();

        if (no_opt_run != opt_run)
            throw new HPersistException("Different outcome with optimization");

        // System.out.println("Time savings: " + (no_opt_time - opt_time));
        return no_opt_run;

    }

    public static void assertTrue(final String str) throws HPersistException {
        assertTrue(null, str);
    }

    public static void assertTrue(final HPersistable recordObj, final String str) throws HPersistException {
        org.junit.Assert.assertTrue(test(recordObj, str));
    }

    public static void assertFalse(final String str) throws HPersistException {
        assertFalse(null, str);
    }

    public static void assertFalse(final HPersistable recordObj, final String str) throws HPersistException {
        org.junit.Assert.assertFalse(test(recordObj, str));
    }
}