package com.imap4j.hbase.hbql.test;

import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.WhereExpr;
import com.imap4j.hbase.hbql.schema.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HTest {

    private static boolean test(final HPersistable recordObj, final String str) throws HPersistException {
        final WhereExpr expr = (WhereExpr)HBqlRule.WHERE.parse("WHERE " + str);
        final ClassSchema classSchema = (recordObj != null) ? ClassSchema.getClassSchema(recordObj) : null;
        return expr.evaluate(new AttribContext(classSchema, recordObj));
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