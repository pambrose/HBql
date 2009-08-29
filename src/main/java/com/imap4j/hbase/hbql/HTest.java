package com.imap4j.hbase.hbql;

import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.WhereExpr;
import com.imap4j.hbase.hbql.schema.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HTest {

    public static boolean test(final String str) throws HPersistException {
        return test(str, null);
    }

    public static boolean test(final String str, final HPersistable recordObj) throws HPersistException {
        final WhereExpr expr = (WhereExpr)HBqlRule.WHERE.parse("WHERE " + str);

        final ClassSchema classSchema = recordObj != null
                                        ? ClassSchema.getClassSchema(recordObj)
                                        : null;
        boolean val = expr.evaluate(classSchema, recordObj);
        System.out.println("Returned value: " + val);
        return val;
    }

    public static void assertTrue(final String str) throws HPersistException {
        assertTrue(null, str);

    }

    public static void assertTrue(final HPersistable recordObj, final String str) throws HPersistException {
        org.junit.Assert.assertTrue(test(str, recordObj));
    }

    public static void assertFalse(final String str) throws HPersistException {
        assertFalse(null, str);

    }

    public static void assertFalse(final HPersistable recordObj, final String str) throws HPersistException {
        org.junit.Assert.assertFalse(test(str, recordObj));
    }
}