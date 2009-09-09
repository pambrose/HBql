package com.imap4j.hbase.hbql.test;

import com.imap4j.hbase.antlr.args.WhereArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.schema.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class WhereValueTests {

    public void assertValidInput(final String expr) throws HPersistException {
        org.junit.Assert.assertTrue(evalWhereValue(expr));
    }

    public void assertInvalidInput(final String expr) throws HPersistException {
        org.junit.Assert.assertFalse(evalWhereValue(expr));
    }

    private static boolean evalWhereValue(final String expr) throws HPersistException {
        final WhereArgs args = (WhereArgs)HBqlRule.WHERE_VALUE.parse(expr, (ClassSchema)null);
        return args != null;
    }

}