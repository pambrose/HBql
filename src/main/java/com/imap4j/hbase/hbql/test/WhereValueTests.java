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
        assertValidInput("", expr);
    }

    public void assertValidInput(final String schema, final String expr) throws HPersistException {
        org.junit.Assert.assertTrue(evalWhereValue(schema, expr));
    }

    public void assertInvalidInput(final String schema, final String expr) throws HPersistException {
        org.junit.Assert.assertFalse(evalWhereValue(schema, expr));
    }

    private static boolean evalWhereValue(final String schema, final String expr) throws HPersistException {

        final ClassSchema classSchema = new ClassSchema(schema);
        final WhereArgs args = (WhereArgs)HBqlRule.WHERE_VALUE.parse(expr, classSchema);

        return args != null;

    }

}