package org.apache.hadoop.hbase.hbql.util;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.antlr.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;

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
        final WhereArgs args = HBql.parseWithClause(expr, (Schema)null);
        return args != null;
    }

}