package util;

import com.google.common.collect.Lists;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbase.HPersistable;
import com.imap4j.hbase.hbql.expr.ExprEvalTree;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.schema.AnnotationSchema;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.hbql.schema.FieldType;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class WhereExprTests {

    public void assertValidInput(final String expr, String... vals) throws HPersistException {
        org.junit.Assert.assertTrue(evalColumnNames(expr, vals));
    }

    public void assertInvalidInput(final String expr, String... vals) throws HPersistException {
        org.junit.Assert.assertFalse(evalColumnNames(expr, vals));
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

    private static boolean evalExpr(final HPersistable recordObj, final String expr) throws HPersistException {

        final AnnotationSchema schema = (recordObj != null) ? AnnotationSchema.getAnnotationSchema(recordObj) : null;
        final ExprEvalTree tree = (ExprEvalTree)HBqlRule.DESC_WHERE_VALUE.parse(expr, schema);
        tree.setSchema(schema);

        final boolean no_opt_run = tree.evaluate(recordObj);
        final long no_opt_time = tree.getElapsedNanos();

        tree.optimize();

        final boolean opt_run = tree.evaluate(recordObj);
        final long opt_time = tree.getElapsedNanos();

        if (no_opt_run != opt_run)
            throw new HPersistException("Different outcome with optimization");

        // System.out.println("Time savings: " + (no_opt_time - opt_time));
        return no_opt_run;

    }

    private static boolean evalColumnNames(final String expr, String... vals) {

        final ExprEvalTree tree = (ExprEvalTree)HBqlRule.DESC_WHERE_VALUE.parse(expr, (ExprSchema)null);

        if (tree == null)
            return false;

        final List<ExprVariable> attribs = tree.getExprVariables();

        boolean retval = true;

        final List<String> valList = Lists.newArrayList(vals);

        for (final String val : valList) {
            if (!attribs.contains(new ExprVariable(val, FieldType.StringType))) {
                System.out.println("Missing column name: " + val);
                retval = false;
            }
        }

        for (final ExprVariable var : attribs) {
            if (!valList.contains(var.getName())) {
                System.out.println("Missing column name: " + var.getName());
                retval = false;
            }
        }

        return retval;
    }

}