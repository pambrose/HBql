package com.imap4j.hbase.hbql.expr;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;
import com.imap4j.hbase.hbql.expr.value.literal.DateLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ExprTree {

    private PredicateExpr expr = null;
    private long start, end;

    private ExprTree(final PredicateExpr expr) {
        this.expr = expr;
    }

    public static ExprTree newExprTree(final PredicateExpr expr) {
        return new ExprTree(expr);
    }

    private PredicateExpr getExpr() {
        return this.expr;
    }

    private void setExpr(final PredicateExpr expr) {
        this.expr = expr;
    }

    public void setSchema(final ExprSchema schema) {
        if (schema != null)
            this.getExpr().setSchema(schema);
    }

    public void optimize() throws HPersistException {
        this.optimizeForConstants(null);
    }

    public List<ExprVariable> getExprVariables() {
        if (this.getExpr() == null)
            return Lists.newArrayList();
        else
            return this.getExpr().getExprVariables();
    }

    private boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.setExpr(new BooleanLiteral(this.getExpr().evaluate(object)));
        else
            retval = false;

        return retval;
    }

    public Boolean evaluate(final Object object) throws HPersistException {

        this.start = System.nanoTime();

        // Set it once per evaluation
        DateLiteral.resetNow();

        final boolean retval = (this.getExpr() == null) || (this.getExpr().evaluate(object));

        this.end = System.nanoTime();

        return retval;
    }

    public long getElapsedNanos() {
        return this.end - this.start;
    }

}