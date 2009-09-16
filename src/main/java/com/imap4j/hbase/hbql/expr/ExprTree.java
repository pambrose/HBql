package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;
import com.imap4j.hbase.hbql.expr.value.literal.DateLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.util.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ExprTree implements Serializable {

    private ExprSchema schema = null;
    private PredicateExpr predicateExpr = null;
    private long start, end;
    private boolean optimized = false;

    private ExprTree(final PredicateExpr predicateExpr) {
        this.predicateExpr = predicateExpr;
    }

    public static ExprTree newExprTree(final PredicateExpr expr) {
        return new ExprTree(expr);
    }

    private PredicateExpr getPredicateExpr() {
        return this.predicateExpr;
    }

    private void setPredicateExpr(final PredicateExpr predicateExpr) {
        this.predicateExpr = predicateExpr;
    }

    public ExprSchema getSchema() {
        return this.schema;
    }

    public boolean isValid() {
        return this.getPredicateExpr() != null;
    }

    public void setSchema(final ExprSchema schema) {
        if (schema != null) {
            this.schema = schema;
            this.getPredicateExpr().setSchema(schema);
        }
    }

    public void optimize() throws HPersistException {
        this.optimizeForConstants(null);
    }

    public List<ExprVariable> getExprVariables() {
        if (this.getPredicateExpr() == null)
            return Lists.newArrayList();
        else
            return this.getPredicateExpr().getExprVariables();
    }

    private boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getPredicateExpr().optimizeForConstants(object))
            this.setPredicateExpr(new BooleanLiteral(this.getPredicateExpr().evaluate(object)));
        else
            retval = false;

        return retval;
    }

    public Boolean evaluate(final Object object) throws HPersistException {

        this.start = System.nanoTime();

        // Set it once per evaluation
        DateLiteral.resetNow();

        final boolean retval = (this.getPredicateExpr() == null) || (this.getPredicateExpr().evaluate(object));

        this.end = System.nanoTime();

        return retval;
    }

    public long getElapsedNanos() {
        return this.end - this.start;
    }

    public ExprTree setSchema(final ExprSchema schema, final List<String> fieldList) throws HPersistException {

        if (this.isValid()) {
            this.setSchema(schema);
            this.optimize();

            // Check if all the variables referenced in the where clause are present in the fieldList.
            final List<ExprVariable> vars = this.getExprVariables();
            for (final ExprVariable var : vars) {
                if (!fieldList.contains(var.getName()))
                    throw new HPersistException("Variable " + var.getName() + " used in where clause but it is not "
                                                + "not in the select list");
            }
        }

        return this;
    }
}