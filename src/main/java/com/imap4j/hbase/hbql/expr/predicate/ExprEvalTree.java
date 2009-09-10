package com.imap4j.hbase.hbql.expr.predicate;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
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
public class ExprEvalTree implements PredicateExpr {

    private PredicateExpr expr = null;
    private ExprSchema schema = null;
    private long start, end;

    // private final static ExprEvalTree defaultExpr = new ExprEvalTree(new BooleanLiteral("true"));

    public ExprEvalTree(final PredicateExpr expr) {
        this.expr = expr;
    }

    private PredicateExpr getExpr() {
        return expr;
    }

    public ExprSchema getSchema() {
        return schema;
    }

    public void setSchema(final ExprSchema schema) {
        if (schema != null)
            this.schema = schema;
    }

    public void optimize() throws HPersistException {
        // Any EvalContext would work here since value is null
        //final ReflectionEvalContext context = new ReflectionEvalContext(null);
        //context.setExprSchema(this.getSchema());
        this.optimizeForConstants(null);
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        if (this.getExpr() == null)
            return Lists.newArrayList();
        else
            return this.getExpr().getExprVariables();
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new BooleanLiteral(this.getExpr().evaluate(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final EvalContext context) throws HPersistException {

        context.setExprSchema(this.getSchema());

        this.start = System.nanoTime();

        // Set it once per evaluation
        DateLiteral.resetNow();

        final boolean retval = (this.getExpr() == null) || (this.getExpr().evaluate(context));

        this.end = System.nanoTime();

        return retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant();
    }

    public long getElapsedNanos() {
        return this.end - this.start;
    }
}