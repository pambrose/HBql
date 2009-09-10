package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:28:06 PM
 */
public class CondFactor implements PredicateExpr {

    private final boolean not;
    private PredicateExpr expr = null;

    public CondFactor(final boolean not, final PredicateExpr expr) {
        this.not = not;
        this.expr = expr;
    }

    private PredicateExpr getExpr() {
        return this.expr;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return this.getExpr().getExprVariables();
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new BooleanLiteral(this.getExpr().evaluate(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {
        final boolean retval = this.getExpr().evaluate(object);
        return (this.not) ? !retval : retval;

    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant();
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        this.getExpr().setSchema(schema);
    }
}
