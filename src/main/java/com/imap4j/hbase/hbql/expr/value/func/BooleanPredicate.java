package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.BooleanValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanPredicate implements BooleanValue {

    private PredicateExpr expr = null;

    public BooleanPredicate(final PredicateExpr expr) {
        this.expr = expr;
    }

    private PredicateExpr getExpr() {
        return expr;
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
    public Boolean getValue(final Object object) throws HPersistException {
        return this.getExpr().evaluate(object);
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