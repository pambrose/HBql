package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;
import com.imap4j.hbase.hbql.expr.value.literal.NumberLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class NumberTernary extends GenericTernary implements NumberValue {

    private NumberValue expr1 = null, expr2 = null;

    public NumberTernary(final PredicateExpr pred, final NumberValue expr1, final NumberValue expr2) {
        super(pred);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    protected NumberValue getExpr1() {
        return this.expr1;
    }

    protected NumberValue getExpr2() {
        return this.expr2;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getPred().optimizeForConstants(context))
            this.pred = new BooleanLiteral(this.getPred().evaluate(context));
        else
            retval = false;

        if (this.getExpr1().optimizeForConstants(context))
            this.expr1 = new NumberLiteral(this.getExpr1().getValue(context));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(context))
            this.expr2 = new NumberLiteral(this.getExpr2().getValue(context));
        else
            retval = false;

        return retval;
    }

    @Override
    public Number getValue(final EvalContext context) throws HPersistException {

        if (this.getPred().evaluate(context))
            return this.getExpr1().getValue(context);
        else
            return this.getExpr2().getValue(context);
    }

    @Override
    public boolean isAContant() {
        return this.getPred().isAContant() && this.getExpr1().isAContant() && this.getExpr2().isAContant();
    }

}
