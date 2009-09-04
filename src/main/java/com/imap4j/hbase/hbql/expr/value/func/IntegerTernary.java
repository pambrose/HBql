package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.IntegerValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.BooleanLiteral;
import com.imap4j.hbase.hbql.expr.value.literal.IntegerLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class IntegerTernary extends GenericTernary implements IntegerValue {

    private IntegerValue expr1 = null, expr2 = null;

    public IntegerTernary(final PredicateExpr pred, final IntegerValue expr1, final IntegerValue expr2) {
        super(pred);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    protected IntegerValue getExpr1() {
        return this.expr1;
    }

    protected IntegerValue getExpr2() {
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
            this.expr1 = new IntegerLiteral(this.getExpr1().getValue(context));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(context))
            this.expr2 = new IntegerLiteral(this.getExpr2().getValue(context));
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
    public boolean isAConstant() {
        return this.getPred().isAConstant() && this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

}
