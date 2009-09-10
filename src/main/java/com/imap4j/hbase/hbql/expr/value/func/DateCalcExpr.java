package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.value.literal.DateLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:51:01 PM
 */
public class DateCalcExpr extends CalcExpr implements DateValue {

    private DateValue expr1 = null, expr2 = null;

    public DateCalcExpr(final DateValue expr1) {
        this(expr1, CalcExpr.OP.NONE, null);
    }

    public DateCalcExpr(final DateValue expr1, final CalcExpr.OP op, final DateValue expr2) {
        super(op);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    private DateValue getExpr1() {
        return this.expr1;
    }

    private DateValue getExpr2() {
        return this.expr2;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr1().getExprVariables();
        if (this.getExpr2() != null)
            retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(object))
            this.expr1 = new DateLiteral(this.getExpr1().getValue(object));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(object))
            this.expr2 = new DateLiteral(this.getExpr2().getValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Long getValue(final Object object) throws HPersistException {

        final long val1 = this.getExpr1().getValue(object);
        final long val2 = (this.getExpr2() != null) ? (this.getExpr2().getValue(object)) : 0;

        switch (this.getOp()) {
            case PLUS:
                return val1 + val2;
            case MINUS:
                return val1 - val2;
        }

        throw new HPersistException("Error in DateCalcExpr.getValue() " + this.getOp());
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        this.getExpr1().setSchema(schema);
        this.getExpr2().setSchema(schema);
    }
}