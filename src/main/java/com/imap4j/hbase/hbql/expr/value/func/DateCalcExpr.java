package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.DateValue;
import com.imap4j.hbase.hbql.expr.value.literal.DateLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:51:01 PM
 */
public class DateCalcExpr extends GenericCalcExpr<DateValue> implements DateValue {


    public DateCalcExpr(final DateValue expr1) {
        this(expr1, GenericCalcExpr.OP.NONE, null);
    }

    public DateCalcExpr(final DateValue expr1, final GenericCalcExpr.OP op, final DateValue expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(object))
            this.setExpr1(new DateLiteral(this.getExpr1().getCurrentValue(object)));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(object))
            this.setExpr2(new DateLiteral(this.getExpr2().getCurrentValue(object)));
        else
            retval = false;

        return retval;
    }

    @Override
    public Long getCurrentValue(final Object object) throws HPersistException {

        final long val1 = this.getExpr1().getCurrentValue(object);
        final long val2 = (this.getExpr2() != null) ? (this.getExpr2().getCurrentValue(object)) : 0;

        switch (this.getOp()) {
            case PLUS:
                return val1 + val2;
            case MINUS:
                return val1 - val2;
        }

        throw new HPersistException("Error in DateCalcExpr.getValue() " + this.getOp());
    }

}