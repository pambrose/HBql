package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.NumberLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberCalcExpr extends GenericCalcExpr<NumberValue> implements NumberValue {

    public NumberCalcExpr(final NumberValue expr1) {
        this(expr1, GenericCalcExpr.OP.NONE, null);
    }

    public NumberCalcExpr(final NumberValue expr1, final GenericCalcExpr.OP op, final NumberValue expr2) {
        super(expr1, op, expr2);
    }


    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(object))
            this.setExpr1(new NumberLiteral(this.getExpr1().getCurrentValue(object)));
        else
            retval = false;

        if (this.getExpr2() != null && this.getExpr2().optimizeForConstants(object))
            this.setExpr2(new NumberLiteral(this.getExpr2().getCurrentValue(object)));
        else
            retval = false;

        return retval;
    }

    @Override
    public Long getCurrentValue(final Object object) throws HPersistException {

        final long val1 = this.getExpr1().getCurrentValue(object).longValue();
        final long val2 = (this.getExpr2() != null) ? (this.getExpr2().getCurrentValue(object)).longValue() : 0;

        switch (this.getOp()) {
            case PLUS:
                return val1 + val2;
            case MINUS:
                return val1 - val2;
            case MULT:
                return val1 * val2;
            case DIV:
                return val1 / val2;
            case MOD:
                return val1 % val2;
            case NEGATIVE:
                return val1 * -1;
            case NONE:
                return val1;
        }

        throw new HPersistException("Error in NumberCalcExpr.getValue() " + this.getOp());

    }
}