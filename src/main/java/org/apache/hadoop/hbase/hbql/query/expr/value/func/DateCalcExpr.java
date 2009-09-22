package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:51:01 PM
 */
public class DateCalcExpr extends GenericCalcExpr<DateValue> implements DateValue {

    public DateCalcExpr(final DateValue expr1, final GenericCalcExpr.OP op, final DateValue expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setExpr1((DateValue)this.getExpr1().getOptimizedValue());
        this.setExpr2((DateValue)this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new DateLiteral(this.getValue(null)) : this;
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

}