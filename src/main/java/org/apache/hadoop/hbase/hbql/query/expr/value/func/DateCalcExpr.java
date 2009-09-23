package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:51:01 PM
 */
public class DateCalcExpr extends GenericCalcExpr<DateValue> implements DateValue {

    public DateCalcExpr(final DateValue expr1, final Operator op, final DateValue expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();

        if (!ExprTree.isOfType(type1, NumberValue.class, DateValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in DateCalcExpr");

        if (this.getExpr2() != null) {
            final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

            if (!ExprTree.isOfType(type2, NumberValue.class, DateValue.class))
                throw new HPersistException("Type " + type2.getName() + " not valid in DateCalcExpr");
        }
        return DateValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setExpr1((DateValue)this.getExpr1().getOptimizedValue());
        if (this.getExpr2() != null)
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