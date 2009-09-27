package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:51:01 PM
 */
public class DateCalculation extends GenericCalculation implements DateValue {

    public DateCalculation(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {
        return this.validateType(DateValue.class);
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {

        this.setExpr1(this.getExpr1().getOptimizedValue());
        if (this.getExpr2() != null)
            this.setExpr2(this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new DateLiteral(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {

        final long val1 = (Long)this.getExpr1().getValue(object);
        final long val2 = (this.getExpr2() != null) ? ((Long)this.getExpr2().getValue(object)) : 0;

        switch (this.getOp()) {
            case PLUS:
                return val1 + val2;
            case MINUS:
                return val1 - val2;
        }

        throw new HBqlException("Invalid operator in DateCalcExpr.getValue() " + this.getOp());
    }

}