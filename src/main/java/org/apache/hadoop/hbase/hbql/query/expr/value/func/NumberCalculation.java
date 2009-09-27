package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.NumberLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberCalculation extends GenericCalculation implements NumberValue {

    public NumberCalculation(final ValueExpr expr1, final Operator operator, final ValueExpr expr2) {
        super(expr1, operator, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {
        return this.validateType(NumberValue.class);
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {

        this.setExpr1(this.getExpr1().getOptimizedValue());
        this.setExpr2(this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new NumberLiteral(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {

        final long val1 = ((Number)this.getExpr1().getValue(object)).longValue();
        final long val2 = (((Number)this.getExpr2().getValue(object))).longValue();

        switch (this.getOperator()) {
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
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }
}