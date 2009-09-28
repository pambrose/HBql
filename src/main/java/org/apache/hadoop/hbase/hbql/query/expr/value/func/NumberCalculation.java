package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberCalculation extends GenericCalculation implements NumberValue {

    public NumberCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(new TypeSignature(NumberValue.class, NumberValue.class, NumberValue.class), arg0, operator, arg1);
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {

        final long val1 = ((Number)this.getArg(0).getValue(object)).longValue();
        final long val2 = (((Number)this.getArg(1).getValue(object))).longValue();

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