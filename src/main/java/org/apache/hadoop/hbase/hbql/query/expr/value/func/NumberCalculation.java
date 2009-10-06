package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberCalculation extends GenericCalculation implements NumberValue {

    public NumberCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(Type.NUMBERCALCULATION, arg0, operator, arg1);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.validateNumericTypes(parentExpr, false);
    }

    @Override
    public Number getValue(final Object object) throws HBqlException {

        final Object obj0 = this.getArg(0).getValue(object);
        final Object obj1 = this.getArg(1).getValue(object);

        this.validateNumericArgTypes(obj0, obj1);

        if (!this.useDecimal()) {

            final long val1 = ((Number)obj0).longValue();
            final long val2 = ((Number)obj1).longValue();
            final long result;

            switch (this.getOperator()) {
                case PLUS:
                    result = val1 + val2;
                    break;
                case MINUS:
                    result = val1 - val2;
                    break;
                case MULT:
                    result = val1 * val2;
                    break;
                case DIV:
                    result = val1 / val2;
                    break;
                case MOD:
                    result = val1 % val2;
                    break;
                case NEGATIVE:
                    result = val1 * -1;
                    break;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }

            return this.getValueWithCast(result);
        }
        else {

            final double val1 = ((Number)obj0).doubleValue();
            final double val2 = ((Number)obj1).doubleValue();
            final double result;

            switch (this.getOperator()) {
                case PLUS:
                    result = val1 + val2;
                    break;
                case MINUS:
                    result = val1 - val2;
                    break;
                case MULT:
                    result = val1 * val2;
                    break;
                case DIV:
                    result = val1 / val2;
                    break;
                case MOD:
                    result = val1 % val2;
                    break;
                case NEGATIVE:
                    result = val1 * -1;
                    break;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }

            return this.getValueWithCast(result);
        }
    }
}