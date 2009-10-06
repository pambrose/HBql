package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class NumberCompare extends GenericCompare {

    public NumberCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.validateType(NumberValue.class);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        final Object obj1 = this.getArg(0).getValue(object);
        final Object obj2 = this.getArg(1).getValue(object);

        this.validateNumericArgTypes(obj1, obj2);

        if (!this.useDecimal()) {

            final long val1 = ((Number)obj1).longValue();
            final long val2 = ((Number)obj2).longValue();

            switch (this.getOperator()) {
                case EQ:
                    return val1 == val2;
                case GT:
                    return val1 > val2;
                case GTEQ:
                    return val1 >= val2;
                case LT:
                    return val1 < val2;
                case LTEQ:
                    return val1 <= val2;
                case NOTEQ:
                    return val1 != val2;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }
        }
        else {

            final double val1 = ((Number)obj1).doubleValue();
            final double val2 = ((Number)obj2).doubleValue();

            switch (this.getOperator()) {
                case EQ:
                    return val1 == val2;
                case GT:
                    return val1 > val2;
                case GTEQ:
                    return val1 >= val2;
                case LT:
                    return val1 < val2;
                case LTEQ:
                    return val1 <= val2;
                case NOTEQ:
                    return val1 != val2;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }
        }
    }
}