package org.apache.hadoop.hbase.hbql.stmt.expr.compare;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.Operator;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.NumberValue;

public class NumberCompare extends GenericCompare {

    public NumberCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {
        return this.validateType(NumberValue.class);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        final Object obj0 = this.getArg(0).getValue(object);
        final Object obj1 = this.getArg(1).getValue(object);

        this.validateNumericArgTypes(obj0, obj1);

        if (!this.useDecimal()) {

            final long val0 = ((Number)obj0).longValue();
            final long val1 = ((Number)obj1).longValue();

            switch (this.getOperator()) {
                case EQ:
                    return val0 == val1;
                case GT:
                    return val0 > val1;
                case GTEQ:
                    return val0 >= val1;
                case LT:
                    return val0 < val1;
                case LTEQ:
                    return val0 <= val1;
                case NOTEQ:
                    return val0 != val1;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }
        }
        else {

            final double val0 = ((Number)obj0).doubleValue();
            final double val1 = ((Number)obj1).doubleValue();

            switch (this.getOperator()) {
                case EQ:
                    return val0 == val1;
                case GT:
                    return val0 > val1;
                case GTEQ:
                    return val0 >= val1;
                case LT:
                    return val0 < val1;
                case LTEQ:
                    return val0 <= val1;
                case NOTEQ:
                    return val0 != val1;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }
        }
    }
}