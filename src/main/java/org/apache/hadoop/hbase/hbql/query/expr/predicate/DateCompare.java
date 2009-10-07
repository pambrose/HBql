package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class DateCompare extends GenericCompare {

    public DateCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }


    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.validateType(DateValue.class);
    }


    public Boolean getValue(final Object object) throws HBqlException {

        final long val1 = (Long)this.getArg(0).getValue(object);
        final long val2 = (Long)this.getArg(1).getValue(object);

        switch (this.getOperator()) {
            case EQ:
                return val1 == val2;
            case NOTEQ:
                return val1 != val2;
            case GT:
                return val1 > val2;
            case GTEQ:
                return val1 >= val2;
            case LT:
                return val1 < val2;
            case LTEQ:
                return val1 <= val2;
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }
}