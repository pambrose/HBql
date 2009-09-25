package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class DateCompare extends GenericCompare {

    public DateCompare(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes() throws HBqlException {
        return this.validateType(DateValue.class);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        final long val1 = (Long)this.getExpr1().getValue(object);
        final long val2 = (Long)this.getExpr2().getValue(object);

        switch (this.getOp()) {
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
        }
        throw new HBqlException("Error in DateCompare.getValue()");
    }

}