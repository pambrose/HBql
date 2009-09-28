package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:51:01 PM
 */
public class DateCalculation extends GenericCalculation implements DateValue {

    public DateCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(new TypeSignature(DateValue.class, DateValue.class, DateValue.class),
              arg0,
              operator,
              arg1);
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {

        final long val1 = (Long)this.getArg(0).getValue(object);
        final long val2 = (Long)this.getArg(1).getValue(object);

        switch (this.getOperator()) {
            case PLUS:
                return val1 + val2;
            case MINUS:
                return val1 - val2;
        }

        throw new HBqlException("Invalid operator:" + this.getOperator());
    }

}