package org.apache.hadoop.hbase.hbql.stmt.expr.calculation;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionType;
import org.apache.hadoop.hbase.hbql.stmt.expr.Operator;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class DateCalculation extends GenericCalculation implements DateValue {

    public DateCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(ExpressionType.DATECALCULATION, arg0, operator, arg1);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {

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