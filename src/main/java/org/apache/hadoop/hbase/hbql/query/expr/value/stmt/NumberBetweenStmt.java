package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class NumberBetweenStmt extends GenericBetweenStmt {

    public NumberBetweenStmt(final GenericValue arg0,
                             final boolean not,
                             final GenericValue arg1,
                             final GenericValue arg2) {
        super(Type.NUMBERBETWEEN, not, arg0, arg1, arg2);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        final Object obj0 = this.getArg(0).getValue(object);
        final Object obj1 = this.getArg(1).getValue(object);
        final Object obj2 = this.getArg(2).getValue(object);

        this.validateNumericArgTypes(obj0, obj1, obj2);

        final boolean retval;

        if (!this.useDecimal()) {

            final long val0 = ((Number)obj0).longValue();
            final long val1 = ((Number)obj1).longValue();
            final long val2 = ((Number)obj2).longValue();

            retval = val0 >= val1 && val0 <= val2;
        }
        else {

            final double val0 = ((Number)obj0).doubleValue();
            final double val1 = ((Number)obj1).doubleValue();
            final double val2 = ((Number)obj2).doubleValue();

            retval = val0 >= val1 && val0 <= val2;
        }

        return (this.isNot()) ? !retval : retval;
    }
}