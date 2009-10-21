package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class NumberNullCompare extends GenericNullCompare {

    public NumberNullCompare(final boolean not, final GenericValue arg0) {
        super(Type.NUMBERNULL, not, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        final Number val = (Number)this.getArg(0).getValue(object);
        final boolean retval = (val == null);
        return (this.isNot()) ? !retval : retval;
    }
}