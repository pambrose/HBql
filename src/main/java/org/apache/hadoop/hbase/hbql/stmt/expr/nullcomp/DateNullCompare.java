package org.apache.hadoop.hbase.hbql.stmt.expr.nullcomp;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionType;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class DateNullCompare extends GenericNullCompare {

    public DateNullCompare(final boolean not, final GenericValue arg0) {
        super(ExpressionType.DATENULL, not, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        final Long val = (Long)this.getArg(0).getValue(object);
        final boolean retval = (val == null);
        return (this.isNot()) ? !retval : retval;
    }
}