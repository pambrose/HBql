package org.apache.expreval.expr.nullcomp;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

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