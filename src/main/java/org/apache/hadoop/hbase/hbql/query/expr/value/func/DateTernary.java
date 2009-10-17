package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class DateTernary extends GenericTernary implements DateValue {

    public DateTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(Type.DATETERNARY, arg0, arg1, arg2);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Long)super.getValue(object);
    }
}