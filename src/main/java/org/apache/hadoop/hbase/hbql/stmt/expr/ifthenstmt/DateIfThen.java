package org.apache.hadoop.hbase.hbql.stmt.expr.ifthenstmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.GenericExpr;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class DateIfThen extends GenericIfThen implements DateValue {

    public DateIfThen(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(GenericExpr.Type.DATEIFTHEN, arg0, arg1, arg2);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Long)super.getValue(object);
    }
}