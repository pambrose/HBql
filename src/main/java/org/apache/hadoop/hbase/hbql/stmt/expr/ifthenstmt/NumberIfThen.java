package org.apache.hadoop.hbase.hbql.stmt.expr.ifthenstmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.NumberValue;

public class NumberIfThen extends GenericIfThen implements NumberValue {

    public NumberIfThen(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(Type.NUMBERIFTHEN, arg0, arg1, arg2);
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Number)super.getValue(object);
    }
}
