package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;

public class NumberTernary extends GenericTernary implements NumberValue {

    public NumberTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(Type.NUMBERTERNARY, arg0, arg1, arg2);
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Number)super.getValue(object);
    }
}
