package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class BooleanTernary extends GenericTernary implements BooleanValue {

    public BooleanTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(Type.BOOLEANTERNARY, arg0, arg1, arg2);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}