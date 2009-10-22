package org.apache.hadoop.hbase.hbql.query.expr.ifthenstmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class BooleanIfThen extends GenericIfThen implements BooleanValue {

    public BooleanIfThen(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(GenericExpr.Type.BOOLEANIFTHEN, arg0, arg1, arg2);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}