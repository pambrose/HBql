package org.apache.expreval.expr.ifthenstmt;

import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class BooleanIfThen extends GenericIfThen implements BooleanValue {

    public BooleanIfThen(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(ExpressionType.BOOLEANIFTHEN, arg0, arg1, arg2);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}