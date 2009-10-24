package org.apache.hadoop.hbase.hbql.stmt.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.NumberValue;

public class NumberCaseWhen extends GenericCaseWhen implements NumberValue {

    public NumberCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(Type.NUMBERCASEWHEN, arg0, arg1);
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Number)super.getValue(object);
    }
}