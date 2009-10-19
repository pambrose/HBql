package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

public class StringIfThen extends GenericIfThen implements StringValue {

    public StringIfThen(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(Type.STRINGIFTHEN, arg0, arg1, arg2);
    }

    public String getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (String)super.getValue(object);
    }
}