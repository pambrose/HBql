package org.apache.hadoop.hbase.hbql.stmt.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionType;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.StringValue;

public class StringCaseWhen extends GenericCaseWhen implements StringValue {

    public StringCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(ExpressionType.STRINGCASEWHEN, arg0, arg1);
    }

    public String getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (String)super.getValue(object);
    }
}