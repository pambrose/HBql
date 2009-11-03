package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public class StringCaseWhen extends GenericCaseWhen implements StringValue {

    public StringCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(ExpressionType.STRINGCASEWHEN, arg0, arg1);
    }

    public String getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (String)super.getValue(object);
    }
}