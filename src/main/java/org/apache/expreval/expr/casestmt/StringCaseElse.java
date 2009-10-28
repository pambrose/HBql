package org.apache.expreval.expr.casestmt;

import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class StringCaseElse extends GenericCaseElse implements StringValue {

    public StringCaseElse(final GenericValue arg0) {
        super(ExpressionType.STRINGCASEELSE, arg0);
    }

    public String getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (String)super.getValue(object);
    }
}