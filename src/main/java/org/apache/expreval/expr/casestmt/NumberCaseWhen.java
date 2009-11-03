package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public class NumberCaseWhen extends GenericCaseWhen implements NumberValue {

    public NumberCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(ExpressionType.NUMBERCASEWHEN, arg0, arg1);
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Number)super.getValue(object);
    }
}