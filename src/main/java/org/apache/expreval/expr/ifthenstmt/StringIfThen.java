package org.apache.expreval.expr.ifthenstmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.StringValue;

public class StringIfThen extends GenericIfThen implements StringValue {

    public StringIfThen(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(ExpressionType.STRINGIFTHEN, arg0, arg1, arg2);
    }

    public String getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (String)super.getValue(object);
    }
}