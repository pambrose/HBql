package org.apache.expreval.expr.ifthenstmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;

public class DateIfThen extends GenericIfThen implements DateValue {

    public DateIfThen(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(ExpressionType.DATEIFTHEN, arg0, arg1, arg2);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Long)super.getValue(object);
    }
}