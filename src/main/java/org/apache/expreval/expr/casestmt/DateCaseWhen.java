package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;

public class DateCaseWhen extends GenericCaseWhen implements DateValue {

    public DateCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(ExpressionType.DATECASEWHEN, arg0, arg1);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Long)super.getValue(object);
    }
}