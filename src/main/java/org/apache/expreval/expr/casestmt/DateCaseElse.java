package org.apache.expreval.expr.casestmt;

import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class DateCaseElse extends GenericCaseElse implements DateValue {

    public DateCaseElse(final GenericValue arg0) {
        super(ExpressionType.DATECASEELSE, arg0);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Long)super.getValue(object);
    }
}