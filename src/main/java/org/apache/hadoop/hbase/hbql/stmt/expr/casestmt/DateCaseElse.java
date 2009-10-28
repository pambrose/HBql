package org.apache.hadoop.hbase.hbql.stmt.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionType;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class DateCaseElse extends GenericCaseElse implements DateValue {

    public DateCaseElse(final GenericValue arg0) {
        super(ExpressionType.DATECASEELSE, arg0);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Long)super.getValue(object);
    }
}