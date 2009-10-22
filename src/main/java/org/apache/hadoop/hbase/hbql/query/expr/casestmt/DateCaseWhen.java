package org.apache.hadoop.hbase.hbql.query.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class DateCaseWhen extends GenericCaseWhen implements DateValue {

    public DateCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(GenericExpr.Type.DATECASEWHEN, arg0, arg1);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Long)super.getValue(object);
    }
}