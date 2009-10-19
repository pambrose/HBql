package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;

import java.util.List;

public class DateCase extends GenericCase implements DateValue {

    public DateCase(final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(Type.DATECASE, whenExprList, elseExpr);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Long)super.getValue(object);
    }
}