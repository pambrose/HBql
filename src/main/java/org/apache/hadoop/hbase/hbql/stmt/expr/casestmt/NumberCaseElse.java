package org.apache.hadoop.hbase.hbql.stmt.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionType;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.NumberValue;

public class NumberCaseElse extends GenericCaseElse implements NumberValue {

    public NumberCaseElse(final GenericValue arg0) {
        super(ExpressionType.NUMBERCASEELSE, arg0);
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Number)super.getValue(object);
    }
}