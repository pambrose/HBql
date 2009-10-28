package org.apache.hadoop.hbase.hbql.stmt.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionType;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class BooleanCaseElse extends GenericCaseElse implements BooleanValue {

    public BooleanCaseElse(final GenericValue arg0) {
        super(ExpressionType.BOOLEANCASEELSE, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}