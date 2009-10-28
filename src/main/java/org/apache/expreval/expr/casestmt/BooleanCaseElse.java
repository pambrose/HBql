package org.apache.expreval.expr.casestmt;

import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class BooleanCaseElse extends GenericCaseElse implements BooleanValue {

    public BooleanCaseElse(final GenericValue arg0) {
        super(ExpressionType.BOOLEANCASEELSE, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}