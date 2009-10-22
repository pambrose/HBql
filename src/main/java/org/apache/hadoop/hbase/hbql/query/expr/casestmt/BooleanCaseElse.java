package org.apache.hadoop.hbase.hbql.query.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class BooleanCaseElse extends GenericCaseElse implements BooleanValue {

    public BooleanCaseElse(final GenericValue arg0) {
        super(GenericExpr.Type.BOOLEANCASEELSE, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}