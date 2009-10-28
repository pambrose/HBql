package org.apache.hadoop.hbase.hbql.stmt.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionType;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class BooleanCaseWhen extends GenericCaseWhen implements BooleanValue {

    public BooleanCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(ExpressionType.BOOLEANCASEWHEN, arg0, arg1);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}