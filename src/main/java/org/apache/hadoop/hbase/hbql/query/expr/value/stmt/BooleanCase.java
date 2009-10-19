package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;

import java.util.List;

public class BooleanCase extends GenericCase implements BooleanValue {

    public BooleanCase(final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(Type.BOOLEANCASE, whenExprList, elseExpr);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}