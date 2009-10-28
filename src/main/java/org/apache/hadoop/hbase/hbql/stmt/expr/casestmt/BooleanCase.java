package org.apache.hadoop.hbase.hbql.stmt.expr.casestmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionType;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.BooleanValue;

import java.util.List;

public class BooleanCase extends GenericCase implements BooleanValue {

    public BooleanCase(final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(ExpressionType.BOOLEANCASE, whenExprList, elseExpr);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}