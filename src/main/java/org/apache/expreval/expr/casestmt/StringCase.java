package org.apache.expreval.expr.casestmt;

import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

import java.util.List;

public class StringCase extends GenericCase implements StringValue {

    public StringCase(final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(ExpressionType.STRINGCASE, whenExprList, elseExpr);
    }

    public String getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (String)super.getValue(object);
    }
}