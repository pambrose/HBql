package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.NumberValue;

import java.util.List;

public class NumberCase extends GenericCase implements NumberValue {

    public NumberCase(final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(ExpressionType.NUMBERCASE, whenExprList, elseExpr);
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Number)super.getValue(object);
    }
}