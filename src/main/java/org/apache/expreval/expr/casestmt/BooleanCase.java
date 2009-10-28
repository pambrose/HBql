package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.BooleanValue;

import java.util.List;

public class BooleanCase extends GenericCase implements BooleanValue {

    public BooleanCase(final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(ExpressionType.BOOLEANCASE, whenExprList, elseExpr);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}