package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;

public class BooleanCaseElse extends GenericCaseElse implements BooleanValue {

    public BooleanCaseElse(final GenericValue arg0) {
        super(ExpressionType.BOOLEANCASEELSE, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}