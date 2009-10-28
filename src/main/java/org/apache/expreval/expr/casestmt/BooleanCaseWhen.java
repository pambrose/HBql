package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;

public class BooleanCaseWhen extends GenericCaseWhen implements BooleanValue {

    public BooleanCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(ExpressionType.BOOLEANCASEWHEN, arg0, arg1);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)super.getValue(object);
    }
}